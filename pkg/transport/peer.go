package transport

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
)

// PeerState describes the current connection state.
type PeerState int

const (
	StateDisconnected PeerState = iota
	StateConnecting
	StateConnected
	StateFailed
)

func (s PeerState) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// PeerEvent is emitted when the peer connection state changes.
type PeerEvent struct {
	State PeerState
	Err   error
}

// Peer wraps a Pion RTCPeerConnection and provides a high-level interface
// for creating and managing WebRTC data channels with automatic reconnection.
type Peer struct {
	mu       sync.RWMutex
	config   *Config
	pc       *webrtc.PeerConnection
	dc       *webrtc.DataChannel
	state    PeerState
	stopCh   chan struct{}
	doneCh   chan struct{}

	// Callbacks
	onStateChange func(PeerEvent)
	onMessage     func([]byte)
	onError       func(error)

	// Reconnection
	reconnectCount int

	// SDP signalling hooks
	// OnLocalDescription is called when an SDP offer/answer is ready to send.
	OnLocalDescription func(sdp webrtc.SessionDescription) error
	// RemoteDescriptionChan receives SDP answers/offers from the remote peer.
	RemoteDescriptionChan chan webrtc.SessionDescription
}

// NewPeer creates a new WebRTC peer.
func NewPeer(cfg *Config) (*Peer, error) {
	if cfg == nil {
		cfg = DefaultConfig(RoleBeacon)
	}

	p := &Peer{
		config:                cfg,
		state:                 StateDisconnected,
		stopCh:                make(chan struct{}),
		doneCh:                make(chan struct{}),
		RemoteDescriptionChan: make(chan webrtc.SessionDescription, 16),
	}

	return p, nil
}

// OnStateChange registers a callback for connection state changes.
func (p *Peer) OnStateChange(fn func(PeerEvent)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onStateChange = fn
}

// OnMessage registers a callback for incoming data channel messages.
func (p *Peer) OnMessage(fn func([]byte)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onMessage = fn
}

// OnError registers a callback for errors.
func (p *Peer) OnError(fn func(error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onError = fn
}

// Connect initiates the WebRTC connection.
// For the operator role, it creates an offer.
// For the beacon role, it waits for a remote offer.
func (p *Peer) Connect(ctx context.Context) error {
	p.mu.Lock()
	if p.state == StateConnected {
		p.mu.Unlock()
		return fmt.Errorf("peer: already connected")
	}
	p.state = StateConnecting
	p.mu.Unlock()

	p.emitState(StateConnecting, nil)

	if err := p.createPeerConnection(); err != nil {
		p.emitState(StateFailed, err)
		return fmt.Errorf("peer: create pc: %w", err)
	}

	// Create data channel (operator initiates)
	if p.config.Role == RoleOperator {
		if err := p.createDataChannel(); err != nil {
			return fmt.Errorf("peer: create dc: %w", err)
		}

		// Create and send offer
		offer, err := p.pc.CreateOffer(nil)
		if err != nil {
			return fmt.Errorf("peer: create offer: %w", err)
		}

		if err := p.pc.SetLocalDescription(offer); err != nil {
			return fmt.Errorf("peer: set local desc: %w", err)
		}

		// Signal the offer to the remote peer
		if p.OnLocalDescription != nil {
			if err := p.OnLocalDescription(offer); err != nil {
				return fmt.Errorf("peer: signal offer: %w", err)
			}
		}
	}

	go p.connectionLoop(ctx)
	return nil
}

// Send sends data over the data channel.
func (p *Peer) Send(data []byte) error {
	p.mu.RLock()
	dc := p.dc
	p.mu.RUnlock()

	if dc == nil {
		return fmt.Errorf("peer: data channel not ready")
	}

	return dc.Send(data)
}

// Close terminates the peer connection.
func (p *Peer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case <-p.stopCh:
		return nil
	default:
		close(p.stopCh)
	}

	if p.pc != nil {
		return p.pc.Close()
	}
	return nil
}

// Done returns a channel that closes when the peer is fully shut down.
func (p *Peer) Done() <-chan struct{} {
	return p.doneCh
}

// Config returns the peer's configuration.
func (p *Peer) Config() *Config {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.config
}

// State returns the current peer state.
func (p *Peer) State() PeerState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

// createPeerConnection sets up the underlying RTCPeerConnection.
func (p *Peer) createPeerConnection() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	settings := webrtc.SettingEngine{}
	settings.DetachDataChannels()

	api := webrtc.NewAPI(webrtc.WithSettingEngine(settings))

	iceServers := make([]webrtc.ICEServer, len(p.config.ICEServers))
	for i, s := range p.config.ICEServers {
		iceServers[i] = webrtc.ICEServer{
			URLs:       s.URLs,
			Username:   s.Username,
			Credential: s.Credential,
		}
	}

	config := webrtc.Configuration{
		ICEServers:   iceServers,
		ICETransportPolicy: webrtc.ICETransportPolicyAll,
	}

	pc, err := api.NewPeerConnection(config)
	if err != nil {
		return fmt.Errorf("new pc: %w", err)
	}

	// Handle ICE connection state changes
	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("[transport] ICE state: %s", state)
		switch state {
		case webrtc.ICEConnectionStateConnected:
			p.emitState(StateConnected, nil)
			p.mu.Lock()
			p.reconnectCount = 0
			p.mu.Unlock()
		case webrtc.ICEConnectionStateDisconnected:
			p.emitState(StateDisconnected, nil)
		case webrtc.ICEConnectionStateFailed:
			p.emitState(StateFailed, fmt.Errorf("ICE failed"))
		}
	})

	// Handle data channels initiated by the remote peer (beacon side)
	if p.config.Role == RoleBeacon {
		pc.OnDataChannel(func(dc *webrtc.DataChannel) {
			log.Printf("[transport] received remote data channel: %s", dc.Label())
			p.handleDataChannel(dc)
		})
	}

	p.pc = pc
	return nil
}

// createDataChannel creates the data channel (operator side).
func (p *Peer) createDataChannel() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	dc, err := p.pc.CreateDataChannel(p.config.DataChannelName, &webrtc.DataChannelInit{
		Ordered: boolPtr(true),
	})
	if err != nil {
		return fmt.Errorf("create dc: %w", err)
	}

	p.handleDataChannel(dc)
	return nil
}

// handleDataChannel sets up the data channel callbacks.
func (p *Peer) handleDataChannel(dc *webrtc.DataChannel) {
	p.mu.Lock()
	p.dc = dc
	p.mu.Unlock()

	dc.OnOpen(func() {
		log.Printf("[transport] data channel '%s' opened", dc.Label())
	})

	dc.OnClose(func() {
		log.Printf("[transport] data channel '%s' closed", dc.Label())
	})

	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		p.mu.RLock()
		cb := p.onMessage
		p.mu.RUnlock()
		if cb != nil {
			cb(msg.Data)
		}
	})
}

// connectionLoop handles the main event loop including reconnection.
func (p *Peer) connectionLoop(ctx context.Context) {
	defer close(p.doneCh)

	for {
		select {
		case <-ctx.Done():
			p.Close()
			return

		case <-p.stopCh:
			return

		case remoteDesc := <-p.RemoteDescriptionChan:
			if err := p.pc.SetRemoteDescription(remoteDesc); err != nil {
				log.Printf("[transport] set remote desc error: %v", err)
				p.emitState(StateFailed, err)
				return
			}

			// If we're the beacon, we need to create an answer
			if p.config.Role == RoleBeacon {
				answer, err := p.pc.CreateAnswer(nil)
				if err != nil {
					log.Printf("[transport] create answer error: %v", err)
					return
				}

				if err := p.pc.SetLocalDescription(answer); err != nil {
					log.Printf("[transport] set local desc error: %v", err)
					return
				}

				if p.OnLocalDescription != nil {
					if err := p.OnLocalDescription(answer); err != nil {
						log.Printf("[transport] signal answer error: %v", err)
					}
				}
			}

		case <-p.reconnectTimer():
			p.mu.RLock()
			count := p.reconnectCount
			maxRetries := p.config.MaxReconnectRetries
			p.mu.RUnlock()

			if count >= maxRetries {
				p.emitState(StateFailed, fmt.Errorf("max reconnection retries exceeded"))
				return
			}

			if p.state == StateDisconnected || p.state == StateFailed {
				log.Printf("[transport] attempting reconnect #%d", count+1)
				p.mu.Lock()
				p.reconnectCount++
				p.mu.Unlock()
				p.reconnect()
			}
		}
	}
}

func (p *Peer) reconnectTimer() <-chan time.Time {
	p.mu.RLock()
	delay := p.config.ReconnectDelay
	p.mu.RUnlock()
	return time.After(delay)
}

func (p *Peer) reconnect() {
	_ = p.pc.Close()
	p.mu.Lock()
	p.pc = nil
	p.dc = nil
	p.mu.Unlock()

	p.emitState(StateConnecting, nil)

	if err := p.createPeerConnection(); err != nil {
		log.Printf("[transport] reconnect create pc error: %v", err)
		p.emitState(StateFailed, err)
		return
	}

	if p.config.Role == RoleOperator {
		if err := p.createDataChannel(); err != nil {
			log.Printf("[transport] reconnect create dc error: %v", err)
			return
		}

		offer, err := p.pc.CreateOffer(nil)
		if err != nil {
			log.Printf("[transport] reconnect create offer error: %v", err)
			return
		}

		if err := p.pc.SetLocalDescription(offer); err != nil {
			log.Printf("[transport] reconnect set local error: %v", err)
			return
		}

		if p.OnLocalDescription != nil {
			_ = p.OnLocalDescription(offer)
		}
	}
}

func (p *Peer) emitState(state PeerState, err error) {
	p.mu.Lock()
	p.state = state
	cb := p.onStateChange
	p.mu.Unlock()

	if cb != nil {
		cb(PeerEvent{State: state, Err: err})
	}
}

// Ensure io.Closer interface
var _ io.Closer = (*Peer)(nil)

func boolPtr(b bool) *bool {
	return &b
}
