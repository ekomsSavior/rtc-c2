// Package transport provides WebRTC-based transport for the rtc-c2 framework.
// It wraps Pion WebRTC to manage peer connections, data channels, and ICE/TURN connectivity.
package transport

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// Role indicates whether this instance creates the offer (Operator) or answers (Beacon).
type Role int

const (
	RoleOperator Role = iota
	RoleBeacon
)

func (r Role) String() string {
	switch r {
	case RoleOperator:
		return "operator"
	case RoleBeacon:
		return "beacon"
	default:
		return "unknown"
	}
}

// ICEServerConfig defines a STUN or TURN server for NAT traversal.
type ICEServerConfig struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

// Config holds all transport layer configuration.
type Config struct {
	// Role of this peer (operator or beacon)
	Role Role `json:"role"`

	// ICE servers for NAT traversal
	ICEServers []ICEServerConfig `json:"ice_servers"`

	// Data channel configuration
	DataChannelName string `json:"data_channel_name"`

	// WebRTC settings
	Mtu             int           `json:"mtu"`
	ReconnectDelay  time.Duration `json:"reconnect_delay"`
	MaxReconnectRetries int       `json:"max_reconnect_retries"`

	// Peer ID — unique identifier for this instance
	PeerID string `json:"peer_id"`
}

// DefaultConfig returns a sensible default configuration.
// Uses Google's public STUN servers by default; TURN can be added.
func DefaultConfig(role Role) *Config {
	peerID := generatePeerID()

	return &Config{
		Role: role,
		ICEServers: []ICEServerConfig{
			{
				URLs: []string{
					"stun:stun.l.google.com:19302",
					"stun:stun1.l.google.com:19302",
				},
			},
		},
		DataChannelName:     "rtc-c2-channel",
		Mtu:                 1500,
		ReconnectDelay:      5 * time.Second,
		MaxReconnectRetries: 10,
		PeerID:              peerID,
	}
}

// WithTURN adds TURN relay servers to the configuration.
// For development, use coturn. For Ghost Calls-style operation, use provider TURN.
func (c *Config) WithTURN(urls []string, username, credential string) *Config {
	c.ICEServers = append(c.ICEServers, ICEServerConfig{
		URLs:       urls,
		Username:   username,
		Credential: credential,
	})
	return c
}

// WithAdditionalSTUN adds STUN servers.
func (c *Config) WithAdditionalSTUN(urls ...string) *Config {
	c.ICEServers = append(c.ICEServers, ICEServerConfig{URLs: urls})
	return c
}

func generatePeerID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("peer-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
