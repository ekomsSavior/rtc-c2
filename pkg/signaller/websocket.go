package signaller

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

// WebSocketSignaller exchanges SDP through WebSocket connections.
// Stealthier than HTTP polling — single persistent connection,
// looks like a normal web app or live data feed (Technique 3).
type WebSocketSignaller struct {
	baseURL    string
	sessionKey string
	role       string
}

// NewWebSocketSignaller creates a WebSocket-based signaller client.
func NewWebSocketSignaller(baseURL, sessionKey, role string) *WebSocketSignaller {
	return &WebSocketSignaller{
		baseURL:    baseURL,
		sessionKey: sessionKey,
		role:       role,
	}
}

// SendLocalDescription sends this peer's SDP via WebSocket.
func (w *WebSocketSignaller) SendLocalDescription(sdp webrtc.SessionDescription) error {
	url := fmt.Sprintf("%s?sess=%s&role=%s&action=sdp", w.baseURL, w.sessionKey, w.role)

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(url, http.Header{})
	if err != nil {
		return fmt.Errorf("ws-signaller: dial: %w", err)
	}
	defer conn.Close()

	data, err := json.Marshal(sdp)
	if err != nil {
		return fmt.Errorf("ws-signaller: marshal: %w", err)
	}

	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("ws-signaller: write: %w", err)
	}

	return nil
}

// ReceiveRemoteDescription waits for and returns the remote peer's SDP.
func (w *WebSocketSignaller) ReceiveRemoteDescription() (*webrtc.SessionDescription, error) {
	url := fmt.Sprintf("%s?sess=%s&role=%s&action=wait", w.baseURL, w.sessionKey, w.role)

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(url, http.Header{})
	if err != nil {
		return nil, fmt.Errorf("ws-signaller: dial: %w", err)
	}
	defer conn.Close()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("ws-signaller: read: %w", err)
		}

		// Check for error response
		var errResp struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(msg, &errResp) == nil && errResp.Error != "" {
			return nil, fmt.Errorf("ws-signaller: %s", errResp.Error)
		}

		var sdp webrtc.SessionDescription
		if err := json.Unmarshal(msg, &sdp); err != nil {
			log.Printf("[ws-signaller] bad sdp: %v", err)
			continue
		}

		return &sdp, nil
	}
}
