// Package signaller handles SDP exchange between the operator and beacon.
// During development, this uses a simple HTTP rendezvous server.
// In Ghost Calls mode, this would be replaced with meeting-channel signalling.
package signaller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

// Signaller is the interface for exchanging SDP descriptions.
type Signaller interface {
	// SendLocalDescription transmits this peer's SDP to the remote.
	SendLocalDescription(sdp webrtc.SessionDescription) error

	// ReceiveRemoteDescription blocks until a remote SDP is received.
	ReceiveRemoteDescription() (*webrtc.SessionDescription, error)
}

// HTTPSignaller exchanges SDP through a rendezvous HTTP server.
// Simple, works for LAN and same-network development.
type HTTPSignaller struct {
	client     *http.Client
	baseURL    string
	sessionKey string
	role       string
}

// NewHTTPSignaller creates a new HTTP signaller.
func NewHTTPSignaller(baseURL, sessionKey, role string) *HTTPSignaller {
	return &HTTPSignaller{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL:    baseURL,
		sessionKey: sessionKey,
		role:       role,
	}
}

func (h *HTTPSignaller) SendLocalDescription(sdp webrtc.SessionDescription) error {
	data, err := json.Marshal(sdp)
	if err != nil {
		return fmt.Errorf("signaller: marshal sdp: %w", err)
	}

	url := fmt.Sprintf("%s/sdp/%s/%s", h.baseURL, h.sessionKey, h.role)
	resp, err := h.client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("signaller: post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("signaller: post %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (h *HTTPSignaller) ReceiveRemoteDescription() (*webrtc.SessionDescription, error) {
	// Determine which role to poll for
	remoteRole := "beacon"
	if h.role == "beacon" {
		remoteRole = "operator"
	}

	url := fmt.Sprintf("%s/sdp/%s/%s", h.baseURL, h.sessionKey, remoteRole)

	for i := 0; i < 60; i++ {
		resp, err := h.client.Get(url)
		if err != nil {
			return nil, fmt.Errorf("signaller: get: %w", err)
		}

		if resp.StatusCode == http.StatusOK {
			var sdp webrtc.SessionDescription
			if err := json.NewDecoder(resp.Body).Decode(&sdp); err != nil {
				resp.Body.Close()
				return nil, fmt.Errorf("signaller: decode: %w", err)
			}
			resp.Body.Close()
			return &sdp, nil
		}
		resp.Body.Close()

		time.Sleep(500 * time.Millisecond)
	}

	return nil, fmt.Errorf("signaller: timeout waiting for remote description")
}

// SignallerServer is the HTTP/WS rendezvous server for SDP exchange.
type SignallerServer struct {
	mu       sync.RWMutex
	sdpMap   map[string]map[string]webrtc.SessionDescription
	server   *http.Server
	upgrader websocket.Upgrader
}

// NewSignallerServer creates a new signaller rendezvous server.
func NewSignallerServer(addr string) *SignallerServer {
	s := &SignallerServer{
		sdpMap: make(map[string]map[string]webrtc.SessionDescription),
	}

	s.upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/sdp/", s.handleSDP)
	mux.HandleFunc("/ws", s.handleWebSocket)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return s
}

// Start begins listening for SDP exchanges.
func (s *SignallerServer) Start() error {
	addr := s.server.Addr
	if addr == "" {
		addr = ":9090"
		s.server.Addr = addr
	}
	fmt.Printf("[signaller] listening on %s\n", addr)
	return s.server.ListenAndServe()
}

// Stop gracefully shuts down the signaller server.
func (s *SignallerServer) Stop() error {
	return s.server.Close()
}

// Addr returns the listening address.
func (s *SignallerServer) Addr() string {
	return s.server.Addr
}

// StoreSDP stores an SDP description for a session/role.
func (s *SignallerServer) StoreSDP(sessionKey, role string, sdp webrtc.SessionDescription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sdpMap[sessionKey] == nil {
		s.sdpMap[sessionKey] = make(map[string]webrtc.SessionDescription)
	}
	s.sdpMap[sessionKey][role] = sdp
}

// GetSDP retrieves an SDP description for a session/role.
func (s *SignallerServer) GetSDP(sessionKey, role string) (*webrtc.SessionDescription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.sdpMap[sessionKey]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	sdp, ok := entry[role]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return &sdp, nil
}

// handleWebSocket upgrades connections for WebSocket SDP exchange.
func (s *SignallerServer) handleWebSocket(rw http.ResponseWriter, r *http.Request) {
	sessionKey := r.URL.Query().Get("sess")
	role := r.URL.Query().Get("role")
	action := r.URL.Query().Get("action")

	if sessionKey == "" || role == "" {
		http.Error(rw, "missing sess or role", http.StatusBadRequest)
		return
	}

	if action == "" {
		http.Error(rw, "missing action (sdp|wait)", http.StatusBadRequest)
		return
	}

	conn, err := s.upgrader.Upgrade(rw, r, nil)
	if err != nil {
		log.Printf("[ws-signaller] upgrade error: %v", err)
		return
	}
	defer conn.Close()

	log.Printf("[ws-signaller] %s joined session %s (action=%s)", role, sessionKey, action)

	switch action {
	case "sdp":
		// Read SDP from WebSocket and store it
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[ws-signaller] read sdp: %v", err)
			return
		}
		var sdp webrtc.SessionDescription
		if err := json.Unmarshal(msg, &sdp); err != nil {
			log.Printf("[ws-signaller] bad sdp: %v", err)
			return
		}
		s.StoreSDP(sessionKey, role, sdp)

	case "wait":
		// Wait for the remote's SDP
		remoteRole := "beacon"
		if role == "beacon" {
			remoteRole = "operator"
		}

		for i := 0; i < 120; i++ {
			sdp, err := s.GetSDP(sessionKey, remoteRole)
			if err == nil && sdp != nil {
				data, _ := json.Marshal(sdp)
				conn.WriteMessage(websocket.TextMessage, data)
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"timeout"}`))
	}
}

func (s *SignallerServer) handleSDP(w http.ResponseWriter, r *http.Request) {
	// Path: /sdp/{sessionKey}/{role}
	parts := r.URL.Path[len("/sdp/"):]
	if parts == "" {
		http.Error(w, "missing session key", http.StatusBadRequest)
		return
	}

	// Split into session key and role
	var sessionKey, role string
	n := 0
	for i, c := range parts {
		if c == '/' {
			sessionKey = parts[:i]
			role = parts[i+1:]
			n = i
			break
		}
	}
	if sessionKey == "" || role == "" {
		http.Error(w, "expected /sdp/{sessionKey}/{role}", http.StatusBadRequest)
		return
	}
	_ = n

	switch r.Method {
	case http.MethodPost, http.MethodPut:
		var sdp webrtc.SessionDescription
		if err := json.NewDecoder(r.Body).Decode(&sdp); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		s.mu.Lock()
		if s.sdpMap[sessionKey] == nil {
			s.sdpMap[sessionKey] = make(map[string]webrtc.SessionDescription)
		}
		s.sdpMap[sessionKey][role] = sdp
		s.mu.Unlock()

		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "stored sdp for %s/%s", sessionKey, role)

	case http.MethodGet:
		s.mu.RLock()
		entry, ok := s.sdpMap[sessionKey]
		if !ok {
			s.mu.RUnlock()
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		sdp, ok := entry[role]
		s.mu.RUnlock()

		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sdp)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
