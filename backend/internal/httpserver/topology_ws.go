// backend/internal/httpserver/topology_ws.go
package httpserver

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"sre-platform/backend/internal/topology"
)

// TopologyWSOptions tunes the /ws/topology connection lifecycle. Zero fields
// take the defaults below.
type TopologyWSOptions struct {
	// WriteTimeout bounds every write; a client that cannot accept a frame
	// within it is considered stuck and disconnected.
	WriteTimeout time.Duration
	// PongWait is how long the server waits for any client traffic (a pong
	// or a message) before giving up on the connection.
	PongWait time.Duration
	// PingPeriod is how often the server pings; must be < PongWait.
	PingPeriod time.Duration
	// ReadLimit caps the size of one client message in bytes.
	ReadLimit int64
	// SyncWait bounds how long a new connection waits for the node
	// provider's informer caches to sync before its namespaces frame is
	// sent (so it is not a blank list during startup). If the wait times
	// out the frame is sent anyway and the hub pushes the real list on a
	// later tick once it changes.
	SyncWait time.Duration
}

const (
	defaultTopologyWriteTimeout = 10 * time.Second
	defaultTopologyPongWait     = 60 * time.Second
	defaultTopologyPingPeriod   = 54 * time.Second
	defaultTopologyReadLimit    = 64 * 1024
	defaultTopologySyncWait     = 10 * time.Second
)

func (o TopologyWSOptions) withDefaults() TopologyWSOptions {
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = defaultTopologyWriteTimeout
	}
	if o.PongWait <= 0 {
		o.PongWait = defaultTopologyPongWait
	}
	if o.PingPeriod <= 0 || o.PingPeriod >= o.PongWait {
		o.PingPeriod = o.PongWait * 9 / 10
	}
	if o.ReadLimit <= 0 {
		o.ReadLimit = defaultTopologyReadLimit
	}
	if o.SyncWait <= 0 {
		o.SyncWait = defaultTopologySyncWait
	}
	return o
}

type topologyWSHandler struct {
	hub   *topology.Hub
	token string
	opts  TopologyWSOptions
}

// NewTopologyWSHandler serves the topology stream, authenticating the
// ?token= query parameter against token (see router.go for why a query
// parameter rather than a header).
func NewTopologyWSHandler(hub *topology.Hub, token string) http.Handler {
	return NewTopologyWSHandlerWithOptions(hub, token, TopologyWSOptions{})
}

// NewTopologyWSHandlerWithOptions is NewTopologyWSHandler with tunable
// timeouts (used by tests).
func NewTopologyWSHandlerWithOptions(hub *topology.Hub, token string, opts TopologyWSOptions) http.Handler {
	return &topologyWSHandler{hub: hub, token: token, opts: opts.withDefaults()}
}

// tokenOK compares in constant time. An unconfigured (empty) expected token
// never authenticates anything.
func tokenOK(presented, expected string) bool {
	if expected == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(expected)) == 1
}

type topologyClientMessage struct {
	Type       string   `json:"type"`
	Namespaces []string `json:"namespaces"`
}

func (h *topologyWSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !tokenOK(r.URL.Query().Get("token"), h.token) {
		slog.Warn("topology ws: rejected request with missing or invalid token", "remote_addr", r.RemoteAddr)
		http.Error(w, "missing or invalid token", http.StatusUnauthorized)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn("topology ws: upgrade failed", "remote_addr", r.RemoteAddr, "error", err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.opts.SyncWait)
	synced := h.hub.WaitProviderSynced(ctx)
	cancel()
	if !synced {
		slog.Warn("topology ws: node provider not synced yet, sending current namespaces", "waited", h.opts.SyncWait.String())
	}
	c := h.hub.Register()
	go h.writeLoop(conn, c)
	h.readLoop(conn, c)
}

// shutdown unregisters c (closing its frame channel, which stops the
// writer) and closes the socket, unblocking whichever loop is still in
// I/O. Safe to call from both loops.
func (h *topologyWSHandler) shutdown(conn *websocket.Conn, c *topology.Client) {
	h.hub.Unregister(c)
	conn.Close()
}

// writeLoop is the connection's only data writer (gorilla's one-writer
// rule); pings go through WriteControl, which is concurrency-safe.
func (h *topologyWSHandler) writeLoop(conn *websocket.Conn, c *topology.Client) {
	ticker := time.NewTicker(h.opts.PingPeriod)
	defer ticker.Stop()
	defer h.shutdown(conn, c)
	for {
		select {
		case b, ok := <-c.Frames():
			if !ok {
				return
			}
			conn.SetWriteDeadline(time.Now().Add(h.opts.WriteTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
				slog.Warn("topology ws: write failed, disconnecting client", "error", err)
				return
			}
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(h.opts.WriteTimeout)); err != nil {
				return
			}
		}
	}
}

// closeWith sends a close frame with code and reason (best effort).
func (h *topologyWSHandler) closeWith(conn *websocket.Conn, code int, reason string) {
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(h.opts.WriteTimeout))
}

func (h *topologyWSHandler) readLoop(conn *websocket.Conn, c *topology.Client) {
	defer h.shutdown(conn, c)
	conn.SetReadLimit(h.opts.ReadLimit)
	extend := func() { conn.SetReadDeadline(time.Now().Add(h.opts.PongWait)) }
	extend()
	conn.SetPongHandler(func(string) error { extend(); return nil })
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return // disconnect, deadline, or read limit (gorilla already sent 1009)
		}
		extend()
		var msg topologyClientMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			h.closeWith(conn, websocket.CloseInvalidFramePayloadData, "invalid JSON")
			return
		}
		if msg.Type != "subscribe" {
			slog.Debug("topology ws: ignoring unknown message type", "type", msg.Type)
			continue
		}
		if err := h.hub.Subscribe(c, msg.Namespaces); err != nil {
			if errors.Is(err, topology.ErrTooManyNamespaces) {
				h.closeWith(conn, websocket.ClosePolicyViolation, "too many namespaces")
			}
			return
		}
	}
}
