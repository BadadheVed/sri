// backend/internal/httpserver/metrics_ws.go
package httpserver

import (
	"log/slog"
	"net/http"
	"strconv"
	"sync"

	"github.com/gorilla/websocket"

	"sre-platform/backend/internal/histogramquantile"
	"sre-platform/backend/internal/metricsagg"
)

// wsClientBufferSize bounds each client's outbound queue. A client this
// far behind is considered unresponsive — trySend drops it rather than
// let one slow browser tab stall the broadcast to everyone else. Sized to
// comfortably absorb one aggregation tick's worth of Publish calls (one
// per distinct route series, fired back-to-back with no pacing) for a
// client that's actively draining, without needing a client to be
// literally instantaneous to avoid a false-positive drop.
const wsClientBufferSize = 64

// metricsMessage is the JSON shape sent to every WebSocket client on each
// published Result. SeriesKey's fields are flattened alongside the
// aggregated numbers for a simpler frontend shape. Quantiles must be
// map[string]float64, not map[float64]float64 — encoding/json only
// accepts map keys that are string, an integer type, or
// encoding.TextMarshaler.
type metricsMessage struct {
	Namespace         string             `json:"namespace"`
	Service           string             `json:"service"`
	Route             string             `json:"route"`
	Method            string             `json:"method"`
	Quantiles         map[string]float64 `json:"quantiles"`
	RequestsPerSecond float64            `json:"requests_per_second"`
	Reset             bool               `json:"reset"`
}

type wsClient struct {
	conn *websocket.Conn
	send chan metricsMessage
}

// MetricsHub fans out histogramquantile.Result values to every currently
// connected WebSocket client. It implements metricsagg.Sink — the
// Aggregator publishes into it with zero knowledge that WebSocket, or
// gorilla/websocket, exists.
type MetricsHub struct {
	mu      sync.Mutex
	clients map[*wsClient]struct{}
}

func NewMetricsHub() *MetricsHub {
	return &MetricsHub{clients: make(map[*wsClient]struct{})}
}

// ClientCount reports the number of currently registered clients — used
// by tests to wait for (un)registration deterministically, since it
// happens asynchronously relative to the HTTP request that triggers it.
func (h *MetricsHub) ClientCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

var upgrader = websocket.Upgrader{
	// No cross-origin restriction: this endpoint is bearer-token gated the
	// same way /internal is (see router.go), and every deployment target
	// so far is same-origin. Revisit if a separately-hosted frontend
	// origin needs this relaxed differently.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ServeWS upgrades the connection, registers a client, and starts its
// writer and reader goroutines — both own the connection's lifetime from
// here on and unregister it on disconnect.
func (h *MetricsHub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn("metrics ws: upgrade failed", "remote_addr", r.RemoteAddr, "error", err)
		return
	}
	c := &wsClient{conn: conn, send: make(chan metricsMessage, wsClientBufferSize)}

	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()

	go h.writeLoop(c)
	go h.readLoop(c)
}

// writeLoop is the sole writer for c's connection, per gorilla/websocket's
// one-writer-goroutine rule. It drains c.send until removeClient closes
// the channel (from any of the three paths that can remove a client).
func (h *MetricsHub) writeLoop(c *wsClient) {
	for msg := range c.send {
		if err := c.conn.WriteJSON(msg); err != nil {
			slog.Warn("metrics ws: write failed, unregistering client", "error", err)
			h.removeClient(c)
			return
		}
	}
}

// readLoop's only purpose is detecting disconnects: clients never send
// real application messages, but gorilla/websocket requires something to
// read control frames and surface a connection error — standard
// gorilla/websocket idiom.
func (h *MetricsHub) readLoop(c *wsClient) {
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			h.removeClient(c)
			return
		}
	}
}

// removeClient unregisters c exactly once no matter which of the three
// call sites (write error, read error, Publish's buffer-full drop) gets
// there first — the map-membership check and the removal happen inside
// one critical section, so a second concurrent caller sees c already gone
// and does nothing.
func (h *MetricsHub) removeClient(c *wsClient) {
	h.mu.Lock()
	_, ok := h.clients[c]
	if ok {
		delete(h.clients, c)
	}
	h.mu.Unlock()
	if !ok {
		return
	}
	close(c.send)
	c.conn.Close()
}

// trySend does a non-blocking send of msg to c, dropping and unregistering
// c if its buffer is full. The full send-or-drop decision runs inside the
// same critical section as the registration check, so it can never race
// with removeClient closing c.send from another goroutine (write/read
// error) — that would otherwise be a send-on-closed-channel panic.
func (h *MetricsHub) trySend(c *wsClient, msg metricsMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; !ok {
		return // already removed by writeLoop/readLoop concurrently
	}
	select {
	case c.send <- msg:
	default:
		slog.Warn("metrics ws: client outbound buffer full, dropping client")
		delete(h.clients, c)
		close(c.send)
		c.conn.Close()
	}
}

// Publish implements metricsagg.Sink.
func (h *MetricsHub) Publish(key metricsagg.SeriesKey, result histogramquantile.Result) {
	msg := metricsMessage{
		Namespace: key.Namespace, Service: key.Service, Route: key.Route, Method: key.Method,
		Quantiles:         formatQuantiles(result.Quantiles),
		RequestsPerSecond: result.RequestsPerSecond,
		Reset:             result.Reset,
	}

	h.mu.Lock()
	clients := make([]*wsClient, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()

	for _, c := range clients {
		h.trySend(c, msg)
	}
}

func formatQuantiles(qs map[float64]float64) map[string]float64 {
	out := make(map[string]float64, len(qs))
	for q, v := range qs {
		out[strconv.FormatFloat(q, 'g', -1, 64)] = v
	}
	return out
}
