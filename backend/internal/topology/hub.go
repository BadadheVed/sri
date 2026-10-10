// backend/internal/topology/hub.go
package topology

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// MaxSubscribedNamespaces caps how many namespaces one client may
	// subscribe to at once.
	MaxSubscribedNamespaces = 50
	// ClientSendBuffer bounds each client's outbound frame queue. When it is
	// full the oldest frame is dropped (latest-frame coalescing): every frame
	// is a full snapshot, so only the newest one matters.
	ClientSendBuffer = 4
)

var (
	// ErrTooManyNamespaces is returned by Subscribe above MaxSubscribedNamespaces.
	ErrTooManyNamespaces = errors.New("topology: too many namespaces in subscription")
	// ErrClientClosed is returned by Subscribe for an unregistered client.
	ErrClientClosed = errors.New("topology: client is not registered")
)

// Client is one subscriber. Its frames (already-marshaled JSON) are read
// from Frames() by a single writer; the channel is closed on Unregister.
// All mutable fields are guarded by the owning Hub's mutex.
type Client struct {
	send       chan []byte
	subscribed bool
	namespaces []string // sorted, deduplicated
	key        string   // strings.Join(namespaces, ",")
	lastHash   string   // hash of the last snapshot queued to this client
	nsHash     string   // hash of the last namespaces frame queued to this client
}

// Frames returns the client's outbound frame queue.
func (c *Client) Frames() <-chan []byte { return c.send }

// Hub fans topology snapshots out to subscribed clients. It is transport
// agnostic (no websocket types); see httpserver for the WebSocket adapter.
// All methods are safe for concurrent use.
type Hub struct {
	provider NodeProvider
	now      func() time.Time

	mu      sync.Mutex
	rates   map[EdgeKey]EdgeRate
	clients map[*Client]struct{}
}

// NewHub builds a hub serving nodes from p.
func NewHub(p NodeProvider) *Hub {
	return &Hub{provider: p, now: time.Now, rates: map[EdgeKey]EdgeRate{}, clients: map[*Client]struct{}{}}
}

// ClientCount reports the number of registered clients.
func (h *Hub) ClientCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

type namespacesFrame struct {
	Type  string          `json:"type"`
	Items []NamespaceInfo `json:"items"`
}

// WaitProviderSynced blocks until the node provider's caches have synced
// (true) or ctx ends (false). Providers that are not a SyncWaiter are
// always ready. Callers use it so a client's first namespaces frame is not
// an empty list just because the informers are still starting.
func (h *Hub) WaitProviderSynced(ctx context.Context) bool {
	if w, ok := h.provider.(SyncWaiter); ok {
		return w.WaitSynced(ctx)
	}
	return true
}

// namespacesFrameBytes marshals the provider's current namespace list and
// returns it with its content hash.
func (h *Hub) namespacesFrameBytes() ([]byte, string) {
	items := h.provider.Namespaces()
	if items == nil {
		items = []NamespaceInfo{}
	}
	b, err := json.Marshal(namespacesFrame{Type: "namespaces", Items: items})
	if err != nil { // cannot happen for these plain types
		slog.Error("topology hub: marshaling namespaces frame", "error", err)
		return nil, ""
	}
	sum := sha256.Sum256(b)
	return b, hex.EncodeToString(sum[:])
}

// Register adds a client and queues the namespaces frame (all namespaces)
// as its first frame. Later changes to the list are pushed by Publish.
func (h *Hub) Register() *Client {
	b, hash := h.namespacesFrameBytes()
	c := &Client{send: make(chan []byte, ClientSendBuffer)}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = struct{}{}
	if b != nil {
		c.nsHash = hash
		h.enqueueLocked(c, b)
	}
	return c
}

// Unregister removes c and closes its frame channel. Idempotent.
func (h *Hub) Unregister(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; !ok {
		return
	}
	delete(h.clients, c)
	close(c.send)
}

// normalize sorts and deduplicates namespace names.
func normalize(namespaces []string) []string {
	set := make(map[string]struct{}, len(namespaces))
	out := make([]string, 0, len(namespaces))
	for _, ns := range namespaces {
		if _, dup := set[ns]; dup {
			continue
		}
		set[ns] = struct{}{}
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

// Subscribe replaces c's namespace set and immediately queues a snapshot
// for it built from the latest published rates. Unknown namespaces are
// allowed (they simply contribute no nodes).
func (h *Hub) Subscribe(c *Client, namespaces []string) error {
	ns := normalize(namespaces)
	if len(ns) > MaxSubscribedNamespaces {
		return ErrTooManyNamespaces
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; !ok {
		return ErrClientClosed
	}
	c.subscribed, c.namespaces, c.key = true, ns, strings.Join(ns, ",")
	b, hash, err := h.buildLocked(ns)
	if err != nil {
		return err
	}
	c.lastHash = hash
	h.enqueueLocked(c, b)
	return nil
}

// Publish records the latest rates and pushes a snapshot to every
// subscribed client whose snapshot hash changed since the last one it was
// sent. Each distinct namespace set is built and marshaled once and the
// bytes shared; namespaces nobody subscribes to are never built.
//
// Before that, every client (subscribed or not) whose last namespaces frame
// differs from the current list (namespaces added/removed, service counts
// changed) is sent a fresh namespaces frame.
func (h *Hub) Publish(rates map[EdgeKey]EdgeRate) {
	if rates == nil {
		rates = map[EdgeKey]EdgeRate{}
	}
	nsBytes, nsHash := h.namespacesFrameBytes()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rates = rates

	if nsBytes != nil {
		for c := range h.clients {
			if c.nsHash != nsHash {
				c.nsHash = nsHash
				h.enqueueLocked(c, nsBytes)
			}
		}
	}

	type built struct {
		b    []byte
		hash string
		ok   bool
	}
	cache := map[string]built{}
	for c := range h.clients {
		if !c.subscribed {
			continue
		}
		bt, seen := cache[c.key]
		if !seen {
			b, hash, err := h.buildLocked(c.namespaces)
			if err != nil {
				slog.Error("topology hub: building snapshot", "namespaces", c.key, "error", err)
			}
			bt = built{b: b, hash: hash, ok: err == nil}
			cache[c.key] = bt
		}
		if !bt.ok || bt.hash == c.lastHash {
			continue
		}
		c.lastHash = bt.hash
		h.enqueueLocked(c, bt.b)
	}
}

// buildLocked builds and marshals the snapshot for ns from h.rates.
func (h *Hub) buildLocked(ns []string) ([]byte, string, error) {
	s := BuildSnapshot(h.now(), ns, h.provider, h.rates)
	b, err := json.Marshal(s)
	if err != nil {
		return nil, "", err
	}
	return b, s.Hash(), nil
}

// enqueueLocked queues b without ever blocking: when the buffer is full the
// oldest queued frame is dropped first. Only the hub sends on c.send, and
// always under h.mu, so after dropping one there is guaranteed room (the
// writer only ever removes frames). Caller holds h.mu and c is registered.
func (h *Hub) enqueueLocked(c *Client, b []byte) {
	for {
		select {
		case c.send <- b:
			return
		default:
		}
		select {
		case <-c.send:
		default:
		}
	}
}
