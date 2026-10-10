package topology_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"sre-platform/backend/internal/topology"
)

// countingProvider records NodesIn calls per namespace so tests can prove
// the hub builds once per distinct set and never builds unsubscribed ones.
type countingProvider struct {
	mu    sync.Mutex
	nodes map[string][]topology.Node
	calls map[string]int
}

func newCountingProvider() *countingProvider {
	return &countingProvider{
		nodes: map[string][]topology.Node{
			"shop":  {node("shop", "web"), node("shop", "api")},
			"data":  {node("data", "db")},
			"other": {node("other", "x")},
		},
		calls: map[string]int{},
	}
}

func (p *countingProvider) Namespaces() []topology.NamespaceInfo {
	return []topology.NamespaceInfo{{Name: "data", Services: 1}, {Name: "other", Services: 1}, {Name: "shop", Services: 2}}
}

func (p *countingProvider) NodesIn(ns string) []topology.Node {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[ns]++
	return p.nodes[ns]
}

func (p *countingProvider) callsFor(ns string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[ns]
}

func (p *countingProvider) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = map[string]int{}
}

type frame struct {
	Type  string                   `json:"type"`
	Items []topology.NamespaceInfo `json:"items"`
	Nodes []topology.Node          `json:"nodes"`
	Edges []topology.Edge          `json:"edges"`
}

func recv(t *testing.T, c *topology.Client) frame {
	t.Helper()
	select {
	case b, ok := <-c.Frames():
		if !ok {
			t.Fatal("frames channel closed")
		}
		var f frame
		if err := json.Unmarshal(b, &f); err != nil {
			t.Fatalf("unmarshal %s: %v", b, err)
		}
		return f
	default:
		t.Fatal("expected a queued frame, got none")
	}
	return frame{}
}

func assertNoFrame(t *testing.T, c *topology.Client) {
	t.Helper()
	select {
	case b := <-c.Frames():
		t.Fatalf("expected no frame, got %s", b)
	default:
	}
}

func rate(cns, c, sns, s string, rps float64) map[topology.EdgeKey]topology.EdgeRate {
	return map[topology.EdgeKey]topology.EdgeRate{
		{ClientNS: cns, Client: c, ServerNS: sns, Server: s}: {RequestsPerSecond: rps},
	}
}

func hasNode(f frame, id string) bool {
	for _, n := range f.Nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}

func TestHub_RegisterQueuesNamespacesFrameFirst(t *testing.T) {
	h := topology.NewHub(newCountingProvider())
	c := h.Register()
	f := recv(t, c)
	if f.Type != "namespaces" || len(f.Items) != 3 || f.Items[2].Name != "shop" || f.Items[2].Services != 2 {
		t.Fatalf("got %+v, want namespaces frame with 3 items", f)
	}
	if h.ClientCount() != 1 {
		t.Fatalf("ClientCount = %d", h.ClientCount())
	}
}

func TestHub_NamespacesFrameItemsIsEmptyArrayNotNull(t *testing.T) {
	h := topology.NewHub(fakeProvider{})
	c := h.Register()
	b := <-c.Frames()
	if string(b) != `{"type":"namespaces","items":[]}` {
		t.Fatalf("got %s", b)
	}
}

func TestHub_SubscribeRepliesImmediatelyWithLatestRates(t *testing.T) {
	h := topology.NewHub(newCountingProvider())
	h.Publish(rate("shop", "web", "shop", "api", 5))
	c := h.Register()
	recv(t, c) // namespaces
	if err := h.Subscribe(c, []string{"shop"}); err != nil {
		t.Fatal(err)
	}
	f := recv(t, c)
	if f.Type != "snapshot" || len(f.Edges) != 1 || f.Edges[0].RequestsPerSecond != 5 {
		t.Fatalf("got %+v", f)
	}
}

func TestHub_UnsubscribedClientGetsNoSnapshots(t *testing.T) {
	h := topology.NewHub(newCountingProvider())
	c := h.Register()
	recv(t, c)
	h.Publish(rate("shop", "web", "shop", "api", 1))
	assertNoFrame(t, c)
}

func TestHub_UnsubscribedNamespaceDataNeverDelivered(t *testing.T) {
	h := topology.NewHub(newCountingProvider())
	c := h.Register()
	recv(t, c)
	if err := h.Subscribe(c, []string{"data"}); err != nil {
		t.Fatal(err)
	}
	recv(t, c)
	h.Publish(map[topology.EdgeKey]topology.EdgeRate{
		{ClientNS: "shop", Client: "web", ServerNS: "shop", Server: "api"}: {RequestsPerSecond: 9},
		{ClientNS: "other", Client: "x", ServerNS: "data", Server: "db"}:   {RequestsPerSecond: 2},
	})
	f := recv(t, c)
	if hasNode(f, "shop/web") || hasNode(f, "shop/api") {
		t.Fatalf("shop data leaked into data subscription: %+v", f)
	}
	if !hasNode(f, "data/db") || len(f.Edges) != 1 {
		t.Fatalf("got %+v", f)
	}
}

func TestHub_ResubscribeReplacesSet(t *testing.T) {
	h := topology.NewHub(newCountingProvider())
	c := h.Register()
	recv(t, c)
	_ = h.Subscribe(c, []string{"shop"})
	recv(t, c)
	if err := h.Subscribe(c, []string{"data"}); err != nil {
		t.Fatal(err)
	}
	f := recv(t, c)
	if !hasNode(f, "data/db") || hasNode(f, "shop/web") {
		t.Fatalf("immediate snapshot after resubscribe wrong: %+v", f)
	}
	h.Publish(rate("shop", "web", "shop", "api", 3))
	// shop traffic does not touch "data": snapshot for {data} unchanged -> no frame.
	assertNoFrame(t, c)
}

func TestHub_UnchangedHashSendsNoFrameNextTick(t *testing.T) {
	h := topology.NewHub(newCountingProvider())
	c := h.Register()
	recv(t, c)
	_ = h.Subscribe(c, []string{"shop"})
	recv(t, c)
	h.Publish(rate("shop", "web", "shop", "api", 1))
	recv(t, c)
	h.Publish(rate("shop", "web", "shop", "api", 1.001)) // rounds to same hash
	assertNoFrame(t, c)
	h.Publish(rate("shop", "web", "shop", "api", 2))
	if f := recv(t, c); f.Edges[0].RequestsPerSecond != 2 {
		t.Fatalf("got %+v", f)
	}
}

func TestHub_UnknownNamespaceAllowedEmptySnapshot(t *testing.T) {
	h := topology.NewHub(newCountingProvider())
	c := h.Register()
	recv(t, c)
	if err := h.Subscribe(c, []string{"nope"}); err != nil {
		t.Fatal(err)
	}
	f := recv(t, c)
	if f.Type != "snapshot" || len(f.Nodes) != 0 || len(f.Edges) != 0 {
		t.Fatalf("got %+v", f)
	}
}

func TestHub_SubscriptionCapRejected(t *testing.T) {
	h := topology.NewHub(newCountingProvider())
	c := h.Register()
	recv(t, c)
	ok := make([]string, topology.MaxSubscribedNamespaces)
	for i := range ok {
		ok[i] = fmt.Sprintf("ns%d", i)
	}
	if err := h.Subscribe(c, ok); err != nil {
		t.Fatalf("exactly the cap should be accepted: %v", err)
	}
	recv(t, c)
	tooMany := append(ok, "one-more")
	if err := h.Subscribe(c, tooMany); !errors.Is(err, topology.ErrTooManyNamespaces) {
		t.Fatalf("err = %v, want ErrTooManyNamespaces", err)
	}
}

func TestHub_BuildsOncePerDistinctSetAndNeverUnsubscribed(t *testing.T) {
	p := newCountingProvider()
	h := topology.NewHub(p)
	a, b, c := h.Register(), h.Register(), h.Register()
	for _, cl := range []*topology.Client{a, b, c} {
		recv(t, cl)
	}
	_ = h.Subscribe(a, []string{"shop", "data"})
	_ = h.Subscribe(b, []string{"data", "shop", "shop"}) // same set, different order/dupes
	_ = h.Subscribe(c, []string{"data"})
	for _, cl := range []*topology.Client{a, b, c} {
		recv(t, cl)
	}
	p.reset()
	h.Publish(rate("shop", "web", "data", "db", 4))
	if got := p.callsFor("shop"); got != 1 {
		t.Errorf("NodesIn(shop) called %d times, want 1 (shared across a and b)", got)
	}
	if got := p.callsFor("data"); got != 2 {
		t.Errorf("NodesIn(data) called %d times, want 2 (one per distinct set)", got)
	}
	if got := p.callsFor("other"); got != 0 {
		t.Errorf("NodesIn(other) called %d times, want 0 (no subscriber)", got)
	}
	fa, fb := <-a.Frames(), <-b.Frames()
	if string(fa) != string(fb) {
		t.Errorf("clients with the same set got different bytes")
	}
	recv(t, c)
}

func TestHub_FullBufferDropsOldestKeepsLatest(t *testing.T) {
	h := topology.NewHub(newCountingProvider())
	c := h.Register() // namespaces frame queued, never drained
	_ = h.Subscribe(c, []string{"shop"})
	for i := 1; i <= 6; i++ {
		h.Publish(rate("shop", "web", "shop", "api", float64(i)))
	}
	if n := len(c.Frames()); n != topology.ClientSendBuffer {
		t.Fatalf("queued %d frames, want %d", n, topology.ClientSendBuffer)
	}
	var last frame
	for i := 0; i < topology.ClientSendBuffer; i++ {
		f := recv(t, c)
		if i == 0 && f.Type == "namespaces" {
			t.Fatal("oldest frame (namespaces) should have been dropped")
		}
		last = f
	}
	if last.Edges[0].RequestsPerSecond != 6 {
		t.Fatalf("latest frame rps = %v, want 6", last.Edges[0].RequestsPerSecond)
	}
}

func TestHub_UnregisterClosesFramesIdempotently(t *testing.T) {
	h := topology.NewHub(newCountingProvider())
	c := h.Register()
	_ = h.Subscribe(c, []string{"shop"})
	h.Unregister(c)
	h.Unregister(c)
	for range c.Frames() {
	}
	if h.ClientCount() != 0 {
		t.Fatalf("ClientCount = %d", h.ClientCount())
	}
	h.Publish(rate("shop", "web", "shop", "api", 1)) // must not panic
	if err := h.Subscribe(c, []string{"shop"}); err == nil {
		t.Fatal("Subscribe on unregistered client should error")
	}
}

func TestHub_ConcurrentUse(t *testing.T) {
	h := topology.NewHub(newCountingProvider())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := h.Register()
			go func() {
				for range c.Frames() {
				}
			}()
			for j := 0; j < 50; j++ {
				_ = h.Subscribe(c, []string{"shop", fmt.Sprint(i % 3)})
			}
			h.Unregister(c)
		}(i)
	}
	for j := 0; j < 50; j++ {
		h.Publish(rate("shop", "web", "shop", "api", float64(j)))
	}
	wg.Wait()
}

// mutableProvider has a namespace list that tests can change, and a
// controllable sync state (implements topology.SyncWaiter).
type mutableProvider struct {
	mu     sync.Mutex
	items  []topology.NamespaceInfo
	synced chan struct{}
}

func (p *mutableProvider) Namespaces() []topology.NamespaceInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]topology.NamespaceInfo(nil), p.items...)
}
func (p *mutableProvider) NodesIn(string) []topology.Node { return nil }
func (p *mutableProvider) set(items ...topology.NamespaceInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.items = items
}
func (p *mutableProvider) WaitSynced(ctx context.Context) bool {
	select {
	case <-p.synced:
		return true
	case <-ctx.Done():
		return false
	}
}

func TestHub_PublishPushesNamespacesFrameWhenListChanges(t *testing.T) {
	p := &mutableProvider{items: []topology.NamespaceInfo{{Name: "a", Services: 1}}}
	h := topology.NewHub(p)
	idle, sub := h.Register(), h.Register()
	recv(t, idle)
	recv(t, sub)
	_ = h.Subscribe(sub, []string{"a"})
	recv(t, sub)

	h.Publish(nil) // unchanged list -> nothing
	assertNoFrame(t, idle)
	assertNoFrame(t, sub)

	p.set(topology.NamespaceInfo{Name: "a", Services: 2}, topology.NamespaceInfo{Name: "b"})
	h.Publish(nil)
	for _, c := range []*topology.Client{idle, sub} {
		f := recv(t, c)
		if f.Type != "namespaces" || len(f.Items) != 2 || f.Items[0].Services != 2 || f.Items[1].Name != "b" {
			t.Fatalf("got %+v, want refreshed namespaces frame", f)
		}
		assertNoFrame(t, c) // snapshot for {a} unchanged (no nodes, no edges)
	}
	h.Publish(nil) // same list again -> no repeat
	assertNoFrame(t, idle)
}

func TestHub_WaitProviderSynced(t *testing.T) {
	p := &mutableProvider{synced: make(chan struct{})}
	h := topology.NewHub(p)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if h.WaitProviderSynced(ctx) {
		t.Fatal("want false before sync (ctx deadline)")
	}
	close(p.synced)
	if !h.WaitProviderSynced(context.Background()) {
		t.Fatal("want true after sync")
	}
	// Providers without SyncWaiter are always ready.
	if !topology.NewHub(newCountingProvider()).WaitProviderSynced(context.Background()) {
		t.Fatal("non-SyncWaiter provider should be ready")
	}
}
