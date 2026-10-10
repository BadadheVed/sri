package httpserver_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"sre-platform/backend/internal/httpserver"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
	"sre-platform/backend/internal/topology"
)

type topoProvider struct{ nodes map[string][]topology.Node }

func (p topoProvider) Namespaces() []topology.NamespaceInfo {
	var out []topology.NamespaceInfo
	for _, ns := range []string{"data", "shop"} {
		out = append(out, topology.NamespaceInfo{Name: ns, Services: len(p.nodes[ns])})
	}
	return out
}
func (p topoProvider) NodesIn(ns string) []topology.Node { return p.nodes[ns] }

func tnode(ns, name string) topology.Node {
	return topology.Node{ID: ns + "/" + name, Namespace: ns, Name: name, Ports: []topology.Port{}}
}

func newTopoProvider() topoProvider {
	return topoProvider{nodes: map[string][]topology.Node{
		"shop": {tnode("shop", "web"), tnode("shop", "api")},
		"data": {tnode("data", "db")},
	}}
}

type topoFrame struct {
	Type  string                   `json:"type"`
	Items []topology.NamespaceInfo `json:"items"`
	Nodes []topology.Node          `json:"nodes"`
	Edges []topology.Edge          `json:"edges"`
}

func readFrame(t *testing.T, conn *websocket.Conn, timeout time.Duration) (topoFrame, error) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(timeout))
	var f topoFrame
	err := conn.ReadJSON(&f)
	return f, err
}

func mustFrame(t *testing.T, conn *websocket.Conn) topoFrame {
	t.Helper()
	f, err := readFrame(t, conn, 2*time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return f
}

func waitTopoClients(t *testing.T, hub *topology.Hub, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if hub.ClientCount() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("ClientCount = %d, want %d", hub.ClientCount(), want)
}

// testTopologyWSToken is deliberately different from testMCPReadonlyToken:
// /ws/topology has its own token (TOPOLOGY_WS_TOKEN).
const testTopologyWSToken = "topology-ws-test-token"

func topoServer(t *testing.T, hub *topology.Hub) *httptest.Server {
	t.Helper()
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	router := httpserver.NewRouter(slackClient, store.NewMemoryStore(), &fakeDiagnosisReceiver{}, testDiagnosisToken, httpserver.NewMetricsHub(), testMCPReadonlyToken, hub, testTopologyWSToken)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return server
}

func dialTopo(t *testing.T, server *httptest.Server) *websocket.Conn {
	t.Helper()
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL(server)+"/ws/topology?token="+testTopologyWSToken, nil)
	if err != nil {
		t.Fatalf("Dial: %v (resp %+v)", err, resp)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func subscribe(t *testing.T, conn *websocket.Conn, ns ...string) {
	t.Helper()
	if ns == nil {
		ns = []string{}
	}
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "namespaces": ns}); err != nil {
		t.Fatal(err)
	}
}

func edgeRate(cns, c, sns, s string, rps float64) map[topology.EdgeKey]topology.EdgeRate {
	return map[topology.EdgeKey]topology.EdgeRate{{ClientNS: cns, Client: c, ServerNS: sns, Server: s}: {RequestsPerSecond: rps}}
}

func hasTopoNode(f topoFrame, id string) bool {
	for _, n := range f.Nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}

func TestTopologyWS_AuthRejectsMissingOrBadTokenBeforeUpgrade(t *testing.T) {
	hub := topology.NewHub(newTopoProvider())
	server := topoServer(t, hub)
	for _, q := range []string{"", "?token=", "?token=wrong", "?token=" + testTopologyWSToken + "x", "?token=" + testMCPReadonlyToken} {
		_, resp, err := websocket.DefaultDialer.Dial(wsURL(server)+"/ws/topology"+q, nil)
		if err == nil {
			t.Fatalf("query %q: expected dial failure", q)
		}
		if resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("query %q: want 401, got %+v (%v)", q, resp, err)
		}
	}
	if hub.ClientCount() != 0 {
		t.Fatalf("rejected dials must not register clients")
	}
}

func TestTopologyWS_BearerHeaderAloneIsNotAccepted(t *testing.T) {
	// The endpoint authenticates ?token= only; it sits outside the bearer
	// middleware, so a header alone must not be enough.
	hub := topology.NewHub(newTopoProvider())
	server := topoServer(t, hub)
	_, resp, err := websocket.DefaultDialer.Dial(wsURL(server)+"/ws/topology", http.Header{"Authorization": {"Bearer " + testTopologyWSToken}})
	if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %+v (%v)", resp, err)
	}
}

func TestTopologyWS_MetricsStillRequiresBearer(t *testing.T) {
	server := topoServer(t, topology.NewHub(newTopoProvider()))
	_, resp, err := websocket.DefaultDialer.Dial(wsURL(server)+"/ws/metrics?token="+testMCPReadonlyToken, nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("/ws/metrics must still require bearer header, got %+v (%v)", resp, err)
	}
}

func TestTopologyWS_ConnectGetsNamespacesFirst(t *testing.T) {
	hub := topology.NewHub(newTopoProvider())
	conn := dialTopo(t, topoServer(t, hub))
	f := mustFrame(t, conn)
	if f.Type != "namespaces" || len(f.Items) != 2 || f.Items[1].Name != "shop" || f.Items[1].Services != 2 {
		t.Fatalf("got %+v", f)
	}
}

func TestTopologyWS_SubscribeSnapshotThenOnlyChanges(t *testing.T) {
	hub := topology.NewHub(newTopoProvider())
	conn := dialTopo(t, topoServer(t, hub))
	mustFrame(t, conn)
	subscribe(t, conn, "shop")
	f := mustFrame(t, conn)
	if f.Type != "snapshot" || !hasTopoNode(f, "shop/web") {
		t.Fatalf("got %+v", f)
	}
	hub.Publish(edgeRate("shop", "web", "shop", "api", 3))
	if f := mustFrame(t, conn); len(f.Edges) != 1 || f.Edges[0].RequestsPerSecond != 3 {
		t.Fatalf("got %+v", f)
	}
	hub.Publish(edgeRate("shop", "web", "shop", "api", 3)) // unchanged hash
	if f, err := readFrame(t, conn, 300*time.Millisecond); err == nil {
		t.Fatalf("expected no frame for unchanged hash, got %+v", f)
	}
}

func TestTopologyWS_UnsubscribedNamespaceNeverDeliveredAndResubscribeSwitches(t *testing.T) {
	hub := topology.NewHub(newTopoProvider())
	server := topoServer(t, hub)
	conn := dialTopo(t, server)
	mustFrame(t, conn)
	subscribe(t, conn, "data")
	mustFrame(t, conn)
	hub.Publish(edgeRate("shop", "web", "shop", "api", 7))
	if f, err := readFrame(t, conn, 300*time.Millisecond); err == nil {
		t.Fatalf("shop traffic delivered to data subscriber: %+v", f)
	}
	conn2 := dialTopo(t, server) // fresh conn: the previous one hit a read deadline
	mustFrame(t, conn2)
	subscribe(t, conn2, "data")
	f := mustFrame(t, conn2)
	if hasTopoNode(f, "shop/web") {
		t.Fatalf("got %+v", f)
	}
	subscribe(t, conn2, "shop")
	f = mustFrame(t, conn2)
	if !hasTopoNode(f, "shop/web") || hasTopoNode(f, "data/db") || len(f.Edges) != 1 {
		t.Fatalf("after resubscribe got %+v", f)
	}
	hub.Publish(edgeRate("data", "db", "data", "db", 1)) // only data changes
	if f, err := readFrame(t, conn2, 300*time.Millisecond); err == nil && hasTopoNode(f, "data/db") {
		t.Fatalf("old namespace still streamed after resubscribe: %+v", f)
	}
}

func TestTopologyWS_SubscriptionCapClosesConnection(t *testing.T) {
	hub := topology.NewHub(newTopoProvider())
	conn := dialTopo(t, topoServer(t, hub))
	mustFrame(t, conn)
	ns := make([]string, topology.MaxSubscribedNamespaces+1)
	for i := range ns {
		ns[i] = fmt.Sprintf("ns%d", i)
	}
	subscribe(t, conn, ns...)
	_, err := readFrame(t, conn, 2*time.Second)
	var ce *websocket.CloseError
	if !errors.As(err, &ce) || ce.Code != websocket.ClosePolicyViolation {
		t.Fatalf("want close 1008, got %v", err)
	}
	waitTopoClients(t, hub, 0)
}

func TestTopologyWS_OversizedMessageClosesConnection(t *testing.T) {
	hub := topology.NewHub(newTopoProvider())
	conn := dialTopo(t, topoServer(t, hub))
	mustFrame(t, conn)
	big := `{"type":"subscribe","namespaces":["` + strings.Repeat("a", 70*1024) + `"]}`
	_ = conn.WriteMessage(websocket.TextMessage, []byte(big))
	if _, err := readFrame(t, conn, 2*time.Second); err == nil {
		t.Fatal("expected connection to be closed after oversized message")
	}
	waitTopoClients(t, hub, 0)
}

func TestTopologyWS_ClientDisconnectUnregisters(t *testing.T) {
	hub := topology.NewHub(newTopoProvider())
	server := topoServer(t, hub)
	conn := dialTopo(t, server)
	mustFrame(t, conn)
	waitTopoClients(t, hub, 1)
	conn.Close()
	waitTopoClients(t, hub, 0)
}

// bigProvider makes each snapshot large (~MBs) so a non-reading client's TCP
// buffers fill and its writer blocks.
type bigProvider struct{ n int }

func (p bigProvider) Namespaces() []topology.NamespaceInfo {
	return []topology.NamespaceInfo{{Name: "big", Services: p.n}}
}
func (p bigProvider) NodesIn(ns string) []topology.Node {
	out := make([]topology.Node, p.n)
	for i := range out {
		out[i] = tnode(ns, fmt.Sprintf("svc-%06d-%s", i, strings.Repeat("x", 64)))
	}
	return out
}

func TestTopologyWS_SlowClientDoesNotBlockOther(t *testing.T) {
	hub := topology.NewHub(bigProvider{n: 5000})
	server := topoServer(t, hub)

	slow := dialTopo(t, server) // never reads after subscribing
	subscribe(t, slow, "big")
	fast := dialTopo(t, server)
	mustFrame(t, fast)
	subscribe(t, fast, "big")
	mustFrame(t, fast)

	got := make(chan float64, 1024)
	fast.SetReadDeadline(time.Time{}) // clear the deadline mustFrame left behind
	go func() {
		for {
			var f topoFrame
			if err := fast.ReadJSON(&f); err != nil {
				close(got)
				return
			}
			if len(f.Edges) == 1 {
				got <- f.Edges[0].RequestsPerSecond
			}
		}
	}()

	const ticks = 30
	start := time.Now()
	for i := 1; i <= ticks; i++ {
		hub.Publish(edgeRate("big", "a", "big", "b", float64(i)))
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("Publish blocked for %v", d)
	}
	deadline := time.After(10 * time.Second)
	for {
		select {
		case v, ok := <-got:
			if !ok {
				t.Fatal("fast client connection closed")
			}
			if v == ticks {
				return // fast client received the latest frame
			}
		case <-deadline:
			t.Fatal("fast client never received the latest frame")
		}
	}
}

func TestTopologyWS_StuckClientDisconnectedAfterWriteTimeout(t *testing.T) {
	hub := topology.NewHub(bigProvider{n: 5000})
	h := httpserver.NewTopologyWSHandlerWithOptions(hub, "tok", httpserver.TopologyWSOptions{
		WriteTimeout: 200 * time.Millisecond, PongWait: time.Minute, PingPeriod: 50 * time.Second,
	})
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	conn, _, err := websocket.DefaultDialer.Dial(wsURL(server)+"?token=tok", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	subscribe(t, conn, "big")
	waitTopoClients(t, hub, 1)
	for i := 1; i <= 40; i++ { // ~0.5MB/frame; never read -> writer blocks
		hub.Publish(edgeRate("big", "a", "big", "b", float64(i)))
		time.Sleep(10 * time.Millisecond)
	}
	waitTopoClients(t, hub, 0)
}

func TestTopologyWS_HandlerChecksTokenItself(t *testing.T) {
	hub := topology.NewHub(newTopoProvider())
	server := httptest.NewServer(httpserver.NewTopologyWSHandler(hub, "tok"))
	t.Cleanup(server.Close)
	_, resp, err := websocket.DefaultDialer.Dial(wsURL(server)+"?token=nope", nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %+v (%v)", resp, err)
	}
	// Empty configured token must never authenticate an empty presented one.
	server2 := httptest.NewServer(httpserver.NewTopologyWSHandler(hub, ""))
	t.Cleanup(server2.Close)
	_, resp, err = websocket.DefaultDialer.Dial(wsURL(server2)+"?token=", nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("empty token: want 401, got %+v (%v)", resp, err)
	}
}

func TestTopologyWS_DisabledWhenTokenEmpty(t *testing.T) {
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	hub := topology.NewHub(newTopoProvider())
	router := httpserver.NewRouter(slackClient, store.NewMemoryStore(), &fakeDiagnosisReceiver{}, testDiagnosisToken, httpserver.NewMetricsHub(), testMCPReadonlyToken, hub, "")
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	for _, q := range []string{"", "?token=", "?token=" + testMCPReadonlyToken} {
		_, resp, err := websocket.DefaultDialer.Dial(wsURL(server)+"/ws/topology"+q, nil)
		if err == nil || resp == nil || resp.StatusCode != http.StatusNotFound {
			t.Fatalf("query %q: want 404 (endpoint disabled), got %+v (%v)", q, resp, err)
		}
	}
	// /ws/metrics unaffected: still bearer-authenticated with the MCP token.
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL(server)+"/ws/metrics", http.Header{"Authorization": {"Bearer " + testMCPReadonlyToken}})
	if err != nil {
		t.Fatalf("/ws/metrics with bearer: %v (%+v)", err, resp)
	}
	conn.Close()
}

// unsyncedProvider reports no namespaces until synced is closed.
type unsyncedProvider struct {
	topoProvider
	synced chan struct{}
}

func (p unsyncedProvider) Namespaces() []topology.NamespaceInfo {
	select {
	case <-p.synced:
		return p.topoProvider.Namespaces()
	default:
		return nil
	}
}

func (p unsyncedProvider) WaitSynced(ctx context.Context) bool {
	select {
	case <-p.synced:
		return true
	case <-ctx.Done():
		return false
	}
}

func TestTopologyWS_WaitsForProviderSyncBeforeNamespacesFrame(t *testing.T) {
	p := unsyncedProvider{topoProvider: newTopoProvider(), synced: make(chan struct{})}
	hub := topology.NewHub(p)
	server := httptest.NewServer(httpserver.NewTopologyWSHandler(hub, "tok"))
	t.Cleanup(server.Close)
	time.AfterFunc(150*time.Millisecond, func() { close(p.synced) })
	conn, _, err := websocket.DefaultDialer.Dial(wsURL(server)+"?token=tok", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	f := mustFrame(t, conn)
	if f.Type != "namespaces" || len(f.Items) != 2 {
		t.Fatalf("first frame = %+v, want the synced namespace list (not a blank one)", f)
	}
}

func TestTopologyWS_SyncWaitIsBounded(t *testing.T) {
	p := unsyncedProvider{topoProvider: newTopoProvider(), synced: make(chan struct{})}
	hub := topology.NewHub(p)
	server := httptest.NewServer(httpserver.NewTopologyWSHandlerWithOptions(hub, "tok", httpserver.TopologyWSOptions{SyncWait: 50 * time.Millisecond}))
	t.Cleanup(server.Close)
	conn, _, err := websocket.DefaultDialer.Dial(wsURL(server)+"?token=tok", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	f := mustFrame(t, conn)
	if f.Type != "namespaces" || len(f.Items) != 0 {
		t.Fatalf("got %+v, want (empty) namespaces frame after the bounded wait", f)
	}
	// Once synced, the next publish pushes the real list.
	close(p.synced)
	hub.Publish(nil)
	if f := mustFrame(t, conn); f.Type != "namespaces" || len(f.Items) != 2 {
		t.Fatalf("got %+v, want refreshed namespaces frame", f)
	}
}
