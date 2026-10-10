package topology_test

import (
	"reflect"
	"testing"
	"time"

	"sre-platform/backend/internal/topology"
)

type fakeProvider struct{ nodes map[string][]topology.Node }

func (f fakeProvider) Namespaces() []topology.NamespaceInfo { return nil }
func (f fakeProvider) NodesIn(ns string) []topology.Node    { return f.nodes[ns] }

func node(ns, name string) topology.Node {
	return topology.Node{ID: ns + "/" + name, Namespace: ns, Name: name, Ports: []topology.Port{{Port: 80, Protocol: "TCP", TargetPort: "80"}}}
}

func provider() fakeProvider {
	return fakeProvider{nodes: map[string][]topology.Node{
		"shop":  {node("shop", "web"), node("shop", "api")},
		"data":  {node("data", "db")},
		"other": {node("other", "x")},
	}}
}

func nodeByID(s topology.Snapshot, id string) (topology.Node, bool) {
	for _, n := range s.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return topology.Node{}, false
}

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestSnapshotOnlyRequestedNamespaces(t *testing.T) {
	rates := map[topology.EdgeKey]topology.EdgeRate{
		{ClientNS: "shop", Client: "web", ServerNS: "shop", Server: "api"}: {RequestsPerSecond: 1},
		{ClientNS: "other", Client: "x", ServerNS: "other", Server: "x"}:   {RequestsPerSecond: 9},
	}
	s := topology.BuildSnapshot(t0, []string{"shop"}, provider(), rates)
	if s.Type != "snapshot" || !s.TS.Equal(t0) {
		t.Fatalf("bad header: %+v", s)
	}
	if len(s.Nodes) != 2 || s.Nodes[0].ID != "shop/api" || s.Nodes[1].ID != "shop/web" {
		t.Fatalf("nodes: %+v", s.Nodes)
	}
	if len(s.Edges) != 1 || s.Edges[0].Source != "shop/web" || s.Edges[0].Target != "shop/api" {
		t.Fatalf("edges: %+v", s.Edges)
	}
}

func TestSnapshotCrossNamespaceStub(t *testing.T) {
	rates := map[topology.EdgeKey]topology.EdgeRate{
		{ClientNS: "shop", Client: "web", ServerNS: "data", Server: "db"}: {RequestsPerSecond: 2, ErrorsPerSecond: 1},
	}
	s := topology.BuildSnapshot(t0, []string{"shop"}, provider(), rates)
	n, ok := nodeByID(s, "data/db")
	if !ok || !n.Stub || len(n.Ports) != 0 {
		t.Fatalf("expected stub data/db, got %+v ok=%v", n, ok)
	}
	if len(s.Edges) != 1 || s.Edges[0].ErrorsPerSecond != 1 {
		t.Fatalf("edges: %+v", s.Edges)
	}
}

func TestSnapshotUnknownEndpointInsideSetIsStub(t *testing.T) {
	rates := map[topology.EdgeKey]topology.EdgeRate{
		{ClientNS: "", Client: "ext", ServerNS: "shop", Server: "web"}: {RequestsPerSecond: 3},
	}
	s := topology.BuildSnapshot(t0, []string{"shop"}, provider(), rates)
	n, ok := nodeByID(s, "ext")
	if !ok || !n.Stub {
		t.Fatalf("expected stub ext: %+v", s.Nodes)
	}
	if len(s.Edges) != 1 || s.Edges[0].Source != "ext" || s.Edges[0].Target != "shop/web" {
		t.Fatalf("edges: %+v", s.Edges)
	}
	// known node in set stays non-stub
	if w, _ := nodeByID(s, "shop/web"); w.Stub {
		t.Fatal("shop/web must not be stub")
	}
	// unknown service in set
	rates2 := map[topology.EdgeKey]topology.EdgeRate{
		{ClientNS: "shop", Client: "web", ServerNS: "shop", Server: "ghost"}: {RequestsPerSecond: 1},
	}
	s2 := topology.BuildSnapshot(t0, []string{"shop"}, provider(), rates2)
	if g, ok := nodeByID(s2, "shop/ghost"); !ok || !g.Stub {
		t.Fatalf("expected stub ghost: %+v", s2.Nodes)
	}
}

func TestSnapshotDeterministic(t *testing.T) {
	rates := map[topology.EdgeKey]topology.EdgeRate{
		{ClientNS: "shop", Client: "web", ServerNS: "shop", Server: "api"}: {RequestsPerSecond: 1},
		{ClientNS: "shop", Client: "api", ServerNS: "data", Server: "db"}:  {RequestsPerSecond: 2},
		{ClientNS: "shop", Client: "web", ServerNS: "data", Server: "db"}:  {RequestsPerSecond: 3},
	}
	a := topology.BuildSnapshot(t0, []string{"shop", "data"}, provider(), rates)
	for i := 0; i < 20; i++ {
		b := topology.BuildSnapshot(t0, []string{"data", "shop"}, provider(), rates)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("non-deterministic:\n%+v\n%+v", a, b)
		}
	}
	for i := 1; i < len(a.Edges); i++ {
		p, q := a.Edges[i-1], a.Edges[i]
		if p.Source > q.Source || (p.Source == q.Source && p.Target > q.Target) {
			t.Fatalf("edges unsorted: %+v", a.Edges)
		}
	}
}

func TestSnapshotHash(t *testing.T) {
	key := topology.EdgeKey{ClientNS: "shop", Client: "web", ServerNS: "shop", Server: "api"}
	mk := func(ts time.Time, rps float64, ns ...string) topology.Snapshot {
		return topology.BuildSnapshot(ts, ns, provider(), map[topology.EdgeKey]topology.EdgeRate{key: {RequestsPerSecond: rps}})
	}
	base := mk(t0, 1.00, "shop")
	if base.Hash() != mk(t0.Add(time.Hour), 1.00, "shop").Hash() {
		t.Fatal("hash must ignore TS")
	}
	if base.Hash() != mk(t0, 1.001, "shop").Hash() {
		t.Fatal("hash must ignore sub-0.01 jitter")
	}
	if base.Hash() == mk(t0, 1.02, "shop").Hash() {
		t.Fatal("hash must flip on rate change > 0.01")
	}
	if base.Hash() == mk(t0, 1.00, "shop", "data").Hash() {
		t.Fatal("hash must flip on node change")
	}
}

// resolvingProvider adds workload->Service alias resolution to fakeProvider.
type resolvingProvider struct {
	fakeProvider
	alias map[string]string // "ns/workload" -> Service node ID
}

func (r resolvingProvider) ResolveEndpoint(ns, name string) string {
	if id, ok := r.alias[ns+"/"+name]; ok {
		return id
	}
	if ns == "" {
		return name
	}
	return ns + "/" + name
}

func TestSnapshotResolvesWorkloadNamesAndMergesCollapsedEdges(t *testing.T) {
	p := resolvingProvider{
		fakeProvider: fakeProvider{nodes: map[string][]topology.Node{
			"shop":        {node("shop", "web"), node("shop", "api")},
			"kube-system": {node("kube-system", "kube-dns")},
		}},
		alias: map[string]string{
			"shop/api-backend":    "shop/api",
			"kube-system/coredns": "kube-system/kube-dns",
		},
	}
	rates := map[topology.EdgeKey]topology.EdgeRate{
		{ClientNS: "shop", Client: "web", ServerNS: "shop", Server: "api-backend"}:            {RequestsPerSecond: 2, ErrorsPerSecond: 1},
		{ClientNS: "shop", Client: "web", ServerNS: "shop", Server: "api"}:                    {RequestsPerSecond: 3},
		{ClientNS: "shop", Client: "api-backend", ServerNS: "kube-system", Server: "coredns"}: {RequestsPerSecond: 4},
	}
	s := topology.BuildSnapshot(t0, []string{"shop"}, p, rates)
	if len(s.Edges) != 2 {
		t.Fatalf("edges: %+v", s.Edges)
	}
	e0, e1 := s.Edges[0], s.Edges[1]
	if e0.Source != "shop/api" || e0.Target != "kube-system/kube-dns" || e0.RequestsPerSecond != 4 {
		t.Fatalf("edge0: %+v", e0)
	}
	if e1.Source != "shop/web" || e1.Target != "shop/api" || e1.RequestsPerSecond != 5 || e1.ErrorsPerSecond != 1 {
		t.Fatalf("edge1 (merged): %+v", e1)
	}
	if _, ok := nodeByID(s, "shop/api-backend"); ok {
		t.Fatal("workload name must not become a stub node when it aliases a Service")
	}
	if n, ok := nodeByID(s, "kube-system/kube-dns"); !ok || !n.Stub || n.Name != "kube-dns" || n.Namespace != "kube-system" {
		t.Fatalf("expected stub kube-system/kube-dns (outside subscription), got %+v ok=%v", n, ok)
	}
}

// Parsed client-side edges carry a raw server host (ServerNS empty). Without
// a ServerResolver, in-cluster DNS names still resolve; anything else is an
// "external/<host>" stub.
func TestSnapshotServerHostWithoutResolver(t *testing.T) {
	rates := map[topology.EdgeKey]topology.EdgeRate{
		{ClientNS: "shop", Client: "web", Server: "api.shop.svc.cluster.local"}: {RequestsPerSecond: 2, ErrorsPerSecond: 1},
		{ClientNS: "shop", Client: "web", Server: "api.github.com"}:             {RequestsPerSecond: 1},
	}
	s := topology.BuildSnapshot(t0, []string{"shop"}, provider(), rates)
	if len(s.Edges) != 2 {
		t.Fatalf("edges: %+v", s.Edges)
	}
	if e := s.Edges[0]; e.Source != "shop/web" || e.Target != "external/api.github.com" {
		t.Fatalf("edge0: %+v", e)
	}
	if e := s.Edges[1]; e.Source != "shop/web" || e.Target != "shop/api" || e.ErrorsPerSecond != 1 {
		t.Fatalf("edge1: %+v", e)
	}
	n, ok := nodeByID(s, "external/api.github.com")
	if !ok || !n.Stub || n.Namespace != "external" || n.Name != "api.github.com" {
		t.Fatalf("external stub: %+v ok=%v", n, ok)
	}
	if a, _ := nodeByID(s, "shop/api"); a.Stub {
		t.Fatal("shop/api must stay a real node")
	}
}

// An edge whose client is outside the subscription is included when its
// server host resolves into a subscribed namespace.
func TestSnapshotServerHostResolvedIntoSubscribedNamespace(t *testing.T) {
	rates := map[topology.EdgeKey]topology.EdgeRate{
		{ClientNS: "other", Client: "x", Server: "db.data.svc.cluster.local"}: {RequestsPerSecond: 4},
		{ClientNS: "other", Client: "x", Server: "api.github.com"}:            {RequestsPerSecond: 4},
	}
	s := topology.BuildSnapshot(t0, []string{"data"}, provider(), rates)
	if len(s.Edges) != 1 || s.Edges[0].Source != "other/x" || s.Edges[0].Target != "data/db" {
		t.Fatalf("edges: %+v", s.Edges)
	}
	if n, ok := nodeByID(s, "other/x"); !ok || !n.Stub {
		t.Fatalf("other/x stub: %+v", s.Nodes)
	}
}

func liveRates(t *testing.T) map[topology.EdgeKey]topology.EdgeRate {
	t.Helper()
	rates := map[topology.EdgeKey]topology.EdgeRate{}
	for k, c := range parseFile(t, "testdata/live_http_client.prom") {
		rates[k] = topology.EdgeRate{RequestsPerSecond: c.Total, ErrorsPerSecond: c.Failed}
	}
	return rates
}

// The live dummy-frontend -> dummy-api.dummy.svc.cluster.local edge attaches
// to the two real Service nodes.
func TestSnapshotLiveSampleDummyEdgeOnRealServices(t *testing.T) {
	p, _ := dummyCluster(t)
	s := topology.BuildSnapshot(t0, []string{"dummy"}, p, liveRates(t))
	var found bool
	for _, e := range s.Edges {
		if e.Source == "dummy/dummy-frontend" && e.Target == "dummy/dummy-api" {
			found = true
			if e.RequestsPerSecond != 22 {
				t.Fatalf("rps: %+v", e)
			}
		}
	}
	if !found {
		t.Fatalf("missing dummy-frontend -> dummy-api edge: %+v", s.Edges)
	}
	for _, id := range []string{"dummy/dummy-frontend", "dummy/dummy-api"} {
		if n, ok := nodeByID(s, id); !ok || n.Stub {
			t.Fatalf("%s must be a real node: %+v ok=%v", id, n, ok)
		}
	}
}

// Edges to the kube-apiserver ClusterIP land on a default/kubernetes stub
// (that Service is excluded from nodes); unknown IPs become external stubs.
func TestSnapshotLiveSampleKubernetesAndExternalStubs(t *testing.T) {
	p, _ := dummyCluster(t)
	s := topology.BuildSnapshot(t0, []string{"sage"}, p, liveRates(t))
	want := map[string]float64{
		"default/kubernetes":     23,
		"dummy/dummy-api":        10,
		"sage/exporter":          10,
		"external/172.31.26.226": 10,
	}
	if len(s.Edges) != len(want) {
		t.Fatalf("edges: %+v", s.Edges)
	}
	for _, e := range s.Edges {
		if e.Source != "sage/sage" || want[e.Target] != e.RequestsPerSecond {
			t.Fatalf("unexpected edge %+v", e)
		}
	}
	if n, ok := nodeByID(s, "default/kubernetes"); !ok || !n.Stub || n.Namespace != "default" || n.Name != "kubernetes" {
		t.Fatalf("kubernetes stub: %+v ok=%v", n, ok)
	}
	if n, ok := nodeByID(s, "external/172.31.26.226"); !ok || !n.Stub || n.Namespace != "external" {
		t.Fatalf("external stub: %+v ok=%v", n, ok)
	}
}
