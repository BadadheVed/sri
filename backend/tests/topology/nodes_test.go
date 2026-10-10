package topology_test

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sre-platform/backend/internal/topology"
)

func ns(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func svc(namespace, name, clusterIP string, ports ...corev1.ServicePort) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       corev1.ServiceSpec{ClusterIP: clusterIP, Ports: ports},
	}
}

func startProvider(t *testing.T, objs ...runtime.Object) (*topology.K8sNodeProvider, *fake.Clientset) {
	t.Helper()
	cs := fake.NewSimpleClientset(objs...)
	p := topology.NewK8sNodeProvider(cs, 0)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return p, cs
}

func TestNamespacesSortedWithCounts(t *testing.T) {
	p, _ := startProvider(t,
		ns("zeta"), ns("alpha"), ns("empty"),
		svc("zeta", "a", "10.0.0.1"), svc("zeta", "b", "10.0.0.2"),
		svc("alpha", "c", "10.0.0.3"),
	)
	got := p.Namespaces()
	want := []topology.NamespaceInfo{{Name: "alpha", Services: 1}, {Name: "empty", Services: 0}, {Name: "zeta", Services: 2}}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestNodesInSortedAndExclusions(t *testing.T) {
	p, _ := startProvider(t,
		ns("default"), ns("shop"),
		svc("shop", "web", "10.0.0.1"), svc("shop", "api", "10.0.0.2"),
		svc("shop", "db", "None"),
		svc("default", "kubernetes", "10.0.0.9"),
		svc("default", "other", "10.0.0.10"),
	)
	nodes := p.NodesIn("shop")
	if len(nodes) != 2 || nodes[0].ID != "shop/api" || nodes[1].ID != "shop/web" {
		t.Fatalf("shop nodes: %+v", nodes)
	}
	if nodes[0].Namespace != "shop" || nodes[0].Name != "api" || nodes[0].Stub {
		t.Fatalf("bad node: %+v", nodes[0])
	}
	d := p.NodesIn("default")
	if len(d) != 1 || d[0].ID != "default/other" {
		t.Fatalf("default nodes: %+v", d)
	}
	for _, n := range p.Namespaces() {
		if n.Name == "default" && n.Services != 1 {
			t.Fatalf("default count %d, want 1", n.Services)
		}
	}
}

func TestNodePorts(t *testing.T) {
	p, _ := startProvider(t, ns("shop"), svc("shop", "web", "10.0.0.1",
		corev1.ServicePort{Port: 80, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(8080)},
		corev1.ServicePort{Port: 443, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromString("https")},
	))
	n := p.NodesIn("shop")
	if len(n) != 1 || len(n[0].Ports) != 2 {
		t.Fatalf("nodes: %+v", n)
	}
	if n[0].Ports[0] != (topology.Port{Port: 80, Protocol: "TCP", TargetPort: "8080"}) {
		t.Fatalf("port0: %+v", n[0].Ports[0])
	}
	if n[0].Ports[1] != (topology.Port{Port: 443, Protocol: "TCP", TargetPort: "https"}) {
		t.Fatalf("port1: %+v", n[0].Ports[1])
	}
}

func TestServiceCreatedAfterStartAppears(t *testing.T) {
	p, cs := startProvider(t, ns("shop"))
	if len(p.NodesIn("shop")) != 0 {
		t.Fatal("expected no nodes initially")
	}
	if _, err := cs.CoreV1().Services("shop").Create(context.Background(), svc("shop", "late", "10.0.0.5"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n := p.NodesIn("shop"); len(n) == 1 && n[0].ID == "shop/late" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("service created after Start never appeared")
}

func TestK8sNodeProvider_SyncedAndWaitSynced(t *testing.T) {
	cs := fake.NewSimpleClientset(ns("shop"))
	p := topology.NewK8sNodeProvider(cs, 0)
	if p.Synced() {
		t.Fatal("must not report synced before Start")
	}
	short, cancelShort := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShort()
	if p.WaitSynced(short) {
		t.Fatal("WaitSynced before Start must time out")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = p.Start(ctx) }()
	wctx, wcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer wcancel()
	if !p.WaitSynced(wctx) || !p.Synced() {
		t.Fatal("want synced after Start")
	}
	if got := p.Namespaces(); len(got) != 1 || got[0].Name != "shop" {
		t.Fatalf("namespaces after sync: %v", got)
	}
}

// slogCapture collects log records for assertions.
type slogCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (c *slogCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *slogCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, r.Level.String()+" "+r.Message)
	return nil
}
func (c *slogCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *slogCapture) WithGroup(string) slog.Handler      { return c }
func (c *slogCapture) has(sub string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.msgs {
		if strings.Contains(m, sub) {
			return true
		}
	}
	return false
}

func TestK8sNodeProvider_StartLogsErrorWhenSyncIsSlow(t *testing.T) {
	cs := fake.NewSimpleClientset()
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	// Namespace list hangs, so the caches never sync.
	cs.PrependReactor("list", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		<-block
		return true, &corev1.NamespaceList{}, nil
	})
	capture := &slogCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(capture))
	t.Cleanup(func() { slog.SetDefault(prev) })

	p := topology.NewK8sNodeProvider(cs, 0)
	p.SyncWarnAfter = 30 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = p.Start(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if capture.has("ERROR topology: informer caches have not synced") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no sync error logged; got %v", capture.msgs)
}
