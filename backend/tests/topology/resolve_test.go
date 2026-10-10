package topology_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/topology"
)

func selSvc(namespace, name, clusterIP string, selector map[string]string) *corev1.Service {
	s := svc(namespace, name, clusterIP)
	s.Spec.Selector = selector
	return s
}

// ownedPod builds a pod controlled by an owner of the given kind/name.
func ownedPod(namespace, name string, labels map[string]string, kind, owner string) *corev1.Pod {
	ctrl := true
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: labels}}
	if kind != "" {
		p.OwnerReferences = []metav1.OwnerReference{{Kind: kind, Name: owner, Controller: &ctrl}}
	}
	return p
}

func TestResolveEndpoint_DeploymentAliasCoreDNSToKubeDNS(t *testing.T) {
	p, _ := startProvider(t,
		ns("kube-system"),
		selSvc("kube-system", "kube-dns", "10.96.0.10", map[string]string{"k8s-app": "kube-dns"}),
		ownedPod("kube-system", "coredns-5d78c9869d-abcde",
			map[string]string{"k8s-app": "kube-dns", "pod-template-hash": "5d78c9869d"}, "ReplicaSet", "coredns-5d78c9869d"),
	)
	if got := p.ResolveEndpoint("kube-system", "coredns"); got != "kube-system/kube-dns" {
		t.Fatalf("got %q, want kube-system/kube-dns", got)
	}
}

func TestResolveEndpoint_DeploymentNameDiffersFromService(t *testing.T) {
	p, _ := startProvider(t,
		ns("shop"),
		selSvc("shop", "api-svc", "10.0.0.2", map[string]string{"app": "api"}),
		// no pod-template-hash label: falls back to stripping the last segment
		ownedPod("shop", "api-backend-7f9c6-xyz12", map[string]string{"app": "api"}, "ReplicaSet", "api-backend-7f9c6"),
	)
	if got := p.ResolveEndpoint("shop", "api-backend"); got != "shop/api-svc" {
		t.Fatalf("got %q, want shop/api-svc", got)
	}
}

func TestResolveEndpoint_ExactServiceNamePreferred(t *testing.T) {
	p, _ := startProvider(t,
		ns("shop"),
		selSvc("shop", "web", "10.0.0.1", map[string]string{"app": "other"}),
		selSvc("shop", "aaa-alias", "10.0.0.3", map[string]string{"app": "web"}),
		ownedPod("shop", "web-6b7f-x1", map[string]string{"app": "web", "pod-template-hash": "6b7f"}, "ReplicaSet", "web-6b7f"),
	)
	if got := p.ResolveEndpoint("shop", "web"); got != "shop/web" {
		t.Fatalf("got %q, want exact Service match shop/web", got)
	}
}

// Two Services selecting the same workload: the edge goes to the first
// Service by name (documented, deterministic choice).
func TestResolveEndpoint_TwoServicesSameWorkloadFirstByName(t *testing.T) {
	p, _ := startProvider(t,
		ns("shop"),
		selSvc("shop", "worker-b", "10.0.0.5", map[string]string{"app": "worker"}),
		selSvc("shop", "worker-a", "10.0.0.4", map[string]string{"app": "worker"}),
		ownedPod("shop", "jobs-0", map[string]string{"app": "worker"}, "StatefulSet", "jobs"),
	)
	if got := p.ResolveEndpoint("shop", "jobs"); got != "shop/worker-a" {
		t.Fatalf("got %q, want shop/worker-a", got)
	}
}

func TestResolveEndpoint_HeadlessServiceAliasedButNotANode(t *testing.T) {
	p, _ := startProvider(t,
		ns("data"),
		selSvc("data", "db", "None", map[string]string{"app": "pg"}),
		ownedPod("data", "postgres-0", map[string]string{"app": "pg"}, "StatefulSet", "postgres"),
		ownedPod("data", "agent-x7", map[string]string{"app": "agent"}, "DaemonSet", "agent"),
		selSvc("data", "agent-metrics", "10.0.0.9", map[string]string{"app": "agent"}),
	)
	if got := p.ResolveEndpoint("data", "postgres"); got != "data/db" {
		t.Fatalf("got %q, want data/db", got)
	}
	if got := p.ResolveEndpoint("data", "agent"); got != "data/agent-metrics" {
		t.Fatalf("DaemonSet: got %q, want data/agent-metrics", got)
	}
	for _, n := range p.NodesIn("data") {
		if n.Name == "db" {
			t.Fatal("headless Service must not be a node")
		}
	}
}

func TestResolveEndpoint_UnknownFallsBack(t *testing.T) {
	p, _ := startProvider(t, ns("shop"), selSvc("shop", "web", "10.0.0.1", map[string]string{"app": "web"}))
	if got := p.ResolveEndpoint("shop", "ghost"); got != "shop/ghost" {
		t.Fatalf("got %q", got)
	}
	if got := p.ResolveEndpoint("", "ext"); got != "ext" {
		t.Fatalf("got %q", got)
	}
}

func TestResolveEndpoint_PodAddedLaterIsPickedUp(t *testing.T) {
	p, cs := startProvider(t, ns("shop"), selSvc("shop", "api-svc", "10.0.0.2", map[string]string{"app": "api"}))
	if got := p.ResolveEndpoint("shop", "api-backend"); got != "shop/api-backend" {
		t.Fatalf("before pod: got %q", got)
	}
	pod := ownedPod("shop", "api-backend-abc-1", map[string]string{"app": "api", "pod-template-hash": "abc"}, "ReplicaSet", "api-backend-abc")
	if _, err := cs.CoreV1().Pods("shop").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p.ResolveEndpoint("shop", "api-backend") == "shop/api-svc" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("alias for pod created after Start never appeared")
}

// ipPod is an ownedPod with a pod IP.
func ipPod(namespace, name, ip string, labels map[string]string, kind, owner string) *corev1.Pod {
	p := ownedPod(namespace, name, labels, kind, owner)
	p.Status.PodIP = ip
	return p
}

func dummyCluster(t *testing.T) (*topology.K8sNodeProvider, *fake.Clientset) {
	t.Helper()
	return startProvider(t,
		ns("dummy"), ns("default"), ns("sage"),
		selSvc("dummy", "dummy-api", "10.0.0.7", map[string]string{"app": "dummy-api"}),
		selSvc("dummy", "dummy-frontend", "10.0.0.8", map[string]string{"app": "dummy-frontend"}),
		svc("default", "kubernetes", "10.100.0.1"),
		ipPod("dummy", "dummy-api-6d9f-abcde", "172.31.1.34",
			map[string]string{"app": "dummy-api", "pod-template-hash": "6d9f"}, "ReplicaSet", "dummy-api-6d9f"),
		ipPod("sage", "exporter-x1", "172.31.19.33", map[string]string{"app": "exporter"}, "DaemonSet", "exporter"),
	)
}

func TestResolveServer_AddressForms(t *testing.T) {
	p, _ := dummyCluster(t)
	cases := []struct{ clientNS, host, want string }{
		{"dummy", "dummy-api.dummy.svc.cluster.local", "dummy/dummy-api"},
		{"sage", "dummy-api.dummy.svc.cluster.local.", "dummy/dummy-api"},
		{"sage", "dummy-api.dummy.svc", "dummy/dummy-api"},
		{"sage", "dummy-api.dummy", "dummy/dummy-api"},
		{"dummy", "dummy-api", "dummy/dummy-api"},
		{"dummy", "Dummy-API.dummy.svc.cluster.local", "dummy/dummy-api"},
		// full in-cluster DNS form of a Service the informer does not know: still that ns/name
		{"dummy", "ghost.dummy.svc.cluster.local", "dummy/ghost"},
		// Service ClusterIP
		{"sage", "10.0.0.7", "dummy/dummy-api"},
		// kube-apiserver ClusterIP -> default/kubernetes (not a node; a stub later)
		{"sage", "10.100.0.1", "default/kubernetes"},
		// pod IP -> owning Service via selector
		{"sage", "172.31.1.34", "dummy/dummy-api"},
		// pod IP with no selecting Service -> its workload
		{"sage", "172.31.19.33", "sage/exporter"},
		// unresolved
		{"sage", "172.31.26.226", "external/172.31.26.226"},
		{"dummy", "api.github.com", "external/api.github.com"},
		{"dummy", "nope", "external/nope"},
		{"sage", "dummy-api", "external/dummy-api"}, // bare name resolves only in the client's namespace
		{"dummy", "dummy-api.nons", "external/dummy-api.nons"},
	}
	for _, c := range cases {
		if got := p.ResolveServer(c.clientNS, c.host); got != c.want {
			t.Errorf("ResolveServer(%q, %q) = %q, want %q", c.clientNS, c.host, got, c.want)
		}
	}
}

func TestResolveServer_PodIPAssignedLaterIsPickedUp(t *testing.T) {
	p, cs := dummyCluster(t)
	pod := ownedPod("dummy", "dummy-api-6d9f-zzzzz", map[string]string{"app": "dummy-api", "pod-template-hash": "6d9f"}, "ReplicaSet", "dummy-api-6d9f")
	created, err := cs.CoreV1().Pods("dummy").Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return p.ResolveServer("sage", "172.31.40.40") == "external/172.31.40.40" })
	created.Status.PodIP = "172.31.40.40"
	if _, err := cs.CoreV1().Pods("dummy").Update(context.Background(), created, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return p.ResolveServer("sage", "172.31.40.40") == "dummy/dummy-api" })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition never met")
}
