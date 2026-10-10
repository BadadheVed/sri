// backend/tests/beylascrape/discover_test.go
package beylascrape_test

import (
	"context"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/beylascrape"
)

func pod(name, namespace, ip string, phase corev1.PodPhase, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Status:     corev1.PodStatus{Phase: phase, PodIP: ip},
	}
}

func TestDiscoverPods_ReturnsRunningPodsMatchingSelector(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		pod("beyla-1", "monitoring", "10.0.0.1", corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "beyla"}),
		pod("beyla-2", "monitoring", "10.0.0.2", corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "beyla"}),
		pod("other-app", "monitoring", "10.0.0.3", corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "not-beyla"}),
	)
	src := beylascrape.NewSource(clientset, nil, beylascrape.Config{
		PodSelector: "app.kubernetes.io/name=beyla",
		Namespace:   "monitoring",
		Port:        9090,
	})

	ips, err := src.DiscoverPods(context.Background())
	if err != nil {
		t.Fatalf("discoverPods: %v", err)
	}
	sort.Strings(ips)
	want := []string{"10.0.0.1", "10.0.0.2"}
	if len(ips) != len(want) || ips[0] != want[0] || ips[1] != want[1] {
		t.Errorf("got %v, want %v", ips, want)
	}
}

func TestDiscoverPods_ExcludesPodsWithoutAnIP(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		pod("beyla-1", "monitoring", "10.0.0.1", corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "beyla"}),
		pod("beyla-2", "monitoring", "", corev1.PodPending, map[string]string{"app.kubernetes.io/name": "beyla"}),
	)
	src := beylascrape.NewSource(clientset, nil, beylascrape.Config{
		PodSelector: "app.kubernetes.io/name=beyla",
		Namespace:   "monitoring",
		Port:        9090,
	})

	ips, err := src.DiscoverPods(context.Background())
	if err != nil {
		t.Fatalf("discoverPods: %v", err)
	}
	if len(ips) != 1 || ips[0] != "10.0.0.1" {
		t.Errorf("got %v, want [10.0.0.1] (the pending pod with no IP must be excluded)", ips)
	}
}

func TestDiscoverPods_ExcludesNonRunningPods(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		pod("beyla-1", "monitoring", "10.0.0.1", corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "beyla"}),
		// A pod can retain a stale PodIP after leaving Running (e.g. while
		// terminating) — phase must be checked too, not just IP presence.
		pod("beyla-2", "monitoring", "10.0.0.2", corev1.PodFailed, map[string]string{"app.kubernetes.io/name": "beyla"}),
	)
	src := beylascrape.NewSource(clientset, nil, beylascrape.Config{
		PodSelector: "app.kubernetes.io/name=beyla",
		Namespace:   "monitoring",
		Port:        9090,
	})

	ips, err := src.DiscoverPods(context.Background())
	if err != nil {
		t.Fatalf("discoverPods: %v", err)
	}
	if len(ips) != 1 || ips[0] != "10.0.0.1" {
		t.Errorf("got %v, want [10.0.0.1] (the failed pod must be excluded)", ips)
	}
}

func TestDiscoverPods_EmptyNamespaceSearchesAllNamespaces(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		pod("beyla-1", "monitoring", "10.0.0.1", corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "beyla"}),
		pod("beyla-2", "kube-system", "10.0.0.2", corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "beyla"}),
	)
	src := beylascrape.NewSource(clientset, nil, beylascrape.Config{
		PodSelector: "app.kubernetes.io/name=beyla",
		Namespace:   "", // all namespaces
		Port:        9090,
	})

	ips, err := src.DiscoverPods(context.Background())
	if err != nil {
		t.Fatalf("discoverPods: %v", err)
	}
	if len(ips) != 2 {
		t.Errorf("got %v, want 2 pods across both namespaces", ips)
	}
}
