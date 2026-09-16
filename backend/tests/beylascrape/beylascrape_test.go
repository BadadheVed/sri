// backend/tests/beylascrape/beylascrape_test.go
package beylascrape_test

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/beylascrape"
	"sre-platform/backend/internal/histogramquantile"
	"sre-platform/backend/internal/metricsagg"
)

const histogramTemplate = `
# TYPE http_server_request_duration_seconds histogram
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",le="0.1"} %d
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",le="+Inf"} %d
http_server_request_duration_seconds_count{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST"} %d
`

// beylaStub is a real HTTP test server standing in for one Beyla pod's
// /metrics endpoint.
func beylaStub(t *testing.T, count int) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, histogramTemplate, count, count, count)
	}))
	t.Cleanup(server.Close)
	return server
}

// serverPodIPAndPort splits an httptest.Server's URL into the bare host
// (used as a fake pod IP) and port (used as Config.Port), since
// DiscoverPods normally returns pod IPs and Poll separately appends
// Config.Port to build the scrape URL — httptest.Server's loopback
// address plays the role of "pod IP" here.
func serverPodIPAndPort(t *testing.T, server *httptest.Server) (string, int) {
	t.Helper()
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parsing test server URL: %v", err)
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("splitting host/port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parsing port: %v", err)
	}
	return host, port
}

func TestSource_Poll_SinglePod(t *testing.T) {
	server := beylaStub(t, 7)
	ip, port := serverPodIPAndPort(t, server)

	clientset := fake.NewSimpleClientset(pod("beyla-1", "monitoring", ip, corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "beyla"}))
	src := beylascrape.NewSource(clientset, http.DefaultClient, beylascrape.Config{
		PodSelector: "app.kubernetes.io/name=beyla",
		Namespace:   "monitoring",
		Port:        port,
		Timeout:     2 * time.Second,
	})

	samples, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	key := metricsagg.SeriesKey{Namespace: "prod", Service: "checkout", Route: "/pay", Method: "POST"}
	sample, ok := samples[key]
	if !ok {
		t.Fatalf("expected series %+v in %v", key, samples)
	}
	want := []histogramquantile.Bucket{{UpperBound: 0.1, CumulativeCount: 7}, {UpperBound: math.Inf(1), CumulativeCount: 7}}
	assertBucketsEqual(t, sample.Buckets, want)
}

func TestSource_Poll_SumsAcrossMultiplePods(t *testing.T) {
	// This sandbox only reliably supports binding/dialing 127.0.0.1 — a
	// second real loopback address (127.0.0.2, etc.) doesn't fail fast, it
	// hangs dialing rather than refusing, so two genuinely distinct pod
	// endpoints aren't practical here. Instead, two fake pod entries both
	// resolve to the SAME real test server; Poll() still discovers two
	// pods and genuinely calls scrapeOne + the merge path twice — that's
	// the actual behavior under test (across-pod bucket summation in
	// Poll(), distinct from the within-pod merge TestParseMetrics_
	// MergesAcrossStatusCodes already covers), independent of whether the
	// two pods happen to share a backend in this test.
	server := beylaStub(t, 3)
	ip, port := serverPodIPAndPort(t, server)

	clientset := fake.NewSimpleClientset(
		pod("beyla-1", "monitoring", ip, corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "beyla"}),
		pod("beyla-2", "monitoring", ip, corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "beyla"}),
	)
	src := beylascrape.NewSource(clientset, http.DefaultClient, beylascrape.Config{
		PodSelector: "app.kubernetes.io/name=beyla",
		Namespace:   "monitoring",
		Port:        port,
		Timeout:     2 * time.Second,
	})

	samples, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	key := metricsagg.SeriesKey{Namespace: "prod", Service: "checkout", Route: "/pay", Method: "POST"}
	sample, ok := samples[key]
	if !ok {
		t.Fatalf("expected series %+v in %v", key, samples)
	}
	// Both discovered pods return count=3; Poll() must scrape each pod
	// entry and sum their contributions, giving 6 — not 3 (only counting
	// once) and not a duplicate-key overwrite.
	want := []histogramquantile.Bucket{{UpperBound: 0.1, CumulativeCount: 6}, {UpperBound: math.Inf(1), CumulativeCount: 6}}
	assertBucketsEqual(t, sample.Buckets, want)
}

func TestSource_Poll_UnreachablePodSkippedNotFatal(t *testing.T) {
	server := beylaStub(t, 5)
	ip, port := serverPodIPAndPort(t, server)

	clientset := fake.NewSimpleClientset(
		pod("beyla-1", "monitoring", ip, corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "beyla"}),
		// 203.0.113.1 is a TEST-NET-3 address (RFC 5737) — guaranteed
		// non-routable, so this scrape reliably fails without depending on
		// an actual unreachable host existing.
		pod("beyla-2", "monitoring", "203.0.113.1", corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "beyla"}),
	)
	src := beylascrape.NewSource(clientset, &http.Client{Timeout: 300 * time.Millisecond}, beylascrape.Config{
		PodSelector: "app.kubernetes.io/name=beyla",
		Namespace:   "monitoring",
		Port:        port,
		Timeout:     300 * time.Millisecond,
	})

	samples, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v, want the unreachable pod to be skipped rather than failing the whole poll", err)
	}
	key := metricsagg.SeriesKey{Namespace: "prod", Service: "checkout", Route: "/pay", Method: "POST"}
	if _, ok := samples[key]; !ok {
		t.Errorf("expected the reachable pod's series to still be reported, got %v", samples)
	}
}

func TestSource_Poll_NoPodsReturnsEmptyMap(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	src := beylascrape.NewSource(clientset, http.DefaultClient, beylascrape.Config{
		PodSelector: "app.kubernetes.io/name=beyla",
		Namespace:   "monitoring",
		Port:        9090,
		Timeout:     time.Second,
	})

	samples, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("expected an empty map with no Beyla pods, got %v", samples)
	}
}

func TestSource_ImplementsMetricsaggSource(t *testing.T) {
	var _ metricsagg.Source = (*beylascrape.Source)(nil)
}
