package beylascrape_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sre-platform/backend/internal/beylascrape"
	"sre-platform/backend/internal/topology"
)

const edgeText = `# TYPE http_client_request_duration_seconds histogram
http_client_request_duration_seconds_bucket{service_name="web",service_namespace="shop",server_address="api.shop.svc.cluster.local",server_port="80",http_response_status_code="200",le="+Inf"} 8
http_client_request_duration_seconds_sum{service_name="web",service_namespace="shop",server_address="api.shop.svc.cluster.local",server_port="80",http_response_status_code="200"} 1
http_client_request_duration_seconds_count{service_name="web",service_namespace="shop",server_address="api.shop.svc.cluster.local",server_port="80",http_response_status_code="200"} 8
http_client_request_duration_seconds_bucket{service_name="web",service_namespace="shop",server_address="api.shop.svc.cluster.local",server_port="80",http_response_status_code="500",le="+Inf"} 2
http_client_request_duration_seconds_sum{service_name="web",service_namespace="shop",server_address="api.shop.svc.cluster.local",server_port="80",http_response_status_code="500"} 1
http_client_request_duration_seconds_count{service_name="web",service_namespace="shop",server_address="api.shop.svc.cluster.local",server_port="80",http_response_status_code="500"} 2
`

var webToAPI = topology.EdgeKey{ClientNS: "shop", Client: "web", Server: "api.shop.svc.cluster.local"}

func edgeSource(t *testing.T, handler http.HandlerFunc, pods int) *beylascrape.Source {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	ip, port := serverPodIPAndPort(t, server)
	var objs []runtime.Object
	for i := 0; i < pods; i++ {
		objs = append(objs, pod(fmt.Sprintf("beyla-%d", i), "monitoring", ip, corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "beyla"}))
	}
	return beylascrape.NewSource(fake.NewSimpleClientset(objs...), http.DefaultClient, beylascrape.Config{
		PodSelector: "app.kubernetes.io/name=beyla", Namespace: "monitoring", Port: port, Timeout: 2 * time.Second,
	})
}

func TestScrapeEdgeCounters_PerPodNotSummed(t *testing.T) {
	src := edgeSource(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, edgeText) }, 3)
	got, _, err := src.ScrapeEdgeCounters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d pods %v, want 3 (keyed per Beyla pod, even when pods share an IP)", len(got), got)
	}
	for _, name := range []string{"beyla-0", "beyla-1", "beyla-2"} {
		if c := got["monitoring/"+name][webToAPI]; c.Total != 10 || c.Failed != 2 {
			t.Fatalf("pod %s: got %+v, want Total=10 Failed=2 (not summed)", name, c)
		}
	}
}

func TestScrapeEdgeCounters_ManyPodsBoundedParallel(t *testing.T) {
	var inflight, peak int32
	src := edgeSource(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&inflight, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&inflight, -1)
		fmt.Fprint(w, edgeText)
	}, 20)
	got, _, err := src.ScrapeEdgeCounters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 20 {
		t.Fatalf("got %d pods, want 20", len(got))
	}
	if p := atomic.LoadInt32(&peak); p > 8 || p < 2 {
		t.Fatalf("peak concurrency = %d, want 2..8", p)
	}
}

func TestScrapeEdgeCounters_FailingPodSkipped(t *testing.T) {
	var n int32
	src := edgeSource(t, func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			http.Error(w, "nope", http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, edgeText)
	}, 2)
	got, _, err := src.ScrapeEdgeCounters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d pods %v, want 1 (failed pod absent, not an empty entry)", len(got), got)
	}
	for _, c := range got {
		if c[webToAPI].Total != 10 {
			t.Fatalf("got %+v", c)
		}
	}
}

func TestScrapeEdgeCounters_DiscoveryErrorReturned(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("forbidden")
	})
	src := beylascrape.NewSource(cs, http.DefaultClient, beylascrape.Config{PodSelector: "x=y", Timeout: time.Second})
	if _, _, err := src.ScrapeEdgeCounters(context.Background()); err == nil {
		t.Fatal("want discovery error")
	}
}
