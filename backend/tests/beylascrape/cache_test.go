package beylascrape_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/beylascrape"
)

// cachedSource builds a Source over `pods` Beyla pods (sharing one stub
// server) whose /metrics serves both the latency histogram and the
// client-side HTTP edge counters, counting every HTTP hit.
func cachedSource(t *testing.T, pods int, ttl time.Duration) (*beylascrape.Source, *int32) {
	t.Helper()
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		time.Sleep(20 * time.Millisecond) // widen the window for concurrent callers
		fmt.Fprintf(w, histogramTemplate, 1, 1, 1)
		fmt.Fprint(w, edgeText)
	}))
	t.Cleanup(server.Close)
	ip, port := serverPodIPAndPort(t, server)
	var objs []runtime.Object
	for i := 0; i < pods; i++ {
		objs = append(objs, pod(fmt.Sprintf("beyla-%d", i), "monitoring", ip, corev1.PodRunning, map[string]string{"app.kubernetes.io/name": "beyla"}))
	}
	src := beylascrape.NewSource(fake.NewSimpleClientset(objs...), http.DefaultClient, beylascrape.Config{
		PodSelector: "app.kubernetes.io/name=beyla", Namespace: "monitoring", Port: port, Timeout: 2 * time.Second, CacheTTL: ttl,
	})
	return src, &hits
}

func TestCache_PollAndEdgeScrapeShareOneFetchPerPodPerTick(t *testing.T) {
	src, hits := cachedSource(t, 2, time.Minute)
	ctx := context.Background()
	series, err := src.Poll(ctx)
	if err != nil || len(series) != 1 {
		t.Fatalf("Poll = %v, %v", series, err)
	}
	edges, _, err := src.ScrapeEdgeCounters(ctx)
	if err != nil || len(edges) != 2 {
		t.Fatalf("ScrapeEdgeCounters = %v, %v", edges, err)
	}
	if h := atomic.LoadInt32(hits); h != 2 {
		t.Fatalf("HTTP hits = %d, want 2 (one per pod, shared by both consumers)", h)
	}
}

func TestCache_ConcurrentCallersShareInFlightFetch(t *testing.T) {
	src, hits := cachedSource(t, 2, time.Minute)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = src.Poll(context.Background()) }()
	go func() { defer wg.Done(); _, _, _ = src.ScrapeEdgeCounters(context.Background()) }()
	wg.Wait()
	if h := atomic.LoadInt32(hits); h != 2 {
		t.Fatalf("HTTP hits = %d, want 2", h)
	}
}

func TestCache_ExpiresAfterTTL(t *testing.T) {
	src, hits := cachedSource(t, 1, 50*time.Millisecond)
	ctx := context.Background()
	_, _, _ = src.ScrapeEdgeCounters(ctx)
	time.Sleep(80 * time.Millisecond)
	_, _, _ = src.ScrapeEdgeCounters(ctx)
	if h := atomic.LoadInt32(hits); h != 2 {
		t.Fatalf("HTTP hits = %d, want 2 (cache expired between ticks)", h)
	}
}

func TestCache_DisabledByDefault(t *testing.T) {
	src, hits := cachedSource(t, 1, 0)
	ctx := context.Background()
	_, _ = src.Poll(ctx)
	_, _, _ = src.ScrapeEdgeCounters(ctx)
	if h := atomic.LoadInt32(hits); h != 2 {
		t.Fatalf("HTTP hits = %d, want 2 (no caching with zero TTL)", h)
	}
}

func TestCache_EdgeScrapeSampleTimeIsFetchTimeOnCacheHit(t *testing.T) {
	src, hits := cachedSource(t, 2, time.Minute)
	ctx := context.Background()
	before := time.Now()
	_, at1, err := src.ScrapeEdgeCounters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	afterFetch := time.Now()
	time.Sleep(60 * time.Millisecond)
	_, at2, err := src.ScrapeEdgeCounters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h := atomic.LoadInt32(hits); h != 2 {
		t.Fatalf("HTTP hits = %d, want 2 (second call must be a cache hit)", h)
	}
	if !at2.Equal(at1) {
		t.Fatalf("cache-hit sample time = %v, want the original fetch time %v", at2, at1)
	}
	if at1.Before(before) || at1.After(afterFetch) {
		t.Fatalf("fetch time %v outside [%v, %v]", at1, before, afterFetch)
	}
}
