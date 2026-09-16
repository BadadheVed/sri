// backend/internal/beylascrape/beylascrape.go
package beylascrape

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"k8s.io/client-go/kubernetes"

	"sre-platform/backend/internal/histogramquantile"
	"sre-platform/backend/internal/metricsagg"
)

// Config holds what's needed to discover and scrape Beyla pods.
type Config struct {
	// PodSelector is a Kubernetes label selector identifying Beyla's own
	// pods (e.g. "app.kubernetes.io/name=beyla") — depends on how Beyla
	// was deployed, so it's configuration, not a constant.
	PodSelector string
	// Namespace restricts pod discovery to one namespace; empty searches
	// every namespace.
	Namespace string
	// Port is the port Beyla's Prometheus exporter listens on
	// (BEYLA_PROMETHEUS_PORT / prometheus_export.port on Beyla's side).
	Port int
	// Timeout bounds each pod's individual scrape HTTP GET.
	Timeout time.Duration
}

// Source implements metricsagg.Source by discovering Beyla pods via the
// Kubernetes API and scraping each one's Prometheus /metrics endpoint.
type Source struct {
	clientset  kubernetes.Interface
	httpClient *http.Client
	cfg        Config
}

// NewSource builds a Source. httpClient may be nil, in which case
// http.DefaultClient is used.
func NewSource(clientset kubernetes.Interface, httpClient *http.Client, cfg Config) *Source {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Source{clientset: clientset, httpClient: httpClient, cfg: cfg}
}

// Poll implements metricsagg.Source: discover the current Beyla pods,
// scrape and parse each one's /metrics, and sum bucket counts across
// every pod that contributes to the same SeriesKey — a service's replicas
// are typically spread across nodes, so its total traffic is only visible
// by combining each node's own Beyla instance.
//
// An individual pod that fails to discover-list, scrape, or parse is
// logged and skipped rather than failing the whole poll, matching
// metricsagg.Aggregator's own log-and-continue philosophy — one
// unreachable or misbehaving Beyla pod shouldn't blank out every other
// series in this tick.
func (s *Source) Poll(ctx context.Context) (map[metricsagg.SeriesKey]histogramquantile.Sample, error) {
	pods, err := s.DiscoverPods(ctx)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	combined := make(map[metricsagg.SeriesKey][]histogramquantile.Bucket)
	for _, podIP := range pods {
		text, err := s.scrapeOne(ctx, podIP)
		if err != nil {
			slog.Warn("beylascrape: scraping pod failed, skipping", "pod_ip", podIP, "error", err)
			continue
		}
		series, err := ParseMetrics(text)
		if err != nil {
			slog.Warn("beylascrape: parsing pod metrics failed, skipping", "pod_ip", podIP, "error", err)
			continue
		}
		for key, buckets := range series {
			if existing, ok := combined[key]; ok {
				summed, err := sumBuckets(existing, buckets)
				if err != nil {
					slog.Warn("beylascrape: merging series across pods failed, dropping this pod's contribution",
						"pod_ip", podIP, "namespace", key.Namespace, "service", key.Service, "route", key.Route, "method", key.Method, "error", err)
					continue
				}
				combined[key] = summed
			} else {
				combined[key] = buckets
			}
		}
	}

	result := make(map[metricsagg.SeriesKey]histogramquantile.Sample, len(combined))
	for key, buckets := range combined {
		result[key] = histogramquantile.Sample{Buckets: buckets, Time: now}
	}
	return result, nil
}
