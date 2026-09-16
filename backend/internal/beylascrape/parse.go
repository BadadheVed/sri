// backend/internal/beylascrape/parse.go
package beylascrape

import (
	"fmt"
	"math"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"sre-platform/backend/internal/histogramquantile"
	"sre-platform/backend/internal/metricsagg"
)

// httpServerDurationMetric is Beyla's Prometheus name for the HTTP server
// request duration histogram (OTel name http.server.request.duration,
// dots converted to underscores per Prometheus exposition rules) —
// verified against grafana/beyla's own docs, not assumed.
const httpServerDurationMetric = "http_server_request_duration_seconds"

// Beyla's label names for the fields SeriesKey needs, again dot-to-
// underscore conversions of the documented OTel attributes.
const (
	labelNamespace = "k8s_namespace_name"
	labelService   = "service_name"
	labelRoute     = "http_route"
	labelMethod    = "http_request_method"
)

// ParseMetrics parses one Beyla pod's Prometheus /metrics text output and
// reduces it to per-SeriesKey cumulative histogram buckets.
//
// A single Beyla pod can report multiple raw series for the same route
// that differ only in labels SeriesKey doesn't track (http_response_status_code,
// in particular) — those are summed bucket-wise into one combined series,
// since histograms are additive as long as bucket boundaries match. A raw
// series missing any of the four labels SeriesKey needs is skipped (not
// an error) rather than failing the whole scrape — e.g. Beyla configured
// without route matching won't emit http_route on some series.
func ParseMetrics(text string) (map[metricsagg.SeriesKey][]histogramquantile.Bucket, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(text))
	if err != nil {
		return nil, fmt.Errorf("beylascrape: parsing Prometheus text: %w", err)
	}

	family, ok := families[httpServerDurationMetric]
	if !ok || family.GetType() != dto.MetricType_HISTOGRAM {
		return map[metricsagg.SeriesKey][]histogramquantile.Bucket{}, nil
	}

	result := make(map[metricsagg.SeriesKey][]histogramquantile.Bucket)
	for _, metric := range family.GetMetric() {
		key, ok := seriesKeyFromLabels(metric.GetLabel())
		if !ok {
			continue
		}
		buckets := bucketsFromHistogram(metric.GetHistogram())

		if existing, ok := result[key]; ok {
			summed, err := sumBuckets(existing, buckets)
			if err != nil {
				return nil, fmt.Errorf("beylascrape: merging series for %+v: %w", key, err)
			}
			result[key] = summed
		} else {
			result[key] = buckets
		}
	}
	return result, nil
}

func seriesKeyFromLabels(labels []*dto.LabelPair) (metricsagg.SeriesKey, bool) {
	values := make(map[string]string, len(labels))
	for _, l := range labels {
		values[l.GetName()] = l.GetValue()
	}
	namespace, ns := values[labelNamespace]
	service, svc := values[labelService]
	route, rt := values[labelRoute]
	method, mth := values[labelMethod]
	if !ns || !svc || !rt || !mth {
		return metricsagg.SeriesKey{}, false
	}
	return metricsagg.SeriesKey{Namespace: namespace, Service: service, Route: route, Method: method}, true
}

// bucketsFromHistogram converts a Prometheus histogram's bucket list into
// histogramquantile.Buckets. Real exporters (Beyla included) conventionally
// write an explicit le="+Inf" line as the final bucket, which the proto
// already carries in Bucket — appending another one unconditionally from
// SampleCount would double-count it. A synthetic +Inf bucket is only
// appended as a fallback when the source genuinely omitted one (valid per
// the exposition format, just not how Beyla writes it in practice), so
// histogramquantile.Quantile's "must end in +Inf" contract is always met.
func bucketsFromHistogram(h *dto.Histogram) []histogramquantile.Bucket {
	src := h.GetBucket()
	buckets := make([]histogramquantile.Bucket, 0, len(src)+1)
	for _, b := range src {
		buckets = append(buckets, histogramquantile.Bucket{
			UpperBound:      b.GetUpperBound(),
			CumulativeCount: b.GetCumulativeCount(),
		})
	}
	if len(buckets) == 0 || !math.IsInf(buckets[len(buckets)-1].UpperBound, 1) {
		buckets = append(buckets, histogramquantile.Bucket{
			UpperBound:      math.Inf(1),
			CumulativeCount: h.GetSampleCount(),
		})
	}
	return buckets
}

// sumBuckets adds two bucket-count slices representing independent
// cumulative histograms into their combined cumulative histogram — valid
// as long as both share identical bucket boundaries in the same order,
// which any two raw series for the same metric family always do (Beyla
// uses one fixed bucket-boundary configuration per metric). Mismatched
// boundaries return an error rather than producing a nonsensical result.
func sumBuckets(a, b []histogramquantile.Bucket) ([]histogramquantile.Bucket, error) {
	if len(a) != len(b) {
		return nil, fmt.Errorf("bucket count mismatch: %d vs %d", len(a), len(b))
	}
	out := make([]histogramquantile.Bucket, len(a))
	for i := range a {
		if a[i].UpperBound != b[i].UpperBound {
			return nil, fmt.Errorf("bucket boundary mismatch at index %d: %v vs %v", i, a[i].UpperBound, b[i].UpperBound)
		}
		out[i] = histogramquantile.Bucket{
			UpperBound:      a[i].UpperBound,
			CumulativeCount: a[i].CumulativeCount + b[i].CumulativeCount,
		}
	}
	return out, nil
}
