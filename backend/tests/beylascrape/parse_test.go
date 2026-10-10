// backend/tests/beylascrape/parse_test.go
package beylascrape_test

import (
	"math"
	"testing"

	"sre-platform/backend/internal/beylascrape"
	"sre-platform/backend/internal/histogramquantile"
	"sre-platform/backend/internal/metricsagg"
)

func TestParseMetrics_SingleSeries(t *testing.T) {
	text := `
# HELP http_server_request_duration_seconds Duration of HTTP service calls from the server side
# TYPE http_server_request_duration_seconds histogram
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",http_response_status_code="200",le="0.1"} 5
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",http_response_status_code="200",le="0.5"} 8
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",http_response_status_code="200",le="+Inf"} 10
http_server_request_duration_seconds_sum{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",http_response_status_code="200"} 1.23
http_server_request_duration_seconds_count{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",http_response_status_code="200"} 10
`
	series, err := beylascrape.ParseMetrics(text)
	if err != nil {
		t.Fatalf("ParseMetrics: %v", err)
	}
	key := metricsagg.SeriesKey{Namespace: "prod", Service: "checkout", Route: "/pay", Method: "POST"}
	buckets, ok := series[key]
	if !ok {
		t.Fatalf("expected series %+v, got keys %v", key, keysOf(series))
	}
	want := []histogramquantile.Bucket{
		{UpperBound: 0.1, CumulativeCount: 5},
		{UpperBound: 0.5, CumulativeCount: 8},
		{UpperBound: math.Inf(1), CumulativeCount: 10},
	}
	assertBucketsEqual(t, buckets, want)
}

func TestParseMetrics_MergesAcrossStatusCodes(t *testing.T) {
	// Same namespace/service/route/method, two different status codes —
	// these are two distinct raw Prometheus series (http_response_status_code
	// differs) that must reduce to ONE SeriesKey with bucket-wise summed
	// counts, since our SeriesKey doesn't track status code.
	text := `
# TYPE http_server_request_duration_seconds histogram
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",http_response_status_code="200",le="0.1"} 5
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",http_response_status_code="200",le="+Inf"} 5
http_server_request_duration_seconds_count{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",http_response_status_code="200"} 5
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",http_response_status_code="500",le="0.1"} 1
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",http_response_status_code="500",le="+Inf"} 2
http_server_request_duration_seconds_count{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",http_response_status_code="500"} 2
`
	series, err := beylascrape.ParseMetrics(text)
	if err != nil {
		t.Fatalf("ParseMetrics: %v", err)
	}
	key := metricsagg.SeriesKey{Namespace: "prod", Service: "checkout", Route: "/pay", Method: "POST"}
	buckets, ok := series[key]
	if !ok {
		t.Fatalf("expected series %+v, got keys %v", key, keysOf(series))
	}
	want := []histogramquantile.Bucket{
		{UpperBound: 0.1, CumulativeCount: 6},
		{UpperBound: math.Inf(1), CumulativeCount: 7},
	}
	assertBucketsEqual(t, buckets, want)
}

func TestParseMetrics_DistinctRoutesProduceDistinctSeries(t *testing.T) {
	text := `
# TYPE http_server_request_duration_seconds histogram
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",le="0.1"} 5
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",le="+Inf"} 5
http_server_request_duration_seconds_count{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST"} 5
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/refund",http_request_method="POST",le="0.1"} 1
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/refund",http_request_method="POST",le="+Inf"} 1
http_server_request_duration_seconds_count{k8s_namespace_name="prod",service_name="checkout",http_route="/refund",http_request_method="POST"} 1
`
	series, err := beylascrape.ParseMetrics(text)
	if err != nil {
		t.Fatalf("ParseMetrics: %v", err)
	}
	if len(series) != 2 {
		t.Fatalf("expected 2 distinct series, got %d: %v", len(series), keysOf(series))
	}
}

func TestParseMetrics_IgnoresOtherMetricFamilies(t *testing.T) {
	text := `
# TYPE process_cpu_time_seconds_total counter
process_cpu_time_seconds_total{k8s_namespace_name="prod"} 12.3
# TYPE http_server_request_duration_seconds histogram
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST",le="+Inf"} 1
http_server_request_duration_seconds_count{k8s_namespace_name="prod",service_name="checkout",http_route="/pay",http_request_method="POST"} 1
`
	series, err := beylascrape.ParseMetrics(text)
	if err != nil {
		t.Fatalf("ParseMetrics: %v", err)
	}
	if len(series) != 1 {
		t.Fatalf("expected only the http_server_request_duration_seconds family to be parsed, got %d series: %v", len(series), keysOf(series))
	}
}

func TestParseMetrics_SkipsSeriesMissingRequiredLabels(t *testing.T) {
	// http_route is absent on this series (e.g. Beyla configured without
	// route matching) — can't build a meaningful SeriesKey, must be
	// skipped rather than erroring the whole scrape.
	text := `
# TYPE http_server_request_duration_seconds histogram
http_server_request_duration_seconds_bucket{k8s_namespace_name="prod",service_name="checkout",http_request_method="POST",le="+Inf"} 1
http_server_request_duration_seconds_count{k8s_namespace_name="prod",service_name="checkout",http_request_method="POST"} 1
`
	series, err := beylascrape.ParseMetrics(text)
	if err != nil {
		t.Fatalf("ParseMetrics: %v", err)
	}
	if len(series) != 0 {
		t.Errorf("expected the series missing http_route to be skipped, got %d series: %v", len(series), keysOf(series))
	}
}

func TestParseMetrics_NoHistogramFamilyReturnsEmptyMap(t *testing.T) {
	text := `
# TYPE process_cpu_time_seconds_total counter
process_cpu_time_seconds_total{k8s_namespace_name="prod"} 12.3
`
	series, err := beylascrape.ParseMetrics(text)
	if err != nil {
		t.Fatalf("ParseMetrics: %v", err)
	}
	if len(series) != 0 {
		t.Errorf("expected an empty map when the histogram family is absent, got %v", keysOf(series))
	}
}

func TestParseMetrics_RejectsMalformedText(t *testing.T) {
	if _, err := beylascrape.ParseMetrics("this is not prometheus text {{{"); err == nil {
		t.Error("expected an error for malformed input, got nil")
	}
}

func keysOf(m map[metricsagg.SeriesKey][]histogramquantile.Bucket) []metricsagg.SeriesKey {
	out := make([]metricsagg.SeriesKey, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func assertBucketsEqual(t *testing.T, got, want []histogramquantile.Bucket) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d buckets, want %d: got=%v want=%v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].UpperBound != want[i].UpperBound {
			t.Errorf("bucket[%d].UpperBound = %v, want %v", i, got[i].UpperBound, want[i].UpperBound)
		}
		if got[i].CumulativeCount != want[i].CumulativeCount {
			t.Errorf("bucket[%d].CumulativeCount = %v, want %v", i, got[i].CumulativeCount, want[i].CumulativeCount)
		}
	}
}
