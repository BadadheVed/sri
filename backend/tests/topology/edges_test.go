// backend/tests/topology/edges_test.go
package topology_test

import (
	"os"
	"testing"

	"sre-platform/backend/internal/topology"
)

func parseFile(t *testing.T, path string) map[topology.EdgeKey]topology.EdgeCounter {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := topology.ParseEdgeCounters(string(b))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func assertCounters(t *testing.T, got, want map[topology.EdgeKey]topology.EdgeCounter) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d edges %v, want %d %v", len(got), got, len(want), want)
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%+v: got %+v want %+v", k, got[k], w)
		}
	}
}

// Real Beyla 3.37.0 output (no # TYPE lines, so the _count series parse as
// an untyped family). Server holds the address host with any port stripped;
// ServerNS stays empty until resolution.
func TestParseEdgeCounters_LiveBeylaSample(t *testing.T) {
	got := parseFile(t, "testdata/live_http_client.prom")
	assertCounters(t, got, map[topology.EdgeKey]topology.EdgeCounter{
		{ClientNS: "dummy", Client: "dummy-frontend", Server: "dummy-api.dummy.svc.cluster.local"}: {Total: 22},
		{ClientNS: "sage", Client: "sage", Server: "10.100.0.1"}:                                   {Total: 23},
		{ClientNS: "sage", Client: "sage", Server: "172.31.1.34"}:                                  {Total: 10},
		{ClientNS: "sage", Client: "sage", Server: "172.31.19.33"}:                                 {Total: 10},
		{ClientNS: "sage", Client: "sage", Server: "172.31.26.226"}:                                {Total: 10},
	})
}

const errSample = `http_client_request_duration_seconds_count{service_name="dummy-frontend",service_namespace="dummy",server_address="dummy-api.dummy.svc.cluster.local",server_port="80",http_response_status_code="200"} 22
http_client_request_duration_seconds_count{service_name="dummy-frontend",service_namespace="dummy",server_address="dummy-api.dummy.svc.cluster.local",server_port="80",http_response_status_code="500"} 3
http_client_request_duration_seconds_count{service_name="dummy-frontend",service_namespace="dummy",server_address="dummy-api.dummy.svc.cluster.local",server_port="80",http_response_status_code="503"} 2
http_client_request_duration_seconds_count{service_name="dummy-frontend",service_namespace="dummy",server_address="dummy-api.dummy.svc.cluster.local",server_port="80",http_response_status_code="404"} 4
`

// Failed counts only 5xx responses; 4xx are client errors and not failures.
func TestParseEdgeCounters_5xxCountedAsFailed(t *testing.T) {
	got, err := topology.ParseEdgeCounters(errSample)
	if err != nil {
		t.Fatal(err)
	}
	assertCounters(t, got, map[topology.EdgeKey]topology.EdgeCounter{
		{ClientNS: "dummy", Client: "dummy-frontend", Server: "dummy-api.dummy.svc.cluster.local"}: {Total: 31, Failed: 5},
	})
}

// With a # TYPE histogram header the counts live in the histogram family's
// sample count; the namespace falls back to k8s_namespace_name; series
// missing service_name or server_address are dropped.
func TestParseEdgeCounters_HistogramFamilyAndFallbacks(t *testing.T) {
	text := `# TYPE http_client_request_duration_seconds histogram
http_client_request_duration_seconds_bucket{service_name="web",k8s_namespace_name="shop",server_address="api:8080",http_response_status_code="502",le="+Inf"} 7
http_client_request_duration_seconds_sum{service_name="web",k8s_namespace_name="shop",server_address="api:8080",http_response_status_code="502"} 1
http_client_request_duration_seconds_count{service_name="web",k8s_namespace_name="shop",server_address="api:8080",http_response_status_code="502"} 7
http_client_request_duration_seconds_bucket{service_name="",k8s_namespace_name="shop",server_address="api",le="+Inf"} 9
http_client_request_duration_seconds_sum{service_name="",k8s_namespace_name="shop",server_address="api"} 1
http_client_request_duration_seconds_count{service_name="",k8s_namespace_name="shop",server_address="api"} 9
http_client_request_duration_seconds_bucket{service_name="web",k8s_namespace_name="shop",le="+Inf"} 5
http_client_request_duration_seconds_sum{service_name="web",k8s_namespace_name="shop"} 1
http_client_request_duration_seconds_count{service_name="web",k8s_namespace_name="shop"} 5
`
	got, err := topology.ParseEdgeCounters(text)
	if err != nil {
		t.Fatal(err)
	}
	assertCounters(t, got, map[topology.EdgeKey]topology.EdgeCounter{
		{ClientNS: "shop", Client: "web", Server: "api"}: {Total: 7, Failed: 7},
	})
}

func TestParseEdgeCounters_IgnoresServiceGraphMetrics(t *testing.T) {
	text := `# TYPE traces_service_graph_request_total counter
traces_service_graph_request_total{client="a",client_namespace="x",server="b",server_namespace="x"} 3
`
	got, err := topology.ParseEdgeCounters(text)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestParseEdgeCounters_NoMetric(t *testing.T) {
	got, err := topology.ParseEdgeCounters("# TYPE up gauge\nup 1\n")
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestParseEdgeCounters_Malformed(t *testing.T) {
	if _, err := topology.ParseEdgeCounters("this is { not prometheus"); err == nil {
		t.Fatal("expected error")
	}
}
