// backend/internal/topology/edges.go
package topology

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// clientDurationMetric is Beyla's client-side HTTP histogram. Edges are
// derived from its sample counts: Beyla 3.37 does not emit
// traces_service_graph_* even with application_service_graph enabled.
const clientDurationMetric = "http_client_request_duration_seconds"

// EdgeKey identifies a directed client->server edge.
//
// Edges parsed by ParseEdgeCounters carry the client's namespace and Beyla
// service_name, and the raw server host (server_address with any port
// stripped: a DNS name, Service ClusterIP or pod IP) in Server with
// ServerNS empty; BuildSnapshot resolves the host to a node. A non-empty
// ServerNS means Server is already a workload/Service name in that
// namespace.
type EdgeKey struct {
	ClientNS, Client, ServerNS, Server string
}

// EdgeCounter holds cumulative request counts for an edge.
type EdgeCounter struct {
	Total, Failed float64
}

// ParseEdgeCounters derives edge counters from Beyla's
// http_client_request_duration_seconds sample counts in Prometheus text
// (either a typed histogram family or bare, untyped "_count" series).
// Client is service_name; ClientNS is service_namespace, falling back to
// k8s_namespace_name. Server is server_address with any ":port" stripped
// (server_port is ignored). Series lacking service_name or server_address
// are skipped. Series sharing (client, host) are summed into Total; Failed
// sums only those with http_response_status_code >= 500 (4xx are treated as
// caller errors, not failures). Each call is counted once, on the client
// side, so Failed <= Total.
func ParseEdgeCounters(text string) (map[EdgeKey]EdgeCounter, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(text))
	if err != nil {
		return nil, fmt.Errorf("topology: parsing Prometheus text: %w", err)
	}
	out := map[EdgeKey]EdgeCounter{}
	add := func(m *dto.Metric, count float64) {
		labels := map[string]string{}
		for _, lp := range m.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		client, addr := labels["service_name"], labels["server_address"]
		if client == "" || addr == "" {
			return
		}
		host := stripPort(addr)
		if host == "" {
			return
		}
		clientNS := labels["service_namespace"]
		if clientNS == "" {
			clientNS = labels["k8s_namespace_name"]
		}
		k := EdgeKey{ClientNS: clientNS, Client: client, Server: host}
		c := out[k]
		c.Total += count
		if code, err := strconv.Atoi(labels["http_response_status_code"]); err == nil && code >= 500 {
			c.Failed += count
		}
		out[k] = c
	}
	if fam, ok := families[clientDurationMetric]; ok && fam.GetType() == dto.MetricType_HISTOGRAM {
		for _, m := range fam.GetMetric() {
			add(m, float64(m.GetHistogram().GetSampleCount()))
		}
	}
	if fam, ok := families[clientDurationMetric+"_count"]; ok {
		for _, m := range fam.GetMetric() {
			switch fam.GetType() {
			case dto.MetricType_UNTYPED:
				add(m, m.GetUntyped().GetValue())
			case dto.MetricType_COUNTER:
				add(m, m.GetCounter().GetValue())
			}
		}
	}
	return out, nil
}

// stripPort returns addr's host: "host:port" and "[v6]:port" lose the
// port; anything else (bare host, bare IPv6) is returned unchanged.
func stripPort(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}
