// backend/internal/topology/snapshot.go
package topology

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
	"strings"
	"time"
)

// nodeID builds "ns/name". An empty namespace yields just the name (Beyla
// reports external/unknown peers without a namespace).
func nodeID(ns, name string) string {
	if ns == "" {
		return name
	}
	return ns + "/" + name
}

// splitNodeID splits "ns/name" (an ID without "/" has an empty namespace).
func splitNodeID(id string) (ns, name string) {
	if i := strings.Index(id, "/"); i >= 0 {
		return id[:i], id[i+1:]
	}
	return "", id
}

// BuildSnapshot assembles a deterministic snapshot for the given namespaces.
// Client endpoints (and server endpoints with a ServerNS) are mapped to node
// IDs via p's EndpointResolver when it has one (Beyla reports workload
// names, nodes are Services). Server hosts of client-side edges (empty
// ServerNS) are mapped via p's ServerResolver, or defaultResolveServer
// without one; unresolved hosts become "external/<host>" stubs. An edge is
// included when its client namespace or its resolved server node's
// namespace is requested. Several Beyla edges can collapse onto one
// (source, target) pair: their rates are summed. Endpoints that are not
// known Service nodes in the set become stub nodes.
func BuildSnapshot(now time.Time, namespaces []string, p NodeProvider, rates map[EdgeKey]EdgeRate) Snapshot {
	inSet := map[string]bool{}
	nodes := map[string]Node{}
	for _, ns := range namespaces {
		inSet[ns] = true
		for _, n := range p.NodesIn(ns) {
			nodes[n.ID] = n
		}
	}
	resolve := func(ns, name string) string { return nodeID(ns, name) }
	if r, ok := p.(EndpointResolver); ok {
		resolve = r.ResolveEndpoint
	}
	resolveServer := defaultResolveServer
	if r, ok := p.(ServerResolver); ok {
		resolveServer = r.ResolveServer
	}
	serverID := func(k EdgeKey) string {
		if k.ServerNS == "" {
			return resolveServer(k.ClientNS, k.Server)
		}
		return resolve(k.ServerNS, k.Server)
	}
	ensure := func(id string) string {
		if _, ok := nodes[id]; !ok {
			ns, name := splitNodeID(id)
			nodes[id] = Node{ID: id, Namespace: ns, Name: name, Stub: true, Ports: []Port{}}
		}
		return id
	}
	type pair struct{ src, dst string }
	// Visit keys in sorted order so merged float sums are deterministic.
	keys := make([]EdgeKey, 0, len(rates))
	for k := range rates {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.ClientNS != b.ClientNS {
			return a.ClientNS < b.ClientNS
		}
		if a.Client != b.Client {
			return a.Client < b.Client
		}
		if a.ServerNS != b.ServerNS {
			return a.ServerNS < b.ServerNS
		}
		return a.Server < b.Server
	})
	merged := map[pair]EdgeRate{}
	for _, k := range keys {
		dst := serverID(k)
		if dstNS, _ := splitNodeID(dst); !inSet[k.ClientNS] && !inSet[k.ServerNS] && !inSet[dstNS] {
			continue
		}
		r := rates[k]
		pk := pair{ensure(resolve(k.ClientNS, k.Client)), ensure(dst)}
		m := merged[pk]
		m.RequestsPerSecond += r.RequestsPerSecond
		m.ErrorsPerSecond += r.ErrorsPerSecond
		merged[pk] = m
	}
	edges := make([]Edge, 0, len(merged))
	for pk, r := range merged {
		edges = append(edges, Edge{
			Source:            pk.src,
			Target:            pk.dst,
			RequestsPerSecond: r.RequestsPerSecond,
			ErrorsPerSecond:   r.ErrorsPerSecond,
		})
	}
	outNodes := make([]Node, 0, len(nodes))
	for _, n := range nodes {
		if n.Ports == nil {
			n.Ports = []Port{}
		}
		outNodes = append(outNodes, n)
	}
	sort.Slice(outNodes, func(i, j int) bool { return outNodes[i].ID < outNodes[j].ID })
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].Source != edges[j].Source {
			return edges[i].Source < edges[j].Source
		}
		return edges[i].Target < edges[j].Target
	})
	return Snapshot{Type: "snapshot", TS: now, Nodes: outNodes, Edges: edges}
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }

// Hash returns a content hash of nodes and edges, ignoring TS. Rates are
// rounded to 2 decimals so jitter does not change it.
func (s Snapshot) Hash() string {
	edges := make([]Edge, len(s.Edges))
	for i, e := range s.Edges {
		e.RequestsPerSecond = round2(e.RequestsPerSecond)
		e.ErrorsPerSecond = round2(e.ErrorsPerSecond)
		edges[i] = e
	}
	b, _ := json.Marshal(struct {
		Nodes []Node `json:"nodes"`
		Edges []Edge `json:"edges"`
	}{s.Nodes, edges})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
