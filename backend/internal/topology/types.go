// backend/internal/topology/types.go
package topology

import "time"

// Port is a service port exposed by a node.
type Port struct {
	Port       int32  `json:"port"`
	Protocol   string `json:"protocol"`
	TargetPort string `json:"targetPort"`
}

// Node is a service in the topology graph. ID is "namespace/name".
type Node struct {
	ID        string `json:"id"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Stub      bool   `json:"stub"`
	Ports     []Port `json:"ports"`
}

// Edge is a directed client->server call relationship with its rates.
// ErrorsPerSecond is the rate of 5xx responses seen by the client (see
// EdgeRate).
type Edge struct {
	Source            string  `json:"source"`
	Target            string  `json:"target"`
	RequestsPerSecond float64 `json:"requests_per_second"`
	ErrorsPerSecond   float64 `json:"errors_per_second"`
}

// Snapshot is one full topology view.
type Snapshot struct {
	Type  string    `json:"type"`
	TS    time.Time `json:"ts"`
	Nodes []Node    `json:"nodes"`
	Edges []Edge    `json:"edges"`
}
