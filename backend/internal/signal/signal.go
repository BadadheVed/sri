package signal

import "time"

type Source string

const (
	SourceK8sEvent Source = "k8s_event"
)

type Signal struct {
	Source    Source
	Type      string
	Severity  string
	Namespace string
	Kind      string
	Name      string
	Labels    map[string]string
	Timestamp time.Time
	Raw       string
}
