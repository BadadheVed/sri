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

	// GroupKey identifies the object's owning controller (e.g. a Deployment's
	// ReplicaSet), not the individual pod name — a crash-looping pod gets
	// deleted and recreated under a brand new name by its ReplicaSet each
	// time, so restart-limit counting (see reconcile.maxAutoRestarts) has to
	// key off something that stays stable across those recreations. Falls
	// back to Namespace+Kind+Name for objects with no owner (bare pods).
	GroupKey string
}
