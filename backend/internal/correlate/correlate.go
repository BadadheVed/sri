// backend/internal/correlate/correlate.go
package correlate

import (
	"time"

	"sre-platform/backend/internal/signal"
)

type Incident struct {
	Namespace string
	Kind      string
	Name      string
	GroupKey  string
	Signals   []signal.Signal
	FirstSeen time.Time
	LastSeen  time.Time
}

type objectKey struct {
	Namespace string
	Kind      string
	Name      string
}

// Correlate groups signals into incidents by involved object, splitting a new
// incident whenever the gap since the last signal for that object exceeds window.
func Correlate(signals []signal.Signal, window time.Duration) []Incident {
	byObject := make(map[objectKey][]signal.Signal)
	for _, s := range signals {
		key := objectKey{s.Namespace, s.Kind, s.Name}
		byObject[key] = append(byObject[key], s)
	}

	var incidents []Incident
	for key, sigs := range byObject {
		sortByTimestamp(sigs)

		var current []signal.Signal
		flush := func() {
			if len(current) == 0 {
				return
			}
			incidents = append(incidents, Incident{
				Namespace: key.Namespace,
				Kind:      key.Kind,
				Name:      key.Name,
				GroupKey:  current[0].GroupKey,
				Signals:   append([]signal.Signal{}, current...),
				FirstSeen: current[0].Timestamp,
				LastSeen:  current[len(current)-1].Timestamp,
			})
		}

		for _, s := range sigs {
			if len(current) > 0 && s.Timestamp.Sub(current[len(current)-1].Timestamp) > window {
				flush()
				current = nil
			}
			current = append(current, s)
		}
		flush()
	}
	return incidents
}

func sortByTimestamp(sigs []signal.Signal) {
	for i := 1; i < len(sigs); i++ {
		for j := i; j > 0 && sigs[j].Timestamp.Before(sigs[j-1].Timestamp); j-- {
			sigs[j], sigs[j-1] = sigs[j-1], sigs[j]
		}
	}
}
