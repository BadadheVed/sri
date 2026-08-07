// backend/tests/correlate/correlate_test.go
package correlate_test

import (
	"testing"
	"time"

	"sre-platform/backend/internal/correlate"
	"sre-platform/backend/internal/signal"
)

func TestCorrelate_GroupsSameObjectWithinWindow(t *testing.T) {
	base := time.Now()
	signals := []signal.Signal{
		{Namespace: "default", Kind: "Pod", Name: "web-1", Type: "BackOff", Timestamp: base},
		{Namespace: "default", Kind: "Pod", Name: "web-1", Type: "BackOff", Timestamp: base.Add(10 * time.Second)},
		{Namespace: "default", Kind: "Pod", Name: "db-1", Type: "BackOff", Timestamp: base.Add(5 * time.Second)},
	}

	incidents := correlate.Correlate(signals, 60*time.Second)

	if len(incidents) != 2 {
		t.Fatalf("expected 2 incidents, got %d", len(incidents))
	}

	var webIncident *correlate.Incident
	for i := range incidents {
		if incidents[i].Name == "web-1" {
			webIncident = &incidents[i]
		}
	}
	if webIncident == nil {
		t.Fatal("expected an incident for web-1")
	}
	if len(webIncident.Signals) != 2 {
		t.Errorf("expected 2 signals grouped for web-1, got %d", len(webIncident.Signals))
	}
}

func TestCorrelate_SplitsSignalsOutsideWindow(t *testing.T) {
	base := time.Now()
	signals := []signal.Signal{
		{Namespace: "default", Kind: "Pod", Name: "web-1", Type: "BackOff", Timestamp: base},
		{Namespace: "default", Kind: "Pod", Name: "web-1", Type: "BackOff", Timestamp: base.Add(5 * time.Minute)},
	}

	incidents := correlate.Correlate(signals, 60*time.Second)

	if len(incidents) != 2 {
		t.Fatalf("expected 2 separate incidents outside the correlation window, got %d", len(incidents))
	}
}
