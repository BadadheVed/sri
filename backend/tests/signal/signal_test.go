package signal_test

import (
	"testing"
	"time"

	"sre-platform/backend/internal/signal"
)

func TestSignal_Fields(t *testing.T) {
	now := time.Now()
	s := signal.Signal{
		Source:    signal.SourceK8sEvent,
		Type:      "CrashLoopBackOff",
		Severity:  "warning",
		Namespace: "default",
		Kind:      "Pod",
		Name:      "web-1",
		Labels:    map[string]string{"app": "web"},
		Timestamp: now,
		Raw:       "Back-off restarting failed container",
	}

	if s.Source != signal.SourceK8sEvent {
		t.Errorf("expected source %q, got %q", signal.SourceK8sEvent, s.Source)
	}
	if s.Labels["app"] != "web" {
		t.Errorf("expected label app=web, got %v", s.Labels)
	}
}
