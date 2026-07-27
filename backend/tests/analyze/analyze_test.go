package analyze_test

import (
	"testing"

	"sre-platform/backend/internal/analyze"
	"sre-platform/backend/internal/correlate"
	"sre-platform/backend/internal/signal"
)

func TestCrashLoopAnalyzer_MatchesKnownSignature(t *testing.T) {
	inc := correlate.Incident{
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Signals: []signal.Signal{{Type: "CrashLoopBackOff"}},
	}

	var a analyze.CrashLoopAnalyzer
	diag, matched := a.Analyze(inc)

	if !matched {
		t.Fatal("expected analyzer to match a CrashLoopBackOff signal")
	}
	if diag.FailureMode != "CrashLoopBackOff" {
		t.Errorf("expected failure mode CrashLoopBackOff, got %q", diag.FailureMode)
	}
	if diag.RecommendedAction != "restart_pod" {
		t.Errorf("expected recommended action restart_pod, got %q", diag.RecommendedAction)
	}
}

func TestCrashLoopAnalyzer_NoMatchForUnrelatedSignal(t *testing.T) {
	inc := correlate.Incident{
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Signals: []signal.Signal{{Type: "FailedScheduling"}},
	}

	var a analyze.CrashLoopAnalyzer
	_, matched := a.Analyze(inc)

	if matched {
		t.Error("expected no match for an unrelated signal type")
	}
}
