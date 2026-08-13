// backend/tests/pxmetrics/queries_test.go
package pxmetrics_test

import (
	"strings"
	"testing"
	"time"

	"sre-platform/backend/internal/pxmetrics"
)

func TestBuildPodCPUScript_EmbedsNamespaceAndPodFilters(t *testing.T) {
	script, err := pxmetrics.BuildPodCPUScript("default", "web-1", 5*time.Minute, 60)
	if err != nil {
		t.Fatalf("BuildPodCPUScript: %v", err)
	}
	if !strings.Contains(script, "table='process_stats'") {
		t.Errorf("expected script to read process_stats, got:\n%s", script)
	}
	if !strings.Contains(script, `df.ctx['namespace'] == 'default'`) {
		t.Errorf("expected a namespace filter on 'default', got:\n%s", script)
	}
	if !strings.Contains(script, `df.ctx['pod'] == 'web-1'`) {
		t.Errorf("expected a pod filter on 'web-1', got:\n%s", script)
	}
	if !strings.Contains(script, "start_time='-300s'") {
		t.Errorf("expected the 5m lookback rendered as -300s, got:\n%s", script)
	}
}

func TestBuildPodTrafficScript_EmbedsNamespaceAndPodFilters(t *testing.T) {
	script, err := pxmetrics.BuildPodTrafficScript("default", "web-1", 5*time.Minute, 60)
	if err != nil {
		t.Fatalf("BuildPodTrafficScript: %v", err)
	}
	if !strings.Contains(script, "table='http_events'") {
		t.Errorf("expected script to read http_events, got:\n%s", script)
	}
	if !strings.Contains(script, `df.ctx['namespace'] == 'default'`) {
		t.Errorf("expected a namespace filter on 'default', got:\n%s", script)
	}
	if !strings.Contains(script, `df.ctx['pod'] == 'web-1'`) {
		t.Errorf("expected a pod filter on 'web-1', got:\n%s", script)
	}
}

func TestBuildPodCPUScript_RejectsInvalidNamespace(t *testing.T) {
	cases := []string{"", "Default", "default;import os", "default namespace", strings.Repeat("a", 254)}
	for _, ns := range cases {
		if _, err := pxmetrics.BuildPodCPUScript(ns, "web-1", time.Minute, 60); err == nil {
			t.Errorf("expected an error for invalid namespace %q, got none", ns)
		}
	}
}

func TestBuildPodCPUScript_RejectsInvalidName(t *testing.T) {
	cases := []string{"", "Web-1", "web_1", "web-1'; DROP TABLE"}
	for _, name := range cases {
		if _, err := pxmetrics.BuildPodCPUScript("default", name, time.Minute, 60); err == nil {
			t.Errorf("expected an error for invalid name %q, got none", name)
		}
	}
}

func TestBuildPodTrafficScript_RejectsInvalidNamespaceAndName(t *testing.T) {
	if _, err := pxmetrics.BuildPodTrafficScript("bad ns", "web-1", time.Minute, 60); err == nil {
		t.Error("expected an error for invalid namespace")
	}
	if _, err := pxmetrics.BuildPodTrafficScript("default", "bad name", time.Minute, 60); err == nil {
		t.Error("expected an error for invalid name")
	}
}
