// backend/tests/store/memory_test.go
package store_test

import (
	"context"
	"testing"
	"time"

	"sre-platform/backend/internal/store"
)

func TestMemoryStore_SatisfiesStoreInterface(t *testing.T) {
	var _ store.Store = store.NewMemoryStore()

	ctx := context.Background()
	s := store.NewMemoryStore()
	now := time.Now().UTC()

	incidentID, err := s.CreateIncident(ctx, "default", "Pod", "web-1", "CrashLoopBackOff", now, now)
	if err != nil || incidentID == "" {
		t.Fatalf("CreateIncident: id=%q err=%v", incidentID, err)
	}

	actionID, err := s.CreateRemediationAction(ctx, incidentID, "restart_pod", false, "auto_approved")
	if err != nil || actionID == "" {
		t.Fatalf("CreateRemediationAction: id=%q err=%v", actionID, err)
	}

	if err := s.MarkExecuted(ctx, actionID); err != nil {
		t.Fatalf("MarkExecuted: %v", err)
	}
	if err := s.MarkVerified(ctx, actionID, "resolved"); err != nil {
		t.Fatalf("MarkVerified: %v", err)
	}
	if err := s.WriteAudit(ctx, incidentID, "remediation_verified", map[string]any{"outcome": "resolved"}); err != nil {
		t.Fatalf("WriteAudit: %v", err)
	}

	got := s.Incidents[incidentID]
	if got.Status != "detected" {
		t.Errorf("expected incident status 'detected', got %q", got.Status)
	}
	if s.Actions[actionID].Status != "verified" {
		t.Errorf("expected action status 'verified', got %q", s.Actions[actionID].Status)
	}
	if len(s.AuditEntries) != 1 {
		t.Errorf("expected 1 audit entry, got %d", len(s.AuditEntries))
	}
}
