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

	incidentID, err := s.CreatePendingIncident(ctx, "default", "Pod", "web-1", now, now)
	if err != nil || incidentID == "" {
		t.Fatalf("CreatePendingIncident: id=%q err=%v", incidentID, err)
	}
	if got := s.Incidents[incidentID].Status; got != "pending_diagnosis" {
		t.Fatalf("expected status 'pending_diagnosis' right after creation, got %q", got)
	}

	if err := s.RecordDiagnosis(ctx, incidentID, "CrashLoopBackOff"); err != nil {
		t.Fatalf("RecordDiagnosis: %v", err)
	}
	if got := s.Incidents[incidentID]; got.Status != "diagnosed" || got.FailureMode != "CrashLoopBackOff" {
		t.Fatalf("expected status 'diagnosed' and failure_mode 'CrashLoopBackOff' after RecordDiagnosis, got status=%q failure_mode=%q", got.Status, got.FailureMode)
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

	if s.Actions[actionID].Status != "verified" {
		t.Errorf("expected action status 'verified', got %q", s.Actions[actionID].Status)
	}
	if len(s.AuditEntries) != 1 {
		t.Errorf("expected 1 audit entry, got %d", len(s.AuditEntries))
	}
}

func TestMemoryStore_RecordDiagnosis_UnknownIncidentErrors(t *testing.T) {
	s := store.NewMemoryStore()
	if err := s.RecordDiagnosis(context.Background(), "does-not-exist", "CrashLoopBackOff"); err == nil {
		t.Fatal("expected an error recording a diagnosis for an unknown incident id")
	}
}
