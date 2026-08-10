package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"sre-platform/backend/internal/store"
)

func testDSN(t *testing.T) string {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set — run `docker compose up -d postgres` and set DATABASE_URL to run this test")
	}
	return dsn
}

func TestPostgresStore_IncidentLifecycle(t *testing.T) {
	ctx := context.Background()
	s, err := store.NewPostgresStore(ctx, testDSN(t))
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}

	now := time.Now().UTC()
	incidentID, err := s.CreatePendingIncident(ctx, "default", "Pod", "web-1", now, now)
	if err != nil {
		t.Fatalf("CreatePendingIncident: %v", err)
	}
	if incidentID == "" {
		t.Fatal("expected non-empty incident ID")
	}
	if err := s.RecordDiagnosis(ctx, incidentID, "CrashLoopBackOff"); err != nil {
		t.Fatalf("RecordDiagnosis: %v", err)
	}

	actionID, err := s.CreateRemediationAction(ctx, incidentID, "restart_pod", true, "manual_mode")
	if err != nil {
		t.Fatalf("CreateRemediationAction: %v", err)
	}

	if err := s.RecordApprovalDecision(ctx, actionID, "alice", "approved"); err != nil {
		t.Fatalf("RecordApprovalDecision: %v", err)
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
}

func TestPostgresStore_RecordDiagnosis_UnknownIncidentErrors(t *testing.T) {
	ctx := context.Background()
	s, err := store.NewPostgresStore(ctx, testDSN(t))
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}

	if err := s.RecordDiagnosis(ctx, "00000000-0000-0000-0000-000000000000", "CrashLoopBackOff"); err == nil {
		t.Fatal("expected an error recording a diagnosis for an unknown incident id")
	}
}
