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
	incidentID, err := s.CreateIncident(ctx, "default", "Pod", "web-1", "CrashLoopBackOff", now, now)
	if err != nil {
		t.Fatalf("CreateIncident: %v", err)
	}
	if incidentID == "" {
		t.Fatal("expected non-empty incident ID")
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
