package store

import (
	"context"
	"time"
)

type Incident struct {
	ID          string
	Namespace   string
	Kind        string
	Name        string
	FailureMode string
	Status      string
	FirstSeen   time.Time
	LastSeen    time.Time
}

type Store interface {
	CreateIncident(ctx context.Context, namespace, kind, name, failureMode string, firstSeen, lastSeen time.Time) (string, error)
	CreateRemediationAction(ctx context.Context, incidentID, actionType string, requiresApproval bool, reason string) (string, error)
	RecordApprovalDecision(ctx context.Context, actionID, decidedBy, decision string) error
	MarkExecuted(ctx context.Context, actionID string) error
	MarkVerified(ctx context.Context, actionID, outcome string) error
	WriteAudit(ctx context.Context, incidentID, eventType string, detail map[string]any) error
}
