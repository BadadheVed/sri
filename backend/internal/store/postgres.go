package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) CreatePendingIncident(ctx context.Context, namespace, kind, name string, firstSeen, lastSeen time.Time) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO incidents (namespace, kind, name, status, first_seen, last_seen)
		 VALUES ($1,$2,$3,'pending_diagnosis',$4,$5) RETURNING id`,
		namespace, kind, name, firstSeen, lastSeen,
	).Scan(&id)
	return id, err
}

func (s *PostgresStore) RecordDiagnosis(ctx context.Context, incidentID, failureMode string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE incidents SET failure_mode = $2, status = 'diagnosed' WHERE id = $1`,
		incidentID, failureMode,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("unknown incident id %q", incidentID)
	}
	return nil
}

func (s *PostgresStore) CreateRemediationAction(ctx context.Context, incidentID, actionType string, requiresApproval bool, reason string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO remediation_actions (incident_id, action_type, requires_approval, approval_reason)
		 VALUES ($1,$2,$3,$4) RETURNING id`,
		incidentID, actionType, requiresApproval, reason,
	).Scan(&id)
	return id, err
}

func (s *PostgresStore) RecordApprovalDecision(ctx context.Context, actionID, decidedBy, decision string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO approvals (remediation_action_id, decided_by, decision, decided_at)
		 VALUES ($1,$2,$3, now())`,
		actionID, decidedBy, decision,
	)
	return err
}

func (s *PostgresStore) MarkExecuted(ctx context.Context, actionID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE remediation_actions SET status = 'executed', executed_at = now() WHERE id = $1`,
		actionID,
	)
	return err
}

func (s *PostgresStore) MarkVerified(ctx context.Context, actionID, outcome string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE remediation_actions SET status = 'verified', verified_at = now(), outcome = $2 WHERE id = $1`,
		actionID, outcome,
	)
	return err
}

func (s *PostgresStore) WriteAudit(ctx context.Context, incidentID, eventType string, detail map[string]any) error {
	raw, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO audit_log (incident_id, event_type, detail) VALUES ($1,$2,$3)`,
		incidentID, eventType, raw,
	)
	return err
}

func (s *PostgresStore) CreateDeadLetterDispatch(ctx context.Context, incidentID, namespace, kind, name, reason string, attempts int) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO dead_letter_dispatches (incident_id, namespace, kind, name, reason, attempts) VALUES ($1,$2,$3,$4,$5,$6)`,
		incidentID, namespace, kind, name, reason, attempts,
	)
	return err
}
