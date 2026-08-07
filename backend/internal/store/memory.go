// backend/internal/store/memory.go
package store

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type memAction struct {
	ID               string
	IncidentID       string
	ActionType       string
	Status           string
	RequiresApproval bool
	ApprovalReason   string
	Outcome          string
}

type memAuditEntry struct {
	IncidentID string
	EventType  string
	Detail     map[string]any
}

type MemoryStore struct {
	mu           sync.Mutex
	seq          int
	Incidents    map[string]Incident
	Actions      map[string]memAction
	AuditEntries []memAuditEntry
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		Incidents: make(map[string]Incident),
		Actions:   make(map[string]memAction),
	}
}

func (s *MemoryStore) nextID(prefix string) string {
	s.seq++
	return fmt.Sprintf("%s-%d", prefix, s.seq)
}

func (s *MemoryStore) CreateIncident(ctx context.Context, namespace, kind, name, failureMode string, firstSeen, lastSeen time.Time) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID("incident")
	s.Incidents[id] = Incident{
		ID: id, Namespace: namespace, Kind: kind, Name: name,
		FailureMode: failureMode, Status: "detected",
		FirstSeen: firstSeen, LastSeen: lastSeen,
	}
	return id, nil
}

func (s *MemoryStore) CreateRemediationAction(ctx context.Context, incidentID, actionType string, requiresApproval bool, reason string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID("action")
	s.Actions[id] = memAction{
		ID: id, IncidentID: incidentID, ActionType: actionType,
		Status: "pending", RequiresApproval: requiresApproval, ApprovalReason: reason,
	}
	return id, nil
}

func (s *MemoryStore) RecordApprovalDecision(ctx context.Context, actionID, decidedBy, decision string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.Actions[actionID]
	if !ok {
		return fmt.Errorf("unknown action id %q", actionID)
	}
	if decision == "approved" {
		a.Status = "approved"
	} else {
		a.Status = "denied"
	}
	s.Actions[actionID] = a
	return nil
}

func (s *MemoryStore) MarkExecuted(ctx context.Context, actionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.Actions[actionID]
	if !ok {
		return fmt.Errorf("unknown action id %q", actionID)
	}
	a.Status = "executed"
	s.Actions[actionID] = a
	return nil
}

func (s *MemoryStore) MarkVerified(ctx context.Context, actionID, outcome string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.Actions[actionID]
	if !ok {
		return fmt.Errorf("unknown action id %q", actionID)
	}
	a.Status = "verified"
	a.Outcome = outcome
	s.Actions[actionID] = a
	return nil
}

func (s *MemoryStore) WriteAudit(ctx context.Context, incidentID, eventType string, detail map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.AuditEntries = append(s.AuditEntries, memAuditEntry{IncidentID: incidentID, EventType: eventType, Detail: detail})
	return nil
}
