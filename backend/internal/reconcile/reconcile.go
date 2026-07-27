// backend/internal/reconcile/reconcile.go
package reconcile

import (
	"context"
	"log"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"

	"sre-platform/backend/internal/analyze"
	"sre-platform/backend/internal/correlate"
	"sre-platform/backend/internal/gate"
	"sre-platform/backend/internal/signal"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
	"sre-platform/backend/internal/verify"
)

// PodRestarter is satisfied by both execute.Executor (direct client-go calls,
// used until Task 14) and mcpexecute.Client (calls restart_pod over MCP,
// wired in by Task 14) — the Reconciler doesn't care which is used, only
// that it can restart a pod. This is what lets Task 14 swap the call path
// without changing this file.
type PodRestarter interface {
	RestartPod(ctx context.Context, namespace, name string) error
}

type Reconciler struct {
	mu                sync.Mutex
	pending           []signal.Signal
	store             store.Store
	restarter         PodRestarter
	slack             *slackapproval.Client
	clientset         kubernetes.Interface
	analyzer          analyze.CrashLoopAnalyzer
	mode              gate.Mode
	correlationWindow time.Duration
	verifyTimeout     time.Duration
}

func New(s store.Store, restarter PodRestarter, slack *slackapproval.Client, clientset kubernetes.Interface, mode gate.Mode, correlationWindow, verifyTimeout time.Duration) *Reconciler {
	return &Reconciler{
		store: s, restarter: restarter, slack: slack, clientset: clientset,
		mode: mode, correlationWindow: correlationWindow, verifyTimeout: verifyTimeout,
	}
}

// OnSignal is the watcher's callback: append the new signal, re-correlate
// everything pending, and drive any newly-recognized incident through
// analyze -> gate -> execute -> verify -> audit.
//
// Once handleIncident has recognized and acted on an incident, every pending
// signal for that object is dropped (forgetObjectSignals) so it can never be
// re-correlated and re-remediated by a later, unrelated signal arrival. That
// is what keeps this from being a "stateless loop" that re-restarts a pod it
// already healed every time an unrelated signal ticks in. A genuinely new
// failure for the same object produces a fresh signal, which starts a new
// incident from scratch — the correct behavior.
func (r *Reconciler) OnSignal(ctx context.Context, s signal.Signal) {
	r.mu.Lock()
	r.pending = append(r.pending, s)
	pendingCopy := append([]signal.Signal{}, r.pending...)
	r.mu.Unlock()

	for _, incident := range correlate.Correlate(pendingCopy, r.correlationWindow) {
		if r.handleIncident(ctx, incident) {
			r.forgetObjectSignals(incident.Namespace, incident.Kind, incident.Name)
		}
	}
}

// handleIncident reports whether the incident was recognized and acted on.
// It returns false only when the analyzer does not match (the incident stays
// unrecognized and its signals must remain pending so they can combine with
// future signals for the same object), or when the incident record could not
// be created at all. Once the incident record exists it returns true —
// regardless of whether the action then required approval or executed
// immediately — so the object's signals are cleared and cannot drive a second
// handleIncident call on a later tick.
func (r *Reconciler) handleIncident(ctx context.Context, incident correlate.Incident) bool {
	diagnosis, matched := r.analyzer.Analyze(incident)
	if !matched {
		return false
	}

	incidentID, err := r.store.CreateIncident(ctx, incident.Namespace, incident.Kind, incident.Name, diagnosis.FailureMode, incident.FirstSeen, incident.LastSeen)
	if err != nil {
		log.Printf("CreateIncident: %v", err)
		return false
	}

	decision := gate.Evaluate(diagnosis.RecommendedAction, r.mode)
	actionID, err := r.store.CreateRemediationAction(ctx, incidentID, diagnosis.RecommendedAction, decision.RequiresApproval, decision.Reason)
	if err != nil {
		log.Printf("CreateRemediationAction: %v", err)
		return true
	}

	if decision.RequiresApproval {
		if _, err := r.slack.PostApproval(ctx, slackapproval.ApprovalRequest{
			IncidentID: incidentID, ActionID: actionID,
			FailureMode: diagnosis.FailureMode, Action: diagnosis.RecommendedAction,
			Namespace: incident.Namespace, Name: incident.Name,
		}); err != nil {
			log.Printf("PostApproval: %v", err)
		}
		return true // execution resumes from the Slack interaction handler in a later plan
	}

	r.executeAndVerify(ctx, incident, incidentID, actionID)
	return true
}

// forgetObjectSignals removes every currently-pending signal belonging to the
// given object — matched on the same Namespace/Kind/Name key that
// correlate.Correlate groups by — from r.pending. It takes its own lock
// section (separate from the correlation-pass copy in OnSignal) and holds r.mu
// across the read-filter-write so it is safe against concurrent OnSignal
// calls.
func (r *Reconciler) forgetObjectSignals(namespace, kind, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	remaining := make([]signal.Signal, 0, len(r.pending))
	for _, s := range r.pending {
		if s.Namespace == namespace && s.Kind == kind && s.Name == name {
			continue
		}
		remaining = append(remaining, s)
	}
	r.pending = remaining
}

func (r *Reconciler) executeAndVerify(ctx context.Context, incident correlate.Incident, incidentID, actionID string) {
	if err := r.restarter.RestartPod(ctx, incident.Namespace, incident.Name); err != nil {
		log.Printf("RestartPod: %v", err)
		return
	}
	if err := r.store.MarkExecuted(ctx, actionID); err != nil {
		log.Printf("MarkExecuted: %v", err)
	}

	healthy, err := verify.CheckPodHealthy(ctx, r.clientset, incident.Namespace, incident.Signals[0].Labels, r.verifyTimeout, time.Second)
	if err != nil {
		log.Printf("CheckPodHealthy: %v", err)
		return
	}
	outcome := "resolved"
	if !healthy {
		outcome = "unresolved"
	}
	if err := r.store.MarkVerified(ctx, actionID, outcome); err != nil {
		log.Printf("MarkVerified: %v", err)
	}
	if err := r.store.WriteAudit(ctx, incidentID, "remediation_verified", map[string]any{"outcome": outcome}); err != nil {
		log.Printf("WriteAudit: %v", err)
	}
}
