// backend/internal/reconcile/reconcile.go
package reconcile

import (
	"context"
	"log/slog"
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

// maxAutoRestarts caps how many times SAGE will auto-execute restart_pod for
// the same underlying object (see signal.Signal.GroupKey) before giving up
// and alerting a human instead. Without this cap, a pod whose crash is
// permanent rather than transient — a bad image, a broken config — gets
// deleted and recreated forever, indistinguishable from progress. The count
// is in-memory and resets on backend restart; that's an accepted tradeoff
// for a v1 safety cap, not a persisted circuit breaker.
const maxAutoRestarts = 5

type Reconciler struct {
	mu                sync.Mutex
	pending           []signal.Signal
	restartAttempts   map[string]int
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
		restartAttempts: make(map[string]int),
	}
}

// recordRestartAttempt increments and returns the running restart-attempt
// count for groupKey.
func (r *Reconciler) recordRestartAttempt(groupKey string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.restartAttempts[groupKey]++
	return r.restartAttempts[groupKey]
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
		slog.Debug("no analyzer match for incident", "namespace", incident.Namespace, "kind", incident.Kind, "name", incident.Name)
		return false
	}

	incidentID, err := r.store.CreateIncident(ctx, incident.Namespace, incident.Kind, incident.Name, diagnosis.FailureMode, incident.FirstSeen, incident.LastSeen)
	if err != nil {
		slog.Error("CreateIncident failed", "namespace", incident.Namespace, "kind", incident.Kind, "name", incident.Name, "error", err)
		return false
	}
	slog.Info("incident detected", "incident_id", incidentID, "namespace", incident.Namespace, "kind", incident.Kind, "name", incident.Name, "failure_mode", diagnosis.FailureMode)

	if diagnosis.RecommendedAction == "restart_pod" {
		attempts := r.recordRestartAttempt(incident.GroupKey)
		if attempts > maxAutoRestarts {
			// Checked before CreateRemediationAction on purpose — a
			// suppressed attempt was never decided on or executed, so it
			// shouldn't leave a dangling remediation_actions row behind.
			r.suppressRestart(ctx, incident, incidentID, diagnosis, attempts)
			return true
		}
	}

	decision := gate.Evaluate(diagnosis.RecommendedAction, r.mode)
	actionID, err := r.store.CreateRemediationAction(ctx, incidentID, diagnosis.RecommendedAction, decision.RequiresApproval, decision.Reason)
	if err != nil {
		slog.Error("CreateRemediationAction failed", "incident_id", incidentID, "error", err)
		return true
	}
	slog.Info("gate decision", "incident_id", incidentID, "action_id", actionID, "action", diagnosis.RecommendedAction, "requires_approval", decision.RequiresApproval, "reason", decision.Reason)

	if decision.RequiresApproval {
		ts, err := r.slack.PostApproval(ctx, slackapproval.ApprovalRequest{
			IncidentID: incidentID, ActionID: actionID,
			FailureMode: diagnosis.FailureMode, Action: diagnosis.RecommendedAction,
			Namespace: incident.Namespace, Name: incident.Name,
		})
		if err != nil {
			slog.Error("PostApproval failed", "incident_id", incidentID, "action_id", actionID, "error", err)
		} else {
			slog.Info("approval request posted to Slack", "incident_id", incidentID, "action_id", actionID, "slack_ts", ts)
		}
		return true // execution resumes from the Slack interaction handler in a later plan
	}

	r.executeAndVerify(ctx, incident, incidentID, actionID, diagnosis)
	return true
}

// suppressRestart records that auto-remediation has given up on incident's
// object after maxAutoRestarts attempts, and alerts a human via Slack — a
// pod that still needs restarting after 5 tries has a permanent problem
// restarting can't fix, and silently continuing would just churn the
// cluster forever.
func (r *Reconciler) suppressRestart(ctx context.Context, incident correlate.Incident, incidentID string, diagnosis *analyze.Diagnosis, attempts int) {
	slog.Warn("restart limit exceeded, suppressing further auto-remediation",
		"incident_id", incidentID, "group_key", incident.GroupKey, "namespace", incident.Namespace, "name", incident.Name,
		"attempts", attempts, "limit", maxAutoRestarts)

	if err := r.store.WriteAudit(ctx, incidentID, "remediation_suppressed", map[string]any{
		"reason": "restart_limit_exceeded", "group_key": incident.GroupKey, "attempts": attempts, "limit": maxAutoRestarts,
	}); err != nil {
		slog.Error("WriteAudit failed", "incident_id", incidentID, "error", err)
	}

	ts, err := r.slack.PostNotification(ctx, slackapproval.NotificationRequest{
		IncidentID: incidentID, ActionID: "",
		FailureMode: diagnosis.FailureMode, Action: "none — restart limit reached, manual intervention required",
		Namespace: incident.Namespace, Name: incident.Name, Outcome: "suppressed",
	})
	if err != nil {
		slog.Error("PostNotification failed", "incident_id", incidentID, "error", err)
	} else {
		slog.Info("restart-limit alert posted to Slack", "incident_id", incidentID, "slack_ts", ts)
	}
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

func (r *Reconciler) executeAndVerify(ctx context.Context, incident correlate.Incident, incidentID, actionID string, diagnosis *analyze.Diagnosis) {
	if err := r.restarter.RestartPod(ctx, incident.Namespace, incident.Name); err != nil {
		slog.Error("RestartPod failed", "incident_id", incidentID, "action_id", actionID, "namespace", incident.Namespace, "name", incident.Name, "error", err)
		return
	}
	slog.Info("remediation executed", "incident_id", incidentID, "action_id", actionID, "action", diagnosis.RecommendedAction, "namespace", incident.Namespace, "name", incident.Name)
	if err := r.store.MarkExecuted(ctx, actionID); err != nil {
		slog.Error("MarkExecuted failed", "action_id", actionID, "error", err)
	}

	healthy, err := verify.CheckPodHealthy(ctx, r.clientset, incident.Namespace, incident.Signals[0].Labels, r.verifyTimeout, time.Second)
	if err != nil {
		slog.Error("CheckPodHealthy failed", "incident_id", incidentID, "action_id", actionID, "error", err)
		return
	}
	outcome := "resolved"
	if !healthy {
		outcome = "unresolved"
	}
	slog.Info("remediation verified", "incident_id", incidentID, "action_id", actionID, "outcome", outcome)
	if err := r.store.MarkVerified(ctx, actionID, outcome); err != nil {
		slog.Error("MarkVerified failed", "action_id", actionID, "error", err)
	}
	if err := r.store.WriteAudit(ctx, incidentID, "remediation_verified", map[string]any{"outcome": outcome}); err != nil {
		slog.Error("WriteAudit failed", "incident_id", incidentID, "error", err)
	}

	// Auto mode never goes through PostApproval (no human is in the loop), so
	// without this the only record of an auto-remediation would be the DB —
	// this is a non-blocking FYI, not a gate, and its failure must never
	// affect the remediation outcome already recorded above.
	ts, err := r.slack.PostNotification(ctx, slackapproval.NotificationRequest{
		IncidentID: incidentID, ActionID: actionID,
		FailureMode: diagnosis.FailureMode, Action: diagnosis.RecommendedAction,
		Namespace: incident.Namespace, Name: incident.Name, Outcome: outcome,
	})
	if err != nil {
		slog.Error("PostNotification failed", "incident_id", incidentID, "action_id", actionID, "error", err)
	} else {
		slog.Info("auto-remediation notification posted to Slack", "incident_id", incidentID, "action_id", actionID, "slack_ts", ts)
	}
}
