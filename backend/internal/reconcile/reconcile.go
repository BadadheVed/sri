// backend/internal/reconcile/reconcile.go
package reconcile

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
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

// Remediator is satisfied by both execute.Executor (direct client-go calls)
// and mcpexecute.Client (calls tools over MCP). The three Deployment-
// targeting methods return the resolved Deployment name so
// executeAndVerify can verify against it without re-resolving Pod->RS->
// Deployment itself.
type Remediator interface {
	RestartPod(ctx context.Context, namespace, name string) error
	ScaleDeployment(ctx context.Context, namespace, podName string, replicas int32) (deploymentName string, err error)
	PatchResources(ctx context.Context, namespace, podName, memoryLimit, cpuLimit string) (deploymentName string, err error)
	RollbackDeployment(ctx context.Context, namespace, podName string) (deploymentName string, err error)
}

// IncidentPublisher dispatches a newly-detected, not-yet-diagnosed incident
// to ai/ for diagnosis. Satisfied by *incidentqueue.Client in production.
type IncidentPublisher interface {
	PublishPendingIncident(ctx context.Context, incidentID string, incident correlate.Incident) error
}

// maxAutoRemediations caps how many times SAGE will auto-execute any
// remediation action for the same underlying object (see
// signal.Signal.GroupKey) before giving up and alerting a human instead. The
// count is in-memory and resets on backend restart; that's an accepted
// tradeoff for a v1 safety cap, not a persisted circuit breaker.
const maxAutoRemediations = 5

// DispatchPublishRetries and DispatchPublishBackoff bound how hard
// dispatchIncident retries a failed NATS publish before giving up and
// dead-lettering the incident. Exported vars, not consts, so tests can
// shrink the backoff instead of eating real wall-clock time.
var (
	DispatchPublishRetries = 3
	DispatchPublishBackoff = 150 * time.Millisecond
)

type Reconciler struct {
	mu              sync.Mutex
	pending         []signal.Signal
	restartAttempts map[string]int
	// awaiting holds the correlate.Incident context for every incident
	// currently dispatched to ai/ and not yet diagnosed — namespace/name/
	// GroupKey/Signals aren't retrievable from store.Store, so this is the
	// only place OnDiagnosis can find them again. In-memory only, same
	// accepted tradeoff as restartAttempts: lost on backend restart, which
	// OnDiagnosis's orphaned-callback path exists specifically to handle
	// safely rather than pretend it can't happen.
	awaiting map[string]correlate.Incident
	// diagnosed marks incident IDs that have already had a diagnosis
	// callback processed, so NATS at-least-once redelivery or an ai/-side
	// retry can't run gate->execute->verify twice. Grows for the process
	// lifetime — acceptable for v1, same tradeoff as the two maps above.
	diagnosed map[string]bool

	store             store.Store
	restarter         Remediator
	publisher         IncidentPublisher
	slack             *slackapproval.Client
	clientset         kubernetes.Interface
	mode              gate.Mode
	correlationWindow time.Duration
	verifyTimeout     time.Duration
}

func New(s store.Store, restarter Remediator, publisher IncidentPublisher, slack *slackapproval.Client, clientset kubernetes.Interface, mode gate.Mode, correlationWindow, verifyTimeout time.Duration) *Reconciler {
	return &Reconciler{
		store: s, restarter: restarter, publisher: publisher, slack: slack, clientset: clientset,
		mode: mode, correlationWindow: correlationWindow, verifyTimeout: verifyTimeout,
		restartAttempts: make(map[string]int),
		awaiting:        make(map[string]correlate.Incident),
		diagnosed:       make(map[string]bool),
	}
}

// recordRemediationAttempt increments and returns the running
// remediation-attempt count for groupKey.
func (r *Reconciler) recordRemediationAttempt(groupKey string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.restartAttempts[groupKey]++
	return r.restartAttempts[groupKey]
}

// OnSignal is the watcher's callback: append the new signal, re-correlate
// everything pending, and dispatch any newly-recognized incident to ai/ for
// diagnosis.
//
// Once dispatchIncident has accepted an incident, every pending signal for
// that object is dropped (forgetObjectSignals) so it can never be
// re-correlated and re-dispatched by a later, unrelated signal arrival. A
// genuinely new failure for the same object produces a fresh signal, which
// starts a new incident from scratch — the correct behavior.
func (r *Reconciler) OnSignal(ctx context.Context, s signal.Signal) {
	r.mu.Lock()
	r.pending = append(r.pending, s)
	pendingCopy := append([]signal.Signal{}, r.pending...)
	r.mu.Unlock()

	for _, incident := range correlate.Correlate(pendingCopy, r.correlationWindow) {
		if r.dispatchIncident(ctx, incident) {
			r.forgetObjectSignals(incident.Namespace, incident.Kind, incident.Name)
		}
	}
}

// dispatchIncident persists incident as pending_diagnosis, remembers its
// context in r.awaiting keyed by the new incident ID, and publishes it to
// ai/ over NATS. Diagnosis, gating, execution, and verification all resume
// later from OnDiagnosis, once ai/ calls back — unlike the old synchronous
// analyzer, Go never itself decides an incident is "unrecognized" anymore.
//
// Returns true if the incident was durably persisted (has an ID and is
// tracked in r.awaiting), whether or not the subsequent NATS publish
// succeeded — a failed publish still leaves the incident with a durable ID,
// so the caller must not re-dispatch it (that would create a duplicate
// incident row); it should instead be surfaced via alertStrandedIncident.
func (r *Reconciler) dispatchIncident(ctx context.Context, incident correlate.Incident) bool {
	incidentID, err := r.store.CreatePendingIncident(ctx, incident.Namespace, incident.Kind, incident.Name, incident.FirstSeen, incident.LastSeen)
	if err != nil {
		slog.Error("CreatePendingIncident failed", "namespace", incident.Namespace, "kind", incident.Kind, "name", incident.Name, "error", err)
		return false
	}
	slog.Info("incident detected, dispatching for diagnosis", "incident_id", incidentID, "namespace", incident.Namespace, "kind", incident.Kind, "name", incident.Name)

	r.mu.Lock()
	r.awaiting[incidentID] = incident
	r.mu.Unlock()

	var publishErr error
	for attempt := 1; attempt <= DispatchPublishRetries; attempt++ {
		publishErr = r.publisher.PublishPendingIncident(ctx, incidentID, incident)
		if publishErr == nil {
			return true
		}
		slog.Warn("PublishPendingIncident attempt failed", "incident_id", incidentID, "attempt", attempt, "max_attempts", DispatchPublishRetries, "error", publishErr)
		if attempt < DispatchPublishRetries {
			time.Sleep(DispatchPublishBackoff)
		}
	}

	if publishErr == nil {
		// Only reachable if DispatchPublishRetries is misconfigured to <= 0,
		// so the loop body above never ran — guard here rather than let
		// alertStrandedIncident's publishErr.Error() call panic on nil.
		publishErr = fmt.Errorf("no publish attempts made (DispatchPublishRetries=%d)", DispatchPublishRetries)
	}
	slog.Error("PublishPendingIncident exhausted retries, dead-lettering incident", "incident_id", incidentID, "attempts", DispatchPublishRetries, "error", publishErr)
	r.alertStrandedIncident(ctx, incidentID, incident, publishErr)
	if err := r.store.CreateDeadLetterDispatch(ctx, incidentID, incident.Namespace, incident.Kind, incident.Name, publishErr.Error(), DispatchPublishRetries); err != nil {
		slog.Error("CreateDeadLetterDispatch failed", "incident_id", incidentID, "error", err)
	}
	return true
}

// alertStrandedIncident posts a Slack alert and audit entry when an
// incident was durably persisted (has an ID, is in r.awaiting) but never
// actually reached ai/ because the NATS publish failed. Without this,
// nothing else retries a failed publish — the incident would otherwise
// sit in pending_diagnosis forever with only a log line as the only trace.
func (r *Reconciler) alertStrandedIncident(ctx context.Context, incidentID string, incident correlate.Incident, publishErr error) {
	if err := r.store.WriteAudit(ctx, incidentID, "dispatch_failed", map[string]any{
		"reason": "PublishPendingIncident failed, incident stranded in pending_diagnosis", "error": publishErr.Error(),
	}); err != nil {
		slog.Error("WriteAudit failed", "incident_id", incidentID, "error", err)
	}

	ts, err := r.slack.PostNotification(ctx, slackapproval.NotificationRequest{
		IncidentID: incidentID, ActionID: "",
		FailureMode: "unknown", Action: "none — failed to dispatch for diagnosis, manual review required",
		Namespace: incident.Namespace, Name: incident.Name, Outcome: "dispatch_failed",
	})
	if err != nil {
		slog.Error("PostNotification failed", "incident_id", incidentID, "error", err)
	} else {
		slog.Info("dispatch-failure alert posted to Slack", "incident_id", incidentID, "slack_ts", ts)
	}
}

// OnDiagnosis resumes the pipeline for incidentID once ai/ has diagnosed it:
// restart-limit check, gate, execute, verify — the same logic the old
// synchronous analyzer used to trigger inline, now triggered by an HTTP
// callback instead (see httpserver's diagnosis route).
func (r *Reconciler) OnDiagnosis(ctx context.Context, incidentID string, diag analyze.Diagnosis) {
	r.mu.Lock()
	if r.diagnosed[incidentID] {
		r.mu.Unlock()
		slog.Info("duplicate diagnosis callback ignored", "incident_id", incidentID)
		return
	}
	incident, ok := r.awaiting[incidentID]
	if ok {
		delete(r.awaiting, incidentID)
	}
	// Marked unconditionally (not just on the ok branch) so a redelivered
	// orphaned diagnosis is just as much a no-op on retry as a redelivered
	// recognized one — both checked-and-marked atomically under the same
	// lock acquisition as the r.diagnosed[incidentID] read above.
	r.diagnosed[incidentID] = true
	r.mu.Unlock()

	if !ok {
		r.handleOrphanedDiagnosis(ctx, incidentID, diag)
		return
	}

	if err := r.store.RecordDiagnosis(ctx, incidentID, diag.FailureMode); err != nil {
		slog.Error("RecordDiagnosis failed", "incident_id", incidentID, "error", err)
	}
	slog.Info("diagnosis received", "incident_id", incidentID, "failure_mode", diag.FailureMode, "recommended_action", diag.RecommendedAction, "confidence", diag.Confidence)

	if diag.RecommendedAction == analyze.ActionNone {
		r.handleNoActionDiagnosis(ctx, incident, incidentID, &diag)
		return
	}

	// diag.RecommendedAction is never analyze.ActionNone here — that case
	// already returned above via handleNoActionDiagnosis — so the cap
	// applies to every auto-remediation action, not just restart_pod.
	attempts := r.recordRemediationAttempt(incident.GroupKey)
	if attempts > maxAutoRemediations {
		r.suppressRemediation(ctx, incident, incidentID, &diag, attempts)
		return
	}

	decision := gate.Evaluate(diag.RecommendedAction, r.mode)
	actionID, err := r.store.CreateRemediationAction(ctx, incidentID, diag.RecommendedAction, decision.RequiresApproval, decision.Reason)
	if err != nil {
		slog.Error("CreateRemediationAction failed", "incident_id", incidentID, "error", err)
		return
	}
	slog.Info("gate decision", "incident_id", incidentID, "action_id", actionID, "action", diag.RecommendedAction, "requires_approval", decision.RequiresApproval, "reason", decision.Reason)

	if decision.RequiresApproval {
		ts, err := r.slack.PostApproval(ctx, slackapproval.ApprovalRequest{
			IncidentID: incidentID, ActionID: actionID,
			FailureMode: diag.FailureMode, Action: FormatActionParams(diag.RecommendedAction, diag.ActionParams),
			Namespace: incident.Namespace, Name: incident.Name,
		})
		if err != nil {
			slog.Error("PostApproval failed", "incident_id", incidentID, "action_id", actionID, "error", err)
		} else {
			slog.Info("approval request posted to Slack", "incident_id", incidentID, "action_id", actionID, "slack_ts", ts)
		}
		return // execution resumes from the Slack interaction handler in a later plan
	}

	r.executeAndVerify(ctx, incident, incidentID, actionID, &diag)
}

// handleOrphanedDiagnosis handles a diagnosis callback whose incident ID
// isn't in r.awaiting — almost always because backend restarted while ai/
// was still working. The Signals needed for verify.CheckPodHealthy are
// gone, so this deliberately does not attempt gate/execute — it records the
// diagnosis for the audit trail and asks a human to look, rather than
// silently dropping it or guessing at execution without the context to
// verify it worked. Same fail-closed instinct gate.go documents for its own
// unrecognized-mode branch.
func (r *Reconciler) handleOrphanedDiagnosis(ctx context.Context, incidentID string, diag analyze.Diagnosis) {
	slog.Warn("diagnosis callback for unknown/orphaned incident, escalating to Slack", "incident_id", incidentID, "failure_mode", diag.FailureMode)

	if err := r.store.RecordDiagnosis(ctx, incidentID, diag.FailureMode); err != nil {
		slog.Error("RecordDiagnosis failed", "incident_id", incidentID, "error", err)
	}
	if err := r.store.WriteAudit(ctx, incidentID, "diagnosis_orphaned", map[string]any{
		"reason":             "incident context lost, likely a backend restart while awaiting diagnosis",
		"failure_mode":       diag.FailureMode,
		"recommended_action": diag.RecommendedAction,
	}); err != nil {
		slog.Error("WriteAudit failed", "incident_id", incidentID, "error", err)
	}

	ts, err := r.slack.PostNotification(ctx, slackapproval.NotificationRequest{
		IncidentID: incidentID, ActionID: "",
		FailureMode: diag.FailureMode,
		Action:      "none — incident context lost after a backend restart, manual review required for incident " + incidentID,
		Outcome:     "orphaned",
	})
	if err != nil {
		slog.Error("PostNotification failed", "incident_id", incidentID, "error", err)
	} else {
		slog.Info("orphaned-diagnosis alert posted to Slack", "incident_id", incidentID, "slack_ts", ts)
	}
}

// handleNoActionDiagnosis handles a diagnosis whose recommended_action is
// "none" — ai/ concluded restarting the pod would not fix the underlying
// problem (or couldn't parse a confident answer, in which case "none" is
// its documented safe fallback — see ai/ai/investigate.py). There is no
// automated remediation for "none" today (executeAndVerify only knows how
// to restart a pod), so this records the outcome and alerts a human rather
// than silently doing nothing or, worse, restarting anyway.
func (r *Reconciler) handleNoActionDiagnosis(ctx context.Context, incident correlate.Incident, incidentID string, diag *analyze.Diagnosis) {
	slog.Info("diagnosis recommended no action, skipping execution", "incident_id", incidentID, "failure_mode", diag.FailureMode)

	if err := r.store.WriteAudit(ctx, incidentID, "diagnosis_no_action", map[string]any{
		"failure_mode": diag.FailureMode, "confidence": diag.Confidence,
	}); err != nil {
		slog.Error("WriteAudit failed", "incident_id", incidentID, "error", err)
	}

	ts, err := r.slack.PostNotification(ctx, slackapproval.NotificationRequest{
		IncidentID: incidentID, ActionID: "",
		FailureMode: diag.FailureMode, Action: "none — ai/ could not find an automated fix; please check the incident report",
		Namespace: incident.Namespace, Name: incident.Name, Outcome: "no_action",
	})
	if err != nil {
		slog.Error("PostNotification failed", "incident_id", incidentID, "error", err)
	} else {
		slog.Info("no-action diagnosis alert posted to Slack", "incident_id", incidentID, "slack_ts", ts)
	}
}

// suppressRemediation records that auto-remediation has given up on
// incident's object after maxAutoRemediations attempts, and alerts a human
// via Slack — an object that still needs remediating after 5 tries has a
// permanent problem auto-remediation can't fix, and silently continuing
// would just churn the cluster forever.
func (r *Reconciler) suppressRemediation(ctx context.Context, incident correlate.Incident, incidentID string, diagnosis *analyze.Diagnosis, attempts int) {
	slog.Warn("remediation limit exceeded, suppressing further auto-remediation",
		"incident_id", incidentID, "group_key", incident.GroupKey, "namespace", incident.Namespace, "name", incident.Name,
		"attempts", attempts, "limit", maxAutoRemediations)

	if err := r.store.WriteAudit(ctx, incidentID, "remediation_suppressed", map[string]any{
		"reason": "remediation_limit_exceeded", "group_key": incident.GroupKey, "attempts": attempts, "limit": maxAutoRemediations,
	}); err != nil {
		slog.Error("WriteAudit failed", "incident_id", incidentID, "error", err)
	}

	ts, err := r.slack.PostNotification(ctx, slackapproval.NotificationRequest{
		IncidentID: incidentID, ActionID: "",
		FailureMode: diagnosis.FailureMode, Action: "none — remediation limit reached, manual intervention required",
		Namespace: incident.Namespace, Name: incident.Name, Outcome: "suppressed",
	})
	if err != nil {
		slog.Error("PostNotification failed", "incident_id", incidentID, "error", err)
	} else {
		slog.Info("remediation-limit alert posted to Slack", "incident_id", incidentID, "slack_ts", ts)
	}
}

// forgetObjectSignals removes every currently-pending signal belonging to the
// given object from r.pending, so it can't be re-correlated later.
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
	deploymentName, err := r.execute(ctx, incident, diagnosis)
	if err != nil {
		slog.Error("remediation execution failed", "incident_id", incidentID, "action_id", actionID, "action", diagnosis.RecommendedAction, "namespace", incident.Namespace, "name", incident.Name, "error", err)
		return
	}
	slog.Info("remediation executed", "incident_id", incidentID, "action_id", actionID, "action", diagnosis.RecommendedAction, "namespace", incident.Namespace, "name", incident.Name)
	if err := r.store.MarkExecuted(ctx, actionID); err != nil {
		slog.Error("MarkExecuted failed", "action_id", actionID, "error", err)
	}

	healthy, err := r.verify(ctx, incident, diagnosis, deploymentName)
	if err != nil {
		slog.Error("verification failed", "incident_id", incidentID, "action_id", actionID, "error", err)
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

	ts, err := r.slack.PostNotification(ctx, slackapproval.NotificationRequest{
		IncidentID: incidentID, ActionID: actionID,
		FailureMode: diagnosis.FailureMode, Action: FormatActionParams(diagnosis.RecommendedAction, diagnosis.ActionParams),
		Namespace: incident.Namespace, Name: incident.Name, Outcome: outcome,
	})
	if err != nil {
		slog.Error("PostNotification failed", "incident_id", incidentID, "action_id", actionID, "error", err)
	} else {
		slog.Info("auto-remediation notification posted to Slack", "incident_id", incidentID, "action_id", actionID, "slack_ts", ts)
	}
}

// execute dispatches to the right Remediator method for diagnosis's
// action, extracting parameters from diagnosis.ActionParams with safe type
// assertions. ai/ already validates these before sending them (see
// ai/ai/investigate.py), but a malformed/missing param here is treated the
// same as a failed Kubernetes call: logged and aborted, never a partial or
// guessed execution. Returns "" for restart_pod (which verifies by pod
// label, not by Deployment name).
func (r *Reconciler) execute(ctx context.Context, incident correlate.Incident, diagnosis *analyze.Diagnosis) (string, error) {
	switch diagnosis.RecommendedAction {
	case analyze.ActionRestartPod:
		return "", r.restarter.RestartPod(ctx, incident.Namespace, incident.Name)
	case analyze.ActionScaleDeployment:
		replicas, err := intParam(diagnosis.ActionParams, "replicas")
		if err != nil {
			return "", fmt.Errorf("scale_deployment: %w", err)
		}
		if replicas < 0 || replicas > 1000 {
			return "", fmt.Errorf("scale_deployment: replicas %d out of sane range [0, 1000]", replicas)
		}
		return r.restarter.ScaleDeployment(ctx, incident.Namespace, incident.Name, int32(replicas))
	case analyze.ActionPatchResources:
		memoryLimit, _ := diagnosis.ActionParams["memory_limit"].(string)
		cpuLimit, _ := diagnosis.ActionParams["cpu_limit"].(string)
		if memoryLimit == "" && cpuLimit == "" {
			return "", fmt.Errorf("patch_resources: action_params missing both memory_limit and cpu_limit")
		}
		return r.restarter.PatchResources(ctx, incident.Namespace, incident.Name, memoryLimit, cpuLimit)
	case analyze.ActionRollbackDeployment:
		return r.restarter.RollbackDeployment(ctx, incident.Namespace, incident.Name)
	default:
		return "", fmt.Errorf("unrecognized recommended_action %q", diagnosis.RecommendedAction)
	}
}

// intParam extracts an int out of a JSON-decoded action_params map.
// encoding/json always decodes a bare JSON number into float64 when the
// target is map[string]any (which is exactly how httpserver/diagnosis.go
// decodes the request body) — int/int32/int64 cases are defense-in-depth
// for any other caller that builds the map directly (e.g. tests).
func intParam(params map[string]any, key string) (int, error) {
	switch v := params[key].(type) {
	case float64:
		return int(v), nil
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("action_params[%q] missing or not a number (got %T)", key, params[key])
	}
}

// FormatActionParams renders diagnosis.ActionParams as a short, human-
// readable suffix for Slack messages — e.g. "scale_deployment
// (replicas=3)" or "patch_resources (memory_limit=512Mi)" — so a human
// approving or reading about a parameterized action can see what it
// actually does, not just its name. Returns action unchanged when there
// are no params (e.g. restart_pod, none, rollback_deployment).
func FormatActionParams(action string, params map[string]any) string {
	if len(params) == 0 {
		return action
	}
	parts := make([]string, 0, len(params))
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, params[k]))
	}
	return fmt.Sprintf("%s (%s)", action, strings.Join(parts, ", "))
}

func (r *Reconciler) verify(ctx context.Context, incident correlate.Incident, diagnosis *analyze.Diagnosis, deploymentName string) (bool, error) {
	if diagnosis.RecommendedAction == analyze.ActionRestartPod {
		return verify.CheckPodHealthy(ctx, r.clientset, incident.Namespace, incident.Signals[0].Labels, r.verifyTimeout, time.Second)
	}
	return verify.CheckDeploymentRolledOut(ctx, r.clientset, incident.Namespace, deploymentName, r.verifyTimeout, time.Second)
}
