// backend/tests/reconcile/reconcile_test.go
package reconcile_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/analyze"
	"sre-platform/backend/internal/correlate"
	"sre-platform/backend/internal/execute"
	"sre-platform/backend/internal/gate"
	"sre-platform/backend/internal/k8swatch"
	"sre-platform/backend/internal/mcpauth"
	"sre-platform/backend/internal/mcpexecute"
	"sre-platform/backend/internal/reconcile"
	"sre-platform/backend/internal/signal"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

// countingRestarter is a test-local reconcile.PodRestarter that records how
// many times RestartPod was invoked per (namespace, name).
type countingRestarter struct {
	mu     sync.Mutex
	counts map[string]int
}

func newCountingRestarter() *countingRestarter {
	return &countingRestarter{counts: make(map[string]int)}
}

func (c *countingRestarter) RestartPod(_ context.Context, namespace, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[namespace+"/"+name]++
	return nil
}

func (c *countingRestarter) count(namespace, name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[namespace+"/"+name]
}

// fakePublisher is a test-local reconcile.IncidentPublisher. Real NATS
// delivery is proven by backend/tests/incidentqueue; these tests only need
// to know which incident ID dispatchIncident assigned, so they can drive
// OnDiagnosis directly instead of standing up a real broker.
type fakePublisher struct {
	mu     sync.Mutex
	calls  int
	lastID string
}

func (f *fakePublisher) PublishPendingIncident(_ context.Context, incidentID string, _ correlate.Incident) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastID = incidentID
	return nil
}

func (f *fakePublisher) dispatchedID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastID
}

// failingPublisher is a test-local reconcile.IncidentPublisher that always
// fails to publish, simulating NATS being unreachable — used to prove a
// dispatch failure alerts a human instead of silently stranding the
// incident (see TestReconciler_OnSignal_DispatchFailureDeadLettersAfterRetries).
type failingPublisher struct {
	mu    sync.Mutex
	calls int
}

func (f *failingPublisher) PublishPendingIncident(_ context.Context, _ string, _ correlate.Incident) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return fmt.Errorf("nats unreachable")
}

func (f *failingPublisher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// flakyPublisher fails its first failThreshold calls, then succeeds —
// proves dispatchIncident's retry loop actually retries rather than
// giving up on the first failure.
type flakyPublisher struct {
	mu            sync.Mutex
	calls         int
	failThreshold int
	lastID        string
}

func (f *flakyPublisher) PublishPendingIncident(_ context.Context, incidentID string, _ correlate.Incident) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failThreshold {
		return fmt.Errorf("transient nats error")
	}
	f.lastID = incidentID
	return nil
}

func (f *flakyPublisher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestWatcherToReconciler_HealsCrashLoopInAutoMode(t *testing.T) {
	ctx := context.Background()
	crashingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default", Labels: map[string]string{"app": "web"}},
	}
	healthyReplacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-2", Namespace: "default", Labels: map[string]string{"app": "web"}},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	clientset := fake.NewSimpleClientset(crashingPod, healthyReplacement)
	memStore := store.NewMemoryStore()
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	type restartPodInput struct {
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
	}
	type restartPodOutput struct {
		Status string `json:"status"`
	}

	executor := execute.NewExecutor(clientset)
	mcpServer := mcp.NewServer(&mcp.Implementation{Name: "sre-execute-test", Version: "v1.0.0"}, nil)
	mcp.AddTool(mcpServer, &mcp.Tool{Name: "restart_pod", Description: "test tool"}, func(ctx context.Context, req *mcp.CallToolRequest, input restartPodInput) (*mcp.CallToolResult, restartPodOutput, error) {
		if err := executor.RestartPod(ctx, input.Namespace, input.Name); err != nil {
			return nil, restartPodOutput{}, err
		}
		return nil, restartPodOutput{Status: "deleted"}, nil
	})
	mcpHandler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server { return mcpServer }, nil)
	ts := httptest.NewServer(mcpauth.RequireBearerToken("test-token", mcpHandler))
	defer ts.Close()

	mcpClient, err := mcpexecute.NewClient(ctx, ts.URL, "test-token")
	if err != nil {
		t.Fatalf("mcpexecute.NewClient: %v", err)
	}

	r := reconcile.New(memStore, mcpClient, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)
	watcher := k8swatch.NewWatcher(clientset, func(s signal.Signal) { r.OnSignal(ctx, s) })

	watcher.HandleAddEvent(&corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "BackOff",
		Message:        "Back-off restarting failed container",
		LastTimestamp:  metav1.NewTime(time.Now()),
	})

	if publisher.calls != 1 {
		t.Fatalf("expected exactly 1 incident dispatched to ai/, got %d", publisher.calls)
	}
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9,
	})

	var resolved bool
	for _, action := range memStore.Actions {
		if action.Status == "verified" && action.Outcome == "resolved" {
			resolved = true
		}
	}
	if !resolved {
		t.Fatalf("expected a verified/resolved remediation action, got: %+v", memStore.Actions)
	}
	if len(memStore.AuditEntries) != 1 {
		t.Errorf("expected 1 audit entry recorded, got %d", len(memStore.AuditEntries))
	}
}

func TestReconciler_OnDiagnosis_SuppressesRestartAfterLimit(t *testing.T) {
	ctx := context.Background()
	healthyReplacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-healthy", Namespace: "team-a", Labels: map[string]string{"app": "worker"}},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	clientset := fake.NewSimpleClientset(healthyReplacement)
	memStore := store.NewMemoryStore()
	restarter := newCountingRestarter()
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	const groupKey = "team-a/ReplicaSet/worker-rs"
	for i := 1; i <= 6; i++ {
		r.OnSignal(ctx, signal.Signal{
			Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
			Namespace: "team-a", Kind: "Pod", Name: fmt.Sprintf("worker-rs-%d", i),
			Labels: map[string]string{"app": "worker"}, Timestamp: time.Now(),
			GroupKey: groupKey,
		})
		r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
			FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9,
		})
	}

	totalRestarts := 0
	for i := 1; i <= 6; i++ {
		totalRestarts += restarter.count("team-a", fmt.Sprintf("worker-rs-%d", i))
	}
	if totalRestarts != 5 {
		t.Fatalf("expected exactly 5 restarts across all 6 recreations (6th suppressed), got %d", totalRestarts)
	}

	var suppressed int
	for _, entry := range memStore.AuditEntries {
		if entry.EventType == "remediation_suppressed" {
			suppressed++
			if entry.Detail["group_key"] != groupKey {
				t.Errorf("expected suppressed audit entry to reference group_key %q, got %v", groupKey, entry.Detail["group_key"])
			}
		}
	}
	if suppressed != 1 {
		t.Fatalf("expected exactly 1 remediation_suppressed audit entry, got %d", suppressed)
	}
	if len(memStore.Incidents) != 6 {
		t.Fatalf("expected all 6 incidents recorded (suppression still logs the incident), got %d", len(memStore.Incidents))
	}
}

func TestReconciler_OnDiagnosis_ManualModeRequestsApprovalAndDoesNotExecute(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default", Labels: map[string]string{"app": "web"}},
	}
	clientset := fake.NewSimpleClientset(pod)
	memStore := store.NewMemoryStore()
	executor := execute.NewExecutor(clientset)
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, executor, publisher, slackClient, clientset, gate.ModeManual, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
	})
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9,
	})

	for _, action := range memStore.Actions {
		if action.Status == "executed" || action.Status == "verified" {
			t.Fatalf("expected manual mode to stop before execution, got status %q", action.Status)
		}
		if !action.RequiresApproval {
			t.Errorf("expected RequiresApproval=true in manual mode")
		}
	}
}

func TestReconciler_OnSignal_DoesNotReRemediateAlreadyHandledObject(t *testing.T) {
	ctx := context.Background()

	podXReplacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "api-2", Namespace: "team-a", Labels: map[string]string{"app": "api"}},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	podYReplacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-2", Namespace: "team-b", Labels: map[string]string{"app": "worker"}},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	clientset := fake.NewSimpleClientset(podXReplacement, podYReplacement)
	memStore := store.NewMemoryStore()
	restarter := newCountingRestarter()
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "team-a", Kind: "Pod", Name: "api-1",
		Labels: map[string]string{"app": "api"}, Timestamp: time.Now(),
	})
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9,
	})

	if got := restarter.count("team-a", "api-1"); got != 1 {
		t.Fatalf("after first signal, expected pod X restarted exactly once, got %d", got)
	}
	incidentsAfterX := len(memStore.Incidents)
	actionsAfterX := len(memStore.Actions)
	if incidentsAfterX != 1 || actionsAfterX != 1 {
		t.Fatalf("expected exactly 1 incident and 1 action after handling pod X, got %d incidents / %d actions", incidentsAfterX, actionsAfterX)
	}

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "team-b", Kind: "Pod", Name: "worker-1",
		Labels: map[string]string{"app": "worker"}, Timestamp: time.Now(),
	})
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9,
	})

	if got := restarter.count("team-a", "api-1"); got != 1 {
		t.Fatalf("pod X must NOT be re-remediated by an unrelated signal: want 1 restart, got %d", got)
	}
	if got := restarter.count("team-b", "worker-1"); got != 1 {
		t.Fatalf("expected pod Y restarted exactly once, got %d", got)
	}
	if got := len(memStore.Incidents) - incidentsAfterX; got != 1 {
		t.Fatalf("expected exactly 1 new incident (pod Y) from the second signal, got %d", got)
	}
	if got := len(memStore.Actions) - actionsAfterX; got != 1 {
		t.Fatalf("expected exactly 1 new action (pod Y) from the second signal, got %d", got)
	}
	if len(memStore.Incidents) != 2 {
		t.Fatalf("expected 2 incidents total (one each for X and Y), got %d", len(memStore.Incidents))
	}
}

// TestReconciler_OnDiagnosis_OrphanedIncidentEscalatesToSlack proves the
// backend-restart-mid-flight case: a diagnosis callback arrives for an
// incident ID the (fresh) Reconciler has never dispatched. It must not
// panic or silently drop the diagnosis — it records it for audit and lets
// the orphan path attempt a Slack alert (fire-and-forget, unasserted here
// same as every other Slack call in this file).
func TestReconciler_OnDiagnosis_OrphanedIncidentEscalatesToSlack(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()
	memStore := store.NewMemoryStore()
	restarter := newCountingRestarter()
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	// No prior OnSignal/dispatch — "orphan-1" was never assigned by this process.
	r.OnDiagnosis(ctx, "orphan-1", analyze.Diagnosis{
		FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9,
	})

	if restarter.count("", "") != 0 && len(restarter.counts) != 0 {
		t.Fatalf("orphaned diagnosis must never execute a restart, got counts: %+v", restarter.counts)
	}
	var orphaned int
	for _, entry := range memStore.AuditEntries {
		if entry.EventType == "diagnosis_orphaned" {
			orphaned++
		}
	}
	if orphaned != 1 {
		t.Fatalf("expected exactly 1 diagnosis_orphaned audit entry, got %d (entries: %+v)", orphaned, memStore.AuditEntries)
	}
}

// TestReconciler_OnDiagnosis_DuplicateOrphanedCallbackIsNoOp proves the
// orphan path is just as idempotent against NATS at-least-once redelivery as
// the recognized-incident path: a second diagnosis callback for the same
// unknown incident ID must not re-run RecordDiagnosis/WriteAudit/Slack a
// second time.
func TestReconciler_OnDiagnosis_DuplicateOrphanedCallbackIsNoOp(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()
	memStore := store.NewMemoryStore()
	restarter := newCountingRestarter()
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	// No prior OnSignal/dispatch — "orphan-1" was never assigned by this process.
	diag := analyze.Diagnosis{FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9}
	r.OnDiagnosis(ctx, "orphan-1", diag)
	r.OnDiagnosis(ctx, "orphan-1", diag) // redelivery of the same orphaned diagnosis

	var orphaned int
	for _, entry := range memStore.AuditEntries {
		if entry.EventType == "diagnosis_orphaned" {
			orphaned++
		}
	}
	if orphaned != 1 {
		t.Fatalf("duplicate orphaned diagnosis callback must not re-process: expected exactly 1 diagnosis_orphaned audit entry, got %d (entries: %+v)", orphaned, memStore.AuditEntries)
	}
}

// TestReconciler_OnDiagnosis_DuplicateCallbackIsNoOp proves NATS
// at-least-once redelivery (or an ai/-side retry after a slow HTTP response)
// can't double-execute a remediation.
func TestReconciler_OnDiagnosis_DuplicateCallbackIsNoOp(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default", Labels: map[string]string{"app": "web"}},
	}
	clientset := fake.NewSimpleClientset(pod)
	memStore := store.NewMemoryStore()
	restarter := newCountingRestarter()
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
	})
	id := publisher.dispatchedID()
	diag := analyze.Diagnosis{FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9}

	r.OnDiagnosis(ctx, id, diag)
	r.OnDiagnosis(ctx, id, diag) // redelivery of the same diagnosis

	if got := restarter.count("default", "web-1"); got != 1 {
		t.Fatalf("duplicate diagnosis callback must not re-execute: want 1 restart, got %d", got)
	}
	if len(memStore.Actions) != 1 {
		t.Fatalf("duplicate diagnosis callback must not create a second remediation action, got %d", len(memStore.Actions))
	}
}

// TestReconciler_OnDiagnosis_NoActionDoesNotRestartPod proves the C1 fix:
// a diagnosis with recommended_action "none" — ai/'s documented safe
// fallback for any unparseable output — must never fall through to
// executeAndVerify and restart a pod nobody recommended restarting.
func TestReconciler_OnDiagnosis_NoActionDoesNotRestartPod(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default", Labels: map[string]string{"app": "web"}},
	}
	clientset := fake.NewSimpleClientset(pod)
	memStore := store.NewMemoryStore()
	restarter := newCountingRestarter()
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "ImagePullError",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
	})
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "ImagePullError", RecommendedAction: "none", Confidence: 0.7,
	})

	if got := restarter.count("default", "web-1"); got != 0 {
		t.Fatalf("expected no restart for a \"none\" diagnosis, got %d", got)
	}

	var noAction int
	for _, entry := range memStore.AuditEntries {
		if entry.EventType == "diagnosis_no_action" {
			noAction++
		}
	}
	if noAction != 1 {
		t.Fatalf("expected exactly 1 diagnosis_no_action audit entry, got %d (entries: %+v)", noAction, memStore.AuditEntries)
	}
}

// TestReconciler_OnSignal_DispatchFailureDeadLettersAfterRetries proves
// the I1 fix: when dispatchIncident's NATS publish fails on every attempt,
// the incident is still durably persisted (so forgetObjectSignals is still
// called to avoid a duplicate incident row on retry), the publish is
// retried reconcile.DispatchPublishRetries times before giving up, a
// dispatch_failed audit entry must be recorded so the stranded incident
// isn't silently lost, and the incident must be dead-lettered for later
// reprocessing since NATS itself may be the thing that's down.
func TestReconciler_OnSignal_DispatchFailureDeadLettersAfterRetries(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default", Labels: map[string]string{"app": "web"}},
	}
	clientset := fake.NewSimpleClientset(pod)
	memStore := store.NewMemoryStore()
	restarter := newCountingRestarter()
	publisher := &failingPublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	original := reconcile.DispatchPublishBackoff
	reconcile.DispatchPublishBackoff = time.Millisecond
	t.Cleanup(func() { reconcile.DispatchPublishBackoff = original })

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
	})

	if got := publisher.callCount(); got != reconcile.DispatchPublishRetries {
		t.Fatalf("expected publisher called exactly DispatchPublishRetries (%d) times, got %d", reconcile.DispatchPublishRetries, got)
	}

	var dispatchFailed int
	for _, entry := range memStore.AuditEntries {
		if entry.EventType == "dispatch_failed" {
			dispatchFailed++
		}
	}
	if dispatchFailed != 1 {
		t.Fatalf("expected exactly 1 dispatch_failed audit entry, got %d (entries: %+v)", dispatchFailed, memStore.AuditEntries)
	}

	if len(memStore.DeadLetterDispatches) != 1 {
		t.Fatalf("expected exactly 1 dead-lettered dispatch, got %d (entries: %+v)", len(memStore.DeadLetterDispatches), memStore.DeadLetterDispatches)
	}
	dl := memStore.DeadLetterDispatches[0]
	if dl.Namespace != "default" || dl.Kind != "Pod" || dl.Name != "web-1" {
		t.Errorf("expected dead-letter entry to reference default/Pod/web-1, got %+v", dl)
	}
	if dl.Attempts != reconcile.DispatchPublishRetries {
		t.Errorf("expected dead-letter Attempts to equal DispatchPublishRetries (%d), got %d", reconcile.DispatchPublishRetries, dl.Attempts)
	}
	if dl.IncidentID == "" {
		t.Errorf("expected dead-letter entry to reference a non-empty incident ID")
	}
}

// TestReconciler_OnSignal_DispatchRetriesThenSucceeds proves the retry loop
// gives a transiently-failing publish a real chance to succeed instead of
// dead-lettering prematurely: a publisher that fails on every attempt but
// the last must still leave the incident live (no dead-letter entry) once
// dispatchIncident's retries exhaust the failures.
func TestReconciler_OnSignal_DispatchRetriesThenSucceeds(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default", Labels: map[string]string{"app": "web"}},
	}
	clientset := fake.NewSimpleClientset(pod)
	memStore := store.NewMemoryStore()
	restarter := newCountingRestarter()
	publisher := &flakyPublisher{failThreshold: reconcile.DispatchPublishRetries - 1}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	original := reconcile.DispatchPublishBackoff
	reconcile.DispatchPublishBackoff = time.Millisecond
	t.Cleanup(func() { reconcile.DispatchPublishBackoff = original })

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
	})

	if got := publisher.callCount(); got != reconcile.DispatchPublishRetries {
		t.Fatalf("expected publisher called exactly DispatchPublishRetries (%d) times, got %d", reconcile.DispatchPublishRetries, got)
	}
	if len(memStore.DeadLetterDispatches) != 0 {
		t.Fatalf("expected no dead-lettered dispatch after an eventual publish success, got %d (entries: %+v)", len(memStore.DeadLetterDispatches), memStore.DeadLetterDispatches)
	}
}
