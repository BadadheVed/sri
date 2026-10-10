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
	appsv1 "k8s.io/api/apps/v1"
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

// countingRestarter is a test-local reconcile.Remediator that records how
// many times RestartPod was invoked per (namespace, name), plus how each of
// the three Deployment-targeting methods were invoked.
type countingRestarter struct {
	mu             sync.Mutex
	counts         map[string]int   // existing restart_pod call counts, unchanged behavior
	scaled         map[string]int32 // last replicas requested, keyed by "namespace/podName"
	patched        []patchCall
	rolledBack     map[string]int
	deploymentName string // returned by every Deployment-targeting method; settable per test, empty is a valid default
	failWith       error  // when non-nil, every Deployment-targeting method returns this error instead
}

type patchCall struct{ MemoryLimit, CPULimit string }

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

func (c *countingRestarter) ScaleDeployment(_ context.Context, namespace, podName string, replicas int32) (string, error) {
	if c.failWith != nil {
		return "", c.failWith
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.scaled == nil {
		c.scaled = map[string]int32{}
	}
	c.scaled[namespace+"/"+podName] = replicas
	return c.deploymentName, nil
}

func (c *countingRestarter) PatchResources(_ context.Context, namespace, podName, memoryLimit, cpuLimit string) (string, error) {
	if c.failWith != nil {
		return "", c.failWith
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.patched = append(c.patched, patchCall{MemoryLimit: memoryLimit, CPULimit: cpuLimit})
	return c.deploymentName, nil
}

func (c *countingRestarter) RollbackDeployment(_ context.Context, namespace, podName string) (string, error) {
	if c.failWith != nil {
		return "", c.failWith
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rolledBack == nil {
		c.rolledBack = map[string]int{}
	}
	c.rolledBack[namespace+"/"+podName]++
	return c.deploymentName, nil
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
	watcher := k8swatch.NewWatcher(clientset, func(s signal.Signal) { r.OnSignal(ctx, s) }, "")

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

// TestReconciler_OnDiagnosis_ScaleDeploymentDispatchesAndVerifiesRollout
// proves the scale_deployment dispatch path end-to-end: execute() extracts
// "replicas" from ActionParams and calls Remediator.ScaleDeployment, and
// verify() checks the returned Deployment name via
// verify.CheckDeploymentRolledOut instead of CheckPodHealthy.
func TestReconciler_OnDiagnosis_ScaleDeploymentDispatchesAndVerifiesRollout(t *testing.T) {
	ctx := context.Background()
	replicas := int32(3)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1, UpdatedReplicas: replicas, ReadyReplicas: replicas,
		},
	}
	clientset := fake.NewSimpleClientset(deployment)
	memStore := store.NewMemoryStore()
	restarter := &countingRestarter{deploymentName: "web"}
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "HighMemory",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
	})
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "HighMemory", RecommendedAction: analyze.ActionScaleDeployment, Confidence: 0.9,
		ActionParams: map[string]any{"replicas": float64(3)},
	})

	if got := restarter.scaled["default/web-1"]; got != 3 {
		t.Fatalf("expected ScaleDeployment called with replicas=3 for default/web-1, got %d (scaled: %+v)", got, restarter.scaled)
	}

	var verified bool
	for _, action := range memStore.Actions {
		if action.Status == "verified" && action.Outcome == "resolved" {
			verified = true
		}
	}
	if !verified {
		t.Fatalf("expected a verified/resolved remediation action, got: %+v", memStore.Actions)
	}
}

// TestReconciler_OnDiagnosis_PatchResourcesExtractsParamsFromDiagnosis proves
// execute() pulls memory_limit/cpu_limit out of ActionParams and passes them
// through to Remediator.PatchResources unchanged.
func TestReconciler_OnDiagnosis_PatchResourcesExtractsParamsFromDiagnosis(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()
	memStore := store.NewMemoryStore()
	restarter := &countingRestarter{}
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "OOMKilled",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
	})
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "OOMKilled", RecommendedAction: analyze.ActionPatchResources, Confidence: 0.9,
		ActionParams: map[string]any{"memory_limit": "512Mi"},
	})

	if len(restarter.patched) != 1 {
		t.Fatalf("expected exactly 1 PatchResources call, got %d (%+v)", len(restarter.patched), restarter.patched)
	}
	want := patchCall{MemoryLimit: "512Mi", CPULimit: ""}
	if restarter.patched[0] != want {
		t.Fatalf("expected patch call %+v, got %+v", want, restarter.patched[0])
	}
}

// TestReconciler_OnDiagnosis_RollbackDeploymentDispatches proves execute()
// dispatches rollback_deployment to Remediator.RollbackDeployment.
func TestReconciler_OnDiagnosis_RollbackDeploymentDispatches(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()
	memStore := store.NewMemoryStore()
	restarter := &countingRestarter{}
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
	})
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "CrashLoopBackOff", RecommendedAction: analyze.ActionRollbackDeployment, Confidence: 0.9,
		ActionParams: map[string]any{},
	})

	if got := restarter.rolledBack["default/web-1"]; got != 1 {
		t.Fatalf("expected RollbackDeployment called exactly once for default/web-1, got %d (rolledBack: %+v)", got, restarter.rolledBack)
	}
}

// TestReconciler_OnDiagnosis_MalformedScaleParamsAbortsWithoutExecuting
// proves execute()'s intParam extraction fails closed: a non-numeric
// "replicas" must abort before ever calling ScaleDeployment or marking the
// action executed.
func TestReconciler_OnDiagnosis_MalformedScaleParamsAbortsWithoutExecuting(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()
	memStore := store.NewMemoryStore()
	restarter := &countingRestarter{}
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "HighMemory",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
	})
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "HighMemory", RecommendedAction: analyze.ActionScaleDeployment, Confidence: 0.9,
		ActionParams: map[string]any{"replicas": "not-a-number"},
	})

	if len(restarter.scaled) != 0 {
		t.Fatalf("expected ScaleDeployment never called for malformed replicas param, got %+v", restarter.scaled)
	}
	for _, action := range memStore.Actions {
		if action.Status == "executed" || action.Status == "verified" {
			t.Fatalf("expected action to never reach executed/verified for malformed params, got status %q", action.Status)
		}
	}
}

// TestReconciler_OnDiagnosis_MalformedPatchResourcesParamsAbortsWithoutExecuting
// proves execute()'s patch_resources guard fails closed: ActionParams with
// neither memory_limit nor cpu_limit must abort before ever calling
// PatchResources or marking the action executed.
func TestReconciler_OnDiagnosis_MalformedPatchResourcesParamsAbortsWithoutExecuting(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()
	memStore := store.NewMemoryStore()
	restarter := &countingRestarter{}
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "OOMKilled",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
	})
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "OOMKilled", RecommendedAction: analyze.ActionPatchResources, Confidence: 0.9,
		ActionParams: map[string]any{},
	})

	if len(restarter.patched) != 0 {
		t.Fatalf("expected PatchResources never called for malformed params, got %+v", restarter.patched)
	}
	for _, action := range memStore.Actions {
		if action.Status == "executed" || action.Status == "verified" {
			t.Fatalf("expected action to never reach executed/verified for malformed params, got status %q", action.Status)
		}
	}
}

// TestReconciler_OnDiagnosis_RemediationCapAppliesToScaleDeploymentNotJustRestartPod
// proves the maxAutoRemediations fix: driving 6 scale_deployment diagnoses
// for the same GroupKey must suppress the 6th exactly the way
// TestReconciler_OnDiagnosis_SuppressesRestartAfterLimit proves for
// restart_pod — before this task's fix, the cap check only ran when
// RecommendedAction == "restart_pod", so 6 scale_deployment calls would all
// have gone through with zero suppression.
func TestReconciler_OnDiagnosis_RemediationCapAppliesToScaleDeploymentNotJustRestartPod(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()
	memStore := store.NewMemoryStore()
	restarter := &countingRestarter{}
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	const groupKey = "team-a/ReplicaSet/worker-rs"
	for i := 1; i <= 6; i++ {
		r.OnSignal(ctx, signal.Signal{
			Source: signal.SourceK8sEvent, Type: "HighMemory",
			Namespace: "team-a", Kind: "Pod", Name: fmt.Sprintf("worker-rs-%d", i),
			Labels: map[string]string{"app": "worker"}, Timestamp: time.Now(),
			GroupKey: groupKey,
		})
		r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
			FailureMode: "HighMemory", RecommendedAction: analyze.ActionScaleDeployment, Confidence: 0.9,
			ActionParams: map[string]any{"replicas": float64(3)},
		})
	}

	totalScaled := 0
	for i := 1; i <= 6; i++ {
		if _, ok := restarter.scaled[fmt.Sprintf("team-a/worker-rs-%d", i)]; ok {
			totalScaled++
		}
	}
	if totalScaled != 5 {
		t.Fatalf("expected exactly 5 scale_deployment calls across all 6 recreations (6th suppressed), got %d (scaled: %+v)", totalScaled, restarter.scaled)
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

// TestReconciler_OnDiagnosis_OutOfRangeReplicasAbortsWithoutExecuting proves
// execute()'s scale_deployment case rejects a replicas value outside the
// sane [0, 1000] range before ever converting it to int32 or calling
// ScaleDeployment — guarding against a negative or absurdly large value
// (neither of which intParam itself rejects) becoming an unintended replica
// count sent to a real cluster.
func TestReconciler_OnDiagnosis_OutOfRangeReplicasAbortsWithoutExecuting(t *testing.T) {
	for _, tc := range []struct {
		name     string
		replicas float64
	}{
		{"negative", -1},
		{"excessively large", 1e10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			clientset := fake.NewSimpleClientset()
			memStore := store.NewMemoryStore()
			restarter := &countingRestarter{}
			publisher := &fakePublisher{}
			slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
			slackClient.APIBaseURL = "http://127.0.0.1:0"

			r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

			r.OnSignal(ctx, signal.Signal{
				Source: signal.SourceK8sEvent, Type: "HighMemory",
				Namespace: "default", Kind: "Pod", Name: "web-1",
				Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
			})
			r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
				FailureMode: "HighMemory", RecommendedAction: analyze.ActionScaleDeployment, Confidence: 0.9,
				ActionParams: map[string]any{"replicas": tc.replicas},
			})

			if len(restarter.scaled) != 0 {
				t.Fatalf("expected ScaleDeployment never called for out-of-range replicas %v, got %+v", tc.replicas, restarter.scaled)
			}
			for _, action := range memStore.Actions {
				if action.Status == "executed" || action.Status == "verified" {
					t.Fatalf("expected action to never reach executed/verified for out-of-range replicas %v, got status %q", tc.replicas, action.Status)
				}
			}
		})
	}
}

// TestFormatActionParams is a plain unit test for
// reconcile.FormatActionParams, independent of OnDiagnosis: it proves empty
// params leave the action string untouched, a single param is rendered as
// "action (key=value)", and multiple params always render in the same
// (sorted-by-key) order so the output — and this test — isn't flaky.
func TestFormatActionParams(t *testing.T) {
	if got, want := reconcile.FormatActionParams("restart_pod", nil), "restart_pod"; got != want {
		t.Errorf("nil params: got %q, want %q", got, want)
	}
	if got, want := reconcile.FormatActionParams("rollback_deployment", map[string]any{}), "rollback_deployment"; got != want {
		t.Errorf("empty params: got %q, want %q", got, want)
	}
	if got, want := reconcile.FormatActionParams("scale_deployment", map[string]any{"replicas": 3}), "scale_deployment (replicas=3)"; got != want {
		t.Errorf("single param: got %q, want %q", got, want)
	}
	got := reconcile.FormatActionParams("patch_resources", map[string]any{"memory_limit": "512Mi", "cpu_limit": "500m"})
	want := "patch_resources (cpu_limit=500m, memory_limit=512Mi)"
	if got != want {
		t.Errorf("multiple params: got %q, want %q (must be sorted by key)", got, want)
	}
}
