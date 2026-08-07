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
// many times RestartPod was invoked per (namespace, name). It lets a test
// prove that an already-remediated pod is never restarted a second time by a
// later, unrelated signal. It never touches the fake clientset, so restart
// counts are independent of pod verification.
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

// TestWatcherToReconciler_HealsCrashLoopInAutoMode drives the full detect ->
// correlate -> analyze -> gate -> execute -> verify -> audit loop for a
// crash-looping pod in auto mode, using the in-memory Store and a fake
// Kubernetes clientset so it needs no live cluster or Slack workspace. This
// is the plan's end-to-end test for the core loop.
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
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0" // unreachable on purpose — auto mode's post-execution notification is fire-and-forget and must not need it to succeed for this assertion

	// Rather than calling execute.Executor in-process, this test proves the
	// real production call path: a real MCP server (backed by the same
	// executor) speaking real HTTP, bearer-token-checked, wired into
	// reconcile.Reconciler via mcpexecute.Client as its PodRestarter.
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

	r := reconcile.New(memStore, mcpClient, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)
	watcher := k8swatch.NewWatcher(clientset, func(s signal.Signal) { r.OnSignal(ctx, s) })

	watcher.HandleAddEvent(&corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "BackOff",
		Message:        "Back-off restarting failed container",
		LastTimestamp:  metav1.NewTime(time.Now()),
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

// TestReconciler_OnSignal_SuppressesRestartAfterLimit reproduces the exact
// scenario found in production: a Deployment-managed pod that crashes
// permanently gets deleted and recreated under a brand new name by its
// ReplicaSet every time SAGE "restarts" it, so pod-name-based correlation
// would never notice it happening again and again. All 6 signals below
// share a GroupKey (the owning ReplicaSet) but use distinct pod names, just
// like real recreations would — proving the cap is enforced across incidents,
// not just within one.
func TestReconciler_OnSignal_SuppressesRestartAfterLimit(t *testing.T) {
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
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0" // unreachable on purpose — the restart-limit alert is fire-and-forget

	r := reconcile.New(memStore, restarter, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	const groupKey = "team-a/ReplicaSet/worker-rs"
	for i := 1; i <= 6; i++ {
		r.OnSignal(ctx, signal.Signal{
			Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
			Namespace: "team-a", Kind: "Pod", Name: fmt.Sprintf("worker-rs-%d", i),
			Labels: map[string]string{"app": "worker"}, Timestamp: time.Now(),
			GroupKey: groupKey,
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

func TestReconciler_OnSignal_ManualModeRequestsApprovalAndDoesNotExecute(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default", Labels: map[string]string{"app": "web"}},
	}
	clientset := fake.NewSimpleClientset(pod)
	memStore := store.NewMemoryStore()
	executor := execute.NewExecutor(clientset)
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0" // unreachable on purpose — approval path must not need it to succeed for this assertion

	r := reconcile.New(memStore, executor, slackClient, clientset, gate.ModeManual, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
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

// TestReconciler_OnSignal_DoesNotReRemediateAlreadyHandledObject reproduces
// and guards against the original "re-correlate all pending forever" bug:
// after pod X is fully handled (auto mode: restarted, verified, audited), an
// UNRELATED signal for a completely different pod Y arrives. Pod X must not be
// diagnosed, recorded, or restarted a second time — only pod Y should get a
// new incident. Before the fix, OnSignal re-correlated over [X, Y] and called
// handleIncident for X again, producing a duplicate incident/action and a
// second, unnecessary RestartPod on the already-healthy pod X.
func TestReconciler_OnSignal_DoesNotReRemediateAlreadyHandledObject(t *testing.T) {
	ctx := context.Background()

	// A Ready pod sharing pod X's labels so verify resolves immediately (no
	// polling delay), and likewise for pod Y. The two pods live in different
	// namespaces with different names/labels — genuinely unrelated objects.
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
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0" // unreachable on purpose — auto mode's post-execution notification is fire-and-forget and must not need it to succeed for this assertion

	r := reconcile.New(memStore, restarter, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	// Signal for pod X: fully handled through execute -> verify -> audit.
	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "team-a", Kind: "Pod", Name: "api-1",
		Labels: map[string]string{"app": "api"}, Timestamp: time.Now(),
	})

	if got := restarter.count("team-a", "api-1"); got != 1 {
		t.Fatalf("after first signal, expected pod X restarted exactly once, got %d", got)
	}
	incidentsAfterX := len(memStore.Incidents)
	actionsAfterX := len(memStore.Actions)
	if incidentsAfterX != 1 || actionsAfterX != 1 {
		t.Fatalf("expected exactly 1 incident and 1 action after handling pod X, got %d incidents / %d actions", incidentsAfterX, actionsAfterX)
	}

	// An UNRELATED signal for a completely different pod Y arrives.
	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "team-b", Kind: "Pod", Name: "worker-1",
		Labels: map[string]string{"app": "worker"}, Timestamp: time.Now(),
	})

	// The core assertion: pod X was NOT restarted again by pod Y's signal.
	if got := restarter.count("team-a", "api-1"); got != 1 {
		t.Fatalf("pod X must NOT be re-remediated by an unrelated signal: want 1 restart, got %d", got)
	}
	// Pod Y is handled exactly once.
	if got := restarter.count("team-b", "worker-1"); got != 1 {
		t.Fatalf("expected pod Y restarted exactly once, got %d", got)
	}
	// Exactly one new incident/action was created — for Y, none re-created for X.
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
