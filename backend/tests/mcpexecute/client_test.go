// backend/tests/mcpexecute/client_test.go
package mcpexecute_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sre-platform/backend/internal/execute"
	"sre-platform/backend/internal/mcpauth"
	"sre-platform/backend/internal/mcpexecute"
)

type restartPodInput struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type restartPodOutput struct {
	Status string `json:"status"`
}

type scaleDeploymentInput struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Replicas  int32  `json:"replicas"`
}

type scaleDeploymentOutput struct {
	Status         string `json:"status"`
	DeploymentName string `json:"deployment_name"`
}

type patchResourcesInput struct {
	Namespace   string `json:"namespace"`
	Name        string `json:"name"`
	MemoryLimit string `json:"memory_limit,omitempty"`
	CPULimit    string `json:"cpu_limit,omitempty"`
}

type patchResourcesOutput struct {
	Status         string `json:"status"`
	DeploymentName string `json:"deployment_name"`
}

type rollbackDeploymentInput struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type rollbackDeploymentOutput struct {
	Status         string `json:"status"`
	DeploymentName string `json:"deployment_name"`
}

// startTestServer spins up a real MCP server, over real HTTP, backed by a
// fake Kubernetes clientset — so the test below exercises the actual MCP
// wire protocol and bearer-token check, not a mock of them.
func startTestServer(t *testing.T, token string, clientset *fake.Clientset) *httptest.Server {
	t.Helper()
	executor := execute.NewExecutor(clientset)

	restartPod := func(ctx context.Context, req *mcp.CallToolRequest, input restartPodInput) (*mcp.CallToolResult, restartPodOutput, error) {
		if err := executor.RestartPod(ctx, input.Namespace, input.Name); err != nil {
			return nil, restartPodOutput{}, err
		}
		return nil, restartPodOutput{Status: "deleted"}, nil
	}

	scaleDeployment := func(ctx context.Context, req *mcp.CallToolRequest, input scaleDeploymentInput) (*mcp.CallToolResult, scaleDeploymentOutput, error) {
		deploymentName, err := executor.ScaleDeployment(ctx, input.Namespace, input.Name, input.Replicas)
		if err != nil {
			return nil, scaleDeploymentOutput{}, err
		}
		return nil, scaleDeploymentOutput{Status: "scaled", DeploymentName: deploymentName}, nil
	}

	patchResources := func(ctx context.Context, req *mcp.CallToolRequest, input patchResourcesInput) (*mcp.CallToolResult, patchResourcesOutput, error) {
		deploymentName, err := executor.PatchResources(ctx, input.Namespace, input.Name, input.MemoryLimit, input.CPULimit)
		if err != nil {
			return nil, patchResourcesOutput{}, err
		}
		return nil, patchResourcesOutput{Status: "patched", DeploymentName: deploymentName}, nil
	}

	rollbackDeployment := func(ctx context.Context, req *mcp.CallToolRequest, input rollbackDeploymentInput) (*mcp.CallToolResult, rollbackDeploymentOutput, error) {
		deploymentName, err := executor.RollbackDeployment(ctx, input.Namespace, input.Name)
		if err != nil {
			return nil, rollbackDeploymentOutput{}, err
		}
		return nil, rollbackDeploymentOutput{Status: "rolled_back", DeploymentName: deploymentName}, nil
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "sre-execute-test", Version: "v1.0.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "restart_pod", Description: "test tool"}, restartPod)
	mcp.AddTool(server, &mcp.Tool{Name: "scale_deployment", Description: "test tool"}, scaleDeployment)
	mcp.AddTool(server, &mcp.Tool{Name: "patch_resources", Description: "test tool"}, patchResources)
	mcp.AddTool(server, &mcp.Tool{Name: "rollback_deployment", Description: "test tool"}, rollbackDeployment)

	handler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server { return server }, nil)
	ts := httptest.NewServer(mcpauth.RequireBearerToken(token, handler))
	t.Cleanup(ts.Close)
	return ts
}

// deploymentsGVR is the GroupVersionResource fake.NewSimpleClientset registers
// Deployment objects under; used by newScaleAwareClientset below to address
// the object tracker directly.
var deploymentsGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}

// newScaleAwareClientset builds a fake clientset that also supports the
// Deployments().GetScale/UpdateScale calls execute.Executor.ScaleDeployment
// relies on.
//
// The generic fake object tracker's default reaction (k8s.io/client-go
// /testing.ObjectReaction) does not know about the "scale" subresource for
// typed clients: it dispatches purely on the Go action type (Get/Update),
// ignoring the subresource string, so a bare GetScale call fetches the
// stored object by its "deployments" GVR and then type-asserts it straight
// to *autoscalingv1.Scale — which panics ("interface conversion:
// runtime.Object is *v1.Deployment, not *v1.Scale"). This is a
// fake-clientset test-infrastructure gap, not a bug in ScaleDeployment:
// against a real API server the deployments/scale subresource is a real,
// distinct object and GetScale/UpdateScale work as written. This is a
// duplicate of the identical helper in
// backend/tests/execute/execute_test.go (package execute_test, which this
// package — mcpexecute_test — cannot import since it's a sibling _test
// package); see that file's comment for the full derivation.
func newScaleAwareClientset(objects ...runtime.Object) *fake.Clientset {
	clientset := fake.NewSimpleClientset(objects...)

	clientset.PrependReactor("get", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		getAction, ok := action.(k8stesting.GetAction)
		if !ok || getAction.GetSubresource() != "scale" {
			return false, nil, nil
		}
		obj, err := clientset.Tracker().Get(deploymentsGVR, getAction.GetNamespace(), getAction.GetName())
		if err != nil {
			return true, nil, err
		}
		dep := obj.(*appsv1.Deployment)
		var replicas int32
		if dep.Spec.Replicas != nil {
			replicas = *dep.Spec.Replicas
		}
		return true, &autoscalingv1.Scale{
			ObjectMeta: metav1.ObjectMeta{Name: dep.Name, Namespace: dep.Namespace, UID: dep.UID, ResourceVersion: dep.ResourceVersion},
			Spec:       autoscalingv1.ScaleSpec{Replicas: replicas},
			Status:     autoscalingv1.ScaleStatus{Replicas: dep.Status.Replicas},
		}, nil
	})

	clientset.PrependReactor("update", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updateAction, ok := action.(k8stesting.UpdateAction)
		if !ok || updateAction.GetSubresource() != "scale" {
			return false, nil, nil
		}
		scale, ok := updateAction.GetObject().(*autoscalingv1.Scale)
		if !ok {
			return false, nil, nil
		}
		obj, err := clientset.Tracker().Get(deploymentsGVR, scale.Namespace, scale.Name)
		if err != nil {
			return true, nil, err
		}
		dep := obj.(*appsv1.Deployment)
		newReplicas := scale.Spec.Replicas
		dep.Spec.Replicas = &newReplicas
		if err := clientset.Tracker().Update(deploymentsGVR, dep, scale.Namespace); err != nil {
			return true, nil, err
		}
		return true, scale, nil
	})

	return clientset
}

func TestClient_RestartPod_CallsToolOverMCP(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default"}}
	clientset := fake.NewSimpleClientset(pod)

	ts := startTestServer(t, "test-token", clientset)

	client, err := mcpexecute.NewClient(ctx, ts.URL, "test-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if err := client.RestartPod(ctx, "default", "web-1"); err != nil {
		t.Fatalf("RestartPod: %v", err)
	}

	_, err = clientset.CoreV1().Pods("default").Get(ctx, "web-1", metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected pod to be deleted via the MCP call, got err=%v", err)
	}
}

func TestClient_NewClient_FailsWithWrongToken(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()
	ts := startTestServer(t, "correct-token", clientset)

	_, err := mcpexecute.NewClient(ctx, ts.URL, "wrong-token")
	if err == nil {
		t.Fatal("expected NewClient to fail the MCP handshake when the bearer token is wrong")
	}
}

// TestClient_RestartPod_SurfacesToolError guards against a real pitfall in
// the go-sdk: a tool handler's returned error is encoded by ToolHandlerFor as
// CallToolResult.IsError with the message in Content, not as a Go error from
// session.CallTool. A RestartPod that only checked the transport-level error
// would treat a genuine Kubernetes failure as success — dangerous for a
// remediation client. Forcing the fake clientset's Delete to fail proves the
// error still reaches the caller.
func TestClient_RestartPod_SurfacesToolError(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()
	clientset.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("simulated api server failure")
	})

	ts := startTestServer(t, "test-token", clientset)
	client, err := mcpexecute.NewClient(ctx, ts.URL, "test-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if err := client.RestartPod(ctx, "default", "web-1"); err == nil {
		t.Fatal("expected RestartPod to surface the tool-level error, got nil")
	}
}

// TestClient_ScaleDeployment_CallsToolAndReturnsDeploymentName seeds the same
// Deployment/ReplicaSet/Pod chain as Task 3's
// TestScaleDeployment_ResolvesOwnerAndUpdatesReplicaCount, round-trips a real
// scale_deployment MCP call, and asserts both the returned deployment name
// and the actual replica count change on the (scale-aware fake) cluster.
func TestClient_ScaleDeployment_CallsToolAndReturnsDeploymentName(t *testing.T) {
	ctx := context.Background()
	depUID := types.UID("dep-uid-1")
	var initialReplicas int32 = 2

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: depUID},
		Spec:       appsv1.DeploymentSpec{Replicas: &initialReplicas},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "web-abc123",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", UID: depUID}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "web-abc123-xyz",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-abc123"}},
		},
	}

	clientset := newScaleAwareClientset(deployment, rs, pod)
	ts := startTestServer(t, "test-token", clientset)

	client, err := mcpexecute.NewClient(ctx, ts.URL, "test-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	name, err := client.ScaleDeployment(ctx, "default", "web-abc123-xyz", 5)
	if err != nil {
		t.Fatalf("ScaleDeployment: %v", err)
	}
	if name != "web" {
		t.Errorf("expected resolved deployment name %q, got %q", "web", name)
	}

	updated, err := clientset.AppsV1().Deployments("default").Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get deployment after scale: %v", err)
	}
	if updated.Spec.Replicas == nil || *updated.Spec.Replicas != 5 {
		t.Errorf("expected replicas == 5, got %v", updated.Spec.Replicas)
	}
}

// TestClient_PatchResources_CallsToolAndReturnsDeploymentName asserts both
// the returned deployment name and that the cluster's container memory
// limit actually changed via a real patch_resources MCP round trip.
func TestClient_PatchResources_CallsToolAndReturnsDeploymentName(t *testing.T) {
	ctx := context.Background()
	depUID := types.UID("dep-uid-1")

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: depUID},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{
						Name: "app",
						Resources: corev1.ResourceRequirements{
							Limits: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("512Mi"),
								corev1.ResourceCPU:    resource.MustParse("1"),
							},
						},
					},
				}},
			},
		},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "web-abc123",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", UID: depUID}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "web-abc123-xyz",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-abc123"}},
		},
	}

	clientset := fake.NewSimpleClientset(deployment, rs, pod)
	ts := startTestServer(t, "test-token", clientset)

	client, err := mcpexecute.NewClient(ctx, ts.URL, "test-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	name, err := client.PatchResources(ctx, "default", "web-abc123-xyz", "1Gi", "")
	if err != nil {
		t.Fatalf("PatchResources: %v", err)
	}
	if name != "web" {
		t.Errorf("expected resolved deployment name %q, got %q", "web", name)
	}

	updated, err := clientset.AppsV1().Deployments("default").Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get deployment after patch: %v", err)
	}
	wantMemory := resource.MustParse("1Gi")
	got := updated.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory]
	if got.Cmp(wantMemory) != 0 {
		t.Errorf("expected memory limit %s, got %s", wantMemory.String(), got.String())
	}
}

// TestClient_RollbackDeployment_CallsToolAndReturnsDeploymentName asserts
// both the returned deployment name and that the cluster's pod template
// actually reverted to the immediately previous revision's image via a real
// rollback_deployment MCP round trip.
func TestClient_RollbackDeployment_CallsToolAndReturnsDeploymentName(t *testing.T) {
	ctx := context.Background()
	depUID := types.UID("dep-uid-1")

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "web",
			Namespace:   "default",
			UID:         depUID,
			Annotations: map[string]string{"deployment.kubernetes.io/revision": "2"},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app:v2"}}},
			},
		},
	}
	rs1 := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "web-rev1",
			Namespace:       "default",
			Labels:          map[string]string{"app": "web"},
			Annotations:     map[string]string{"deployment.kubernetes.io/revision": "1"},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", UID: depUID}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app:v1"}}},
			},
		},
	}
	rs2 := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "web-rev2",
			Namespace:       "default",
			Labels:          map[string]string{"app": "web"},
			Annotations:     map[string]string{"deployment.kubernetes.io/revision": "2"},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", UID: depUID}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app:v2"}}},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "web-rev2-xyz",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-rev2"}},
		},
	}

	clientset := fake.NewSimpleClientset(deployment, rs1, rs2, pod)
	ts := startTestServer(t, "test-token", clientset)

	client, err := mcpexecute.NewClient(ctx, ts.URL, "test-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	name, err := client.RollbackDeployment(ctx, "default", "web-rev2-xyz")
	if err != nil {
		t.Fatalf("RollbackDeployment: %v", err)
	}
	if name != "web" {
		t.Errorf("expected resolved deployment name %q, got %q", "web", name)
	}

	updated, err := clientset.AppsV1().Deployments("default").Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get deployment after rollback: %v", err)
	}
	if got := updated.Spec.Template.Spec.Containers[0].Image; got != "app:v1" {
		t.Errorf("expected rollback to app:v1 (the immediately previous revision), got %s", got)
	}
}

// TestClient_ScaleDeployment_SurfacesToolError mirrors
// TestClient_RestartPod_SurfacesToolError: a pod with no ReplicaSet owner
// makes executor.ScaleDeployment fail, the tool handler returns that error,
// the SDK encodes it as CallToolResult.IsError, and callTool's shared
// error-surfacing path must turn that back into a non-nil Go error for the
// caller. This is the only one of the three new methods that needs this
// specific proof — callTool's error path is shared code, already exercised
// once here and once for restart_pod.
func TestClient_ScaleDeployment_SurfacesToolError(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: "default"},
	}
	clientset := fake.NewSimpleClientset(pod)
	ts := startTestServer(t, "test-token", clientset)

	client, err := mcpexecute.NewClient(ctx, ts.URL, "test-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if _, err := client.ScaleDeployment(ctx, "default", "orphan", 3); err == nil {
		t.Fatal("expected ScaleDeployment to surface the tool-level error, got nil")
	}
}
