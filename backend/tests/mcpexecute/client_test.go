// backend/tests/mcpexecute/client_test.go
package mcpexecute_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

	server := mcp.NewServer(&mcp.Implementation{Name: "sre-execute-test", Version: "v1.0.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "restart_pod", Description: "test tool"}, restartPod)

	handler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server { return server }, nil)
	ts := httptest.NewServer(mcpauth.RequireBearerToken(token, handler))
	t.Cleanup(ts.Close)
	return ts
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
