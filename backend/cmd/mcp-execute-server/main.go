// backend/cmd/mcp-execute-server/main.go
package main

import (
	"context"
	"log"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"sre-platform/backend/internal/execute"
	"sre-platform/backend/internal/mcpauth"
	"sre-platform/backend/internal/settings"
)

type RestartPodInput struct {
	Namespace string `json:"namespace" jsonschema:"the pod's namespace"`
	Name      string `json:"name" jsonschema:"the pod's name"`
}

type RestartPodOutput struct {
	Status string `json:"status" jsonschema:"result of the restart, e.g. 'deleted'"`
}

func main() {
	// settings.Load() fails fast (log.Fatal) if MCP_EXECUTE_TOKEN or any
	// other required var is missing — this process cannot reach
	// ListenAndServe below with incomplete config.
	s := settings.Load()

	restConfig, err := clientcmd.BuildConfigFromFlags("", s.Kubeconfig)
	if err != nil {
		log.Fatalf("building kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Fatalf("building clientset: %v", err)
	}
	executor := execute.NewExecutor(clientset)

	restartPod := func(ctx context.Context, req *mcp.CallToolRequest, input RestartPodInput) (*mcp.CallToolResult, RestartPodOutput, error) {
		if err := executor.RestartPod(ctx, input.Namespace, input.Name); err != nil {
			return nil, RestartPodOutput{}, err
		}
		return nil, RestartPodOutput{Status: "deleted"}, nil
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "sre-execute", Version: "v1.0.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "restart_pod",
		Description: "Deletes a pod so its owning controller recreates it. Idempotent — safe to call on an already-gone pod.",
	}, restartPod)

	handler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return server
	}, nil)

	log.Printf("mcp-execute-server listening on %s", s.MCPExecuteAddr)
	log.Fatal(http.ListenAndServe(s.MCPExecuteAddr, mcpauth.RequireBearerToken(s.MCPExecuteToken, handler)))
}
