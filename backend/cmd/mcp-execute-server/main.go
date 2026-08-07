// backend/cmd/mcp-execute-server/main.go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

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
	// settings.Load() fails fast (os.Exit) if MCP_EXECUTE_TOKEN or any
	// other required var is missing — this process cannot reach
	// ListenAndServe below with incomplete config.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	s := settings.Load()

	restConfig, err := clientcmd.BuildConfigFromFlags("", s.Kubeconfig)
	if err != nil {
		slog.Error("building kubeconfig", "error", err)
		os.Exit(1)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		slog.Error("building clientset", "error", err)
		os.Exit(1)
	}
	executor := execute.NewExecutor(clientset)

	restartPod := func(ctx context.Context, req *mcp.CallToolRequest, input RestartPodInput) (*mcp.CallToolResult, RestartPodOutput, error) {
		if err := executor.RestartPod(ctx, input.Namespace, input.Name); err != nil {
			slog.Error("restart_pod failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, RestartPodOutput{}, err
		}
		slog.Info("restart_pod executed", "namespace", input.Namespace, "name", input.Name)
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

	// /healthz is deliberately mounted outside the bearer-token wrapper — a
	// Kubernetes liveness/readiness probe has no way to present a token, and
	// this endpoint reports nothing about the cluster, only that the process
	// is up. Every other route stays behind RequireBearerToken.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/", mcpauth.RequireBearerToken(s.MCPExecuteToken, handler))

	slog.Info("mcp-execute-server listening", "addr", s.MCPExecuteAddr)
	if err := http.ListenAndServe(s.MCPExecuteAddr, mux); err != nil {
		slog.Error("http server exited", "error", err)
		os.Exit(1)
	}
}
