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

type ScaleDeploymentInput struct {
	Namespace string `json:"namespace" jsonschema:"the pod's namespace"`
	Name      string `json:"name" jsonschema:"the pod's own name — the owning Deployment is resolved automatically"`
	Replicas  int32  `json:"replicas" jsonschema:"desired replica count"`
}
type ScaleDeploymentOutput struct {
	Status         string `json:"status"`
	DeploymentName string `json:"deployment_name"`
}

type PatchResourcesInput struct {
	Namespace   string `json:"namespace" jsonschema:"the pod's namespace"`
	Name        string `json:"name" jsonschema:"the pod's own name — the owning Deployment is resolved automatically"`
	MemoryLimit string `json:"memory_limit,omitempty" jsonschema:"new memory limit, e.g. 512Mi (optional if cpu_limit set)"`
	CPULimit    string `json:"cpu_limit,omitempty" jsonschema:"new CPU limit, e.g. 500m (optional if memory_limit set)"`
}
type PatchResourcesOutput struct {
	Status         string `json:"status"`
	DeploymentName string `json:"deployment_name"`
}

type RollbackDeploymentInput struct {
	Namespace string `json:"namespace" jsonschema:"the pod's namespace"`
	Name      string `json:"name" jsonschema:"the pod's own name — the owning Deployment is resolved automatically"`
}
type RollbackDeploymentOutput struct {
	Status         string `json:"status"`
	DeploymentName string `json:"deployment_name"`
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

	scaleDeployment := func(ctx context.Context, req *mcp.CallToolRequest, input ScaleDeploymentInput) (*mcp.CallToolResult, ScaleDeploymentOutput, error) {
		deploymentName, err := executor.ScaleDeployment(ctx, input.Namespace, input.Name, input.Replicas)
		if err != nil {
			slog.Error("scale_deployment failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, ScaleDeploymentOutput{}, err
		}
		slog.Info("scale_deployment executed", "namespace", input.Namespace, "deployment", deploymentName, "replicas", input.Replicas)
		return nil, ScaleDeploymentOutput{Status: "scaled", DeploymentName: deploymentName}, nil
	}

	patchResources := func(ctx context.Context, req *mcp.CallToolRequest, input PatchResourcesInput) (*mcp.CallToolResult, PatchResourcesOutput, error) {
		deploymentName, err := executor.PatchResources(ctx, input.Namespace, input.Name, input.MemoryLimit, input.CPULimit)
		if err != nil {
			slog.Error("patch_resources failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, PatchResourcesOutput{}, err
		}
		slog.Info("patch_resources executed", "namespace", input.Namespace, "deployment", deploymentName)
		return nil, PatchResourcesOutput{Status: "patched", DeploymentName: deploymentName}, nil
	}

	rollbackDeployment := func(ctx context.Context, req *mcp.CallToolRequest, input RollbackDeploymentInput) (*mcp.CallToolResult, RollbackDeploymentOutput, error) {
		deploymentName, err := executor.RollbackDeployment(ctx, input.Namespace, input.Name)
		if err != nil {
			slog.Error("rollback_deployment failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, RollbackDeploymentOutput{}, err
		}
		slog.Info("rollback_deployment executed", "namespace", input.Namespace, "deployment", deploymentName)
		return nil, RollbackDeploymentOutput{Status: "rolled_back", DeploymentName: deploymentName}, nil
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "sre-execute", Version: "v1.0.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "restart_pod",
		Description: "Deletes a pod so its owning controller recreates it. Idempotent — safe to call on an already-gone pod.",
	}, restartPod)
	mcp.AddTool(server, &mcp.Tool{Name: "scale_deployment", Description: "Resolves the pod's owning Deployment and sets its replica count."}, scaleDeployment)
	mcp.AddTool(server, &mcp.Tool{Name: "patch_resources", Description: "Bumps memory/CPU limits on every container of the pod's owning Deployment."}, patchResources)
	mcp.AddTool(server, &mcp.Tool{Name: "rollback_deployment", Description: "Reverts the pod's owning Deployment to its immediately previous ReplicaSet revision."}, rollbackDeployment)

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
