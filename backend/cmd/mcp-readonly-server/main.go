// backend/cmd/mcp-readonly-server/main.go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"sre-platform/backend/internal/introspect"
	"sre-platform/backend/internal/mcpauth"
	"sre-platform/backend/internal/settings"
)

type GetPodLogsInput struct {
	Namespace string `json:"namespace" jsonschema:"the pod's namespace"`
	Name      string `json:"name" jsonschema:"the pod's name"`
	TailLines int64  `json:"tail_lines" jsonschema:"how many lines from the end of the log to return"`
}
type GetPodLogsOutput struct {
	Logs string `json:"logs"`
}

type GetPodEventsInput struct {
	Namespace string `json:"namespace" jsonschema:"the pod's namespace"`
	Name      string `json:"name" jsonschema:"the pod's name"`
}
type GetPodEventsOutput struct {
	Events []introspect.EventSummary `json:"events"`
}

type DescribePodInput struct {
	Namespace string `json:"namespace" jsonschema:"the pod's namespace"`
	Name      string `json:"name" jsonschema:"the pod's name"`
}
type DescribePodOutput struct {
	Summary introspect.PodSummary `json:"summary"`
}

func main() {
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

	getPodLogs := func(ctx context.Context, req *mcp.CallToolRequest, input GetPodLogsInput) (*mcp.CallToolResult, GetPodLogsOutput, error) {
		logs, err := introspect.GetPodLogs(ctx, clientset, input.Namespace, input.Name, input.TailLines)
		if err != nil {
			slog.Error("get_pod_logs failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, GetPodLogsOutput{}, err
		}
		return nil, GetPodLogsOutput{Logs: logs}, nil
	}
	getPodEvents := func(ctx context.Context, req *mcp.CallToolRequest, input GetPodEventsInput) (*mcp.CallToolResult, GetPodEventsOutput, error) {
		events, err := introspect.GetPodEvents(ctx, clientset, input.Namespace, input.Name)
		if err != nil {
			slog.Error("get_pod_events failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, GetPodEventsOutput{}, err
		}
		return nil, GetPodEventsOutput{Events: events}, nil
	}
	describePod := func(ctx context.Context, req *mcp.CallToolRequest, input DescribePodInput) (*mcp.CallToolResult, DescribePodOutput, error) {
		summary, err := introspect.DescribePod(ctx, clientset, input.Namespace, input.Name)
		if err != nil {
			slog.Error("describe_pod failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, DescribePodOutput{}, err
		}
		return nil, DescribePodOutput{Summary: summary}, nil
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "sre-readonly", Version: "v1.0.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "get_pod_logs", Description: "Returns the tail of a pod's container logs. Read-only."}, getPodLogs)
	mcp.AddTool(server, &mcp.Tool{Name: "get_pod_events", Description: "Returns Kubernetes Events involving a pod. Read-only."}, getPodEvents)
	mcp.AddTool(server, &mcp.Tool{Name: "describe_pod", Description: "Returns a compact status summary of a pod: phase, container states, restart counts. Read-only."}, describePod)

	handler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return server
	}, nil)

	// /healthz stays outside the bearer-token wrapper, same reasoning as
	// mcp-execute-server: a kubelet probe can't present a token, and this
	// endpoint reveals nothing about the cluster.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/", mcpauth.RequireBearerToken(s.MCPReadonlyToken, handler))

	slog.Info("mcp-readonly-server listening", "addr", s.MCPReadonlyAddr)
	if err := http.ListenAndServe(s.MCPReadonlyAddr, mux); err != nil {
		slog.Error("http server exited", "error", err)
		os.Exit(1)
	}
}
