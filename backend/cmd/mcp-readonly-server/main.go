// backend/cmd/mcp-readonly-server/main.go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"sre-platform/backend/internal/introspect"
	"sre-platform/backend/internal/mcpauth"
	"sre-platform/backend/internal/pxmetrics"
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

type GetPodResourceUsageInput struct {
	Namespace       string `json:"namespace" jsonschema:"the pod's namespace"`
	Name            string `json:"name" jsonschema:"the pod's name"`
	LookbackSeconds int64  `json:"lookback_seconds" jsonschema:"how far back to look, in seconds (e.g. 300)"`
}
type GetPodResourceUsageOutput struct {
	Samples   []pxmetrics.CPUSample `json:"samples"`
	Truncated bool                  `json:"truncated,omitempty"`
	Note      string                `json:"note,omitempty"`
}

type GetPodTrafficStatsInput struct {
	Namespace       string `json:"namespace" jsonschema:"the pod's namespace"`
	Name            string `json:"name" jsonschema:"the pod's name"`
	LookbackSeconds int64  `json:"lookback_seconds" jsonschema:"how far back to look, in seconds (e.g. 300)"`
}
type GetPodTrafficStatsOutput struct {
	Samples   []pxmetrics.TrafficSample `json:"samples"`
	Truncated bool                      `json:"truncated,omitempty"`
	Note      string                    `json:"note,omitempty"`
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

	var pxClient *pxmetrics.Client
	if s.PixieEnabled {
		pxClient, err = pxmetrics.NewClient(context.Background(), pxmetrics.Config{
			ConnMode: s.PixieConnMode, VizierAddr: s.PixieVizierAddr,
			APIKey: s.PixieAPIKey, ClusterID: s.PixieClusterID,
			InsecureSkipTLSVerify: s.PixieInsecureSkipTLSVerify,
		})
		if err != nil {
			slog.Error("pxmetrics.NewClient failed", "error", err)
			os.Exit(1)
		}
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

	getPodResourceUsage := func(ctx context.Context, req *mcp.CallToolRequest, input GetPodResourceUsageInput) (*mcp.CallToolResult, GetPodResourceUsageOutput, error) {
		samples, truncated, err := pxClient.GetPodCPUUsage(ctx, input.Namespace, input.Name, time.Duration(input.LookbackSeconds)*time.Second)
		if err != nil {
			slog.Error("get_pod_resource_usage failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, GetPodResourceUsageOutput{}, err
		}
		output := GetPodResourceUsageOutput{Samples: samples, Truncated: truncated}
		if len(samples) == 0 {
			output.Note = "no Pixie data found for this pod in the requested window — it may not have been running, or may predate Pixie's retention window; this does not necessarily mean zero usage"
		}
		return nil, output, nil
	}
	getPodTrafficStats := func(ctx context.Context, req *mcp.CallToolRequest, input GetPodTrafficStatsInput) (*mcp.CallToolResult, GetPodTrafficStatsOutput, error) {
		samples, truncated, err := pxClient.GetPodTrafficStats(ctx, input.Namespace, input.Name, time.Duration(input.LookbackSeconds)*time.Second)
		if err != nil {
			slog.Error("get_pod_traffic_stats failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, GetPodTrafficStatsOutput{}, err
		}
		output := GetPodTrafficStatsOutput{Samples: samples, Truncated: truncated}
		if len(samples) == 0 {
			output.Note = "no Pixie data found for this pod in the requested window — it may not have been running, or may predate Pixie's retention window; this does not necessarily mean zero usage"
		}
		return nil, output, nil
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "sre-readonly", Version: "v1.0.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "get_pod_logs", Description: "Returns the tail of a pod's container logs. Read-only."}, getPodLogs)
	mcp.AddTool(server, &mcp.Tool{Name: "get_pod_events", Description: "Returns Kubernetes Events involving a pod. Read-only."}, getPodEvents)
	mcp.AddTool(server, &mcp.Tool{Name: "describe_pod", Description: "Returns a compact status summary of a pod: phase, container states, restart counts. Read-only."}, describePod)

	if s.PixieEnabled {
		mcp.AddTool(server, &mcp.Tool{Name: "get_pod_resource_usage", Description: "Returns recent windowed CPU usage (fraction of one core) for a pod, via eBPF-collected metrics (Pixie). Read-only."}, getPodResourceUsage)
		mcp.AddTool(server, &mcp.Tool{Name: "get_pod_traffic_stats", Description: "Returns recent windowed HTTP request rate, error rate, and latency percentiles for traffic involving a pod — includes both inbound (this pod serving requests) and outbound (this pod calling other services) traffic, not distinguished. A spike here may reflect a downstream dependency's failures, not necessarily this pod's own behavior. Via eBPF (Pixie). Read-only."}, getPodTrafficStats)
	}

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
