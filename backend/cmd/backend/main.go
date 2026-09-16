// backend/cmd/backend/main.go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"sre-platform/backend/internal/beylascrape"
	"sre-platform/backend/internal/httpserver"
	"sre-platform/backend/internal/incidentqueue"
	"sre-platform/backend/internal/k8swatch"
	"sre-platform/backend/internal/mcpexecute"
	"sre-platform/backend/internal/metricsagg"
	"sre-platform/backend/internal/reconcile"
	"sre-platform/backend/internal/settings"
	"sre-platform/backend/internal/signal"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

// metricsQuantiles is the fixed set of latency percentiles the metrics
// WebSocket feed reports on every publish. Not a Settings field: nothing
// today needs it configurable; promote it if a real caller does.
var metricsQuantiles = []float64{0.5, 0.95, 0.99}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg := settings.Load()
	ctx := context.Background()

	clientset := buildClientset(cfg.Kubeconfig)
	pgStore, err := store.NewPostgresStore(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("connecting to Postgres", "error", err)
		os.Exit(1)
	}
	slackClient := slackapproval.NewClient(cfg.SlackBotToken, cfg.SlackApprovalChannel, cfg.SlackSigningSecret, http.DefaultClient)
	restarter, err := mcpexecute.NewClient(ctx, cfg.MCPExecuteURL, cfg.MCPExecuteToken)
	if err != nil {
		slog.Error("connecting to mcp-execute-server", "error", err)
		os.Exit(1)
	}
	publisher, err := incidentqueue.NewClient(ctx, cfg.NATSURL)
	if err != nil {
		slog.Error("connecting to NATS", "error", err)
		os.Exit(1)
	}

	reconciler := reconcile.New(pgStore, restarter, publisher, slackClient, clientset, cfg.Mode, cfg.CorrelationWindow, cfg.VerifyTimeout)
	watcher := k8swatch.NewWatcher(clientset, func(s signal.Signal) { reconciler.OnSignal(ctx, s) })

	// metricsHub fans out live latency/throughput to WebSocket clients; the
	// Aggregator publishes into it. Source is beylascrape.NewSource when a
	// Beyla deployment has been configured (cfg.BeylaEnabled), matching
	// the same "off by default, real infra prerequisite" pattern as Pixie
	// — otherwise NoopSource, so every poll reports zero series but the
	// hub/route/ws pipeline still runs end-to-end.
	var metricsSource metricsagg.Source = metricsagg.NoopSource{}
	if cfg.BeylaEnabled {
		metricsSource = beylascrape.NewSource(clientset, http.DefaultClient, beylascrape.Config{
			PodSelector: cfg.BeylaPodSelector,
			Namespace:   cfg.BeylaNamespace,
			Port:        cfg.BeylaPort,
			Timeout:     10 * time.Second,
		})
	}
	metricsHub := httpserver.NewMetricsHub()
	aggregator := metricsagg.New(metricsSource, metricsHub, metricsQuantiles)
	go func() {
		if err := aggregator.Run(ctx, cfg.MetricsPollInterval); err != nil {
			slog.Error("metricsagg.Aggregator.Run exited", "error", err)
		}
	}()

	router := httpserver.NewRouter(slackClient, pgStore, reconciler, cfg.DiagnosisCallbackToken, metricsHub, cfg.MCPReadonlyToken)
	go func() {
		slog.Info("listening", "addr", cfg.HTTPAddr)
		if err := http.ListenAndServe(cfg.HTTPAddr, router); err != nil {
			slog.Error("http server exited", "error", err)
			os.Exit(1)
		}
	}()

	if err := watcher.Run(ctx); err != nil {
		slog.Error("watcher.Run", "error", err)
		os.Exit(1)
	}
}

func buildClientset(kubeconfig string) kubernetes.Interface {
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		slog.Error("building kubeconfig", "error", err)
		os.Exit(1)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		slog.Error("building clientset", "error", err)
		os.Exit(1)
	}
	return clientset
}
