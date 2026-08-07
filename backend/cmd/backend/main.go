// backend/cmd/backend/main.go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"sre-platform/backend/internal/httpserver"
	"sre-platform/backend/internal/k8swatch"
	"sre-platform/backend/internal/mcpexecute"
	"sre-platform/backend/internal/reconcile"
	"sre-platform/backend/internal/settings"
	"sre-platform/backend/internal/signal"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

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

	reconciler := reconcile.New(pgStore, restarter, slackClient, clientset, cfg.Mode, cfg.CorrelationWindow, cfg.VerifyTimeout)
	watcher := k8swatch.NewWatcher(clientset, func(s signal.Signal) { reconciler.OnSignal(ctx, s) })

	router := httpserver.NewRouter(slackClient, pgStore)
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
