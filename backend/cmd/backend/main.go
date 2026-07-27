// backend/cmd/backend/main.go
package main

import (
	"context"
	"log"
	"net/http"

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
	cfg := settings.Load()
	ctx := context.Background()

	clientset := buildClientset(cfg.Kubeconfig)
	pgStore, err := store.NewPostgresStore(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connecting to Postgres: %v", err)
	}
	slackClient := slackapproval.NewClient(cfg.SlackBotToken, cfg.SlackApprovalChannel, cfg.SlackSigningSecret, http.DefaultClient)
	restarter, err := mcpexecute.NewClient(ctx, cfg.MCPExecuteURL, cfg.MCPExecuteToken)
	if err != nil {
		log.Fatalf("connecting to mcp-execute-server: %v", err)
	}

	reconciler := reconcile.New(pgStore, restarter, slackClient, clientset, cfg.Mode, cfg.CorrelationWindow, cfg.VerifyTimeout)
	watcher := k8swatch.NewWatcher(clientset, func(s signal.Signal) { reconciler.OnSignal(ctx, s) })

	router := httpserver.NewRouter(slackClient, pgStore)
	go func() {
		log.Printf("listening on %s", cfg.HTTPAddr)
		log.Fatal(http.ListenAndServe(cfg.HTTPAddr, router))
	}()

	if err := watcher.Run(ctx); err != nil {
		log.Fatalf("watcher.Run: %v", err)
	}
}

func buildClientset(kubeconfig string) kubernetes.Interface {
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("building kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Fatalf("building clientset: %v", err)
	}
	return clientset
}
