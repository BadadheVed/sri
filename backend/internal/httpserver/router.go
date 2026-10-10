// backend/internal/httpserver/router.go
package httpserver

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"sre-platform/backend/internal/mcpauth"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
	"sre-platform/backend/internal/topology"
)

// NewRouter builds backend/'s entire HTTP surface, grouped by prefix, so
// main.go never defines a route directly. New route groups attach here.
func NewRouter(slackClient *slackapproval.Client, s store.Store, diagnosisReceiver DiagnosisReceiver, diagnosisCallbackToken string, metricsHub *MetricsHub, mcpReadonlyToken string, topologyHub *topology.Hub, topologyWSToken string) http.Handler {
	r := chi.NewRouter()

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	r.Route("/slack", func(r chi.Router) {
		r.Post("/interactions", slackClient.InteractionHandler(s))
	})

	// /internal is called by ai/ only, never by an external client — bearer
	// token checked the same way mcp-execute-server checks its own, an
	// independent layer rather than trusting network placement alone.
	r.Route("/internal", func(r chi.Router) {
		r.Use(func(next http.Handler) http.Handler {
			return mcpauth.RequireBearerToken(diagnosisCallbackToken, next)
		})
		r.Post("/incidents/{id}/diagnosis", diagnosisHandler(diagnosisReceiver))
	})

	// /ws/metrics streams live per-route latency (p50/p95/p99) and
	// requests/sec, gated by the same MCPReadonlyToken already used for
	// read-only MCP access — reused deliberately (same "read-only
	// observability" trust tier) rather than minting a new token.
	//
	// Scope boundary, deliberate: a browser's native WebSocket API cannot
	// set a custom Authorization header on the upgrade handshake, so this
	// endpoint is only reachable today by header-capable clients
	// (service-to-service callers, test clients, or a future frontend-side
	// proxy that holds the token server-side). Solving browser-side token
	// delivery is frontend-consumption work, out of scope here.
	r.Route("/ws", func(r chi.Router) {
		r.With(func(next http.Handler) http.Handler {
			return mcpauth.RequireBearerToken(mcpReadonlyToken, next)
		}).Get("/metrics", metricsHub.ServeWS)

		// /ws/topology streams the live service graph for the namespaces a
		// client subscribes to. It sits OUTSIDE the bearer middleware and
		// instead authenticates ?token= against its own TOPOLOGY_WS_TOKEN
		// (constant-time compare, 401 before any upgrade) — deliberately
		// not MCPReadonlyToken, because this token is handed to a browser
		// and must be rotatable/revocable without touching MCP access.
		// With no token configured the route is not registered at all (404);
		// main logs that at startup.
		//
		// DEV-ONLY AUTH: browsers cannot set an Authorization header on a
		// WebSocket handshake, so the token travels in the query string,
		// where it can leak into access logs, proxies and browser history.
		// Production should replace this with a short-lived, single-use
		// ticket minted by an authenticated HTTP endpoint.
		if topologyHub != nil && topologyWSToken != "" {
			r.Method(http.MethodGet, "/topology", NewTopologyWSHandler(topologyHub, topologyWSToken))
		}
	})

	return r
}
