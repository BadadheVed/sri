// backend/internal/httpserver/router.go
package httpserver

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"sre-platform/backend/internal/mcpauth"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

// NewRouter builds backend/'s entire HTTP surface, grouped by prefix, so
// main.go never defines a route directly. New route groups attach here.
func NewRouter(slackClient *slackapproval.Client, s store.Store, diagnosisReceiver DiagnosisReceiver, diagnosisCallbackToken string) http.Handler {
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

	return r
}
