// backend/internal/httpserver/router.go
package httpserver

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

// NewRouter builds backend/'s entire HTTP surface, grouped by prefix, so
// main.go never defines a route directly. New route groups (a future REST
// API for the dashboard, additional webhooks) attach here.
func NewRouter(slackClient *slackapproval.Client, s store.Store) http.Handler {
	r := chi.NewRouter()

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	r.Route("/slack", func(r chi.Router) {
		r.Post("/interactions", slackClient.InteractionHandler(s))
	})

	return r
}
