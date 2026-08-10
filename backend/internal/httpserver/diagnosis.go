// backend/internal/httpserver/diagnosis.go
package httpserver

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"sre-platform/backend/internal/analyze"
)

// DiagnosisReceiver is satisfied by *reconcile.Reconciler — defined here,
// at the point of use, rather than importing the concrete type, matching
// this codebase's existing pattern (see reconcile.PodRestarter).
type DiagnosisReceiver interface {
	OnDiagnosis(ctx context.Context, incidentID string, diag analyze.Diagnosis)
}

type diagnosisRequest struct {
	FailureMode       string  `json:"failure_mode"`
	RecommendedAction string  `json:"recommended_action"`
	Confidence        float64 `json:"confidence"`
}

// diagnosisHandler is deliberately the one place that enforces ai/'s output
// vocabulary — restart_pod or none, nothing else — rather than trusting
// ai/'s prompt/rules alone (see docs/superpowers/specs/2026-08-08-ai-diagnosis-service-design.md §5).
func diagnosisHandler(receiver DiagnosisReceiver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		incidentID := chi.URLParam(r, "id")
		if incidentID == "" {
			http.Error(w, "missing incident id", http.StatusBadRequest)
			return
		}

		var req diagnosisRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			slog.Warn("diagnosis callback: bad request body", "incident_id", incidentID, "error", err)
			http.Error(w, "bad request body", http.StatusBadRequest)
			return
		}
		if req.RecommendedAction != "restart_pod" && req.RecommendedAction != "none" {
			slog.Warn("diagnosis callback: rejected out-of-vocabulary recommended_action", "incident_id", incidentID, "recommended_action", req.RecommendedAction)
			http.Error(w, `recommended_action must be "restart_pod" or "none"`, http.StatusBadRequest)
			return
		}

		slog.Info("diagnosis callback received", "incident_id", incidentID, "failure_mode", req.FailureMode, "recommended_action", req.RecommendedAction, "confidence", req.Confidence)
		// context.WithoutCancel strips the parent's cancellation/deadline
		// while keeping any request-scoped values — OnDiagnosis can run for
		// up to VERIFY_TIMEOUT_SECONDS (default 60s) inside executeAndVerify's
		// health-check poll, which must not be aborted just because ai/'s
		// own HTTP client (a much shorter timeout) gives up and disconnects.
		receiver.OnDiagnosis(context.WithoutCancel(r.Context()), incidentID, analyze.Diagnosis{
			FailureMode:       req.FailureMode,
			RecommendedAction: req.RecommendedAction,
			Confidence:        req.Confidence,
		})
		w.WriteHeader(http.StatusOK)
	}
}
