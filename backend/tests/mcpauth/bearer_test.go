// backend/tests/mcpauth/bearer_test.go
package mcpauth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"sre-platform/backend/internal/mcpauth"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func TestRequireBearerToken_AllowsMatchingToken(t *testing.T) {
	handler := mcpauth.RequireBearerToken("secret-token", okHandler())

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestRequireBearerToken_RejectsWrongToken(t *testing.T) {
	handler := mcpauth.RequireBearerToken("secret-token", okHandler())

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestRequireBearerToken_RejectsMissingHeader(t *testing.T) {
	handler := mcpauth.RequireBearerToken("secret-token", okHandler())

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}
