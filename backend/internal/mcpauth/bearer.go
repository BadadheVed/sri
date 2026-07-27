// backend/internal/mcpauth/bearer.go
package mcpauth

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// RequireBearerToken wraps an http.Handler so every request must present
// "Authorization: Bearer <token>" matching expectedToken, checked in
// constant time. This is the MCP server's own independent check of the
// backend/'s permission token — it does not trust backend/'s Gate decision
// alone (design doc §6, §8).
func RequireBearerToken(expectedToken string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) {
			http.Error(w, "missing bearer token", http.StatusUnauthorized)
			return
		}
		token := strings.TrimPrefix(auth, prefix)
		if subtle.ConstantTimeCompare([]byte(token), []byte(expectedToken)) != 1 {
			http.Error(w, "invalid bearer token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
