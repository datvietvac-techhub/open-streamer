package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

// serverError logs a 500-class failure with full request + error context, then
// returns the underlying err message to the caller. This is the single place
// that bridges "server-side error" → "API response" so the wrapped error chain
// is preserved in BOTH places (logs for ops, response body for the API client).
//
// op is a short imperative label of what was being attempted (e.g. "save stream",
// "list hooks") — it shows up as the log message and helps grep.
func serverError(w http.ResponseWriter, r *http.Request, code, op string, err error) {
	slog.Error("api: "+op+" failed",
		"code", code,
		"method", r.Method,
		"path", r.URL.Path,
		"err", err,
	)
	writeError(w, http.StatusInternalServerError, code, err.Error())
}

func writeStoreError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "NOT_FOUND", err.Error())
		return
	}
	serverError(w, r, "STORE_ERROR", "store operation", err)
}
