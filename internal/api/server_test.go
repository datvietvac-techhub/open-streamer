package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"

	"github.com/datvietvac-techhub/open-streamer/config"
)

func TestHealthzReturnsOK(t *testing.T) {
	w := httptest.NewRecorder()
	healthz(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusOK {
		t.Errorf("status=%d", w.Code)
	}
	if w.Body.String() != "ok" {
		t.Errorf("body=%q", w.Body.String())
	}
}

func TestReadyzReturnsOK(t *testing.T) {
	w := httptest.NewRecorder()
	readyz(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusOK || w.Body.String() != "ok" {
		t.Errorf("unexpected: %d %q", w.Code, w.Body.String())
	}
}

func TestSkipMediaLoggerBypassesMediaExtensions(t *testing.T) {
	var loggerCalls, nextCalls int
	logger := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			loggerCalls++
			next.ServeHTTP(w, r)
		})
	}
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { nextCalls++ })
	wrapped := skipMediaLogger(logger)(next)

	mediaPaths := []string{"/live/index.m3u8", "/live/seg1.ts", "/live/init.m4s", "/live/index.mpd", "/live/clip.mp4"}
	for _, p := range mediaPaths {
		wrapped.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, p, nil))
	}
	if loggerCalls != 0 {
		t.Errorf("logger should not run for media paths, ran %d times", loggerCalls)
	}
	if nextCalls != len(mediaPaths) {
		t.Errorf("next must still run for media paths, ran %d, want %d", nextCalls, len(mediaPaths))
	}
}

func TestSkipMediaLoggerLogsNonMediaPaths(t *testing.T) {
	var loggerCalls int
	logger := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			loggerCalls++
			next.ServeHTTP(w, r)
		})
	}
	wrapped := skipMediaLogger(logger)(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))

	for _, p := range []string{"/streams", "/streams/abc", "/healthz", "/config"} {
		wrapped.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, p, nil))
	}
	if loggerCalls != 4 {
		t.Errorf("logger should run for non-media paths, ran %d", loggerCalls)
	}
}

func TestCorsOptionsDefaults(t *testing.T) {
	opts := corsOptions(config.CORSConfig{})
	if len(opts.AllowedOrigins) != 1 || opts.AllowedOrigins[0] != "*" {
		t.Errorf("default origins should be [*], got %v", opts.AllowedOrigins)
	}
	if len(opts.AllowedMethods) == 0 {
		t.Error("default methods must be set")
	}
	if len(opts.AllowedHeaders) == 0 {
		t.Error("default headers must be set")
	}
}

func TestCorsOptionsExplicitValues(t *testing.T) {
	cfg := config.CORSConfig{
		AllowedOrigins: []string{"https://a.com", "https://b.com"},
		AllowedMethods: []string{"GET", "POST"},
		AllowedHeaders: []string{"X-Foo"},
		ExposedHeaders: []string{"X-Bar"},
		MaxAge:         600,
	}
	opts := corsOptions(cfg)
	if len(opts.AllowedOrigins) != 2 {
		t.Errorf("origins=%v", opts.AllowedOrigins)
	}
	if opts.MaxAge != 600 {
		t.Errorf("max-age=%d", opts.MaxAge)
	}
}

func TestCorsOptionsDisablesCredentialsForWildcard(t *testing.T) {
	opts := corsOptions(config.CORSConfig{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
	})
	if opts.AllowCredentials {
		t.Error("AllowCredentials must be forced false when origin is *")
	}
}

func TestCorsOptionsKeepsCredentialsForExplicitOrigins(t *testing.T) {
	opts := corsOptions(config.CORSConfig{
		AllowedOrigins:   []string{"https://a.com"},
		AllowCredentials: true,
	})
	if !opts.AllowCredentials {
		t.Error("AllowCredentials must stay true for explicit origins")
	}
}

func TestServeSwaggerIndexEmitsHTML(t *testing.T) {
	w := httptest.NewRecorder()
	serveSwaggerIndex(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/swagger/", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Errorf("content-type=%s", got)
	}
	if !strings.Contains(w.Body.String(), "swagger-ui") {
		t.Errorf("body missing swagger-ui marker")
	}
}

// Sanity check: corsOptions output is consumable by the cors middleware
// without panicking — guards against future field renames in cors lib.
func TestCorsOptionsBuildsValidMiddleware(t *testing.T) {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(cors.Handler(corsOptions(config.CORSConfig{
		AllowedOrigins: []string{"https://example.com"},
	})))
	r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	r.ServeHTTP(w, req)
	if w.Code >= 500 {
		t.Fatalf("preflight returned 5xx: %d", w.Code)
	}
}
