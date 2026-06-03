package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/api/handler"
	"github.com/datvietvac-techhub/open-streamer/internal/metrics"
)

// newRouterTestServer assembles a Server with bare-bones handlers — enough
// for buildRouter to wire every route. Routes that need a real handler
// body must not be hit by callers; this fixture is intended for routes
// that don't dereference the handler (healthz, metrics, swagger,
// 404 fall-through). Each test case picks routes accordingly.
func newRouterTestServer() *Server {
	return &Server{
		hlsDir:     "./hls",
		dashDir:    "./dash",
		streamH:    &handler.StreamHandler{},
		recordingH: &handler.RecordingHandler{},
		hookH:      &handler.HookHandler{},
		configH:    &handler.ConfigHandler{},
		vodH:       &handler.VODHandler{},
		sessionH:   &handler.SessionHandler{},
		watermarkH: &handler.WatermarkHandler{},
	}
}

// ─── buildRouter ─────────────────────────────────────────────────────────────

func TestBuildRouter_Healthz(t *testing.T) {
	t.Parallel()
	s := newRouterTestServer()
	r := s.buildRouter(&config.ServerConfig{})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "ok", w.Body.String())
}

func TestBuildRouter_Readyz(t *testing.T) {
	t.Parallel()
	s := newRouterTestServer()
	r := s.buildRouter(&config.ServerConfig{})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusOK, w.Code)
}

func TestBuildRouter_Metrics(t *testing.T) {
	t.Parallel()
	s := newRouterTestServer()
	r := s.buildRouter(&config.ServerConfig{})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, w.Code)
	// promhttp emits text/plain by default for Prometheus scrapes.
	assert.Contains(t, w.Header().Get("Content-Type"), "text/plain")
}

func TestBuildRouter_SwaggerRedirect(t *testing.T) {
	t.Parallel()
	s := newRouterTestServer()
	r := s.buildRouter(&config.ServerConfig{})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/swagger", nil))
	assert.Equal(t, http.StatusMovedPermanently, w.Code)
	assert.Equal(t, "/swagger/", w.Header().Get("Location"))
}

func TestBuildRouter_SwaggerIndex(t *testing.T) {
	t.Parallel()
	s := newRouterTestServer()
	r := s.buildRouter(&config.ServerConfig{})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/swagger/", nil))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "swagger-ui")
}

func TestBuildRouter_UnknownRouteIs404(t *testing.T) {
	t.Parallel()
	s := newRouterTestServer()
	r := s.buildRouter(&config.ServerConfig{})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/this-does-not-exist", nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestBuildRouter_CORSEnabledPreflight(t *testing.T) {
	t.Parallel()
	s := newRouterTestServer()
	r := s.buildRouter(&config.ServerConfig{
		CORS: config.CORSConfig{
			Enabled:        true,
			AllowedOrigins: []string{"https://example.com"},
		},
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/healthz", nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	// CORS middleware short-circuits OPTIONS preflight with 204.
	if w.Code != http.StatusNoContent && w.Code != http.StatusOK {
		t.Errorf("preflight status=%d, expected 204 or 200", w.Code)
	}
	assert.Equal(t, "https://example.com", w.Header().Get("Access-Control-Allow-Origin"))
}

func TestBuildRouter_CORSDisabledStripsCORSHeaders(t *testing.T) {
	t.Parallel()
	s := newRouterTestServer()
	r := s.buildRouter(&config.ServerConfig{
		CORS: config.CORSConfig{Enabled: false},
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "https://example.com")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
}

// ─── httpDurationMiddleware ──────────────────────────────────────────────────

func TestHTTPDurationMiddleware_NilMetricsBypasses(t *testing.T) {
	t.Parallel()
	s := &Server{} // no metrics
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	wrapped := s.httpDurationMiddleware(next)

	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/whatever", nil))
	assert.True(t, called)
}

func TestHTTPDurationMiddleware_RecordsLatency(t *testing.T) {
	t.Parallel()
	// Use an isolated Prometheus registry so this test doesn't collide with
	// the default-registry metrics already wired by metrics.New under DI.
	reg := prometheus.NewRegistry()
	hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "test_http_duration",
	}, []string{"route", "method", "status"})
	require.NoError(t, reg.Register(hist))

	s := &Server{m: &metrics.Metrics{HTTPRequestDuration: hist}}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	r := newRouterTestServer().buildRouter(&config.ServerConfig{})
	// Replace healthz handler with our instrumented next via a fresh chi
	// router: simpler to just wrap directly.
	_ = r
	wrapped := s.httpDurationMiddleware(next)
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil))
	assert.Equal(t, http.StatusTeapot, w.Code)

	// Verify the histogram saw a sample.
	mfs, err := reg.Gather()
	require.NoError(t, err)
	if assert.Len(t, mfs, 1) {
		assert.Greater(t, len(mfs[0].GetMetric()), 0)
	}
}

func TestHTTPDurationMiddleware_BypassesScraperPaths(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "test_http_duration_bypass",
	}, []string{"route", "method", "status"})
	require.NoError(t, reg.Register(hist))

	s := &Server{m: &metrics.Metrics{HTTPRequestDuration: hist}}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	wrapped := s.httpDurationMiddleware(next)

	for _, p := range []string{"/metrics", "/healthz", "/readyz"} {
		w := httptest.NewRecorder()
		wrapped.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, p, nil))
	}

	// All three paths bypass — histogram MUST be empty.
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, m := range mfs {
		for _, sample := range m.GetMetric() {
			if sample.GetHistogram().GetSampleCount() > 0 {
				t.Errorf("scraper path produced a sample: %v", sample)
			}
		}
	}
}

// ─── slogAccessLogger ────────────────────────────────────────────────────────

// NOTE: the two slog-access-logger tests below mutate the package-global
// slog.Default(); any t.Parallel() sibling that exercises the router
// (TestBuildRouter_Readyz / SwaggerIndex / StartWithConfig…) would then
// fire the chi middleware chain into THIS test's strings.Builder,
// triggering a race (CI-reproduced: -shuffle=1778518171305235108).
// Run them serially — they each complete in <1 ms and the parallelism
// gain wasn't load-bearing.

func TestSlogAccessLogger_EmitsAtInfoLevel(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	buf := &strings.Builder{}
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})))

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	wrapped := slogAccessLogger(next)
	wrapped.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/streams", nil))

	out := buf.String()
	assert.Contains(t, out, "api: http request")
	assert.Contains(t, out, "method=GET")
	assert.Contains(t, out, "status=200")
}

func TestSlogAccessLogger_SuppressedWhenBelowLevel(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	buf := &strings.Builder{}
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelError})))

	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	wrapped := slogAccessLogger(next)
	wrapped.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/streams", nil))

	assert.True(t, called, "next must still run when logger is silenced")
	assert.Empty(t, buf.String(), "no log lines should be emitted at ERROR level")
}

// reserveEphemeralAddr binds to ":0" to grab a free port, then closes
// the listener and returns the address. Lint enforces context-aware
// listeners so we go through net.ListenConfig.
func reserveEphemeralAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// holdEphemeralAddr binds to ":0" and returns BOTH the listener (still
// open) and its address. The caller is responsible for closing the
// listener once the test asserts the port-collision behaviour.
func holdEphemeralAddr(t *testing.T) (net.Listener, string) {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	return ln, ln.Addr().String()
}

// ctxGet performs a context-aware HTTP GET — the linter rejects bare
// http.Get because it has no cancellation hook.
func ctxGet(t *testing.T, url string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

// ─── startPprofListener ──────────────────────────────────────────────────────

func TestStartPprofListener_EmptyAddrIsNoOp(t *testing.T) {
	t.Parallel()
	// Empty addr → returns immediately without starting any goroutine.
	// We can't directly assert "no goroutine" but a t.Context() that's
	// never cancelled would block any spawned listener forever — so the
	// fact that this test returns proves the no-op path was taken.
	startPprofListener(t.Context(), "")
}

func TestStartPprofListener_BindsAndShutsDown(t *testing.T) {
	t.Parallel()
	// Pick an ephemeral port to avoid colliding with any local pprof.
	addr := reserveEphemeralAddr(t)

	ctx, cancel := context.WithCancel(t.Context())
	startPprofListener(ctx, addr)

	// Poll until the listener is up. It runs on its own goroutine so we
	// need a small window; capping at 1s keeps the test snappy.
	deadline := time.Now().Add(time.Second)
	var (
		resp *http.Response
		err  error
	)
	for time.Now().Before(deadline) {
		resp, err = ctxGet(t, fmt.Sprintf("http://%s/debug/pprof/", addr))
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, err, "pprof listener never came up at %s", addr)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Cancelling the ctx triggers Shutdown; subsequent requests fail
	// quickly. We don't assert that the shutdown completes here — the
	// deferred goroutines clean up on their own.
	cancel()
}

// ─── StartWithConfig ─────────────────────────────────────────────────────────

func TestStartWithConfig_BindsAndShutsDownOnContextCancel(t *testing.T) {
	t.Parallel()
	s := newRouterTestServer()

	// Reserve an ephemeral port so the test never fights another server.
	addr := reserveEphemeralAddr(t)

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.StartWithConfig(ctx, &config.ServerConfig{HTTPAddr: addr})
	}()

	// Poll until /healthz responds.
	deadline := time.Now().Add(2 * time.Second)
	var (
		resp *http.Response
		err  error
	)
	for time.Now().Before(deadline) {
		resp, err = ctxGet(t, fmt.Sprintf("http://%s/healthz", addr))
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, err, "server never came up at %s", addr)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Cancelling the ctx triggers graceful shutdown — StartWithConfig
	// returns nil because http.ErrServerClosed is filtered.
	cancel()
	select {
	case err := <-errCh:
		// http.ErrServerClosed is filtered; any other error is a regression.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("StartWithConfig returned unexpected error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("StartWithConfig did not return after ctx cancel")
	}
}

func TestStartWithConfig_BindFailureSurfacesError(t *testing.T) {
	t.Parallel()
	// Bind a real listener to a port, then ask Server to bind the same one
	// → ListenAndServe must fail and StartWithConfig must propagate it.
	ln, addr := holdEphemeralAddr(t)
	defer func() { _ = ln.Close() }()

	s := newRouterTestServer()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := s.StartWithConfig(ctx, &config.ServerConfig{HTTPAddr: addr})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "api: server")
}

// ─── serveSwaggerJSON ────────────────────────────────────────────────────────

func TestServeSwaggerJSON(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	serveSwaggerJSON(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/swagger/doc.json", nil))
	// swag.ReadDoc reads from the global registry populated by the
	// generated api/docs init blob — that's imported via the blank import
	// in server.go, so this should always succeed in the test binary.
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")
}
