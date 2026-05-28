// Package api implements the HTTP API server.
// All routes follow the REST conventions defined in api-conventions.mdc.
package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof" // registers /debug/pprof/* on a dedicated mux when PprofAddr is set
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	_ "github.com/ntt0601zcoder/open-streamer/api/docs" // swag Register(SwaggerInfo)
	"github.com/ntt0601zcoder/open-streamer/config"
	"github.com/ntt0601zcoder/open-streamer/internal/api/handler"
	"github.com/ntt0601zcoder/open-streamer/internal/metrics"
	"github.com/ntt0601zcoder/open-streamer/internal/publisher"
	"github.com/ntt0601zcoder/open-streamer/internal/sessions"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/samber/do/v2"
)

// Server is the HTTP API server.
type Server struct {
	hlsDir  string
	dashDir string

	// Handler references stored for router rebuilding on config change.
	streamH    *handler.StreamHandler
	templateH  *handler.TemplateHandler
	recordingH *handler.RecordingHandler
	hookH      *handler.HookHandler
	configH    *handler.ConfigHandler
	vodH       *handler.VODHandler
	sessionH   *handler.SessionHandler
	watermarkH *handler.WatermarkHandler

	// sessTracker is the shared play-sessions tracker. Used to wrap the
	// mediaserve mount with a tracking middleware so HLS / DASH segment
	// fetches are recorded as sessions. nil disables tracking entirely.
	sessTracker sessions.Tracker

	// pub serves the on-demand /<code>/mpegts endpoint by subscribing to
	// the playback buffer hub for the requested stream. nil when publisher
	// is not wired (e.g. minimal API-only test harness) — the route is
	// then skipped at mount time.
	pub *publisher.Service

	// m exposes the Prometheus metrics surface; nil when running tests
	// without DI. Used by the request-duration middleware below.
	m *metrics.Metrics

	router *chi.Mux
	http   *http.Server
}

// New creates a Server and registers it with the DI injector.
// The server is constructed but not started — call StartWithConfig to begin accepting connections.
func New(i do.Injector) (*Server, error) {
	pub := do.MustInvoke[config.PublisherConfig](i)

	dashDir := strings.TrimSpace(pub.DASH.Dir)
	if dashDir == "" {
		dashDir = "./dash"
	}

	s := &Server{
		hlsDir:     pub.HLS.Dir,
		dashDir:    dashDir,
		streamH:    do.MustInvoke[*handler.StreamHandler](i),
		templateH:  do.MustInvoke[*handler.TemplateHandler](i),
		recordingH: do.MustInvoke[*handler.RecordingHandler](i),
		hookH:      do.MustInvoke[*handler.HookHandler](i),
		configH:    do.MustInvoke[*handler.ConfigHandler](i),
		vodH:       do.MustInvoke[*handler.VODHandler](i),
		sessionH:   do.MustInvoke[*handler.SessionHandler](i),
		watermarkH: do.MustInvoke[*handler.WatermarkHandler](i),
	}
	// Tracker is optional: only wrap mediaserve when the sessions feature is
	// wired in DI. Treats a missing provider as "feature disabled" rather
	// than a startup error.
	if t, err := do.Invoke[*sessions.Service](i); err == nil {
		s.sessTracker = t
	}
	// Publisher is optional for the same reason — the MPEG-TS-over-HTTP
	// endpoint is skipped if it isn't wired.
	if pub, err := do.Invoke[*publisher.Service](i); err == nil {
		s.pub = pub
	}
	// Metrics is optional for the same reason — minimal harness tests
	// build the server without DI.
	if m, err := do.Invoke[*metrics.Metrics](i); err == nil {
		s.m = m
	}
	return s, nil
}

// httpDurationMiddleware records request latency into the
// open_streamer_http_request_duration_seconds histogram. Routes use chi
// route patterns (e.g. "/streams/{code}") rather than full paths so label
// cardinality stays bounded by the number of registered handlers, not by
// the number of unique stream codes / hook IDs / segment numbers in
// flight. Pre-promhttp metrics endpoint and /healthz both fast-path bypass
// the wrapper to avoid self-instrumenting the scraper.
func (s *Server) httpDurationMiddleware(next http.Handler) http.Handler {
	if s.m == nil || s.m.HTTPRequestDuration == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Self-scraping carries no useful signal and would skew percentile
		// dashboards downward (it's fast and constant-rate). /healthz and
		// /readyz are similarly noise — usually probed every 1-5s by a
		// load balancer.
		switch r.URL.Path {
		case "/metrics", "/healthz", "/readyz":
			next.ServeHTTP(w, r)
			return
		}
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()
		next.ServeHTTP(ww, r)
		// chi populates the route ctx during routing; reading after
		// ServeHTTP returns gives the matched template (or "" for 404).
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unknown"
		}
		s.m.HTTPRequestDuration.WithLabelValues(
			route,
			r.Method,
			fmt.Sprintf("%d", ww.Status()),
		).Observe(time.Since(start).Seconds())
	})
}

func (s *Server) buildRouter(serverCfg *config.ServerConfig) *chi.Mux {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	// middleware.RealIP is deprecated in chi v5.3 (vulnerable to X-Forwarded-For
	// spoofing — it mutates r.RemoteAddr from untrusted headers). Kept for now
	// because this deployment runs behind a trusted reverse proxy that sets the
	// forwarded headers. The safe migration to middleware.ClientIPFrom* +
	// GetClientIP (which never mutate r.RemoteAddr) is tracked in docs/todo.
	r.Use(middleware.RealIP) //nolint:staticcheck // trusted-proxy deploy; migration tracked in docs/todo
	if serverCfg.CORS.Enabled {
		r.Use(cors.Handler(corsOptions(serverCfg.CORS)))
	}
	r.Use(middleware.Recoverer)
	r.Use(skipMediaLogger(slogAccessLogger))
	r.Use(s.httpDurationMiddleware)
	r.Use(middleware.Timeout(120 * time.Second))

	r.Get("/healthz", healthz)
	r.Get("/readyz", readyz)
	r.Handle("/metrics", promhttp.Handler())
	r.Get("/config", s.configH.GetConfig)
	r.Post("/config", s.configH.UpdateConfig)
	r.Get("/config/defaults", s.configH.GetConfigDefaults)
	r.Post("/config/transcoder/probe", s.configH.ProbeTranscoder)
	r.Get("/config/yaml", s.configH.GetConfigYAML)
	r.Put("/config/yaml", s.configH.ReplaceConfigYAML)

	r.Get("/swagger/doc.json", serveSwaggerJSON)
	r.Get("/swagger", http.RedirectHandler("/swagger/", http.StatusMovedPermanently).ServeHTTP)
	r.Get("/swagger/", serveSwaggerIndex)

	// Admin: /streams. Stream codes may contain '/' (namespacing) so the
	// per-code routes are served by a catch-all dispatcher that parses
	// (code, action) from the suffix. See dispatch.go for the rules.
	r.Get("/streams", s.streamH.List)
	r.HandleFunc("/streams/*", s.dispatchStreamsSubpath())

	// Templates. Codes are single-segment (a-zA-Z0-9_-) so the chi
	// single-param routes are fine — no catch-all dispatcher needed.
	r.Route("/templates", func(r chi.Router) {
		r.Get("/", s.templateH.List)
		r.Route("/{code}", func(r chi.Router) {
			r.Get("/", s.templateH.Get)
			r.Post("/", s.templateH.Put)
			r.Delete("/", s.templateH.Delete)
		})
	})

	r.Route("/sessions", func(r chi.Router) {
		r.Get("/", s.sessionH.List)
		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", s.sessionH.Get)
			r.Delete("/", s.sessionH.Delete)
		})
	})

	r.Route("/hooks", func(r chi.Router) {
		r.Get("/", s.hookH.List)
		r.Post("/", s.hookH.Create)
		r.Route("/{hid}", func(r chi.Router) {
			r.Get("/", s.hookH.Get)
			r.Put("/", s.hookH.Update)
			r.Delete("/", s.hookH.Delete)
			r.Post("/test", s.hookH.Test)
		})
	})

	r.Route("/watermarks", func(r chi.Router) {
		r.Get("/", s.watermarkH.List)
		r.Post("/", s.watermarkH.Upload)
		r.Route("/{filename}", func(r chi.Router) {
			r.Get("/", s.watermarkH.Get)
			r.Delete("/", s.watermarkH.Delete)
			r.Get("/raw", s.watermarkH.Raw)
		})
	})

	r.Route("/vod", func(r chi.Router) {
		r.Get("/", s.vodH.List)
		r.Post("/", s.vodH.Create)
		r.Route("/{name}", func(r chi.Router) {
			r.Get("/", s.vodH.Get)
			r.Put("/", s.vodH.Update)
			r.Delete("/", s.vodH.Delete)
			r.Get("/files", s.vodH.ListFiles)
			r.Post("/files", s.vodH.UploadFile)
			r.Delete("/files/*", s.vodH.DeleteFile)
			r.Get("/raw/*", s.vodH.Raw)
		})
	})

	// Media: catch-all for /<code>/<tail>. Mounted last so all named admin
	// prefixes above (e.g. /streams, /sessions, /hooks) win on chi's trie.
	// The play-sessions middleware is scoped to this group so only media
	// fetches accrue session events — admin calls don't pollute the tracker.
	r.Group(func(mr chi.Router) {
		if s.sessTracker != nil {
			mr.Use(sessions.HTTPMiddleware(s.sessTracker))
		}
		mr.HandleFunc("/*", s.dispatchMedia())
	})

	return r
}

// slogAccessLogger is a chi-style access-log middleware that emits via slog
// at INFO level (one structured event per request). Replacing chi's
// middleware.Logger — which writes via the std `log` package and so
// bypasses slog level filtering — means setting `log.level: error` in the
// global config now actually silences these access lines as the operator
// expects. Format: "api: http request" with method/path/status/duration_ms
// and remote ip; chi's RequestID is included when present.
func slogAccessLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip the work entirely when slog will discard the line — saves
		// the ResponseWriter wrap + status capture on hot paths when the
		// operator runs at WARN/ERROR level.
		if !slog.Default().Enabled(r.Context(), slog.LevelInfo) {
			next.ServeHTTP(w, r)
			return
		}
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()
		next.ServeHTTP(ww, r)
		slog.Info("api: http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", ww.Status(),
			"bytes", ww.BytesWritten(),
			"duration_ms", time.Since(start).Milliseconds(),
			"remote", r.RemoteAddr,
			"request_id", middleware.GetReqID(r.Context()),
		)
	})
}

// skipMediaLogger wraps a logging middleware so that requests for HLS/DASH
// media files (.m3u8, .ts, .m4s, .mpd, .mp4) bypass the logger entirely.
func skipMediaLogger(logger func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	skip := map[string]bool{
		".m3u8": true,
		".ts":   true,
		".m4s":  true,
		".mpd":  true,
		".mp4":  true,
	}
	return func(next http.Handler) http.Handler {
		logged := logger(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if skip[strings.ToLower(filepath.Ext(r.URL.Path))] {
				next.ServeHTTP(w, r)
				return
			}
			logged.ServeHTTP(w, r)
		})
	}
}

func corsOptions(cfg config.CORSConfig) cors.Options {
	origins := cfg.AllowedOrigins
	if len(origins) == 0 {
		origins = []string{"*"}
	}
	methods := cfg.AllowedMethods
	if len(methods) == 0 {
		methods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
	}
	headers := cfg.AllowedHeaders
	if len(headers) == 0 {
		headers = []string{"Accept", "Authorization", "Content-Type", "X-Request-ID"}
	}
	allowCredentials := cfg.AllowCredentials
	if allowCredentials {
		for _, o := range origins {
			if o == "*" {
				allowCredentials = false
				break
			}
		}
	}
	opts := cors.Options{
		AllowedOrigins:   origins,
		AllowedMethods:   methods,
		AllowedHeaders:   headers,
		ExposedHeaders:   cfg.ExposedHeaders,
		AllowCredentials: allowCredentials,
	}
	if cfg.MaxAge > 0 {
		opts.MaxAge = cfg.MaxAge
	}
	return opts
}

// StartWithConfig builds the router from the given ServerConfig and begins accepting
// HTTP connections. Blocks until ctx is cancelled.
// Called by the RuntimeManager each time the HTTP service is (re)started.
func (s *Server) StartWithConfig(ctx context.Context, cfg *config.ServerConfig) error {
	s.router = s.buildRouter(cfg)
	s.http = &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: s.router,
	}

	// Optional pprof listener on a separate addr (typically 127.0.0.1:6060)
	// so heap / goroutine / CPU profiles are reachable for memory-leak
	// triage without exposing them on the public API port. Spawned on its
	// own goroutine — startup of the main HTTP server is the fail-fast
	// path; pprof failures are logged and ignored so a port collision on
	// 6060 doesn't take the whole API down.
	startPprofListener(ctx, strings.TrimSpace(cfg.PprofAddr))

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := s.http.Shutdown(shutCtx); err != nil {
			slog.Warn("api: HTTP shutdown", "err", err)
		}
	}()

	slog.Info("api: HTTP server listening", "addr", cfg.HTTPAddr)
	if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("api: server: %w", err)
	}
	return nil
}

// startPprofListener brings up a dedicated http.Server bound to addr that
// serves Go's net/http/pprof endpoints under /debug/pprof/*. No-op when
// addr is empty. Block + mutex profilers are also enabled so the captured
// snapshots cover the full diagnostic surface — both come with measurable
// overhead at high contention rates so the rates are tuned conservatively
// (block every 10ms; 1 in 100 mutex events).
//
// Security note: the endpoints expose goroutine stacks and full heap
// layouts. Bind to 127.0.0.1 (or a private interface fronted by a firewall)
// — never to 0.0.0.0 in production. The default sample config ships with
// PprofAddr empty so the listener is opt-in.
func startPprofListener(ctx context.Context, addr string) {
	if addr == "" {
		return
	}

	mux := http.NewServeMux()
	// Enumerate explicitly rather than relying on the side-effect mount
	// onto http.DefaultServeMux — keeps pprof off any other listener that
	// might inherit DefaultServeMux later, and surfaces the route set in
	// the source for grep'ability.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Block / mutex profilers default to disabled (rate=0). Turn them on
	// here so the captured profile actually contains samples — operators
	// chasing a leak won't think to set these manually before triggering
	// the snapshot. Rates are conservative trade-offs: high enough to see
	// real contention, low enough to not perturb the production hot path.
	runtime.SetBlockProfileRate(int(10 * time.Millisecond))
	runtime.SetMutexProfileFraction(100)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	go func() {
		slog.Info("api: pprof listener started", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Warn("api: pprof listener failed", "addr", addr, "err", err)
		}
	}()
}

// healthz liveness probe.
// @Summary Liveness
// @Description Returns 200 if the process is up.
// @Tags system
// @Success 200 {string} string "ok"
// @Router /healthz [get].
func healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// readyz readiness probe.
// @Summary Readiness
// @Description Returns 200 when the server accepts traffic (basic check).
// @Tags system
// @Success 200 {string} string "ok"
// @Router /readyz [get].
func readyz(w http.ResponseWriter, r *http.Request) {
	healthz(w, r)
}
