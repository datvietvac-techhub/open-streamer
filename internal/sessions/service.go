// Package sessions tracks live playback sessions across every protocol
// Open-Streamer serves (HLS, DASH, RTMP, SRT, RTSP) so operators can answer
// "who is watching <stream> right now?".
//
// Design rules:
//   - State is in-memory only. Restart = lost; viewers reconnect → new sessions.
//   - HTTP-based sessions (HLS/DASH) are keyed by a deterministic fingerprint
//     so consecutive segment GETs from one viewer collapse onto a single record.
//   - Connection-bound sessions (RTMP/SRT/RTSP) are keyed by a fresh UUID; the
//     transport layer signals close via the Closer returned from OpenConn.
//   - All open / close events are published on the event bus so analytics or
//     persistence can be added without changing this package.
package sessions

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"github.com/samber/do/v2"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/metrics"
)

// Service is the public DI handle. It implements Tracker.
type Service struct {
	*service

	// geoSwap is the SwappableGeoIP shared with the underlying service so
	// UpdateConfig can hot-reload the .mmdb file when geoip_db_path changes
	// in the persisted config. nil when DI did not register a *SwappableGeoIP
	// (e.g. NewServiceForTesting wires a plain GeoIPResolver) — UpdateConfig
	// then skips the reload step.
	geoSwap *SwappableGeoIP

	// geoMu protects geoPath against concurrent UpdateConfig calls. The
	// reload path is rare (only when the operator edits config) so the
	// mutex is uncontended in practice.
	geoMu   sync.Mutex
	geoPath string // last successfully-loaded mmdb path; "" = NullGeoIP
}

// New constructs the Service for samber/do. *SwappableGeoIP is optional in
// DI — when no provider is registered, the service runs with a fresh
// NullGeoIP-backed swappable so UpdateConfig can later promote it to a
// real backend without restarting. Metrics is also optional so unit tests
// that wire only sessions can construct the service.
func New(i do.Injector) (*Service, error) {
	cfg := do.MustInvoke[config.SessionsConfig](i)
	bus := do.MustInvoke[events.Bus](i)

	swap, err := do.Invoke[*SwappableGeoIP](i)
	if err != nil {
		swap = NewSwappableGeoIP()
	}

	svc := newService(cfg, bus, swap)
	if m, err := do.Invoke[*metrics.Metrics](i); err == nil {
		svc.m = &metricsHooks{
			active: m.SessionsActive,
			opened: m.SessionsOpenedTotal,
			closed: m.SessionsClosedTotal,
		}
	}
	out := &Service{service: svc, geoSwap: swap}
	out.geoPath = strings.TrimSpace(cfg.GeoIPDBPath)
	return out, nil
}

// Run starts the idle reaper. Always runs — the reaper itself checks the
// hot-reloadable enabled flag each tick and skips reaping when disabled.
// This way an operator toggling Enabled at runtime via /config takes effect
// without restarting the goroutine. Blocks until ctx is cancelled, then
// closes every still-active session with reason=shutdown so subscribers
// get a final event.
func (s *Service) Run(ctx context.Context) {
	s.runReaper(ctx)
}

// UpdateConfig hot-swaps the runtime config. Called by runtime.Manager.diff
// when the persisted SessionsConfig section changes — no restart needed.
// In-flight sessions keep their state; new idle/max-lifetime windows take
// effect on the next reaper tick.
//
// Also handles GeoIP DB hot-reload: when geoip_db_path differs from the
// path loaded at boot (or by a previous UpdateConfig), the new file is
// opened and published via SwappableGeoIP.Set. Open failures are logged
// at WARN and leave the previous resolver in place — refusing to update
// any other session config because the GeoIP file moved would be more
// disruptive than degrading Country to "" for a misconfigured run.
func (s *Service) UpdateConfig(cfg config.SessionsConfig) {
	s.applyConfig(cfg)
	s.maybeReloadGeoIP(strings.TrimSpace(cfg.GeoIPDBPath))
}

// maybeReloadGeoIP attempts to swap the GeoIP backend when the configured
// path changes. No-op when the path is unchanged or when the service was
// constructed without a SwappableGeoIP (NewServiceForTesting).
func (s *Service) maybeReloadGeoIP(newPath string) {
	if s.geoSwap == nil {
		return
	}
	s.geoMu.Lock()
	defer s.geoMu.Unlock()
	if newPath == s.geoPath {
		return
	}
	if newPath == "" {
		slog.Info("geoip: hot-reload disabling resolver (geoip_db_path cleared); PlaySession.Country will be empty",
			"prev_path", s.geoPath)
		s.geoSwap.Set(NullGeoIP{})
		s.geoPath = ""
		return
	}
	mm, err := NewMaxMindGeoIP(newPath)
	if err != nil {
		slog.Warn("geoip: hot-reload failed; keeping previous resolver",
			"prev_path", s.geoPath, "new_path", newPath, "err", err)
		return
	}
	slog.Info("geoip: hot-reloaded MaxMind reader",
		"prev_path", s.geoPath, "new_path", newPath)
	s.geoSwap.Set(mm) // Set closes the previous mmap'd file
	s.geoPath = newPath
}

// NewServiceForTesting builds a Service without going through DI — for unit
// tests that need a real Tracker but don't want a do.Injector. Bus and
// resolver may be nil. Hot-reload of GeoIP is disabled in this path
// (UpdateConfig only refreshes timeouts, not the resolver).
func NewServiceForTesting(cfg config.SessionsConfig, bus events.Bus, geo GeoIPResolver) *Service {
	if geo == nil {
		geo = NullGeoIP{}
	}
	return &Service{service: newService(cfg, bus, geo)}
}
