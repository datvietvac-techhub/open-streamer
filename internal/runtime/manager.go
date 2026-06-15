// Package runtime manages the lifecycle of all long-running services based on
// the persisted GlobalConfig.  Each service is independently startable and
// stoppable; the Manager diffs old vs new config and applies changes surgically.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/api"
	"github.com/datvietvac-techhub/open-streamer/internal/autopublish"
	"github.com/datvietvac-techhub/open-streamer/internal/coordinator"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/hooks"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor"
	"github.com/datvietvac-techhub/open-streamer/internal/manager"
	"github.com/datvietvac-techhub/open-streamer/internal/publisher"
	"github.com/datvietvac-techhub/open-streamer/internal/sessions"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
	"github.com/datvietvac-techhub/open-streamer/internal/transcoder"
	"github.com/datvietvac-techhub/open-streamer/pkg/logger"
)

// serviceEntry tracks one running service goroutine.
type serviceEntry struct {
	cancel context.CancelFunc
	done   chan struct{} // closed when the goroutine exits
}

// Deps holds all service references needed by the Manager.
// Populated by main.go from the DI injector.
type Deps struct {
	Ingestor         *ingestor.Service
	Publisher        *publisher.Service
	Coordinator      *coordinator.Coordinator
	Transcoder       *transcoder.Service
	StreamMgr        *manager.Service
	HooksSvc         *hooks.Service
	SessionsSvc      *sessions.Service
	AutoPublish      *autopublish.Service
	APISrv           *api.Server
	Bus              events.Bus
	StreamRepo       store.StreamRepository
	GlobalConfigRepo store.GlobalConfigRepository
}

// Manager owns the lifecycle of all long-running services.
// It reads GlobalConfig from the store and starts/stops services accordingly.
type Manager struct {
	deps Deps

	mu       sync.Mutex
	services map[string]*serviceEntry
	current  *domain.GlobalConfig

	// rootCtx is the top-level application context (cancelled on SIGINT/SIGTERM).
	rootCtx context.Context
}

// New creates a Manager.  Call Bootstrap() to load config and start services.
func New(ctx context.Context, deps Deps) *Manager {
	return &Manager{
		deps:     deps,
		services: make(map[string]*serviceEntry),
		rootCtx:  ctx,
	}
}

// Bootstrap loads GlobalConfig from the store (seeding defaults on first boot),
// then starts all configured services.
func (m *Manager) Bootstrap() error {
	gcfg, err := m.loadOrSeed()
	if err != nil {
		return fmt.Errorf("runtime: bootstrap: %w", err)
	}

	m.mu.Lock()
	m.current = gcfg
	m.mu.Unlock()

	m.applyAll(gcfg)
	return nil
}

// BootstrapWith starts all configured services using the provided GlobalConfig.
// Use this when main.go has already loaded/seeded the config from the store.
func (m *Manager) BootstrapWith(gcfg *domain.GlobalConfig) {
	m.mu.Lock()
	m.current = gcfg
	m.mu.Unlock()

	m.applyAll(gcfg)
}

// CurrentConfig returns a copy of the active GlobalConfig.
func (m *Manager) CurrentConfig() *domain.GlobalConfig {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}

// Apply saves a new GlobalConfig to the store and diffs against the current
// config to start/stop services as needed.
func (m *Manager) Apply(ctx context.Context, newCfg *domain.GlobalConfig) error {
	if err := m.deps.GlobalConfigRepo.Set(ctx, newCfg); err != nil {
		return fmt.Errorf("runtime: save config: %w", err)
	}

	m.mu.Lock()
	old := m.current
	m.current = newCfg
	m.mu.Unlock()

	m.diff(old, newCfg) //nolint:contextcheck // services derive context from rootCtx, not the request ctx
	return nil
}

// WaitAll blocks until all running services have exited.
func (m *Manager) WaitAll() {
	m.mu.Lock()
	entries := make([]*serviceEntry, 0, len(m.services))
	for _, e := range m.services {
		entries = append(entries, e)
	}
	m.mu.Unlock()
	for _, e := range entries {
		<-e.done
	}
}

// --- internal ---

func (m *Manager) loadOrSeed() (*domain.GlobalConfig, error) {
	gcfg, err := m.deps.GlobalConfigRepo.Get(context.Background())
	if err == nil {
		slog.Info("runtime: loaded global config from store")
		return gcfg, nil
	}
	if errors.Is(err, store.ErrNotFound) {
		slog.Info("runtime: no global config in store, starting unconfigured")
		return &domain.GlobalConfig{}, nil
	}
	return nil, fmt.Errorf("read stored config: %w", err)
}

// applyAll starts all services that are configured (initial boot).
func (m *Manager) applyAll(cfg *domain.GlobalConfig) {
	if cfg.Server != nil {
		m.startService("http", func(ctx context.Context) error {
			return m.deps.APISrv.StartWithConfig(ctx, cfg.Server)
		})
	}

	// Ingestor owns the shared RTMP listener. Even when no other ingestor
	// settings are configured, having listeners.rtmp.enabled is reason enough
	// to start it so external play clients work too.
	if cfg.Ingestor != nil || rtmpListenerEnabled(cfg.Listeners) {
		m.startService("ingestor", func(ctx context.Context) error {
			return m.deps.Ingestor.Run(ctx)
		})
	}

	if rtspListenerEnabled(cfg.Listeners) {
		m.startService("pub_rtsp", func(ctx context.Context) error {
			return m.deps.Publisher.RunRTSPPlayServer(ctx)
		})
	}
	if srtListenerEnabled(cfg.Listeners) {
		m.startService("pub_srt", func(ctx context.Context) error {
			return m.deps.Publisher.RunSRTPlayServer(ctx)
		})
	}

	if cfg.Hooks != nil {
		m.startService("hooks", func(ctx context.Context) error {
			return m.deps.HooksSvc.Start(ctx)
		})
	}

	// Sessions tracker: idle reaper runs only when the feature is enabled in
	// config. Tracker.Run is a no-op (and returns immediately) when disabled,
	// so it's safe to spawn unconditionally — but we gate on the section
	// being present to keep the service list honest.
	if cfg.Sessions != nil && m.deps.SessionsSvc != nil {
		m.startService("sessions", func(ctx context.Context) error {
			m.deps.SessionsSvc.Run(ctx)
			return nil
		})
	}

	// Event bus is always running (lightweight).
	events.Start(m.rootCtx, m.deps.Bus)

	// Wire ingestor → publisher RTMP play handler so the shared listener can
	// serve external play clients in addition to push ingest.
	m.deps.Ingestor.SetRTMPPlayHandler(m.deps.Publisher.HandleRTMPPlay)

	// Auto-publish: wire the resolver into the RTMP push server so a
	// template-prefix match can materialise a runtime stream from an
	// otherwise-unknown publish path. The matcher snapshot must be warm
	// before encoders can connect, otherwise the first wave of pushes
	// would race against the first refresh and get rejected.
	if m.deps.AutoPublish != nil {
		m.deps.AutoPublish.SetCoordinator(m.deps.Coordinator)
		if err := m.deps.AutoPublish.RefreshTemplates(m.rootCtx); err != nil {
			slog.Warn("runtime: autopublish initial template refresh failed", "err", err)
		}
		m.deps.Ingestor.SetAutoPublishResolver(m.deps.AutoPublish)
		go m.deps.AutoPublish.RunReaper(m.rootCtx)
	}

	// Bootstrap persisted streams.
	coordinator.BootstrapPersistedStreams(
		m.rootCtx, slog.Default(), m.deps.StreamRepo, m.deps.Coordinator,
	)

	// Reconciler: re-asserts "every enabled stream is running" on a timer.
	// Recovers from any path that left a stream stopped against the
	// operator's intent — bootstrap Start failures (transient source
	// outages), restart errors, or create-handler edge cases.
	go m.deps.Coordinator.RunReconciler(m.rootCtx)
}

// diff compares old and new configs and starts/stops services as needed.
func (m *Manager) diff(old, new *domain.GlobalConfig) {
	// HTTP server
	m.diffService("http", old.Server != nil, new.Server != nil,
		configChanged(old.Server, new.Server),
		func(ctx context.Context) error {
			return m.deps.APISrv.StartWithConfig(ctx, new.Server)
		})

	// API auth: hot-swap users/roles without restarting the HTTP server. The
	// middleware reads the new set atomically on its next request.
	if m.deps.APISrv != nil && configChanged(old.Auth, new.Auth) {
		m.deps.APISrv.SetAuthConfig(new.Auth)
	}

	// Push the new listeners snapshot to ingestor + publisher BEFORE
	// diffService can decide to restart them — each Run() reads the cached
	// listeners value at startup, so the swap must happen before the new
	// goroutine is spawned. When the listener config is unchanged this is
	// a cheap atomic store; when it IS changed, diffService also restarts
	// the service so the new bind address takes effect.
	if configChanged(old.Listeners, new.Listeners) {
		listeners := config.ListenersConfig{}
		if new.Listeners != nil {
			listeners = *new.Listeners
		}
		if m.deps.Ingestor != nil {
			m.deps.Ingestor.SetListeners(listeners)
		}
		if m.deps.Publisher != nil {
			m.deps.Publisher.SetListeners(listeners)
		}
	}

	// Ingestor — owns the shared RTMP listener. Restart on changes to either
	// IngestorConfig or listeners.RTMP, and treat the listener being enabled
	// as sufficient reason to keep the service running even when IngestorConfig
	// is nil.
	wasIng := old.Ingestor != nil || rtmpListenerEnabled(old.Listeners)
	nowIng := new.Ingestor != nil || rtmpListenerEnabled(new.Listeners)
	ingChanged := configChanged(old.Ingestor, new.Ingestor) ||
		configChanged(rtmpListenerOf(old.Listeners), rtmpListenerOf(new.Listeners))
	m.diffService("ingestor", wasIng, nowIng, ingChanged,
		func(ctx context.Context) error {
			return m.deps.Ingestor.Run(ctx)
		})

	// Publisher RTSP listener
	oldRTSP := rtspListenerEnabled(old.Listeners)
	newRTSP := rtspListenerEnabled(new.Listeners)
	m.diffService("pub_rtsp", oldRTSP, newRTSP,
		configChanged(rtspListenerOf(old.Listeners), rtspListenerOf(new.Listeners)),
		func(ctx context.Context) error {
			return m.deps.Publisher.RunRTSPPlayServer(ctx)
		})

	// Publisher SRT listener
	oldSRT := srtListenerEnabled(old.Listeners)
	newSRT := srtListenerEnabled(new.Listeners)
	m.diffService("pub_srt", oldSRT, newSRT,
		configChanged(srtListenerOf(old.Listeners), srtListenerOf(new.Listeners)),
		func(ctx context.Context) error {
			return m.deps.Publisher.RunSRTPlayServer(ctx)
		})

	// Hooks
	m.diffService("hooks", old.Hooks != nil, new.Hooks != nil,
		configChanged(old.Hooks, new.Hooks),
		func(ctx context.Context) error {
			return m.deps.HooksSvc.Start(ctx)
		})

	// Sessions: hot-reload via UpdateConfig instead of bouncing the reaper —
	// the in-memory session map survives idle/timeout / enable changes so we
	// don't lose viewer attribution mid-edit. The reaper goroutine itself is
	// started once in applyAll; here we only need to push the new config and
	// (on first enable after boot) spawn the goroutine if it wasn't.
	if configChanged(old.Sessions, new.Sessions) && m.deps.SessionsSvc != nil {
		cfg := config.SessionsConfig{}
		if new.Sessions != nil {
			cfg = *new.Sessions
		}
		m.deps.SessionsSvc.UpdateConfig(cfg)
		slog.Info("runtime: sessions config applied",
			"enabled", cfg.Enabled,
			"idle_timeout_sec", cfg.IdleTimeoutSec,
			"max_lifetime_sec", cfg.MaxLifetimeSec,
		)
	}
	// First boot may not have started the reaper if cfg.Sessions was nil at
	// the time. If it's set now, ensure the goroutine is up.
	m.diffService("sessions", old.Sessions != nil, new.Sessions != nil, false,
		func(ctx context.Context) error {
			m.deps.SessionsSvc.Run(ctx)
			return nil
		})

	// Log — apply both slog and LAL (nazalog) levels via the shared helper
	// so a runtime config change actually silences LAL's per-RTMP-session
	// INFO chatter the way an operator expects. Calling slog.SetDefault
	// alone (the previous behaviour) left nazalog stuck at whatever level
	// was set at boot, which is the most common "I lowered log.level but
	// LAL keeps flooding journalctl" footgun.
	if configChanged(old.Log, new.Log) {
		if new.Log != nil {
			logger.Apply(*new.Log)
			slog.Info("runtime: log config applied", "level", new.Log.Level, "format", new.Log.Format)
		}
	}

	// Manager — packet-timeout threshold lives behind an atomic on
	// manager.Service, so SetConfig swaps the value and the next monitor
	// tick (≤ 2s) picks it up. No streams restarted, no readers reconnected:
	// only the silence-detection threshold changes.
	if configChanged(old.Manager, new.Manager) && m.deps.StreamMgr != nil {
		cfg := config.ManagerConfig{}
		if new.Manager != nil {
			cfg = *new.Manager
		}
		m.deps.StreamMgr.SetConfig(cfg)
		slog.Info("runtime: manager config applied",
			"input_packet_timeout_sec", cfg.InputPacketTimeoutSec,
		)
	}
}

// rtmpListenerEnabled reports whether the shared RTMP listener should run.
func rtmpListenerEnabled(l *config.ListenersConfig) bool {
	return l != nil && l.RTMP.Enabled && l.RTMP.Port > 0
}

// rtspListenerEnabled reports whether the publisher RTSP listener should run.
func rtspListenerEnabled(l *config.ListenersConfig) bool {
	return l != nil && l.RTSP.Enabled && l.RTSP.Port > 0
}

// srtListenerEnabled reports whether the shared SRT listener should run.
func srtListenerEnabled(l *config.ListenersConfig) bool {
	return l != nil && l.SRT.Enabled && l.SRT.Port > 0
}

// rtmpListenerOf returns the RTMP listener config (or nil) for diff comparison.
func rtmpListenerOf(l *config.ListenersConfig) *config.RTMPListenerConfig {
	if l == nil {
		return nil
	}
	r := l.RTMP
	return &r
}

// rtspListenerOf returns the RTSP listener config (or nil) for diff comparison.
func rtspListenerOf(l *config.ListenersConfig) *config.RTSPListenerConfig {
	if l == nil {
		return nil
	}
	r := l.RTSP
	return &r
}

// srtListenerOf returns the SRT listener config (or nil) for diff comparison.
func srtListenerOf(l *config.ListenersConfig) *config.SRTListenerConfig {
	if l == nil {
		return nil
	}
	r := l.SRT
	return &r
}

// diffService handles the transition for a single service:
//   - was off, now on → start
//   - was on, now off → stop
//   - was on, now on, config changed → restart (stop + start)
//   - unchanged → no-op
func (m *Manager) diffService(
	name string,
	wasOn, nowOn bool,
	changed bool,
	startFn func(context.Context) error,
) {
	switch {
	case !wasOn && nowOn:
		slog.Info("runtime: starting service", "service", name)
		m.startService(name, startFn)
	case wasOn && !nowOn:
		slog.Info("runtime: stopping service", "service", name)
		m.stopService(name)
	case wasOn && nowOn && changed:
		slog.Info("runtime: restarting service (config changed)", "service", name)
		m.stopService(name)
		m.startService(name, startFn)
	}
}

func (m *Manager) startService(name string, fn func(context.Context) error) {
	ctx, cancel := context.WithCancel(m.rootCtx)
	done := make(chan struct{})

	m.mu.Lock()
	// Stop any existing instance first.
	if old, ok := m.services[name]; ok {
		old.cancel()
		m.mu.Unlock()
		<-old.done
		m.mu.Lock()
	}
	m.services[name] = &serviceEntry{cancel: cancel, done: done}
	m.mu.Unlock()

	go func() {
		defer close(done)
		if err := fn(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("runtime: service exited with error", "service", name, "err", err)
		}
	}()
}

func (m *Manager) stopService(name string) {
	m.mu.Lock()
	e, ok := m.services[name]
	if ok {
		delete(m.services, name)
	}
	m.mu.Unlock()
	if ok {
		e.cancel()
		<-e.done
		slog.Info("runtime: service stopped", "service", name)
	}
}

// configChanged compares two config sections by value.
// Returns true when the values differ or one is nil and the other is not.
func configChanged(a, b any) bool {
	if a == nil && b == nil {
		return false
	}
	if a == nil || b == nil {
		return true
	}
	return !reflect.DeepEqual(a, b)
}
