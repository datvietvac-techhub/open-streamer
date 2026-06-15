package autopublish

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samber/do/v2"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/metrics"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

// IdleTimeout is how long a runtime stream may go without a packet
// reaching its buffer hub before the reaper stops it. The value is
// intentionally short — runtime streams are tied to a single live
// push session; idling that long means the encoder is gone or the
// network has died.
const IdleTimeout = 30 * time.Second

// reapInterval is how often the reaper sweeps for idle runtime streams.
// A fraction of IdleTimeout so the worst-case delay from "encoder gone"
// to "stream torn down" is bounded at ~IdleTimeout + reapInterval.
const reapInterval = 5 * time.Second

// streamCoordinator narrows *coordinator.Coordinator to the methods this
// service needs. Defined as an interface so unit tests can swap in a
// minimal fake instead of wiring the full pipeline graph.
type streamCoordinator interface {
	Start(ctx context.Context, stream *domain.Stream) error
	Stop(ctx context.Context, code domain.StreamCode)
	IsRunning(code domain.StreamCode) bool
}

// Service is the per-process auto-publish orchestrator. It owns the
// in-memory matcher snapshot, the runtime stream registry, and the
// idle-eviction loop.
type Service struct {
	templates store.TemplateRepository
	coord     streamCoordinator
	buf       *buffer.Service
	bus       events.Bus
	m         *metrics.Metrics // optional; nil in tests without DI

	mu      sync.RWMutex
	entries map[domain.StreamCode]*runtimeEntry
	cur     atomic.Pointer[matcher] // hot-swappable matcher snapshot
}

// runtimeEntry tracks one live runtime stream. lastPacketAt is updated
// by the liveness-observer goroutine and read by the reaper; both go
// through atomic.Int64 for race-free access without a per-entry lock.
type runtimeEntry struct {
	code         domain.StreamCode
	templateCode domain.TemplateCode
	lastPacketAt atomic.Int64 // unix nano
	cancelObs    context.CancelFunc
}

// New constructs an autopublish Service and registers it with DI.
func New(i do.Injector) (*Service, error) {
	s := &Service{
		templates: do.MustInvoke[store.TemplateRepository](i),
		buf:       do.MustInvoke[*buffer.Service](i),
		bus:       do.MustInvoke[events.Bus](i),
		entries:   make(map[domain.StreamCode]*runtimeEntry),
	}
	// Coordinator may not be registered yet during DI bootstrap when the
	// graph cycles through buf → coord → autopublish. Plumb it explicitly
	// via SetCoordinator after the rest of the graph is up.
	if c, err := do.Invoke[streamCoordinator](i); err == nil {
		s.coord = c
	}
	if m, err := do.Invoke[*metrics.Metrics](i); err == nil {
		s.m = m
	}
	s.cur.Store(newMatcher(nil))
	return s, nil
}

// SetCoordinator wires the coordinator dependency after construction.
// Required because the autopublish service is constructed BEFORE the
// coordinator in the DI graph (coordinator depends on stores; auto-
// publish depends on coordinator + stores → coord depends on auto-
// publish? It does not — but this setter keeps the graph acyclic and
// the wiring explicit).
func (s *Service) SetCoordinator(c streamCoordinator) {
	s.coord = c
}

// RefreshTemplates rebuilds the matcher snapshot from the template
// repository. Call after every template create/update/delete so prefix
// changes propagate. Errors are returned for the caller to log; the
// matcher is only swapped on a successful list.
func (s *Service) RefreshTemplates(ctx context.Context) error {
	tpls, err := s.templates.List(ctx)
	if err != nil {
		return fmt.Errorf("autopublish: list templates: %w", err)
	}
	s.cur.Store(newMatcher(tpls))
	return nil
}

// Match returns the template code whose prefix accepts path, or false
// when no template owns this path. The lookup is lock-free against the
// atomic.Pointer snapshot, so it is safe to call from the push-server
// goroutine without contending with template-handler updates.
func (s *Service) Match(path string) (domain.TemplateCode, bool) {
	m := s.cur.Load()
	if m == nil {
		return "", false
	}
	return m.match(path)
}

// ErrNoMatch is returned by ResolveOrCreate when no template prefix
// accepts the path. The push server should reject the encoder with a
// "stream not registered" status.
var ErrNoMatch = errors.New("autopublish: no template matches path")

// ErrTemplateNoPush is returned when a matching template lacks any
// publish:// input. Without an input source there is nothing for the
// runtime stream to subscribe to, so we refuse rather than spin up a
// dead pipeline.
var ErrTemplateNoPush = errors.New("autopublish: matching template has no publish:// input")

// ErrUnauthorized is returned when the matching template requires a StreamKey
// and the pusher's supplied secret does not match (S-8). Checked before any
// runtime stream is materialised, so an unauthorised push never spins a
// pipeline.
var ErrUnauthorized = errors.New("autopublish: stream key mismatch")

// ResolveOrCreate looks up (and, if necessary, creates) the runtime
// stream whose code is path. Returns the resolved stream code (always
// equal to the canonical form of path). Concurrent callers racing on
// the same path see only one Start; the second call returns the
// already-running stream code.
//
// The returned stream is ready for the push server's registry.Acquire
// the moment this function returns nil — coordinator.Start spawns the
// ingestor's push registration synchronously.
func (s *Service) ResolveOrCreate(ctx context.Context, path, secret string) (domain.StreamCode, error) {
	code := domain.StreamCode(canonPath(path))
	if code == "" {
		return "", fmt.Errorf("autopublish: empty path")
	}
	if err := domain.ValidateStreamCode(string(code)); err != nil {
		return "", fmt.Errorf("autopublish: invalid stream code %q: %w", code, err)
	}

	// Fast path: an entry already exists.
	s.mu.RLock()
	if _, ok := s.entries[code]; ok {
		s.mu.RUnlock()
		return code, nil
	}
	s.mu.RUnlock()

	tplCode, ok := s.Match(string(code))
	if !ok {
		return "", ErrNoMatch
	}
	tpl, err := s.templates.FindByCode(ctx, tplCode)
	if err != nil {
		return "", fmt.Errorf("autopublish: load template %q: %w", tplCode, err)
	}
	if !domain.TemplateAcceptsPush(tpl) {
		return "", ErrTemplateNoPush
	}
	// Push auth (S-8): when the template configures a StreamKey, the pusher
	// must supply a matching ?key= secret. Empty key = the template opted out.
	// Checked before coordinator.Start so an unauthorised push never
	// materialises a runtime stream.
	if tpl.StreamKey != "" &&
		subtle.ConstantTimeCompare([]byte(secret), []byte(tpl.StreamKey)) != 1 {
		return "", ErrUnauthorized
	}

	// Acquire write lock and re-check to defend against a parallel
	// caller that won the race between the RLock release and now.
	s.mu.Lock()
	if _, ok := s.entries[code]; ok {
		s.mu.Unlock()
		return code, nil
	}
	resolved := domain.ResolveStream(&domain.Stream{Code: code, Template: &tplCode}, tpl)
	if s.coord == nil {
		s.mu.Unlock()
		return "", errors.New("autopublish: coordinator not wired")
	}
	if err := s.coord.Start(ctx, resolved); err != nil {
		s.mu.Unlock()
		return "", fmt.Errorf("autopublish: start runtime stream %q: %w", code, err)
	}
	entry := &runtimeEntry{code: code, templateCode: tplCode}
	entry.lastPacketAt.Store(time.Now().UnixNano())
	// Detach from the request context — the observer outlives the
	// HTTP / push callback that triggered ResolveOrCreate; cancellation
	// goes through entry.cancelObs from the reaper instead.
	obsCtx, cancel := context.WithCancel(context.WithoutCancel(ctx)) //nolint:contextcheck // observer is per-stream, not per-request
	entry.cancelObs = cancel
	s.entries[code] = entry
	s.mu.Unlock()

	s.metricCreated(tplCode)
	go s.observeLiveness(obsCtx, entry)
	s.publishRuntimeEvent(ctx, domain.EventStreamRuntimeCreated, code, tplCode)
	slog.Info("autopublish: runtime stream materialised",
		"stream_code", code, "template", tplCode)
	return code, nil
}

// observeLiveness subscribes to the buffer hub for the runtime stream
// and updates lastPacketAt on every packet. The subscriber's chan is
// the standard write-never-blocks buffer-hub channel; if this loop
// falls behind, packets get dropped — that is fine for liveness
// purposes (we just need ONE packet per 30 s to keep the stream alive).
//
// Two exit paths matter:
//
//   - ctx.Done() — the entry was torn down by stopRuntimeStream (idle
//     reaper or future operator action). The entry has already been
//     removed from s.entries and coordinator.Stop has been called;
//     nothing else to do.
//   - sub.Recv() returns ok=false — the buffer was destroyed by
//     SOMEONE ELSE (operator DELETE /streams/<code>, coordinator-level
//     stop for any other reason). The entry still sits in s.entries
//     and would prevent a subsequent push from re-materialising the
//     stream because ResolveOrCreate's fast path returns immediately
//     when it sees the stale entry — meanwhile the ingestor's push
//     registry slot is already gone, so the second registry.Acquire
//     fails and the publisher is rejected forever. Clear the entry
//     here (defensively, only when we still own it) so the next push
//     gets a fresh ResolveOrCreate path.
//
// Subscribe failure at startup follows the same logic: the entry was
// just inserted by ResolveOrCreate but has no liveness backing, so a
// reaper sweep would never fire — strand the runtime stream visible
// in the API forever. Remove the orphan entry on the way out.
func (s *Service) observeLiveness(ctx context.Context, entry *runtimeEntry) {
	sub, err := s.buf.Subscribe(entry.code)
	if err != nil {
		slog.Warn("autopublish: liveness subscribe failed",
			"stream_code", entry.code, "err", err)
		s.removeOrphanedEntry(ctx, entry, "subscribe_failed")
		return
	}
	defer s.buf.Unsubscribe(entry.code, sub)
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-sub.Recv():
			if !ok {
				s.removeOrphanedEntry(ctx, entry, "buffer_closed")
				return
			}
			entry.lastPacketAt.Store(time.Now().UnixNano())
		}
	}
}

// removeOrphanedEntry clears `entry` from the runtime registry when its
// buffer-hub subscription has died from external causes (not via
// stopRuntimeStream — that path already cleared the entry before
// cancelling the observer). The "still owned by" check defends against
// a parallel push that re-inserted a fresh entry for the same code
// between the subscribe death and this delete: stomping that newer
// entry would lose a live stream.
func (s *Service) removeOrphanedEntry(ctx context.Context, entry *runtimeEntry, reason string) {
	s.mu.Lock()
	cur, ok := s.entries[entry.code]
	if !ok || cur != entry {
		s.mu.Unlock()
		return
	}
	delete(s.entries, entry.code)
	s.mu.Unlock()
	s.metricReaped(entry.templateCode, reason)
	s.publishRuntimeEvent(ctx, domain.EventStreamRuntimeExpired, entry.code, entry.templateCode)
	slog.Info("autopublish: runtime stream cleared (external teardown)",
		"stream_code", entry.code, "template", entry.templateCode, "reason", reason)
}

// RunReaper sweeps the runtime registry every reapInterval, stopping
// any stream whose lastPacketAt is older than IdleTimeout. Block until
// ctx is cancelled (process shutdown). The first sweep fires
// immediately so a process that boots into a stale state cleans up
// without waiting a full interval.
func (s *Service) RunReaper(ctx context.Context) {
	s.reapOnce(ctx)
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reapOnce(ctx)
		}
	}
}

func (s *Service) reapOnce(ctx context.Context) {
	cutoff := time.Now().Add(-IdleTimeout).UnixNano()
	s.mu.RLock()
	victims := make([]domain.StreamCode, 0)
	for code, e := range s.entries {
		if e.lastPacketAt.Load() < cutoff {
			victims = append(victims, code)
		}
	}
	s.mu.RUnlock()
	for _, code := range victims {
		s.stopRuntimeStream(ctx, code, "idle")
	}
}

// stopRuntimeStream tears down the runtime stream identified by code.
// Idempotent: a code that is not in the registry is a no-op.
func (s *Service) stopRuntimeStream(ctx context.Context, code domain.StreamCode, reason string) {
	s.mu.Lock()
	entry, ok := s.entries[code]
	if ok {
		delete(s.entries, code)
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	if entry.cancelObs != nil {
		entry.cancelObs()
	}
	if s.coord != nil {
		s.coord.Stop(ctx, code)
	}
	s.metricReaped(entry.templateCode, reason)
	s.publishRuntimeEvent(ctx, domain.EventStreamRuntimeExpired, code, entry.templateCode)
	slog.Info("autopublish: runtime stream expired",
		"stream_code", code, "template", entry.templateCode, "reason", reason)
}

// metricCreated records a runtime-stream materialisation (nil-safe).
func (s *Service) metricCreated(tpl domain.TemplateCode) {
	if s.m == nil {
		return
	}
	s.m.AutopublishStreamsCreatedTotal.WithLabelValues(string(tpl)).Inc()
	s.m.AutopublishStreamsActive.WithLabelValues(string(tpl)).Inc()
}

// metricReaped records a runtime-stream teardown (nil-safe). Called exactly
// once per entry from whichever teardown path actually removed it, so the
// active gauge stays balanced against metricCreated.
func (s *Service) metricReaped(tpl domain.TemplateCode, reason string) {
	if s.m == nil {
		return
	}
	s.m.AutopublishStreamsReapedTotal.WithLabelValues(string(tpl), reason).Inc()
	s.m.AutopublishStreamsActive.WithLabelValues(string(tpl)).Dec()
}

// RuntimeEntry is the API-facing view of one runtime stream. Returned
// by ListRuntime so the stream handler can compose config + runtime
// streams into a single GET /streams response.
type RuntimeEntry struct {
	Code         domain.StreamCode   `json:"code"`
	TemplateCode domain.TemplateCode `json:"template"`
	LastPacketAt time.Time           `json:"last_packet_at"`
}

// ListRuntime returns a snapshot of all live runtime streams. The
// slice is freshly allocated so the caller may mutate it freely; the
// underlying entries are read under the service mutex.
func (s *Service) ListRuntime() []RuntimeEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RuntimeEntry, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, RuntimeEntry{
			Code:         e.code,
			TemplateCode: e.templateCode,
			LastPacketAt: time.Unix(0, e.lastPacketAt.Load()),
		})
	}
	return out
}

// IsRuntime reports whether the given stream code points to a live
// runtime stream. Used by the stream handler to tag list entries.
func (s *Service) IsRuntime(code domain.StreamCode) bool {
	s.mu.RLock()
	_, ok := s.entries[code]
	s.mu.RUnlock()
	return ok
}

// Lookup returns the RuntimeEntry for code, or (zero, false) when no
// live runtime stream owns that code. Used by the stream handler's Get
// path so a runtime stream resolves to a 200 instead of a 404 (the
// on-disk repo never holds runtime records).
func (s *Service) Lookup(code domain.StreamCode) (RuntimeEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[code]
	if !ok {
		return RuntimeEntry{}, false
	}
	return RuntimeEntry{
		Code:         e.code,
		TemplateCode: e.templateCode,
		LastPacketAt: time.Unix(0, e.lastPacketAt.Load()),
	}, true
}

// StopRuntime tears down the runtime stream identified by code. Used by
// the stream handler's Restart / Delete paths so the operator can force
// a clean teardown without waiting for the 30 s idle reaper. The
// encoder's current push session breaks on the next write, reconnects,
// and the prefix matcher materialises a fresh runtime stream — that is
// the natural "restart" semantics for an encoder-driven lifecycle.
// Returns true when a runtime entry was actually removed.
func (s *Service) StopRuntime(ctx context.Context, code domain.StreamCode) bool {
	s.mu.RLock()
	_, ok := s.entries[code]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	s.stopRuntimeStream(ctx, code, "external_request")
	return true
}

func (s *Service) publishRuntimeEvent(
	ctx context.Context,
	typ domain.EventType,
	code domain.StreamCode,
	tpl domain.TemplateCode,
) {
	if s.bus == nil {
		return
	}
	s.bus.Publish(ctx, domain.Event{
		Type:       typ,
		StreamCode: code,
		Payload:    map[string]any{"template_code": string(tpl)},
	})
}
