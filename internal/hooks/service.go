// Package hooks implements the Hook dispatcher.
// It subscribes to the Event Bus and delivers events to registered external hooks
// via batched HTTP webhook OR per-event JSON-lines file — asynchronously.
//
// Per-protocol delivery shape:
//
//   - HTTP: events buffered into a per-hook batcher; flushed when the batch
//     hits BatchMaxItems OR when the BatchFlushIntervalSec timer fires. Failed
//     batches re-queue at the front for the next flush. The queue is bounded
//     by BatchMaxQueueItems — older events drop on overflow.
//
//   - File: each event appended as a single JSON line. No batching — log
//     shippers (Filebeat / Vector / Promtail) tail one line at a time.
package hooks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/do/v2"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/metrics"
	"github.com/datvietvac-techhub/open-streamer/internal/netguard"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

// metricsHooks abstracts the Prometheus surface the dispatcher writes to.
// Nil-safe: tests that wire only hooks can omit the metrics dependency.
type metricsHooks struct {
	delivery     *prometheus.CounterVec   // {hook_id, hook_type, outcome, reason}
	eventDropped *prometheus.CounterVec   // {hook_id}
	queueDepth   *prometheus.GaugeVec     // {hook_id}
	batchSize    *prometheus.HistogramVec // {hook_id}
}

// ErrHookTestUnsupported is returned when the hook type cannot receive a synthetic test delivery.
var ErrHookTestUnsupported = errors.New("hooks: test delivery not supported for this hook type")

// Service subscribes to the event bus and dispatches events to registered hooks.
//
// HTTP hooks share an http.Client and route through per-hook batchers.
// File hooks share a per-target write mutex map so concurrent deliveries to
// the same path serialise without interleaving partial JSON lines.
type Service struct {
	// cfgPtr holds the latest HooksConfig, stored atomically so SetConfig can
	// hot-swap it (e.g. file_root_dir) and the next hook create/update + file
	// delivery observe the new value without a reboot. Hooks have no listener to
	// restart, so this atomic store is the entire hot-reload mechanism.
	cfgPtr   atomic.Pointer[config.HooksConfig]
	hookRepo store.HookRepository
	bus      events.Bus
	client   *http.Client
	m        *metricsHooks

	// HTTP batchers: lazy-created on first dispatch for an HTTP hook.
	// Lifetime-tied to Start's ctx — every batcher is stopped + drained
	// when ctx ends.
	batchersMu sync.Mutex
	batchers   map[domain.HookID]*httpBatcher
	startCtx   context.Context // captured in Start so dispatch can pass it to batchers

	// File hook serialisation: one mutex per absolute target path.
	fileMu    sync.Mutex
	fileLocks map[string]*sync.Mutex
}

// New creates a Service and registers it with the DI injector.
func New(i do.Injector) (*Service, error) {
	cfg := do.MustInvoke[config.HooksConfig](i)
	hookRepo := do.MustInvoke[store.HookRepository](i)
	bus := do.MustInvoke[events.Bus](i)

	svc := &Service{
		hookRepo: hookRepo,
		bus:      bus,
		// No client-level timeout — each delivery applies its own per-hook
		// timeout via context.WithTimeout in postOnce / deliverFile. The client
		// carries the SSRF dial guard (S-2): loopback / link-local /
		// cloud-metadata (169.254.169.254) are blocked so a webhook target can't
		// pivot to the local admin API or instance metadata. Private RFC1918 is
		// permitted because internal webhooks are a legitimate use.
		client:    netguard.NewClient(netguard.Policy{AllowPrivate: true}, 0),
		batchers:  make(map[domain.HookID]*httpBatcher),
		fileLocks: make(map[string]*sync.Mutex),
	}
	svc.cfgPtr.Store(&cfg)
	if m, err := do.Invoke[*metrics.Metrics](i); err == nil {
		svc.m = &metricsHooks{
			delivery:     m.HooksDeliveryTotal,
			eventDropped: m.HooksEventsDroppedTotal,
			queueDepth:   m.HooksBatchQueueDepth,
			batchSize:    m.HooksBatchSize,
		}
	}
	return svc, nil
}

// SetConfig hot-swaps the HooksConfig. The next hook create/update validates
// against the new file_root_dir and the next file delivery writes under it —
// no reboot needed. Already-running HTTP batchers keep their batch settings
// for their lifetime (see batcherFor).
func (s *Service) SetConfig(cfg config.HooksConfig) {
	cp := cfg
	s.cfgPtr.Store(&cp)
}

// currentCfg returns the latest HooksConfig snapshot. Always non-nil after
// construction.
func (s *Service) currentCfg() config.HooksConfig {
	if p := s.cfgPtr.Load(); p != nil {
		return *p
	}
	return config.HooksConfig{}
}

// FileRootDir returns the configured containment root for file hooks, read
// live so a runtime config change applies to the next hook validation. Empty
// string means "no containment configured" (validation falls back to its
// default behaviour).
func (s *Service) FileRootDir() string {
	return s.currentCfg().FileRootDir
}

// DeliverTestEvent sends a single synthetic event to the hook using the same
// path as live delivery. For HTTP hooks the test event is enqueued into the
// batcher and shipped on the next flush — typically within a second when
// BatchFlushIntervalSec is 5 and the queue is otherwise empty (the test
// event triggers a size-1 batch only if BatchMaxItems=1).
//
// To make tests visible quickly without forcing the operator to set
// BatchMaxItems=1, we explicitly signal the batcher right after enqueuing.
// The signal is a no-op when MaxItems hasn't been reached, so production
// behaviour is unaffected.
func (s *Service) DeliverTestEvent(ctx context.Context, id domain.HookID) error {
	h, err := s.hookRepo.FindByID(ctx, id)
	if err != nil {
		return err
	}
	ev := domain.Event{
		ID:         fmt.Sprintf("test-%d", time.Now().UnixNano()),
		Type:       domain.EventStreamCreated,
		StreamCode: "_open_streamer_test_",
		OccurredAt: time.Now(),
		Payload:    map[string]any{"test": true, "hook_id": string(h.ID)},
	}
	switch h.Type {
	case domain.HookTypeHTTP:
		b := s.batcherFor(ctx, h)
		b.enqueue(ev)
		// Test path: don't make the operator wait a full flush interval.
		// signal() forces an immediate dispatch attempt; if the receiver is
		// reachable the test response shows up in seconds.
		b.signal()
		return nil
	case domain.HookTypeFile:
		err := s.deliverFile(h, ev)
		if err != nil {
			s.observeDelivery(h, "failure", classifyFileDeliveryErr(err))
		} else {
			s.observeDelivery(h, "success", "ok")
		}
		return err
	default:
		return fmt.Errorf("%w: %s", ErrHookTestUnsupported, h.Type)
	}
}

// classifyFileDeliveryErr maps a file-sink error onto the same `reason`
// taxonomy as HTTP delivery so dashboards can use one expression to
// alert across both hook types. File errors are simpler — there's no
// transport / status distinction — so we collapse most failures into
// "transport" (filesystem-level) with "encode" reserved for marshal
// failures emitted by deliverFile itself.
func classifyFileDeliveryErr(err error) string {
	if err == nil {
		return "ok"
	}
	msg := err.Error()
	switch {
	case strings.HasPrefix(msg, "file delivery: marshal"):
		return "encode"
	case strings.Contains(msg, "permission denied"), strings.Contains(msg, "no such file"):
		return "transport"
	}
	return "transport"
}

// Start subscribes to all domain events and begins dispatching.
// It blocks until ctx is cancelled, then drains and stops every active
// HTTP batcher so buffered events get one final flush before exit.
func (s *Service) Start(ctx context.Context) error {
	allEvents := []domain.EventType{
		domain.EventStreamCreated, domain.EventStreamStarted,
		domain.EventStreamStopped, domain.EventStreamDeleted,
		domain.EventInputConnected, domain.EventInputReconnecting,
		domain.EventInputDegraded, domain.EventInputFailed,
		domain.EventInputFailover, domain.EventRecordingStarted,
		domain.EventRecordingStopped, domain.EventRecordingFailed,
		domain.EventSegmentWritten,
		domain.EventTranscoderStarted, domain.EventTranscoderStopped,
		domain.EventTranscoderError,
		domain.EventSessionOpened, domain.EventSessionClosed,
	}

	s.startCtx = ctx

	unsubs := make([]func(), 0, len(allEvents))
	for _, et := range allEvents {
		unsub := s.bus.Subscribe(et, func(ctx context.Context, event domain.Event) error {
			return s.dispatch(ctx, event)
		})
		unsubs = append(unsubs, unsub)
	}

	<-ctx.Done()

	for _, unsub := range unsubs {
		unsub()
	}

	// Stop every HTTP batcher — each does a best-effort drain of pending
	// events on its way out so a graceful shutdown loses minimal data.
	s.batchersMu.Lock()
	batchers := make([]*httpBatcher, 0, len(s.batchers))
	for _, b := range s.batchers {
		batchers = append(batchers, b)
	}
	s.batchers = make(map[domain.HookID]*httpBatcher)
	s.batchersMu.Unlock()
	for _, b := range batchers {
		b.stop()
	}
	return nil
}

func (s *Service) dispatch(ctx context.Context, event domain.Event) error {
	hooks, err := s.hookRepo.List(ctx)
	if err != nil {
		return fmt.Errorf("hooks: list: %w", err)
	}

	for _, h := range hooks {
		if !h.Enabled {
			continue
		}
		if !s.matches(h, event) {
			continue
		}

		ev := withMetadata(event, h.Metadata)
		switch h.Type {
		case domain.HookTypeHTTP:
			s.batcherFor(ctx, h).enqueue(ev)
		case domain.HookTypeFile:
			if err := s.deliverFile(h, ev); err != nil {
				slog.Error("hooks: file delivery failed",
					"hook_id", h.ID,
					"event_type", event.Type,
					"stream_code", event.StreamCode,
					"err", err,
				)
				s.observeDelivery(h, "failure", classifyFileDeliveryErr(err))
			} else {
				s.observeDelivery(h, "success", "ok")
			}
		default:
			slog.Warn("hooks: unknown hook type", "hook_id", h.ID, "type", h.Type)
		}
	}
	return nil
}

// batcherFor returns (and lazily creates) the HTTP batcher for one hook.
//
// The dispatch ctx parameter is intentionally unused for batcher lifetime
// — batchers must outlive any single dispatch call so future deliveries
// can land on them. We bind to s.startCtx (set in Start) so the batcher
// dies on graceful shutdown rather than on the bus worker's per-event ctx
// completing. The unused ctx argument keeps callers in the bus dispatch
// path satisfied (contextcheck linter).
//
// Existing batchers keep their original config for their lifetime; hot-
// reloading per-hook batch settings without a restart would require
// deeper plumbing — out of scope for this revision.
func (s *Service) batcherFor(_ context.Context, h *domain.Hook) *httpBatcher {
	s.batchersMu.Lock()
	defer s.batchersMu.Unlock()
	b, ok := s.batchers[h.ID]
	if ok {
		return b
	}
	hcfg := s.currentCfg()
	cfg := mergeBatchConfig(h, batchGlobalDefaults{
		maxItems:         hcfg.BatchMaxItems,
		flushIntervalSec: hcfg.BatchFlushIntervalSec,
		maxQueueItems:    hcfg.BatchMaxQueueItems,
	})
	bctx := s.startCtx
	if bctx == nil {
		bctx = context.Background()
	}
	// Batcher must be tied to the SERVICE lifetime (s.startCtx), NOT the
	// per-event dispatch ctx — using the dispatch ctx here would kill the
	// batcher as soon as the bus worker that happened to create it returns.
	b = newHTTPBatcher(bctx, cfg, s.client, s.batchMetricsFor(h)) //nolint:contextcheck // service-lifetime ctx, see comment above
	s.batchers[h.ID] = b
	slog.Info("hooks: HTTP batcher started",
		"hook_id", h.ID,
		"target", h.Target,
		"max_items", cfg.maxItems,
		"flush_interval", cfg.flushInterval,
		"max_queue", cfg.maxQueue,
	)
	return b
}

// withMetadata returns a shallow copy of event with hook metadata merged into the payload.
// Hook metadata keys are prefixed with "meta." to avoid collisions with system payload fields.
func withMetadata(event domain.Event, metadata map[string]string) domain.Event {
	if len(metadata) == 0 {
		return event
	}
	merged := make(map[string]any, len(event.Payload)+len(metadata))
	for k, v := range event.Payload {
		merged[k] = v
	}
	for k, v := range metadata {
		merged["meta."+k] = v
	}
	event.Payload = merged
	return event
}

func (s *Service) matches(h *domain.Hook, event domain.Event) bool {
	if len(h.EventTypes) > 0 {
		found := false
		for _, t := range h.EventTypes {
			if t == event.Type {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return h.StreamCodes.Matches(event.StreamCode)
}

// deliverFile appends one JSON-encoded event followed by a newline to the
// file at h.Target. Each call is atomic on POSIX filesystems thanks to
// O_APPEND — concurrent deliveries from different processes interleave at
// line boundaries. We additionally hold a per-target mutex so different
// goroutines in THIS process don't race the open/write/close cycle.
//
// We do NOT batch file deliveries — log shippers (Filebeat, Vector, Promtail)
// expect one event per line and tail-and-ship that way. Batching would
// either need a custom delimiter (breaking the JSON-lines contract) or
// chunked writes (defeating the per-line atomicity).
func (s *Service) deliverFile(h *domain.Hook, event domain.Event) error {
	// Re-validate at delivery time (S-2): the same containment the REST/YAML
	// paths enforce, applied here too so a hook loaded straight from the store
	// (or pre-dating the validation) can't write outside the configured root.
	if err := h.Validate(s.currentCfg().FileRootDir); err != nil {
		return fmt.Errorf("file delivery: %w", err)
	}
	target := strings.TrimSpace(h.Target)

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	body = append(body, '\n')

	mu := s.lockForFile(target)
	mu.Lock()
	defer mu.Unlock()

	f, err := os.OpenFile(target, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("file delivery: open %s: %w", target, err)
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Write(body); err != nil {
		return fmt.Errorf("file delivery: write %s: %w", target, err)
	}
	return nil
}

// observeDelivery bumps the per-hook delivery counter labelled with the
// outcome ("success"/"failure") and reason (see classifyHookDeliveryErr
// for the reason taxonomy; pass "ok" on success). Nil-safe: tests
// without metrics injection pass through without writing anything.
func (s *Service) observeDelivery(h *domain.Hook, outcome, reason string) {
	if s.m == nil {
		return
	}
	s.m.delivery.With(prometheus.Labels{
		"hook_id":   string(h.ID),
		"hook_type": string(h.Type),
		"outcome":   outcome,
		"reason":    reason,
	}).Inc()
}

// batchMetricsFor pre-binds the per-hook Prometheus instruments the batcher
// hot path uses so each enqueue/flush avoids a WithLabelValues map lookup
// for the high-frequency drop / queue-depth gauges. Delivery counter is
// looked up dynamically (per outcome+reason combination) because pre-
// binding all 7+ outcome/reason pairs would clutter the struct without
// meaningful CPU savings — the lookup is one ns-scale map hit per batch
// flush. Returns the zero value when the metrics dependency wasn't
// injected.
func (s *Service) batchMetricsFor(h *domain.Hook) batchMetrics {
	if s.m == nil {
		return batchMetrics{}
	}
	hookID := string(h.ID)
	hookType := string(h.Type)
	return batchMetrics{
		observeDelivery: func(outcome, reason string) {
			s.m.delivery.With(prometheus.Labels{
				"hook_id":   hookID,
				"hook_type": hookType,
				"outcome":   outcome,
				"reason":    reason,
			}).Inc()
		},
		dropped:    s.m.eventDropped.WithLabelValues(hookID),
		queueDepth: s.m.queueDepth.WithLabelValues(hookID),
		batchSize:  s.batchSizeFor(h),
	}
}

// batchSizeFor returns the per-hook batch-size histogram observer, or nil
// when the metric isn't registered (tests). Split out from batchMetricsFor
// to keep the s.m == nil guard from spreading.
func (s *Service) batchSizeFor(h *domain.Hook) prometheus.Observer {
	if s.m == nil || s.m.batchSize == nil {
		return nil
	}
	return s.m.batchSize.WithLabelValues(string(h.ID))
}

// lockForFile returns the per-target mutex, lazy-creating one on first use.
func (s *Service) lockForFile(target string) *sync.Mutex {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	mu, ok := s.fileLocks[target]
	if !ok {
		mu = &sync.Mutex{}
		s.fileLocks[target] = mu
	}
	return mu
}
