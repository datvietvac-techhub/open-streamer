package hooks

// batcher.go — per-hook HTTP delivery queue with size+timer triggered flush.
//
// Design summary:
//   - One httpBatcher per HTTP hook (lazy-created in dispatch).
//   - Each batcher owns a single goroutine that wakes on either a flush-tick
//     or a "queue full" signal pushed by enqueue().
//   - Flush takes the entire pending queue, POSTs it as a JSON array, and
//     on failure re-queues the events for the next flush. Re-queued events
//     are merged at the FRONT of the buffer so chronological ordering is
//     preserved across retries.
//   - The queue is bounded by maxQueue. When new events would push it past
//     the cap, the OLDEST events are dropped (with a counter+warn) — this
//     stops a persistently-down target from ballooning memory.
//
// The Service holds a map[HookID]*httpBatcher; lifecycle is tied to the
// Service.Start ctx — every batcher is stopped + drained when ctx ends.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// batchMetrics holds the pre-bound Prometheus instruments for one batcher.
// Pre-binding once (instead of WithLabelValues per write) keeps the hot
// path map-lookup-free. Any field may be nil when metrics aren't injected.
//
// observeDelivery is a closure that increments
// `open_streamer_hooks_delivery_total` with the right (outcome, reason)
// labels — pre-binding ALL six failure reasons would explode label
// cardinality in the struct, so the closure looks them up via a tiny map
// owned by service.batchMetricsFor. Single counter lookup per delivery
// (~ns), well below the HTTP request cost it accompanies.
type batchMetrics struct {
	observeDelivery func(outcome, reason string)
	dropped         prometheus.Counter
	queueDepth      prometheus.Gauge
	batchSize       prometheus.Observer
}

// batchConfig snapshots the per-hook dispatch parameters at batcher
// construction time. The values are resolved by mergeBatchConfig from
// HooksConfig + Hook fields + code defaults so the batcher itself stays
// agnostic of the layered defaults.
type batchConfig struct {
	maxItems      int
	flushInterval time.Duration
	maxQueue      int
	maxRetries    int
	timeout       time.Duration
	secret        string
	target        string
	hookID        domain.HookID
}

// httpBatcher accumulates events for one HTTP hook and posts them as a
// JSON array. Methods are safe for concurrent use.
type httpBatcher struct {
	cfg    batchConfig
	client *http.Client
	m      batchMetrics // zero-value safe

	mu          sync.Mutex
	queue       []domain.Event
	droppedSize int64 // cumulative count of events evicted on overflow

	flushSig chan struct{} // size-trigger wake-up; depth 1 (coalesced)

	// Lifecycle plumbing. closeOnce guards stop()'s "send done + wait wg" so
	// duplicate stop calls (Service.Start ctx exit + a hot-removal path)
	// don't double-close the channel.
	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// newHTTPBatcher constructs a batcher and launches its flusher goroutine.
// The goroutine returns when ctx is cancelled OR stop() is called; in
// either case the queue is drained on the way out (best-effort POST).
func newHTTPBatcher(ctx context.Context, cfg batchConfig, client *http.Client, m batchMetrics) *httpBatcher {
	b := &httpBatcher{
		cfg:      cfg,
		client:   client,
		m:        m,
		queue:    make([]domain.Event, 0, cfg.maxItems),
		flushSig: make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	b.wg.Add(1)
	go b.run(ctx)
	return b
}

// enqueue appends an event to the queue, trimming the oldest entries when
// the cap is exceeded. Signals the flusher when the queue reaches the
// maxItems threshold so the batch ships immediately rather than waiting
// for the next tick.
func (b *httpBatcher) enqueue(ev domain.Event) {
	b.mu.Lock()
	b.queue = append(b.queue, ev)
	dropped := 0
	if len(b.queue) > b.cfg.maxQueue {
		dropped = len(b.queue) - b.cfg.maxQueue
		b.queue = b.queue[dropped:]
		b.droppedSize += int64(dropped)
		slog.Warn("hooks: batch queue overflow — oldest events dropped",
			"hook_id", b.cfg.hookID,
			"dropped", dropped,
			"dropped_total", b.droppedSize,
			"queue_cap", b.cfg.maxQueue,
		)
	}
	depth := len(b.queue)
	full := depth >= b.cfg.maxItems
	b.mu.Unlock()
	if dropped > 0 && b.m.dropped != nil {
		b.m.dropped.Add(float64(dropped))
	}
	if b.m.queueDepth != nil {
		b.m.queueDepth.Set(float64(depth))
	}
	if full {
		b.signal()
	}
}

// signal nudges the flusher to wake up. The buffered channel + non-blocking
// send means repeated signals coalesce into one wake-up, matching the
// "flush at most once per cycle" behaviour we want.
func (b *httpBatcher) signal() {
	select {
	case b.flushSig <- struct{}{}:
	default:
	}
}

// stop signals the flusher to exit and waits for it to finish draining.
// Idempotent. Subsequent calls return immediately.
func (b *httpBatcher) stop() {
	b.closeOnce.Do(func() { close(b.done) })
	b.wg.Wait()
}

// run is the flusher loop. It exits on ctx cancellation OR stop().
func (b *httpBatcher) run(ctx context.Context) {
	defer b.wg.Done()
	ticker := time.NewTicker(b.cfg.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Best-effort drain on shutdown. We deliberately detach from
			// the cancelled parent ctx so the final POST gets a fair
			// chance to land — using ctx here would cancel the in-flight
			// HTTP request immediately.
			b.drainOnShutdown(context.Background()) //nolint:contextcheck // see comment above
			return
		case <-b.done:
			b.drainOnShutdown(context.Background()) //nolint:contextcheck // graceful drain — see ctx.Done branch
			return
		case <-ticker.C:
			b.flushOnce(ctx)
		case <-b.flushSig:
			b.flushOnce(ctx)
		}
	}
}

// classifyHookDeliveryErr maps a deliverBatch error onto the `reason`
// label used by `open_streamer_hooks_delivery_total`. The taxonomy
// matches what operators want to alert on:
//
//   - "encode"    — JSON marshal / request build / HMAC failure (bug, not transient)
//   - "timeout"   — context deadline / TCP timeout (configure / scale)
//   - "http_4xx"  — target rejected (auth / payload schema; needs human)
//   - "http_5xx"  — target overloaded / down (transient, expected to retry)
//   - "transport" — connection refused, DNS, TLS handshake failure
//   - "unknown"   — shouldn't happen; if seen, classifier is missing a case
//
// The classifier inspects the wrapped error message because deliverBatch
// emits strings rather than typed errors. Acceptable for a bounded vocab.
func classifyHookDeliveryErr(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	msg := err.Error()
	switch {
	case strings.HasPrefix(msg, "marshal batch:"), strings.HasPrefix(msg, "create request:"):
		return "encode"
	case strings.HasPrefix(msg, "unexpected status:"):
		return classifyHTTPStatusErr(msg)
	case strings.HasPrefix(msg, "http post:"):
		// Could be timeout disguised as a generic transport error from net/http.
		if strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline exceeded") {
			return "timeout"
		}
		return "transport"
	}
	return "unknown"
}

// classifyHTTPStatusErr extracts the status code from an "unexpected
// status: NNN ..." error and buckets it. Returns "unknown" on parse
// failure rather than mis-classifying.
func classifyHTTPStatusErr(msg string) string {
	// "unexpected status: 4XX ..." — status starts at offset len("unexpected status: ").
	const prefix = "unexpected status: "
	if !strings.HasPrefix(msg, prefix) {
		return "unknown"
	}
	rest := msg[len(prefix):]
	// Status is the first whitespace- or end-delimited token.
	end := strings.IndexAny(rest, " \t")
	if end < 0 {
		end = len(rest)
	}
	status, convErr := strconv.Atoi(rest[:end])
	if convErr != nil {
		return "unknown"
	}
	switch {
	case status >= 400 && status < 500:
		return "http_4xx"
	case status >= 500 && status < 600:
		return "http_5xx"
	default:
		return "unknown"
	}
}

// flushOnce takes the current queue and ships it. On failure the events
// are merged back into the queue head so the next flush retries them
// in chronological order. Records the batch size on every flush attempt
// (success or failure) so the histogram captures the real distribution
// — failed batches still represent work the batcher is doing.
func (b *httpBatcher) flushOnce(ctx context.Context) {
	batch := b.takeBatch()
	if len(batch) == 0 {
		return
	}
	if b.m.batchSize != nil {
		b.m.batchSize.Observe(float64(len(batch)))
	}
	if err := b.deliverBatch(ctx, batch); err != nil {
		if b.m.observeDelivery != nil {
			b.m.observeDelivery("failure", classifyHookDeliveryErr(err))
		}
		b.requeue(batch, err)
		return
	}
	if b.m.observeDelivery != nil {
		b.m.observeDelivery("success", "ok")
	}
}

// drainOnShutdown flushes the whole backlog in maxItems-sized batches, then
// counts whatever could NOT be shipped (oversized backlog or a persistently
// failing target) as dropped. A single flushOnce only ships maxItems, so
// without this loop+count the rest is abandoned silently and invisible to
// open_streamer_hooks_events_dropped_total.
func (b *httpBatcher) drainOnShutdown(ctx context.Context) {
	denom := b.cfg.maxItems
	if denom <= 0 {
		denom = 1
	}
	// Bound the loop by the initial backlog so a failing target can't spin.
	for iters := b.queueLen()/denom + 1; iters > 0; iters-- {
		before := b.queueLen()
		if before == 0 {
			return
		}
		b.flushOnce(ctx)
		if b.queueLen() >= before {
			break // delivery failed + requeued — no progress, give up
		}
	}
	if remaining := b.queueLen(); remaining > 0 && b.m.dropped != nil {
		b.m.dropped.Add(float64(remaining))
		slog.Warn("hooks: events abandoned on shutdown drain",
			"hook_id", b.cfg.hookID, "abandoned", remaining)
	}
}

// takeBatch pulls up to maxItems events off the queue head and returns
// them. The remaining queue stays in place — partial flushes (when more
// events arrived than the batch can hold) preserve order for the next tick.
func (b *httpBatcher) takeBatch() []domain.Event {
	b.mu.Lock()
	if len(b.queue) == 0 {
		b.mu.Unlock()
		return nil
	}
	n := len(b.queue)
	if n > b.cfg.maxItems {
		n = b.cfg.maxItems
	}
	out := make([]domain.Event, n)
	copy(out, b.queue[:n])
	b.queue = b.queue[n:]
	depth := len(b.queue)
	b.mu.Unlock()
	if b.m.queueDepth != nil {
		b.m.queueDepth.Set(float64(depth))
	}
	return out
}

// requeue inserts the failed batch at the FRONT of the queue so retries
// preserve event order. The combined (failed + new) queue is still capped
// at maxQueue — when over-capacity, we drop from the OLDEST end (which
// after re-queue is the failed batch's head — older events lose to fresher
// ones, matching the "best effort, don't ballon RAM" contract).
func (b *httpBatcher) requeue(batch []domain.Event, deliverErr error) {
	b.mu.Lock()
	combined := make([]domain.Event, 0, len(batch)+len(b.queue))
	combined = append(combined, batch...)
	combined = append(combined, b.queue...)
	dropped := 0
	if len(combined) > b.cfg.maxQueue {
		dropped = len(combined) - b.cfg.maxQueue
		combined = combined[dropped:]
		b.droppedSize += int64(dropped)
		slog.Warn("hooks: requeue overflow — oldest events dropped",
			"hook_id", b.cfg.hookID,
			"dropped", dropped,
			"dropped_total", b.droppedSize,
			"queue_cap", b.cfg.maxQueue,
		)
	}
	b.queue = combined
	depth := len(b.queue)
	b.mu.Unlock()

	if dropped > 0 && b.m.dropped != nil {
		b.m.dropped.Add(float64(dropped))
	}
	if b.m.queueDepth != nil {
		b.m.queueDepth.Set(float64(depth))
	}

	slog.Warn("hooks: batch delivery failed, re-queued for next flush",
		"hook_id", b.cfg.hookID,
		"batch_size", len(batch),
		"err", deliverErr,
	)
}

// deliverBatch is the single HTTP attempt with internal retry/backoff. It
// returns nil only when one of the attempts returned 2xx; otherwise the
// last error is returned and the caller re-queues.
//
// MaxRetries here governs RETRIES INSIDE this flush — if the target is
// momentarily flaky, we'd rather stay on this batch (~36s with default
// backoff) than fail fast and immediately re-queue. Operators wanting
// pure flush-cadence retries can set MaxRetries=0.
func (b *httpBatcher) deliverBatch(ctx context.Context, batch []domain.Event) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	backoffs := []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second}
	maxRetries := b.cfg.maxRetries
	if maxRetries > len(backoffs) {
		maxRetries = len(backoffs)
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		lastErr = b.postOnce(ctx, body, len(batch))
		if lastErr == nil {
			return nil
		}
		if attempt < maxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoffs[attempt]):
			}
		}
	}
	return lastErr
}

// postOnce makes a single HTTP request with the per-hook timeout applied.
// HMAC signs the entire body when Hook.Secret is set.
func (b *httpBatcher) postOnce(ctx context.Context, body []byte, batchSize int) error {
	deliverCtx, cancel := context.WithTimeout(ctx, b.cfg.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(deliverCtx, http.MethodPost, b.cfg.target, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-OpenStreamer-Batch-Size", strconv.Itoa(batchSize))
	if b.cfg.secret != "" {
		mac := hmac.New(sha256.New, []byte(b.cfg.secret))
		mac.Write(body)
		req.Header.Set("X-OpenStreamer-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("http post: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		const maxBody = 512
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxBody))
		trimmed := strings.TrimSpace(string(snippet))
		if trimmed == "" {
			return fmt.Errorf("unexpected status: %d", resp.StatusCode)
		}
		return fmt.Errorf("unexpected status: %d body=%q", resp.StatusCode, trimmed)
	}
	return nil
}

// queueLen returns the current number of pending events. Test-only helper
// — production code should never poll this.
func (b *httpBatcher) queueLen() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue)
}

// mergeBatchConfig resolves the layered defaults for one hook.
// Order: hook value → global config value → code default.
func mergeBatchConfig(h *domain.Hook, gc batchGlobalDefaults) batchConfig {
	maxItems := h.BatchMaxItems
	if maxItems <= 0 {
		maxItems = gc.maxItems
	}
	if maxItems <= 0 {
		maxItems = domain.DefaultHookBatchMaxItems
	}

	flushSec := h.BatchFlushIntervalSec
	if flushSec <= 0 {
		flushSec = gc.flushIntervalSec
	}
	if flushSec <= 0 {
		flushSec = domain.DefaultHookBatchFlushIntervalSec
	}

	maxQueue := h.BatchMaxQueueItems
	if maxQueue <= 0 {
		maxQueue = gc.maxQueueItems
	}
	if maxQueue <= 0 {
		maxQueue = domain.DefaultHookBatchMaxQueueItems
	}

	timeoutSec := h.TimeoutSec
	if timeoutSec <= 0 {
		timeoutSec = domain.DefaultHookTimeoutSec
	}

	maxRetries := h.MaxRetries
	if maxRetries <= 0 {
		maxRetries = domain.DefaultHookMaxRetries
	}

	return batchConfig{
		maxItems:      maxItems,
		flushInterval: time.Duration(flushSec) * time.Second,
		maxQueue:      maxQueue,
		maxRetries:    maxRetries,
		timeout:       time.Duration(timeoutSec) * time.Second,
		secret:        h.Secret,
		target:        h.Target,
		hookID:        h.ID,
	}
}

// batchGlobalDefaults is the subset of HooksConfig the resolver needs.
// Wrapping it here keeps mergeBatchConfig free of config import cycles
// in test utilities.
type batchGlobalDefaults struct {
	maxItems         int
	flushIntervalSec int
	maxQueueItems    int
}
