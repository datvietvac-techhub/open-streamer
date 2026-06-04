// Package buffer implements the Buffer Hub — the central in-memory ring buffer.
// It is the single data pipeline between Ingestor and all consumers (Transcoder, Publisher, DVR).
// Each stream has its own ring buffer; consumers subscribe and get an independent read cursor.
package buffer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/do/v2"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/metrics"
)

// Subscriber is a read cursor into a stream's ring buffer.
type Subscriber struct {
	ch chan Packet
}

// Recv returns the channel from which the subscriber reads packets.
func (s *Subscriber) Recv() <-chan Packet { return s.ch }

// ringBuffer is a bounded in-memory queue for a single stream.
//
// `drops` is a pre-bound Prometheus counter for this stream's drop events
// — pre-binding once (instead of `WithLabelValues` per write) keeps the
// hot path map-lookup-free. Nil when metrics aren't injected (tests).
//
// `session` is the current StreamSession snapshot for this buffer. Mutated
// only by Service.SetSession (under rb.mu) and read by ringBuffer.write to
// stamp each outgoing Packet with SessionID / SessionStart. `sessionStartPending`
// is a one-shot latch: SetSession sets it to true so the very next write
// emits SessionStart=true, then write clears it. nil session means
// "no session declared yet" — packets ship with SessionID=0.
type ringBuffer struct {
	mu                  sync.Mutex
	subs                []*Subscriber
	drops               prometheus.Counter
	session             *domain.StreamSession
	sessionStartPending bool
}

func (rb *ringBuffer) write(pkt Packet) {
	if pkt.empty() {
		return
	}
	// Hold the lock during fan-out so a concurrent unsubscribe can't close
	// a subscriber channel while we're sending to it. Sends use `select default`
	// so the writer still never blocks on slow consumers — the dropped packet
	// is counted into rb.drops so operators can see slow-consumer pressure
	// before users complain about frame drops.
	rb.mu.Lock()
	defer rb.mu.Unlock()
	// Stamp the session metadata onto the outgoing packet. The caller may
	// also have set SessionID/SessionStart directly (e.g. a mixer fan-out
	// that owns its own session table); we only overwrite when the caller
	// left them zero, so explicit values pass through.
	if pkt.SessionID == 0 && rb.session != nil {
		pkt.SessionID = rb.session.ID
		if rb.sessionStartPending {
			pkt.SessionStart = true
			rb.sessionStartPending = false
		}
	}
	for _, s := range rb.subs {
		pc := clonePacket(pkt)
		select {
		case s.ch <- pc:
		default:
			if rb.drops != nil {
				rb.drops.Inc()
			}
		}
	}
}

func (rb *ringBuffer) subscribe(chanSize int) *Subscriber {
	s := &Subscriber{ch: make(chan Packet, chanSize)}
	rb.mu.Lock()
	rb.subs = append(rb.subs, s)
	rb.mu.Unlock()
	return s
}

func (rb *ringBuffer) unsubscribe(s *Subscriber) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for i, sub := range rb.subs {
		if sub == s {
			rb.subs = append(rb.subs[:i], rb.subs[i+1:]...)
			close(s.ch)
			return
		}
	}
}

// unsubscribeAll closes every subscriber's channel and clears the list.
// Used by Service.UnsubscribeAll to signal "no more data from this buffer"
// to all live consumers (e.g. when a downstream needs to detect upstream
// teardown via channel close rather than indefinite blocking).
func (rb *ringBuffer) unsubscribeAll() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for _, s := range rb.subs {
		close(s.ch)
	}
	rb.subs = nil
}

// Service manages ring buffers for all active streams.
//
// `sessionCounter` produces a process-wide monotonic ID for every
// SetSession call — each new session is observable as a distinct uint64
// without needing per-buffer counters or wall-clock reads on the hot path.
type Service struct {
	cfg            config.BufferConfig
	m              *metrics.Metrics
	mu             sync.RWMutex
	buffers        map[domain.StreamCode]*ringBuffer
	sessionCounter atomic.Uint64
}

// New creates a Service and registers it with the DI injector.
func New(i do.Injector) (*Service, error) {
	cfg := do.MustInvoke[config.BufferConfig](i)
	if cfg.Capacity <= 0 {
		cfg.Capacity = domain.DefaultBufferCapacity
	}
	// Metrics is registered as a separate provider; tolerate absence so
	// tests that wire only buffer can still construct the service.
	var m *metrics.Metrics
	if mm, err := do.Invoke[*metrics.Metrics](i); err == nil {
		m = mm
	}
	svc := &Service{
		cfg:     cfg,
		m:       m,
		buffers: make(map[domain.StreamCode]*ringBuffer),
	}
	if m != nil {
		// Sampler runs forever — it's cheap (constant-time per stream) and
		// has no graceful-stop contract because the process owns its own
		// lifecycle. 2s is fast enough for the 15s default Prometheus
		// scrape interval to see fresh-ish values without overlap, while
		// being slow enough that the sampler doesn't show up in CPU
		// profiles for production-sized stream counts (~hundreds).
		go svc.sampleLoop(2 * time.Second)
	}
	return svc, nil
}

// sampleLoop periodically computes BufferCapacityUsed (max channel depth
// across subscribers, normalised to 0..1) and BufferSubscribers (count) per
// stream and writes them to the gauges. Reads are RLock-only — no impact
// on the write hot path.
func (s *Service) sampleLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for range t.C {
		s.sampleOnce()
	}
}

// sampleOnce takes one snapshot. Split out so a future graceful-shutdown
// hook can call it once on the way out for a final-tick refresh, and so
// tests can drive the metrics deterministically without relying on time.
func (s *Service) sampleOnce() {
	if s.m == nil {
		return
	}
	type bufSnap struct {
		code     domain.StreamCode
		maxDepth int
		nSubs    int
		cap      int
	}
	s.mu.RLock()
	snaps := make([]bufSnap, 0, len(s.buffers))
	for code, rb := range s.buffers {
		rb.mu.Lock()
		max := 0
		for _, sub := range rb.subs {
			if d := len(sub.ch); d > max {
				max = d
			}
		}
		snaps = append(snaps, bufSnap{code: code, maxDepth: max, nSubs: len(rb.subs), cap: s.cfg.Capacity})
		rb.mu.Unlock()
	}
	s.mu.RUnlock()
	for _, sn := range snaps {
		var frac float64
		if sn.cap > 0 {
			frac = float64(sn.maxDepth) / float64(sn.cap)
		}
		s.m.BufferCapacityUsed.WithLabelValues(string(sn.code)).Set(frac)
		s.m.BufferSubscribers.WithLabelValues(string(sn.code)).Set(float64(sn.nSubs))
	}
}

// NewServiceForTesting creates a Service without DI, for use in unit tests.
func NewServiceForTesting(capacity int) *Service {
	return &Service{
		cfg:     config.BufferConfig{Capacity: capacity},
		buffers: make(map[domain.StreamCode]*ringBuffer),
	}
}

// Create initialises a ring buffer for the given stream.
func (s *Service) Create(id domain.StreamCode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buffers[id]; !ok {
		rb := &ringBuffer{}
		if s.m != nil {
			rb.drops = s.m.BufferDropsTotal.WithLabelValues(string(id))
		}
		s.buffers[id] = rb
	}
}

// Write pushes a packet into the stream's ring buffer (deep-copied for subscribers).
// Only the active Ingestor goroutine for this stream should call Write.
func (s *Service) Write(id domain.StreamCode, pkt Packet) error {
	s.mu.RLock()
	rb, ok := s.buffers[id]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("buffer: stream %s not found", id)
	}
	rb.write(pkt)
	return nil
}

// Subscribe registers a new consumer for the stream's buffer.
// The caller must call Unsubscribe when done to avoid a goroutine/channel leak.
func (s *Service) Subscribe(id domain.StreamCode) (*Subscriber, error) {
	s.mu.RLock()
	rb, ok := s.buffers[id]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("buffer: stream %s not found", id)
	}
	return rb.subscribe(s.cfg.Capacity), nil
}

// Unsubscribe removes a consumer and closes its channel.
func (s *Service) Unsubscribe(id domain.StreamCode, sub *Subscriber) {
	s.mu.RLock()
	rb, ok := s.buffers[id]
	s.mu.RUnlock()
	if ok {
		rb.unsubscribe(sub)
	}
}

// Delete tears down the ring buffer for a stream (call when stream is stopped).
// Closes every subscriber's channel BEFORE removing the map entry so consumers
// observe a clean EOF (`pkt, ok := <-sub.Recv(); !ok`) and can react — the
// mixer / copy taps in particular rely on this signal to retry against the
// freshly-created buffer when an upstream stream restarts. Without the
// channel-close, taps stay blocked on a stale ringBuffer indefinitely while
// the new ringBuffer for the same id receives packets they never see.
func (s *Service) Delete(id domain.StreamCode) {
	s.mu.Lock()
	rb, ok := s.buffers[id]
	if ok {
		delete(s.buffers, id)
	}
	s.mu.Unlock()
	if ok {
		rb.unsubscribeAll()
	}
	// Drop the per-buffer metric series so a deleted buffer (including the
	// short-lived auto-publish runtime + per-rendition/raw buffers) doesn't
	// keep reporting its last value forever.
	if s.m != nil {
		s.m.BufferCapacityUsed.DeleteLabelValues(string(id))
		s.m.BufferSubscribers.DeleteLabelValues(string(id))
		s.m.BufferDropsTotal.DeleteLabelValues(string(id))
	}
}

// UnsubscribeAll closes every subscriber's channel for the given buffer.
// Subscribers' next Recv will see ok=false (a clean EOF signal). The buffer
// itself is left in place — call Delete after if you also want it removed.
// No-op when the buffer doesn't exist.
func (s *Service) UnsubscribeAll(id domain.StreamCode) {
	s.mu.RLock()
	rb, ok := s.buffers[id]
	s.mu.RUnlock()
	if !ok {
		return
	}
	rb.unsubscribeAll()
}

// SetSession declares that subsequent packets written to buffer `id` belong
// to a new StreamSession. The buffer mints a fresh monotonic SessionID and
// latches a one-shot SessionStart=true marker on the very next packet,
// regardless of whether that packet is a TS chunk or an AVPacket.
//
// `reason` is recorded on the session for telemetry. `video` and `audio`
// are optional config snapshots — either may be nil for video-only / audio-
// only streams, or both may be nil when the caller hasn't parsed the
// init params yet (downstream consumers fall back to in-band parsing).
//
// Returns the StreamSession that was minted so callers can publish it on
// the event bus / log it. Returns nil and does nothing when the buffer
// hasn't been Create'd yet — that case is logged at the call site.
func (s *Service) SetSession(id domain.StreamCode, reason domain.SessionStartReason, video *domain.SessionVideoConfig, audio *domain.SessionAudioConfig) *domain.StreamSession {
	s.mu.RLock()
	rb, ok := s.buffers[id]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	sess := &domain.StreamSession{
		ID:        s.sessionCounter.Add(1),
		StartedAt: time.Now(),
		Reason:    reason,
		Video:     video,
		Audio:     audio,
	}
	rb.mu.Lock()
	rb.session = sess
	rb.sessionStartPending = true
	rb.mu.Unlock()
	return sess
}

// Session returns a snapshot of the buffer's current StreamSession, or nil
// when SetSession has not been called yet or the buffer doesn't exist.
// The returned pointer is the live session — callers must not mutate it.
func (s *Service) Session(id domain.StreamCode) *domain.StreamSession {
	s.mu.RLock()
	rb, ok := s.buffers[id]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.session
}
