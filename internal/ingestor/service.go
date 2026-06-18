// Package ingestor handles raw stream ingestion.
//
// The ingestor accepts a plain URL and automatically derives everything from it:
//   - Protocol (RTMP, RTSP, SRT, UDP, HLS, file, …) → from URL scheme
//   - Mode (pull vs push-listen) → from URL host (wildcard = listen)
//
// Pull workers are created per-stream and reconnect automatically.
// Push servers (RTMP, SRT) are global, started once, and route by stream key.
package ingestor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor/pull"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor/push"
	"github.com/datvietvac-techhub/open-streamer/internal/metrics"
	"github.com/datvietvac-techhub/open-streamer/internal/timeline"
	"github.com/datvietvac-techhub/open-streamer/internal/vod"
	"github.com/datvietvac-techhub/open-streamer/pkg/protocol"
	"github.com/samber/do/v2"
)

// ErrNoPusherConnected is the fail-fast signal the push registration path
// raises right after reserving a slot in the registry: the encoder hasn't
// connected yet, so the manager should fall over to a lower-priority input
// instead of waiting the full packet-timeout window. Exported so the
// manager can recognise it via errors.Is and SKIP the degrade-then-fallover
// dance when the stream has no other input to swap to (single-input
// publish:// — typical for auto-publish runtime streams). Without that
// skip, the signal races with the first inbound packet and can leave the
// stream stuck in Degraded even though the pipeline is healthy.
var ErrNoPusherConnected = errors.New("ingestor: no pusher connected")

type pullWorkerEntry struct {
	inputPriority int
	cancel        context.CancelFunc
}

// Service manages the full ingest layer.
//
//   - Pull inputs: one goroutine per active stream, auto-reconnects.
//   - Push inputs: stream key is registered in the push server routing table;
//     the next encoder connection for that key is accepted and routed here.
type Service struct {
	// cfg holds the server-level ingestor config (e.g. HLSMaxSegmentBuffer),
	// captured at construction and read when a reader is built.
	cfg config.IngestorConfig
	// listenersPtr holds the latest ListenersConfig. Stored atomically so
	// SetListeners can hot-swap the value while Run reads it once at
	// startup. Runtime.diff calls SetListeners THEN restarts the service
	// so each new Run() observes the fresh config without a server reboot.
	listenersPtr atomic.Pointer[config.ListenersConfig]
	buf          *buffer.Service
	bus          events.Bus
	m            *metrics.Metrics
	registry     *Registry
	vods         *vod.Registry
	onPacket     func(streamID domain.StreamCode, inputPriority int)
	onInputError func(streamID domain.StreamCode, inputPriority int, err error)
	onMedia      func(streamID domain.StreamCode, inputPriority int, p *domain.AVPacket)
	// streamLookup resolves an upstream stream by code; used by the
	// `copy://` reader to find the upstream's main buffer ID. Set via
	// SetStreamLookup after the store is wired in main.go (avoids a
	// store-driver dependency in this package).
	streamLookup pull.StreamLookup

	mu              sync.Mutex
	workers         map[domain.StreamCode]*pullWorkerEntry
	rtmpSrv         *push.RTMPServer // set during Run if listeners.RTMP.Enabled
	pendingPlayFunc push.PlayFunc    // stored before Run, wired in once server is created
	// pendingPushCallbacks holds per-stream callbacks queued before Run
	// created the RTMP server. Drained and applied in Run; nil once the
	// server is live (subsequent registrations call rtmpSrv directly).
	pendingPushCallbacks map[domain.StreamCode]push.StreamCallbacks
	// pendingAutoPublish holds an auto-publish resolver registered before
	// Run created the RTMP server. Same deferred-wiring pattern as the
	// PlayFunc / push callbacks above.
	pendingAutoPublish push.AutoPublishResolver
	// pendingStreamKeyFn holds the push-auth (StreamKey) resolver registered
	// before Run created the RTMP server. Same deferred-wiring pattern.
	pendingStreamKeyFn push.StreamKeyResolver
}

// New creates a Service and registers it with the DI injector.
func New(i do.Injector) (*Service, error) {
	cfg := do.MustInvoke[config.IngestorConfig](i)
	listeners := do.MustInvoke[config.ListenersConfig](i)
	buf := do.MustInvoke[*buffer.Service](i)
	bus := do.MustInvoke[events.Bus](i)
	m := do.MustInvoke[*metrics.Metrics](i)
	vods := do.MustInvoke[*vod.Registry](i)

	svc := &Service{
		cfg:      cfg,
		buf:      buf,
		bus:      bus,
		m:        m,
		vods:     vods,
		registry: NewRegistry(),
		workers:  make(map[domain.StreamCode]*pullWorkerEntry),
	}
	svc.listenersPtr.Store(&listeners)
	return svc, nil
}

// SetListeners hot-swaps the shared listeners config. The next invocation
// of Run() reads the new value at the top of its body. An already-running
// RTMP listener keeps the old bind address until runtime restarts the
// ingestor service — see runtime.diff for the "SetListeners then restart"
// sequencing.
func (s *Service) SetListeners(l config.ListenersConfig) {
	cp := l
	s.listenersPtr.Store(&cp)
}

// currentListeners returns the latest ListenersConfig snapshot. Always
// non-nil after construction.
func (s *Service) currentListeners() config.ListenersConfig {
	if p := s.listenersPtr.Load(); p != nil {
		return *p
	}
	return config.ListenersConfig{}
}

// SetPacketObserver configures a callback invoked for each packet read.
func (s *Service) SetPacketObserver(fn func(streamID domain.StreamCode, inputPriority int)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onPacket = fn
}

// SetInputErrorObserver configures a callback invoked on input read/open failures.
func (s *Service) SetInputErrorObserver(fn func(streamID domain.StreamCode, inputPriority int, err error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onInputError = fn
}

// SetMediaPacketObserver configures a callback invoked once per AVPacket
// (after demux). Used by the manager to track per-track codec, bitrate and
// resolution for the runtime "input media info" panel. Hot path — the
// callback should be allocation-free; avoid retaining p.Data across calls.
func (s *Service) SetMediaPacketObserver(fn func(streamID domain.StreamCode, inputPriority int, p *domain.AVPacket)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onMedia = fn
}

// SetStreamLookup wires the upstream-stream resolver used by `copy://`
// inputs. Called once from main.go after the store is available; passing
// `nil` (or never calling) means copy:// inputs will fail at NewPacketReader
// with an explicit error message.
func (s *Service) SetStreamLookup(fn pull.StreamLookup) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streamLookup = fn
}

// SetRTMPPlayHandler registers an external play handler on the RTMP server.
// Must be called before Run. If the RTMP server is not enabled this is a no-op.
func (s *Service) SetRTMPPlayHandler(fn push.PlayFunc) {
	s.mu.Lock()
	srv := s.rtmpSrv
	s.mu.Unlock()
	if srv != nil {
		srv.SetPlayFunc(fn)
		return
	}
	// Server not yet created (Run not started); store for deferred wiring.
	s.mu.Lock()
	s.pendingPlayFunc = fn
	s.mu.Unlock()
}

// SetAutoPublishResolver installs the fallback resolver consulted by the
// RTMP push server when an encoder targets a key that is not in the
// registry. Same deferred-wiring semantics as SetRTMPPlayHandler — safe
// to call before or after Run.
func (s *Service) SetAutoPublishResolver(r push.AutoPublishResolver) {
	s.mu.Lock()
	srv := s.rtmpSrv
	s.mu.Unlock()
	if srv != nil {
		srv.SetAutoPublishResolver(r)
		return
	}
	s.mu.Lock()
	s.pendingAutoPublish = r
	s.mu.Unlock()
}

// SetPushStreamKeyResolver installs the push-ingest auth resolver (S-8): the
// RTMP server consults it for the configured StreamKey of a stream before
// accepting a publisher. Same deferred-wiring semantics as
// SetAutoPublishResolver — safe to call before or after Run. nil disables
// push key enforcement.
func (s *Service) SetPushStreamKeyResolver(fn push.StreamKeyResolver) {
	s.mu.Lock()
	srv := s.rtmpSrv
	s.mu.Unlock()
	if srv != nil {
		srv.SetStreamKeyResolver(fn)
		return
	}
	s.mu.Lock()
	s.pendingStreamKeyFn = fn
	s.mu.Unlock()
}

// Run starts the shared push/play listeners (RTMP) and blocks until ctx is
// cancelled. Other shared listeners (SRT, RTSP) are owned by the publisher and
// run from runtime.Manager; the same network port serves both ingest and play
// because the listeners config is the single source of truth.
func (s *Service) Run(ctx context.Context) error {
	g, _ := errgroup.WithContext(ctx)

	listeners := s.currentListeners()
	if listeners.RTMP.Enabled {
		rtmpSrv, err := push.NewRTMPServer(
			rtmpListenAddr(listeners.RTMP),
			s.registry,
			s.normaliserConfig(),
		)
		if err != nil {
			return fmt.Errorf("ingestor: create rtmp server: %w", err)
		}

		s.mu.Lock()
		s.rtmpSrv = rtmpSrv
		if s.pendingPlayFunc != nil {
			rtmpSrv.SetPlayFunc(s.pendingPlayFunc)
			s.pendingPlayFunc = nil
		}
		if s.pendingAutoPublish != nil {
			rtmpSrv.SetAutoPublishResolver(s.pendingAutoPublish)
			s.pendingAutoPublish = nil
		}
		if s.pendingStreamKeyFn != nil {
			rtmpSrv.SetStreamKeyResolver(s.pendingStreamKeyFn)
			s.pendingStreamKeyFn = nil
		}
		// Wire pending push callbacks (deferred from startPushRegistration
		// when the server didn't exist yet — typical on cold start where
		// streams are activated before Run binds the listener).
		for streamID, cb := range s.pendingPushCallbacks {
			rtmpSrv.SetStreamCallbacks(streamID, cb)
		}
		s.pendingPushCallbacks = nil
		s.mu.Unlock()
		g.Go(func() error { return rtmpSrv.Run(ctx) })
	}

	return g.Wait()
}

// pushStreamCallbacks builds the per-stream callback set the push server
// fires for one publish session. Closures capture streamID + priority +
// protocol so the push server itself stays unaware of those concepts —
// without this set the Stream Manager never sees packets from a push
// input (Path B has no loopback pull worker) and the stream stays
// Exhausted forever.
//
// All observer pointers are resolved at fire time so SetPacketObserver /
// SetInputErrorObserver / SetMediaPacketObserver (wired by runtime.Manager
// after Service is constructed) don't race with the server starting up.
func (s *Service) pushStreamCallbacks(streamID domain.StreamCode, priority int, proto string) push.StreamCallbacks {
	return push.StreamCallbacks{
		OnConnect: func() {
			//nolint:contextcheck // event publish must outlive any per-session ctx; uses Background by design
			s.bus.Publish(context.Background(), domain.Event{
				Type:       domain.EventInputConnected,
				StreamCode: streamID,
				Payload:    map[string]any{"input_priority": priority, "protocol": proto},
			})
		},
		OnPacket: func() {
			s.mu.Lock()
			fn := s.onPacket
			s.mu.Unlock()
			if fn != nil {
				fn(streamID, priority)
			}
		},
		OnPacketBytes: func(n int) {
			s.m.IngestorBytesTotal.WithLabelValues(string(streamID), proto).Add(float64(n))
			s.m.IngestorPacketsTotal.WithLabelValues(string(streamID), proto).Inc()
		},
		OnMedia: func(p *domain.AVPacket) {
			s.mu.Lock()
			fn := s.onMedia
			s.mu.Unlock()
			if fn != nil {
				fn(streamID, priority, p)
			}
		},
		OnDisconnect: func(err error) {
			s.mu.Lock()
			fn := s.onInputError
			s.mu.Unlock()
			// Push session ended (encoder disconnected) — symmetric with the
			// pull path's "failover" so push streams aren't silent on drop.
			if s.m != nil {
				s.m.IngestorErrorsTotal.WithLabelValues(string(streamID), "disconnect").Inc()
			}
			if fn != nil {
				fn(streamID, priority, err)
			}
			//nolint:contextcheck // event publish must outlive any per-session ctx; uses Background by design
			s.bus.Publish(context.Background(), domain.Event{
				Type:       domain.EventInputFailed,
				StreamCode: streamID,
				Payload:    map[string]any{"input_priority": priority, "protocol": proto, "error": err.Error()},
			})
		},
	}
}

// rtmpListenAddr builds the bind address from the listeners config, applying
// the standard 0.0.0.0 default for a missing host.
func rtmpListenAddr(cfg config.RTMPListenerConfig) string {
	host := cfg.ListenHost
	if host == "" {
		host = domain.DefaultListenHost
	}
	return fmt.Sprintf("%s:%d", host, cfg.Port)
}

// Start activates ingest for the given input on streamID.
//
// The mode is derived from the URL:
//   - publish:// → push-listen: register the stream code in the push server
//     routing table so encoders can connect with just the stream code as key.
//   - rtmp://0.0.0.0:... / srt://0.0.0.0:... → legacy push-listen (same effect).
//   - Any other URL → pull: start a goroutine that connects to the URL.
//
// Calling Start again for the same stream replaces the previous worker/registration.
// bufferWriteID is the Buffer Hub slot for MPEG-TS writes; if empty, streamID is used.
func (s *Service) Start(ctx context.Context, streamID domain.StreamCode, input domain.Input, bufferWriteID domain.StreamCode) error {
	if bufferWriteID == "" {
		bufferWriteID = streamID
	}
	if protocol.IsPushListen(input.URL) {
		//nolint:contextcheck // push registration is sync metadata only; per-session ctx lives inside push.RTMPServer.
		return s.startPushRegistration(streamID, input, bufferWriteID)
	}
	return s.startPullWorker(ctx, streamID, input, bufferWriteID)
}

// Probe performs a short pull-read health probe for one input.
// It is used by the manager to verify degraded inputs before failback.
// Probe sustained-recovery thresholds. A degraded input is only declared
// recovered when it delivers a *sustained* stream — not a single packet. A
// source that merely sputters (an intermittent multicast feed emitting an
// occasional blip while still effectively down) would pass a one-packet check,
// trigger a failback, then starve to the packet-timeout and degrade again —
// flapping between the source and its backup every ~timeout seconds. Requiring
// payload to keep arriving across minProbeSpan kills that blip: a real stream
// streams continuously, a sputtering one does not.
const (
	minProbeSpan    = 1500 * time.Millisecond // payload must keep arriving for at least this long
	minProbePackets = 10                      // and total at least this many payload packets
	minProbeBytes   = 16 * 1024               // and this many payload bytes
)

// sustainedProbeOK reports whether the observed payload constitutes a sustained
// live stream rather than a transient blip. Pure for testability.
func sustainedProbeOK(payloadBytes, payloadPackets int, span time.Duration) bool {
	return payloadPackets >= minProbePackets &&
		payloadBytes >= minProbeBytes &&
		span >= minProbeSpan
}

// probeStats accumulates payload observed during a Probe and decides whether the
// source looks sustained. Payload "span" is measured by arrival time, so a
// sputtering feed (a burst then quiet) never reaches minProbeSpan.
type probeStats struct {
	bytes, packets  int
	firstAt, lastAt time.Time
}

func (a *probeStats) add(batch []domain.AVPacket, now time.Time) {
	for _, p := range batch {
		if len(p.Data) == 0 {
			continue
		}
		a.bytes += len(p.Data)
		a.packets++
		if a.firstAt.IsZero() {
			a.firstAt = now
		}
		a.lastAt = now
	}
}

func (a *probeStats) sustained() bool {
	return sustainedProbeOK(a.bytes, a.packets, a.lastAt.Sub(a.firstAt))
}

func (a *probeStats) notSustainedErr(cause error) error {
	if a.packets == 0 {
		return fmt.Errorf("ingestor: probe got no payload: %w", cause)
	}
	return fmt.Errorf("ingestor: probe source not sustained (%d pkts, %d bytes over %s)",
		a.packets, a.bytes, a.lastAt.Sub(a.firstAt))
}

// Probe reports whether a (degraded) input source is deliverable right now.
// It returns nil only when the source produces a *sustained* stream within ctx;
// a source that is silent or merely sputtering yields an error so the manager
// does not fail back to it prematurely. The caller's ctx deadline must exceed
// minProbeSpan.
func (s *Service) Probe(ctx context.Context, input domain.Input) error {
	if protocol.IsPushListen(input.URL) {
		return fmt.Errorf("ingestor: probe unsupported for push-listen input %q", input.URL)
	}
	reader, err := NewPacketReader(input, s.cfg, s.vods, s.buf, s.streamLookup)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	if err := reader.Open(ctx); err != nil {
		return err
	}

	// Read until the source proves it is sustained (success) or ctx expires
	// (the caller's probe deadline must exceed minProbeSpan).
	var stats probeStats
	for {
		batch, rerr := reader.ReadPackets(ctx)
		stats.add(batch, time.Now())
		if stats.sustained() {
			return nil
		}
		if rerr != nil {
			return stats.notSustainedErr(rerr)
		}
	}
}

// Stop cancels the pull worker or unregisters the push key for streamID.
func (s *Service) Stop(streamID domain.StreamCode) {
	s.mu.Lock()
	if w, ok := s.workers[streamID]; ok {
		slog.Info("ingestor: stopping pull worker",
			"stream_code", streamID,
			"input_priority", w.inputPriority,
		)
		w.cancel()
		delete(s.workers, streamID)
	}
	srv := s.rtmpSrv
	delete(s.pendingPushCallbacks, streamID)
	s.mu.Unlock()
	if srv != nil {
		srv.ClearStreamCallbacks(streamID)
	}
}

// ---- private ----

func (s *Service) startPullWorker(ctx context.Context, streamID domain.StreamCode, input domain.Input, bufferWriteID domain.StreamCode) error {
	reader, err := NewPacketReader(input, s.cfg, s.vods, s.buf, s.streamLookup)
	if err != nil {
		return fmt.Errorf("ingestor: create packet reader: %w", err)
	}

	// Register new worker immediately so Stop() can cancel it at any time.
	//
	// Cancel-prev policy splits on whether this restart is a config update
	// (same priority — operator changed the input URL / params) or a failover
	// (different priority — manager promoted a backup):
	//
	//   - Same priority → cancel previous worker NOW. Its config is stale by
	//     definition; pre-connect handoff would let it keep pumping data from
	//     the now-invalid URL into the buffer, hiding the user's edit and
	//     leaving Status=Active in the UI even when the new URL is unreachable.
	//   - Different priority → keep handoff. The previous source is still
	//     valid; cancelling it before the new one connects creates a buffer
	//     gap that the failover code path is specifically designed to avoid.
	s.mu.Lock()
	var (
		prevCancel      context.CancelFunc
		cancelImmediate bool
	)
	if prev, ok := s.workers[streamID]; ok {
		prevCancel = prev.cancel
		cancelImmediate = prev.inputPriority == input.Priority
	}
	workerCtx, cancel := context.WithCancel(ctx)
	s.workers[streamID] = &pullWorkerEntry{inputPriority: input.Priority, cancel: cancel}
	s.mu.Unlock()

	if cancelImmediate && prevCancel != nil {
		slog.Info("ingestor: config update detected — cancelling previous worker for same priority",
			"stream_code", streamID,
			"input_priority", input.Priority,
		)
		prevCancel()
		prevCancel = nil // already cancelled — don't fire again from onHandoff
	}

	slog.Info("ingestor: starting pull worker",
		"stream_code", streamID,
		"input_priority", input.Priority,
		"url", input.URL,
		"protocol", protocol.Detect(input.URL),
	)

	proto := string(protocol.Detect(input.URL))

	go func() {
		s.mu.Lock()
		cb := pullWorkerCallbacks{
			onPacket: s.onPacket,
			onInputError: func(id domain.StreamCode, prio int, err error) {
				// Classify the read/open error so dashboards can split
				// connect failures from clean EOF from fatal stream errors.
				// ErrNoPusherConnected is the push-slot fast-fail signal, not
				// a real ingest error — skip it.
				if s.m != nil && !errors.Is(err, ErrNoPusherConnected) {
					reason := "open"
					switch {
					case errors.Is(err, io.EOF):
						reason = "eof"
					case shouldFailoverImmediately(err):
						reason = "fatal"
					}
					s.m.IngestorErrorsTotal.WithLabelValues(string(id), reason).Inc()
				}
				s.mu.Lock()
				fn := s.onInputError
				s.mu.Unlock()
				if fn != nil {
					fn(id, prio, err)
				}
			},
			onMedia: s.onMedia,
			onStall: func(id domain.StreamCode, _ int) {
				if s.m != nil {
					s.m.IngestorErrorsTotal.WithLabelValues(string(id), "stall").Inc()
				}
			},
			onConnect: func(id domain.StreamCode, priority int) { //nolint:contextcheck // workerCtx is cancelled on stop; publish must outlive it
				s.bus.Publish(context.Background(), domain.Event{
					Type:       domain.EventInputConnected,
					StreamCode: id,
					Payload:    map[string]any{"input_priority": priority, "url": input.URL},
				})
			},
			onReconnect: func(id domain.StreamCode, priority int, err error) { //nolint:contextcheck // workerCtx is cancelled on stop; publish must outlive it
				s.bus.Publish(context.Background(), domain.Event{
					Type:       domain.EventInputReconnecting,
					StreamCode: id,
					Payload:    map[string]any{"input_priority": priority, "error": err.Error()},
				})
				s.m.IngestorErrorsTotal.WithLabelValues(string(id), "reconnect").Inc()
			},
			onPacketBytes: func(id domain.StreamCode, _ int, n int) {
				s.m.IngestorBytesTotal.WithLabelValues(string(id), proto).Add(float64(n))
				s.m.IngestorPacketsTotal.WithLabelValues(string(id), proto).Inc()
			},
			onHandoff: func() {
				if prevCancel != nil {
					slog.Info("ingestor: pre-connect handoff — releasing previous source",
						"stream_code", streamID,
						"new_input_priority", input.Priority,
					)
					prevCancel()
				}
			},
			// A non-nil prevCancel at scheduling time means the manager
			// is replacing an active worker — i.e. a failover. Otherwise
			// this is a cold start.
			firstSessionReason: firstSessionReasonFor(prevCancel),
			normaliserCfg:      s.normaliserConfig(),
		}
		s.mu.Unlock()
		runPullWorker(workerCtx, streamID, bufferWriteID, input, reader, s.buf, cb)
		// A worker that returned on its own (EOF / fatal read error) is a
		// genuine failover; one whose ctx WE cancelled (Stop, or a same-priority
		// config swap) is not. Capture this BEFORE cancel(), which always sets Err().
		genuineFailover := workerCtx.Err() == nil
		// Ensure the previous worker is always released — handles the case where Stop()
		// is called during pre-connect before onHandoff had a chance to fire.
		if prevCancel != nil {
			prevCancel()
		}
		cancel()
		if genuineFailover {
			if s.m != nil {
				s.m.IngestorErrorsTotal.WithLabelValues(string(streamID), "failover").Inc()
			}
			//nolint:contextcheck // worker ctx is cancelled; publish must outlive it for hooks/manager.
			s.bus.Publish(context.Background(), domain.Event{
				Type:       domain.EventInputFailed,
				StreamCode: streamID,
			})
		}
	}()

	return nil
}

func (s *Service) startPushRegistration(streamID domain.StreamCode, input domain.Input, bufferWriteID domain.StreamCode) error {
	key := pushStreamKey(streamID, input)
	if key == "" {
		return fmt.Errorf("ingestor: cannot determine stream key from URL %q", input.URL)
	}

	s.registry.Register(key, streamID, s.buf, bufferWriteID)

	// Install per-stream callbacks the push server fires when an encoder
	// connects. The callbacks capture priority + protocol so the push
	// server itself never needs those concepts. If Run hasn't created
	// the server yet, queue them on pendingPushCallbacks for deferred
	// wiring.
	cb := s.pushStreamCallbacks(streamID, input.Priority, string(protocol.Detect(input.URL)))
	s.mu.Lock()
	srv := s.rtmpSrv
	if srv == nil {
		if s.pendingPushCallbacks == nil {
			s.pendingPushCallbacks = make(map[domain.StreamCode]push.StreamCallbacks)
		}
		s.pendingPushCallbacks[streamID] = cb
	}
	errObserver := s.onInputError
	s.mu.Unlock()
	if srv != nil {
		srv.SetStreamCallbacks(streamID, cb)
	}

	slog.Info("ingestor: push slot registered",
		"stream_code", streamID,
		"input_priority", input.Priority,
		"url", input.URL,
		"stream_key", key,
	)

	// No pusher is connected yet — notify the manager so it can fall back to
	// a lower-priority input immediately rather than waiting for the packet timeout.
	if errObserver != nil {
		errObserver(streamID, input.Priority, ErrNoPusherConnected)
	}
	return nil
}

// normaliserConfig returns the AV-path PTS Normaliser settings used by
// both the pull worker (one Normaliser per worker lifetime) and the push
// server (one per pub session). PTS normalisation is a server-level
// invariant — operators don't tune it; the server always anchors
// timestamps to local wallclock so upstream clock skew can never poison
// the buffer hub. Thresholds are fixed domain defaults; if any need to
// change, change the constants in domain/defaults.go.
func (s *Service) normaliserConfig() timeline.Config {
	return timeline.Config{
		Enabled:           true,
		JumpThresholdMs:   domain.DefaultPTSJumpThresholdMs,
		MaxAheadMs:        domain.DefaultPTSMaxAheadMs,
		MaxBehindMs:       domain.DefaultPTSMaxBehindMs,
		CrossTrackSnapMs:  1000,
		SameTimebaseMaxMs: 5000,
		// Hold the first track's first packets for up to 1 s while
		// waiting for the partner track's first packet so the joint
		// anchor preserves source-side V/A offset. Without this, feeds
		// whose multicast scheduler emits audio pre-buffered ahead of
		// video by several hundred ms lose that offset to the wallclock
		// arrival fallback, producing visible audio lag in the player.
		// 1 s is comfortably above worst-case observed first-packet
		// arrival skew (~80 ms) and bounds startup latency for single-
		// track streams (audio-only / video-only).
		SeedHoldTimeoutMs: 1000,
	}
}

// pushStreamKey returns the routing key that encoders must use when connecting.
//
// For publish:// URLs the key is the stream code itself — the encoder only
// needs to know the stream code, no app-name prefix is required.
//
// For legacy wildcard-host URLs (rtmp://0.0.0.0:1935/live/<key>) the last
// path segment is used, preserving backward compatibility.
func pushStreamKey(streamID domain.StreamCode, input domain.Input) string {
	if protocol.Detect(input.URL) == protocol.KindPublish {
		return string(streamID)
	}
	// Legacy: extract last path segment from rtmp://0.0.0.0:…/live/<key>
	raw := strings.TrimRight(input.URL, "/")
	if idx := strings.LastIndex(raw, "/"); idx >= 0 {
		return raw[idx+1:]
	}
	return ""
}
