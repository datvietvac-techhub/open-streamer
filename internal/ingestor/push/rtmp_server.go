package push

// rtmp_server.go — RTMP push-ingest server using lal/pkg/rtmp.
//
// Architecture (Path B — no loopback): each encoder pushes to this server
// in publish mode. The server's lal session decodes RTMP chunks into
// `base.RtmpMsg` values, which we convert in place to domain.AVPacket via
// pull.RTMPMsgConverter and write directly into the Buffer Hub.
//
//   Encoder ──RTMP push──► lal.Server (in this file)
//                              │ OnReadRtmpAvMsg(RtmpMsg)
//                              ▼
//                         RTMPMsgConverter (shared with pull reader)
//                              │
//                              ▼
//                         buffer.Service.Write(bufID, Packet{AV: ...})
//
// External RTMP play clients (common play clients) connect to the
// same TCP port; lal raises OnNewRtmpSubSession which delegates to the
// publisher-side PlayFunc — see internal/publisher/serve_rtmp.go for the
// reader-side that streams from the buffer hub into the lal session.
//
// Lifecycle:
//
//   - NewRTMPServer(addr, registry, buf): construct, no I/O yet.
//   - SetPlayFunc(fn): optional, register external play handler.
//   - Run(ctx): bind listener, accept connections until ctx is cancelled.
//
// Each ServerSession runs on its own goroutine inside lal; our callbacks
// (OnReadRtmpAvMsg for pubs, the PlayFunc loop for subs) execute on those
// goroutines and must be cheap. Heavy work (codec config parsing,
// buffer-hub fan-out) happens inside RTMPMsgConverter / buffer.Service
// which are designed for concurrent callers.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/q191201771/lal/pkg/base"
	"github.com/q191201771/lal/pkg/rtmp"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor/pull"
	"github.com/datvietvac-techhub/open-streamer/internal/timeline"
)

// rtmpRouteKey reconstructs the routing key from an RTMP session's app +
// stream name. RTMP carries the path as two pieces — `app` is set during
// the `connect` AMF command (everything between host and the last `/` the
// client treats as the app), `streamName` is set during `publish` / `play`
// (the last URL segment). Combining them with a `/` yields the full URL
// path, from which we always strip a leading `live/` so 1-segment stream
// codes map to `rtmp://host/live/<code>` while multi-segment codes map to
// `rtmp://host/<seg1>/<seg2>/...` directly (no `live/` prefix). A
// single-segment URL that did not come with the `live/` prefix returns ""
// — bare codes are rejected so encoders cannot accidentally hit a 1-segment
// stream from `rtmp://host/<code>`.
func rtmpRouteKey(app, streamName string) string {
	app = strings.TrimSpace(app)
	streamName = strings.TrimSpace(streamName)
	var combined string
	switch {
	case app == "" && streamName == "":
		return ""
	case app == "":
		combined = streamName
	case streamName == "":
		combined = app
	default:
		combined = app + "/" + streamName
	}
	hadLivePrefix := strings.HasPrefix(combined, "live/")
	combined = strings.TrimPrefix(combined, "live/")
	if combined == "" {
		return ""
	}
	if !hadLivePrefix && !strings.Contains(combined, "/") {
		return ""
	}
	return combined
}

// PlayFunc is invoked when an external play client connects to the RTMP
// server. Implementations should subscribe to the Buffer Hub for `key`
// and feed the lal session frames until ctx is cancelled or the session
// disconnects.
//
// Returning a non-nil error causes the session to be torn down with a
// "stream not found" status code.
type PlayFunc func(ctx context.Context, key string, info PlayInfo, session *rtmp.ServerSession) error

// PlayInfo describes the remote peer of an external RTMP play client.
type PlayInfo struct {
	// RemoteAddr is the peer's "ip:port" string from the underlying TCP
	// connection. Useful for the play-sessions tracker / abuse mitigation.
	RemoteAddr string
}

// StreamCallbacks receives per-publish-session events for a single stream.
// The ingestor.Service registers these via SetStreamCallbacks at the same
// time it registers the routing slot — closures capture the input priority
// (and any other manager-side context) so the push server stays agnostic
// of those concerns. Without these the Stream Manager never sees that an
// encoder is connected and the stream stays "Exhausted" forever (Path B
// has no loopback pull worker to surface packets).
//
// Any field may be nil. Callbacks fire on lal's read goroutine and must
// be cheap; defer heavy work.
type StreamCallbacks struct {
	// OnConnect fires once when the encoder finishes the publish handshake.
	OnConnect func()
	// OnPacket fires for every RTMP AV message — drives the manager's
	// Active-status / clear-Exhausted logic.
	OnPacket func()
	// OnPacketBytes fires for every RTMP AV message with the payload byte
	// count, for ingest bytes / packets metrics.
	OnPacketBytes func(n int)
	// OnMedia fires for every emitted AVPacket — drives the manager's
	// per-track codec / bitrate / resolution panel.
	OnMedia func(p *domain.AVPacket)
	// OnDisconnect fires when the publish session ends (encoder closed,
	// network error, server shutdown). The manager uses this to mark
	// the input degraded and trigger failover.
	OnDisconnect func(err error)
}

// AutoPublishResolver materialises a runtime stream when the registry has
// no entry for an incoming push path. Implementations look the path up
// against a set of template prefixes; on a match they synthesise the
// stream (no config record needed) and register it with the ingestor so
// the push server's subsequent registry.Acquire succeeds. Returns
// ErrNoMatch (or a similar sentinel — caller compares with errors.Is)
// when no template prefix accepts the path.
//
// The push server treats this interface as optional: nil disables auto-
// publish, in which case any push to an unregistered path is rejected
// with the existing "stream not registered" semantics.
type AutoPublishResolver interface {
	ResolveOrCreate(ctx context.Context, path string) (domain.StreamCode, error)
}

// RTMPServer accepts RTMP push connections from encoders, validates them
// against the push Registry, and writes decoded AVPackets into the Buffer
// Hub. External RTMP play clients are served via the optional PlayFunc.
type RTMPServer struct {
	addr          string
	registry      Registry
	normaliserCfg timeline.Config

	mu       sync.Mutex
	pubs     map[*rtmp.ServerSession]*pubState
	subs     map[*rtmp.ServerSession]*subState
	playFunc PlayFunc
	resolver AutoPublishResolver

	// streamCallbacks: streamID → per-stream callback set. Populated by
	// the ingestor.Service at startPushRegistration time so the closures
	// can capture priority / metric labels without the push server having
	// to know those concepts.
	cbMu            sync.Mutex
	streamCallbacks map[domain.StreamCode]StreamCallbacks

	server *rtmp.Server
}

// pubState is the per-publish-session bookkeeping: codec converter +
// buffer hub target + per-session Normaliser. A new pubState is built on
// every fresh publisher connection (see OnNewRtmpPubSession), so the
// Normaliser is naturally scoped to one session — OnSession is called
// from there to align it with the SetSession on the buffer hub.
type pubState struct {
	key           string
	bufferWriteID domain.StreamCode
	streamID      domain.StreamCode
	buf           *buffer.Service

	cb StreamCallbacks

	converter  *pull.RTMPMsgConverter
	normaliser *timeline.Normaliser
}

// subState is the per-play-session bookkeeping: cancel func to stop the
// PlayFunc goroutine when the session ends.
type subState struct {
	cancel context.CancelFunc
}

// NewRTMPServer creates an RTMPServer. `addr` is the TCP bind address
// (e.g. ":1935"). `registry` resolves push keys to buffer hub targets.
// `normaliserCfg` controls AV-path PTS anchoring; pass the zero value
// (Enabled=false) to leave incoming PTS untouched.
func NewRTMPServer(addr string, registry Registry, normaliserCfg timeline.Config) (*RTMPServer, error) {
	return &RTMPServer{
		addr:          addr,
		registry:      registry,
		normaliserCfg: normaliserCfg,
		pubs:          make(map[*rtmp.ServerSession]*pubState),
		subs:          make(map[*rtmp.ServerSession]*subState),
	}, nil
}

// SetPlayFunc registers a handler for external RTMP play clients.
// Safe to call concurrently with Run.
func (s *RTMPServer) SetPlayFunc(fn PlayFunc) {
	s.mu.Lock()
	s.playFunc = fn
	s.mu.Unlock()
}

// SetAutoPublishResolver installs the fallback resolver consulted when a
// pusher targets a key that is not in the registry. Passing nil disables
// auto-publish (the server reverts to rejecting unregistered pushes).
// Safe to call concurrently with Run.
func (s *RTMPServer) SetAutoPublishResolver(r AutoPublishResolver) {
	s.mu.Lock()
	s.resolver = r
	s.mu.Unlock()
}

// acquireOrAutoPublish tries to claim the registry slot for key. On miss,
// the auto-publish resolver (when wired) is consulted: it walks template
// prefixes, materialises a runtime stream if one matches, and registers
// the slot synchronously. The acquire is then retried. Any non-nil error
// surfaces as a publish-rejection at the caller.
func (s *RTMPServer) acquireOrAutoPublish(
	key, remoteAddr string,
) (domain.StreamCode, domain.StreamCode, *buffer.Service, error) {
	bufWriteID, streamID, buf, err := s.registry.Acquire(key)
	if err == nil {
		return bufWriteID, streamID, buf, nil
	}
	s.mu.Lock()
	resolver := s.resolver
	s.mu.Unlock()
	if resolver == nil {
		return "", "", nil, err
	}
	// Use a fresh background context — the resolver may outlive the
	// callback (it spawns goroutines for the runtime stream). lal's
	// OnNewRtmpPubSession blocks until we return, so the timeout is
	// effectively the publish handshake's own deadline; nothing extra
	// to enforce here.
	code, rerr := resolver.ResolveOrCreate(context.Background(), key)
	if rerr != nil {
		slog.Debug("rtmp server: auto-publish resolver declined",
			"key", key, "remote", remoteAddr, "err", rerr)
		return "", "", nil, err // surface the original "not registered" error
	}
	return s.registry.Acquire(string(code))
}

// SetStreamCallbacks installs the per-session callback set for streamID.
// The ingestor.Service calls this once it has registered the routing
// slot (see startPushRegistration). Without callbacks the Stream Manager
// never sees that an encoder connected and the stream stays Exhausted.
//
// Pass an empty StreamCallbacks{} to detach (or call ClearStreamCallbacks).
// Replaces any previous callbacks for the same stream.
func (s *RTMPServer) SetStreamCallbacks(streamID domain.StreamCode, cb StreamCallbacks) {
	s.cbMu.Lock()
	if s.streamCallbacks == nil {
		s.streamCallbacks = make(map[domain.StreamCode]StreamCallbacks)
	}
	s.streamCallbacks[streamID] = cb
	s.cbMu.Unlock()
}

// ClearStreamCallbacks removes the callbacks for streamID. Safe to call
// for an unregistered stream; subsequent push sessions for streamID will
// simply have no observer hooks (the buffer-write path is unaffected).
func (s *RTMPServer) ClearStreamCallbacks(streamID domain.StreamCode) {
	s.cbMu.Lock()
	delete(s.streamCallbacks, streamID)
	s.cbMu.Unlock()
}

// callbacksFor returns the registered callbacks for streamID, or a zero
// value if none were installed. Zero StreamCallbacks fields are safe to
// invoke (each pubState method nil-checks before calling).
func (s *RTMPServer) callbacksFor(streamID domain.StreamCode) StreamCallbacks {
	s.cbMu.Lock()
	defer s.cbMu.Unlock()
	return s.streamCallbacks[streamID]
}

// Run binds the TCP listener and accepts RTMP connections until ctx is
// cancelled. lal's RunLoop blocks until the listener closes; we close
// the listener via Dispose() when ctx fires so RunLoop returns nil.
func (s *RTMPServer) Run(ctx context.Context) error {
	srv := rtmp.NewServer(s.addr, s)
	s.mu.Lock()
	s.server = srv
	s.mu.Unlock()

	if err := srv.Listen(); err != nil {
		return fmt.Errorf("rtmp server: listen %q: %w", s.addr, err)
	}
	slog.Info("rtmp server: listening", "addr", s.addr)

	// Watchdog: close the listener on context cancellation so RunLoop
	// unblocks. Spawned before RunLoop so it's already armed when accepts
	// start coming in.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			srv.Dispose()
		case <-done:
		}
	}()

	err := srv.RunLoop()
	close(done)
	if ctx.Err() != nil {
		//nolint:nilerr // listener was closed by our watchdog on ctx.Done — shutdown, not failure.
		return nil
	}
	if err != nil {
		return fmt.Errorf("rtmp server: run loop: %w", err)
	}
	return nil
}

// ─── lal IServerObserver ────────────────────────────────────────────────

// OnRtmpConnect is invoked once per accepted TCP connection at the end of
// the RTMP `connect` command. We log the remote app/tcUrl combo for ops
// and don't reject anything here — rejections happen at OnNewRtmpPubSession
// / OnNewRtmpSubSession when we know whether the key is registered.
func (s *RTMPServer) OnRtmpConnect(session *rtmp.ServerSession, _ rtmp.ObjectPairArray) {
	slog.Debug("rtmp server: client connected",
		"remote", session.GetStat().RemoteAddr,
		"app", session.AppName(),
	)
}

// OnNewRtmpPubSession is invoked when an encoder issues a `publish`
// command. We acquire the registry slot for the stream key and wire the
// session's AV-message observer to feed our converter.
//
// Returning an error rejects the session — lal sends a NetStream.Publish.BadName
// status code and tears down the connection.
func (s *RTMPServer) OnNewRtmpPubSession(session *rtmp.ServerSession) error {
	key := rtmpRouteKey(session.AppName(), session.StreamName())
	if key == "" {
		slog.Warn("rtmp server: rejected publisher (invalid stream path)",
			"app", session.AppName(),
			"stream_name", session.StreamName(),
			"remote", session.GetStat().RemoteAddr,
		)
		return errors.New("rtmp server: invalid stream path")
	}
	bufWriteID, streamID, buf, err := s.acquireOrAutoPublish(key, session.GetStat().RemoteAddr)
	if err != nil {
		slog.Warn("rtmp server: rejected publisher",
			"key", key,
			"remote", session.GetStat().RemoteAddr,
			"err", err,
		)
		return err
	}

	cb := s.callbacksFor(streamID)
	state := &pubState{
		key:           key,
		bufferWriteID: bufWriteID,
		streamID:      streamID,
		buf:           buf,
		cb:            cb,
		converter:     pull.NewRTMPMsgConverter(),
		normaliser:    timeline.New(s.normaliserCfg),
	}
	// Mint a new StreamSession on the target buffer AND reset the
	// Normaliser's per-track anchor state in lockstep. A pusher
	// connecting is functionally equivalent to a pull reader's Reconnect
	// — fresh PTS origin, fresh codec config — so we use that reason
	// regardless of whether this is the very first publisher.
	sess := buf.SetSession(bufWriteID, domain.SessionStartReconnect, nil, nil)
	state.normaliser.OnSession(sess)
	session.SetPubSessionObserver(state)

	s.mu.Lock()
	s.pubs[session] = state
	s.mu.Unlock()

	if cb.OnConnect != nil {
		cb.OnConnect()
	}

	slog.Info("rtmp server: publisher accepted",
		"key", key, "stream_id", streamID,
		"remote", session.GetStat().RemoteAddr,
	)
	return nil
}

// OnDelRtmpPubSession is invoked when a publish session ends (encoder
// disconnect, network error, server shutdown). Releases the registry
// slot so the next pusher can claim the key, and tells the manager the
// input has gone away so it can fail over.
func (s *RTMPServer) OnDelRtmpPubSession(session *rtmp.ServerSession) {
	s.mu.Lock()
	state, ok := s.pubs[session]
	delete(s.pubs, session)
	s.mu.Unlock()
	if !ok {
		return
	}
	s.registry.Release(state.key)
	if state.cb.OnDisconnect != nil {
		state.cb.OnDisconnect(errPusherDisconnected)
	}
	slog.Info("rtmp server: publisher disconnected",
		"key", state.key, "stream_id", state.streamID,
	)
}

// errPusherDisconnected is the sentinel handed to OnDisconnect. The manager
// only uses it for logging / event payload — the failover decision is
// driven by the input priority going degraded.
var errPusherDisconnected = errors.New("rtmp pusher disconnected")

// OnNewRtmpSubSession is invoked when a play client connects. We delegate
// to the registered PlayFunc (publisher side). If no PlayFunc is wired,
// reject — playback isn't available without the publisher being present.
func (s *RTMPServer) OnNewRtmpSubSession(session *rtmp.ServerSession) error {
	s.mu.Lock()
	fn := s.playFunc
	s.mu.Unlock()
	key := rtmpRouteKey(session.AppName(), session.StreamName())
	if key == "" {
		slog.Warn("rtmp server: play rejected (invalid stream path)",
			"app", session.AppName(),
			"stream_name", session.StreamName(),
			"remote", session.GetStat().RemoteAddr,
		)
		return errors.New("rtmp server: invalid stream path")
	}
	if fn == nil {
		slog.Warn("rtmp server: play rejected (no PlayFunc registered)",
			"key", key,
		)
		return errors.New("rtmp server: play handler not configured")
	}

	info := PlayInfo{RemoteAddr: session.GetStat().RemoteAddr}

	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.subs[session] = &subState{cancel: cancel}
	s.mu.Unlock()

	// Run the play handler on its own goroutine — lal will block this
	// callback until we return, so we can't drive the read loop from
	// here. The handler is responsible for stopping when ctx fires.
	go func() {
		err := fn(ctx, key, info, session)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Debug("rtmp server: play session ended",
				"key", key, "err", err)
		}
		_ = session.Dispose()
	}()

	slog.Info("rtmp server: play client accepted",
		"key", key, "remote", info.RemoteAddr,
	)
	return nil
}

// OnDelRtmpSubSession is invoked when a play session ends. Cancels the
// PlayFunc goroutine so it stops reading from the buffer hub.
func (s *RTMPServer) OnDelRtmpSubSession(session *rtmp.ServerSession) {
	s.mu.Lock()
	state, ok := s.subs[session]
	delete(s.subs, session)
	s.mu.Unlock()
	if ok {
		state.cancel()
	}
}

// ─── lal IPubSessionObserver (implemented by *pubState) ────────────────

// OnReadRtmpAvMsg is invoked by lal for every audio/video RtmpMsg the
// publisher sends. Convert to AVPackets, write to the buffer hub, and
// fire ingest observers so the Stream Manager sees liveness and codec
// metadata for this push input (without these the manager treats the
// stream as Exhausted forever — there is no loopback worker to surface
// packets in Path B).
//
// The session-boundary cue (first packet after a fresh publish session)
// is conveyed via the buffer hub's SessionStart=true marker on the next
// Write — already latched by SetSession in OnNewRtmpPubSession. No
// per-packet Discontinuity flag is set here; consumers read the session
// boundary explicitly.
func (p *pubState) OnReadRtmpAvMsg(msg base.RtmpMsg) {
	if p.cb.OnPacket != nil {
		p.cb.OnPacket()
	}
	if p.cb.OnPacketBytes != nil {
		p.cb.OnPacketBytes(len(msg.Payload))
	}
	for _, av := range p.converter.Convert(msg) {
		cl := av.Clone()
		// Anchor PTS/DTS via the Normaliser. Push-mode encoders are
		// normally already wallclock-aligned, so the drift cap rarely
		// fires — but skip the buffer write when the Normaliser drops a
		// packet (sustained input ahead of wallclock past MaxAheadMs) or
		// holds it during joint-seed hold. Joint-seed completion emits
		// the buffered packets alongside the current one — iterate so
		// each lands in the buffer hub with its stamped PTS/DTS.
		for _, q := range p.normaliser.Apply(cl, time.Now()) {
			if err := p.buf.Write(p.bufferWriteID, buffer.Packet{AV: q}); err != nil {
				slog.Debug("rtmp server: buffer write failed",
					"key", p.key, "err", err)
			}
			if p.cb.OnMedia != nil {
				p.cb.OnMedia(q)
			}
		}
	}
}
