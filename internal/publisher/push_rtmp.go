package publisher

// push_rtmp.go — outbound RTMP/RTMPS re-stream (push to external endpoint).
//
// Data flow:
//
//	Buffer Hub → tsmux.FeedWirePacket → tsBuffer → mpeg2.TSDemuxer
//	                                                        │
//	                                        H.264 Annex-B + AAC ADTS frames
//	                                                        │
//	                          lal.AvPacket2RtmpRemuxer → base.RtmpMsg
//	                                                        │
//	                          lal.PushSession.WriteMsg → remote RTMP(S) server
//
// Why lal (q191201771/lal) instead of yapingcat/gomedia:
//
// gomedia's RtmpClient panics on AMF marker bytes it doesn't recognise (we hit
// "unsupport amf type 98" pushing to a strict media server — the panic killed the whole
// service). lal's AMF parser returns errors instead of panicking, and its
// publish handshake is a strict synchronous Push() that returns only after the
// remote replies — no separate publish.start state machine to gate writes
// against, no pending-frame queue.
//
// Codec adapter: lal's PushSession transports already-FLV-encoded RtmpMsg
// (header + payload). Conversion from raw codec bytes (H.264 Annex-B NALU,
// AAC ADTS frame) to FLV tags is delegated to lal.AvPacket2RtmpRemuxer:
//   - Tracks SPS/PPS in the NALU stream and emits AVCDecoderConfigurationRecord
//     as the first video FLV tag automatically.
//   - Strips the 7-byte ADTS header from each AAC frame and emits
//     AudioSpecificConfig as the first audio FLV tag automatically.
//   - The OnRtmpMsg callback receives the wrapped FLV-tag base.RtmpMsg ready
//     to write.
//
// Reconnect: on any Push handshake failure, write error, or session close the
// caller (serveRTMPPush) waits RetryTimeoutSec and starts a fresh packager.
// Each packager builds a fresh remuxer, so SPS/PPS / ASC are re-emitted on
// every reconnect — no codec state needs to survive across sessions.
//
// Input switching: when the ingestor switches input source, the next
// buffer.Packet carries SessionStart=true. feedLoop signals the packager
// to end via errDiscontinuity. serveRTMPPush starts a fresh session with
// no retry delay so the new source can take over with minimum gap.
//
// RTMPS: rtmps:// URLs are handled natively by lal via PushSessionOption.TlsConfig.
// Default TLS config uses ServerName from the URL host.

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/q191201771/lal/pkg/base"
	"github.com/q191201771/lal/pkg/rtmp"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/tsdemux"
	"github.com/datvietvac-techhub/open-streamer/internal/tsmux"
)

const (
	// defaultPushTimeout is the publish handshake budget when dest.TimeoutSec is unset.
	defaultPushTimeout = time.Duration(domain.DefaultPushTimeoutSec) * time.Second
	// pushHandshakeBackstop adds a small grace period over the lal-internal
	// timeout so we only see our own deadline tick if lal itself wedges.
	pushHandshakeBackstop = 2 * time.Second
)

// errDiscontinuity signals that the input source changed (failover / manual
// switch). The session must be torn down and restarted because the new source
// may have different codec parameters.
var errDiscontinuity = errors.New("input discontinuity")

// rtmpPushPackager holds state for one outbound RTMP push session.
type rtmpPushPackager struct {
	streamID   domain.StreamCode
	url        string
	timeoutSec int

	// onPublishStart fires once after the lal Push handshake completes —
	// surfaced so the runtime status can flip from "starting" to "active"
	// without leaking PushSession internals to the Service layer.
	onPublishStart func()

	// onBytesWritten observes the byte size of each RtmpMsg payload sent
	// to the remote target. Used by the egress-cost dashboard. Nil-safe —
	// tests build the packager without metrics.
	onBytesWritten func(n int)

	session *rtmp.PushSession
	codec   *pushCodecAdapter

	// Base DTS for relative timestamps. mpeg2.TSDemuxer already converts
	// 90 kHz ticks to ms before calling onTSFrame; baseDTS captures the
	// first frame's value so the published timeline starts at 0 regardless
	// of source clock origin.
	baseDTS    int64
	baseDTSSet bool

	// connErr holds the first session failure. failOnce ensures the first
	// error wins so subsequent goroutines don't overwrite it.
	connErr   atomic.Pointer[error]
	failOnce  sync.Once
	closeOnce sync.Once

	// tsBuf is the TS buffer fed by feedLoop and read by the demuxer. Stored
	// on the packager so other goroutines (failConn) can close it on
	// connection failure to unblock the demuxer.
	tsBuf *tsBuffer

	// gotDiscontinuity is set by feedLoop when it sees
	// buffer.Packet.SessionStart=true on an established session. run()
	// then returns errDiscontinuity and serveRTMPPush starts a fresh
	// session with no backoff delay. (Naming kept for historical
	// continuity with the original AVPacket.Discontinuity-based design.)
	gotDiscontinuity atomic.Bool
}

// failConn marks the session as failed and tears down the connection so the
// demuxer goroutine unblocks and run() can exit. Idempotent.
func (p *rtmpPushPackager) failConn(err error) {
	p.failOnce.Do(func() {
		p.connErr.Store(&err)
		p.closeConn()
		if p.tsBuf != nil {
			p.tsBuf.Close()
		}
	})
}

// run is the entry point: drives publish handshake, then pumps demuxed frames
// into the lal RTMP session until ctx is cancelled, the input ends, or any
// session error occurs.
func (p *rtmpPushPackager) run(ctx context.Context, sub *buffer.Subscriber) error {
	if err := p.connect(ctx); err != nil {
		return err
	}

	// Surface session-side errors (network drop, server close, write timeout)
	// asynchronously into our connErr so the demuxer loop unwinds promptly.
	go p.watchSessionExit()

	tb := newTSBuffer(p.streamID)
	p.tsBuf = tb
	go p.feedLoop(ctx, sub, tb)

	dmx := tsdemux.New()
	dmx.OnFrame = p.onTSFrame
	demuxDone := make(chan error, 1)
	go func() { demuxDone <- dmx.Input(ctx, tb) }()

	select {
	case <-ctx.Done():
		tb.Close()
		<-demuxDone
	case err := <-demuxDone:
		if err != nil && ctx.Err() == nil && !p.gotDiscontinuity.Load() {
			slog.Warn("publisher: RTMP push TS demux ended",
				"stream_code", p.streamID, "url", p.url, "err", err)
		}
	}

	p.closeConn()

	if p.gotDiscontinuity.Load() {
		slog.Info("publisher: RTMP push session ending on discontinuity",
			"stream_code", p.streamID, "url", p.url)
		return errDiscontinuity
	}
	if errPtr := p.connErr.Load(); errPtr != nil {
		return *errPtr
	}
	return nil
}

// watchSessionExit converts an async lal session error into a packager fail
// so the demuxer / feedLoop chain unblocks instead of silently spinning on a
// dead connection.
func (p *rtmpPushPackager) watchSessionExit() {
	if p.session == nil {
		return
	}
	err, ok := <-p.session.WaitChan()
	if !ok {
		return
	}
	if err != nil {
		p.failConn(fmt.Errorf("rtmp session ended: %w", err))
	}
}

// connect drives the lal PushSession handshake. Push() blocks until the
// remote replies with the publish ack, so when it returns nil we are
// guaranteed the server is ready to accept media — no separate ready gate.
func (p *rtmpPushPackager) connect(ctx context.Context) error {
	u, err := url.Parse(p.url)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}
	if u.Scheme != "rtmp" && u.Scheme != "rtmps" {
		return fmt.Errorf("unsupported scheme %q (only rtmp/rtmps)", u.Scheme)
	}

	timeout := time.Duration(p.timeoutSec) * time.Second
	if timeout <= 0 {
		timeout = defaultPushTimeout
	}

	slog.Info("publisher: RTMP push connecting",
		"stream_code", p.streamID, "url", p.url)

	p.session = rtmp.NewPushSession(func(option *rtmp.PushSessionOption) {
		option.PushTimeoutMs = int(timeout / time.Millisecond)
		// Complex handshake matches FFmpeg and common encoders — required by most strict
		// peers (common RTMP destinations). Plain handshake breaks
		// silently on those.
		option.HandshakeComplexFlag = true
		if u.Scheme == "rtmps" {
			option.TlsConfig = &tls.Config{
				ServerName: u.Hostname(),
				MinVersion: tls.VersionTLS12,
			}
		}
	})

	// Push() blocks. Run it in a goroutine so we can honour ctx cancellation
	// AND apply a backstop deadline (lal-internal PushTimeoutMs is the primary
	// timer; the backstop only fires if lal itself wedges).
	pushDone := make(chan error, 1)
	go func() { pushDone <- p.session.Push(p.url) }()

	select {
	case err := <-pushDone:
		if err != nil {
			_ = p.session.Dispose()
			return fmt.Errorf("push handshake: %w", err)
		}
	case <-ctx.Done():
		_ = p.session.Dispose()
		return ctx.Err()
	case <-time.After(timeout + pushHandshakeBackstop):
		_ = p.session.Dispose()
		return fmt.Errorf("push handshake timeout after %s", timeout)
	}

	slog.Info("publisher: RTMP push publish started",
		"stream_code", p.streamID, "url", p.url)

	if p.onPublishStart != nil {
		p.onPublishStart()
	}

	// Wire the codec → FLV tag → RtmpMsg adapter. SPS/PPS and AAC ASC are
	// extracted from the input stream itself and emitted as sequence headers
	// the first time they appear. composition_time = (PTS - DTS) is preserved
	// so B-frames render correctly at the receiver — see push_codec.go for
	// rationale (lal's high-level remuxer drops PTS).
	p.codec = newPushCodecAdapter(func(msg base.RtmpMsg) error {
		if err := p.session.WriteMsg(msg); err != nil {
			return err
		}
		if p.onBytesWritten != nil {
			// Counts FLV/RTMP payload bytes only — RTMP chunk header
			// overhead (~12 B per message) is excluded because the
			// metric is meant for "bytes seen by the receiver", not
			// "bytes on the wire including framing". Operators sizing
			// their CDN egress care about the former.
			p.onBytesWritten(len(msg.Payload))
		}
		return nil
	})

	return nil
}

// feedLoop reads packets from sub, reassembles 188-byte TS packets via
// alignedFeed, and pipes them into tb. Runs in its own goroutine.
//
// Session-boundary handling: every fresh StreamSession on the buffer hub
// (cold start, reconnect, failover, mixer cycle, codec change) carries
// buffer.Packet.SessionStart=true on its first packet. Once we have an
// active push session (connErr nil), any SessionStart must restart the
// downstream push session because the remote codec params and PCR base
// may have changed. The first SessionStart of the worker (before connErr
// is settled) is consumed by the initial setup path, not here.
func (p *rtmpPushPackager) feedLoop(ctx context.Context, sub *buffer.Subscriber, tb *tsBuffer) {
	defer tb.Close()
	var tsCarry []byte
	var avMux *tsmux.FromAV

	for {
		select {
		case <-ctx.Done():
			return
		case pkt, ok := <-sub.Recv():
			if !ok {
				return
			}
			if pkt.SessionStart && p.connErr.Load() == nil {
				p.gotDiscontinuity.Store(true)
				return
			}
			tsmux.FeedWirePacket(ctx, pkt.TS, pkt.AV, &avMux, func(b []byte) {
				alignedFeed(b, &tsCarry, func(pkt188 []byte) bool {
					_, err := tb.Write(pkt188)
					return err == nil
				})
			})
		}
	}
}

// onTSFrame is the TSDemuxer callback; runs in the demuxer goroutine.
// Dispatches by stream type to the codec adapter, which composes the FLV
// tag (with proper composition_time for B-frames) and writes via lal.
//
//nolint:exhaustive // RTMP push only carries H.264 + AAC; H.265 needs Enhanced RTMP, MP3 unsupported by most ingest servers
func (p *rtmpPushPackager) onTSFrame(cid tsdemux.StreamType, frame []byte, pts, dts uint64) {
	if p.connErr.Load() != nil || len(frame) == 0 {
		return
	}

	if !p.baseDTSSet {
		p.baseDTS = int64(dts) //nolint:gosec
		p.baseDTSSet = true
	}
	// tsdemux exposes PTS / DTS in milliseconds already (astits's raw
	// 90 kHz tick value is divided by 90 in the wrapper). Do NOT
	// divide again here.
	relDTS := int64(dts) - p.baseDTS //nolint:gosec
	if relDTS < 0 {
		relDTS = 0
	}
	relPTS := int64(pts) - p.baseDTS //nolint:gosec
	if relPTS < 0 {
		relPTS = 0
	}

	var err error
	switch cid { //nolint:exhaustive // see top-of-function comment
	case tsdemux.StreamTypeH264:
		err = p.codec.FeedH264(frame, relDTS, relPTS)
	case tsdemux.StreamTypeAAC:
		err = p.codec.FeedAAC(frame, relDTS)
	default:
		return
	}
	if err != nil {
		p.failConn(fmt.Errorf("write rtmp msg: %w", err))
	}
}

// closeConn disposes the lal session exactly once. Safe from multiple
// goroutines (failConn from various paths, plus run() at end).
func (p *rtmpPushPackager) closeConn() {
	p.closeOnce.Do(func() {
		if p.session != nil {
			_ = p.session.Dispose()
		}
	})
}

// serveRTMPPush is the publisher goroutine for one outbound PushDestination.
// It creates a fresh packager session per connection attempt and reconnects
// on error or input discontinuity.
//
// Runtime state: each lifecycle event updates the per-(stream,url) pushState
// so the API can show what each push destination is doing right now —
// starting, active, reconnecting (with last 5 errors), or failed.
func (s *Service) serveRTMPPush(
	ctx context.Context,
	streamID domain.StreamCode,
	mediaBufferID domain.StreamCode,
	dest domain.PushDestination,
) {
	if !strings.HasPrefix(dest.URL, "rtmp://") && !strings.HasPrefix(dest.URL, "rtmps://") {
		slog.Warn("publisher: RTMP push unsupported scheme (only rtmp:// and rtmps:// supported)",
			"stream_code", streamID, "url", dest.URL)
		return
	}

	// Track this destination from spawn through teardown so the API never
	// shows a stale "starting" entry after the goroutine has exited.
	s.setPushStatus(streamID, dest.URL, PushStatusStarting)
	defer s.removePushState(streamID, dest.URL)

	retryDelay := time.Duration(dest.RetryTimeoutSec) * time.Second
	if retryDelay <= 0 {
		retryDelay = time.Duration(domain.DefaultPushRetryTimeoutSec) * time.Second
	}

	attempts := 0

	for {
		if ctx.Err() != nil {
			return
		}
		if dest.Limit > 0 && attempts >= dest.Limit {
			slog.Warn("publisher: RTMP push reached retry limit",
				"stream_code", streamID, "url", dest.URL, "limit", dest.Limit)
			s.setPushStatus(streamID, dest.URL, PushStatusFailed)
			return
		}
		attempts++
		s.setPushAttempt(streamID, dest.URL, attempts)

		slog.Info("publisher: RTMP push session starting",
			"stream_code", streamID, "url", dest.URL, "attempt", attempts)

		sessionErr := s.runOnePushSession(ctx, streamID, mediaBufferID, dest)

		if ctx.Err() != nil {
			return
		}

		if errors.Is(sessionErr, errDiscontinuity) {
			slog.Info("publisher: RTMP push input discontinuity, restarting session",
				"stream_code", streamID, "url", dest.URL)
			attempts = 0
			s.setPushAttempt(streamID, dest.URL, 0)
			continue
		}

		if sessionErr != nil {
			slog.Warn("publisher: RTMP push session ended, retrying",
				"stream_code", streamID, "url", dest.URL, "err", sessionErr,
				"retry_in", retryDelay)
			s.recordPushError(streamID, dest.URL, sessionErr.Error())
		} else {
			slog.Info("publisher: RTMP push session ended cleanly, retrying",
				"stream_code", streamID, "url", dest.URL,
				"retry_in", retryDelay)
		}
		s.setPushStatus(streamID, dest.URL, PushStatusReconnecting)

		select {
		case <-ctx.Done():
			return
		case <-time.After(retryDelay):
		}
	}
}

// runOnePushSession subscribes to the media buffer, runs one packager session,
// then unsubscribes.
func (s *Service) runOnePushSession(
	ctx context.Context,
	streamID domain.StreamCode,
	mediaBufferID domain.StreamCode,
	dest domain.PushDestination,
) error {
	sub, err := s.buf.Subscribe(mediaBufferID)
	if err != nil {
		slog.Warn("publisher: RTMP push subscribe failed",
			"stream_code", streamID, "url", dest.URL, "err", err)
		return err
	}

	p := &rtmpPushPackager{
		streamID:   streamID,
		url:        dest.URL,
		timeoutSec: dest.TimeoutSec,
		onPublishStart: func() {
			s.setPushStatus(streamID, dest.URL, PushStatusActive)
		},
		onBytesWritten: s.pushBytesObserver(streamID, dest.URL),
	}

	sessionErr := p.run(ctx, sub)
	s.buf.Unsubscribe(mediaBufferID, sub)
	return sessionErr
}
