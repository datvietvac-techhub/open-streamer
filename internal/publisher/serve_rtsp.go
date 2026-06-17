package publisher

// serve_rtsp.go — RTSP play server (publisher-side).
//
// Architecture (two ingest paths into the same RTP encoder):
//
//	Buffer Hub → subscriber ──┬─ AV packet  ────────────────────────┐
//	                          │  (direct: keeps PTS / DTS / IDR     │
//	                          │   intact, no MPEG-TS round-trip)    │
//	                          │                                     ▼
//	                          │                            rtspSession
//	                          │                       (codec detection +
//	                          │                        RTP encoding)
//	                          │                                     ▲
//	                          └─ TS bytes ─→ tsdemux.Demuxer ──────┘
//	                              (legacy raw-TS sources only:
//	                               UDP multicast, file:// .ts, copy://)
//	                                                  │
//	                                gortsplib.ServerStream.WritePacketRTP
//	                                                  │
//	                                             RTSP clients
//
// AV packets bypass the MPEG-TS round-trip because the upstream pipeline
// already produces clean Annex-B (H.264) / ADTS (AAC) byte streams with
// millisecond PTS / DTS metadata. Re-muxing to TS just to re-demux
// introduces a 1-AU latency in the demuxer's access-unit detection, and
// round-trips PTS/DTS through an extra 90 kHz ↔ ms scaling step that's
// been the source of every "non-monotonic DTS / freezes after 1s / 0
// frames muxed" regression we've chased.
//
// Stream registration lifecycle:
//  1. serveRTSP waits for the gortsplib server to start, then subscribes to the Buffer Hub.
//  2. Frames are collected until SPS/PPS (H.264) and AAC AudioSpecificConfig are known
//     (or up to maxRTSPPendingFrames video frames if audio never arrives).
//  3. A ServerStream is created and mounted at /live/<stream_code>; the handler starts
//     returning it to DESCRIBE/SETUP requests.
//  4. On stream stop, the mount is removed and the ServerStream is closed.
//
// Client URL: rtsp://host:port/live/<stream_code> for single-segment codes;
// rtsp://host:port/<seg1>/<seg2>/... for multi-segment codes (live/ omitted).
// Port from listeners.rtsp.port.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtph264"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtpmpeg4audio"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/sessions"
	"github.com/datvietvac-techhub/open-streamer/internal/tsdemux"
	"github.com/datvietvac-techhub/open-streamer/internal/tsmux"
)

// maxRTSPPendingFrames is the maximum number of video frames buffered while
// waiting for AAC codec info. After this limit, the stream is initialised with
// video-only (no audio track in SDP).
const maxRTSPPendingFrames = 90

// rtspMaxBackJumpMs is the threshold below which a backwards source DTS jump
// is treated as a discontinuity (loop / restart) and the RTP base is reset.
// 250ms tolerates normal B-frame reordering (≤ a few frame periods) without
// false-positive rebase, and catches real source resets (which jump by
// seconds or back to 0). H.264 spec allows DTS reorder up to a few frames
// for B-pyramid; 250ms ≈ 6 frames at 25fps which is the practical max.
//
// Smaller jitter (<250ms back) is handled by the monotonic clamp at the end
// of computeVideoRTP / computeAudioRTP — it forces the RTP timestamp to
// strictly increase even if the source DTS jitters within the rebase window.
const rtspMaxBackJumpMs = 250

// Pacing constants — enforce wallclock-based RTP delivery so the on-wire
// packet rate matches the source's media-time rate, regardless of how the
// upstream feeds the buffer hub.
//
// Bursty sources (HLS pulls deliver one segment ~every targetduration; NVENC
// transcoders empty their queue much faster than realtime) push hundreds of
// AV packets in <1s, then idle for 4-6s. Forwarding that pattern straight to
// `gortsplib.ServerStream.WritePacketRTP` makes strict RTSP clients (VLC,
// strict consumers including ffmpeg copy) underrun their jitter buffer between bursts and
// either freeze, drop frames, or surface "non monotonically increasing dts"
// because the inter-packet wallclock delta no longer matches the RTP/PTS
// delta the demuxer expected.
//
// Pacing each packet against a (paceBase wallclock, paceMedia DTS) anchor
// converts those bursts back into a smooth, realtime-paced stream — costs
// at most one segment-duration of latency on top of whatever the source
// already adds, in exchange for "every RTSP client just works".
const (
	// rtspPaceMaxSleep caps a single pace sleep. PTS jumps far ahead of
	// wallclock (looped source, very fresh keyframe, encoder restart) trip
	// this and force a re-anchor instead of stalling forever.
	rtspPaceMaxSleep = 5 * time.Second

	// rtspPaceMaxLag is how far behind realtime we tolerate before
	// re-anchoring. If we ever fall this far behind (sustained source
	// underruns), continuing to chase the old anchor adds latency without
	// helping playback — restart the timeline at "now".
	rtspPaceMaxLag = 2 * time.Second
)

// rtspLiveMountPath returns the URL path a stream is mounted at. Single-segment
// codes (no '/') get the `live/` prefix so clients dial
// rtsp://host/live/<code>; multi-segment codes are mounted at their raw path
// (`<seg1>/<seg2>/...`) and clients omit the `live/` prefix entirely.
func rtspLiveMountPath(code domain.StreamCode) string {
	s := string(code)
	if strings.Contains(s, "/") {
		return s
	}
	return "live/" + s
}

// RunRTSPPlayServer starts the RTSP play listener.
// Returns nil immediately when listeners.rtsp is disabled.
func (s *Service) RunRTSPPlayServer(ctx context.Context) error {
	rtsp := s.currentListeners().RTSP
	if !rtsp.Enabled || rtsp.Port == 0 {
		close(s.rtspSrvReady)
		return nil
	}
	host := rtsp.ListenHost
	if host == "" {
		host = domain.DefaultListenHost
	}
	addr := fmt.Sprintf("%s:%d", host, rtsp.Port)

	srv := &gortsplib.Server{
		RTSPAddress: addr,
		Handler:     &rtspHandler{svc: s},
		// Per-session RTP write queue, sized for 4K25 / 1080p60 without
		// operator tuning. gortsplib's library default (256) is far too
		// tight for HD: at 1080p ~70 packets/frame × 25fps ≈ 1750 pps
		// the default gives ~146 ms of slack and any client jitter past
		// that triggers "write queue is full" + "RTP packets lost".
		//
		// Sizing math by resolution / bitrate:
		//
		//	1080p ≤ 5 Mbps    ~1750 pps → 4096 ≈ 2.3 s slack
		//	1080p60 ≈ 8 Mbps  ~3500 pps → 4096 ≈ 1.2 s
		//	4K25  ≈ 25 Mbps   ~2500 pps → 4096 ≈ 1.6 s
		//	4K60  ≈ 50 Mbps   ~5000 pps → 4096 ≈ 800 ms (still OK)
		//
		// Memory is light: gortsplib's queue is a ring of `*rtp.Packet`
		// (~24 B per slot when empty) — 4096 × 24 ≈ 100 KB per session
		// idle. Real packet bytes only allocate when a client actually
		// stalls; capping at 4096 bounds one stalled client to ~6 MB.
		// 100 concurrent viewers ≈ 10 MB idle / 600 MB worst case —
		// safe upper bound for production hosts.
		//
		// Power of two required (gortsplib rejects others at Start).
		WriteQueueSize: 4096,
	}
	if err := srv.Start(); err != nil {
		return fmt.Errorf("publisher rtsp: listen %q: %w", addr, err)
	}

	s.mu.Lock()
	s.rtspSrv = srv
	s.mu.Unlock()
	close(s.rtspSrvReady)

	slog.Info("publisher: RTSP play server listening", "addr", addr)

	<-ctx.Done()
	srv.Close()
	_ = srv.Wait()
	return nil
}

// ─── handler ─────────────────────────────────────────────────────────────────

type rtspHandler struct{ svc *Service }

// rtspPathStreamCode extracts the StreamCode from an RTSP request path.
// Accepts "/live/<code>" for single-segment codes and "/<seg1>/<seg2>/..."
// for multi-segment codes (the `live/` prefix is always stripped when present,
// so a multi-segment code that happens to be addressed via `live/<seg>/<seg>`
// still resolves correctly). The path may also carry a trailing
// `/trackID=<N>` segment from SETUP — gortsplib emits that as the control URL
// for each media — which we strip here. Stream codes only allow
// `[A-Za-z0-9_/-]`, so `trackID=…` (containing `=`) can never be a real code
// segment and the strip cannot collide with user data.
func rtspPathStreamCode(path string) domain.StreamCode {
	p := strings.TrimPrefix(path, "/")
	p = strings.TrimPrefix(p, "live/")
	if i := strings.LastIndex(p, "/trackID="); i >= 0 {
		p = p[:i]
	}
	return domain.StreamCode(strings.TrimSpace(p))
}

// OnDescribe handles DESCRIBE — returns the ServerStream for the requested path.
func (h *rtspHandler) OnDescribe(ctx *gortsplib.ServerHandlerOnDescribeCtx) (*base.Response, *gortsplib.ServerStream, error) {
	stream := h.lookupStream(ctx.Path)
	if stream == nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}
	return &base.Response{StatusCode: base.StatusOK}, stream, nil
}

// OnSetup handles SETUP — associates the client session with the ServerStream.
func (h *rtspHandler) OnSetup(ctx *gortsplib.ServerHandlerOnSetupCtx) (*base.Response, *gortsplib.ServerStream, error) {
	stream := h.lookupStream(ctx.Path)
	if stream == nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}
	return &base.Response{StatusCode: base.StatusOK}, stream, nil
}

// OnPlay opens a tracker session for one RTSP client. Bytes are not credited
// (gortsplib writes the wire format internally and we have no per-subscriber
// hook); the open / close events still record the viewer's IP, UA, duration.
func (h *rtspHandler) OnPlay(ctx *gortsplib.ServerHandlerOnPlayCtx) (*base.Response, error) {
	code := rtspPathStreamCode(ctx.Path)
	if code == "" {
		return &base.Response{StatusCode: base.StatusBadRequest}, nil
	}

	// A repeated PLAY on the same session (e.g. after PAUSE) is already counted
	// and tracked — don't re-acquire the cap or overwrite the entry.
	h.svc.rtspSessionsMu.Lock()
	_, already := h.svc.rtspSessions[ctx.Session]
	h.svc.rtspSessionsMu.Unlock()
	if already {
		return &base.Response{StatusCode: base.StatusOK}, nil
	}

	ua := ""
	if ctx.Request != nil {
		if v, ok := ctx.Request.Header["User-Agent"]; ok && len(v) > 0 {
			ua = v[0]
		}
	}

	// Media-plane authorization (token / IP / country / UA) before allocating.
	// RTSP carries the token in the URL query (rtsp://h/live/code?token=…).
	remote := ""
	if c := ctx.Conn.NetConn().RemoteAddr(); c != nil {
		remote = c.String()
	}
	if !h.svc.playAllowed(code, "rtsp", remote, sessions.TokenFromQuery(ctx.Query), ua) {
		return &base.Response{StatusCode: base.StatusUnauthorized}, nil
	}

	// Cap concurrent playback connections before tracking the session (A-1).
	// The rtspSessions entry (created below on success) is the release marker
	// OnSessionClose uses, so it is stored even when the tracker is absent.
	if !h.svc.limiter.acquire(code) {
		slog.Warn("publisher: RTSP play rejected — playback cap reached", "stream_code", code)
		return &base.Response{StatusCode: base.StatusServiceUnavailable}, nil
	}

	sess := openRTSPSession(context.Background(), h.svc.tracker, code, ctx.Conn.NetConn().RemoteAddr(), ua)
	h.svc.rtspSessionsMu.Lock()
	h.svc.rtspSessions[ctx.Session] = &rtspClient{ps: sess, code: code}
	h.svc.rtspSessionsMu.Unlock()
	return &base.Response{StatusCode: base.StatusOK}, nil
}

// OnSessionClose closes the tracker session that mirrored this RTSP one.
// Idempotent on missing entries (a session that never reached PLAY).
//
// Reads gortsplib's per-session `OutboundBytes` counter one final time and
// pushes the delta since the last touch through the standard add()→close()
// path — that way the cumulative bytes the tracker accumulated via the 5 s
// touch ticker are completed by whatever was sent in the final partial
// window. Without this read the tracker would lose the trailing bytes on
// every RTSP session.
func (h *rtspHandler) OnSessionClose(ctx *gortsplib.ServerHandlerOnSessionCloseCtx) {
	h.svc.rtspSessionsMu.Lock()
	rc, ok := h.svc.rtspSessions[ctx.Session]
	if ok {
		delete(h.svc.rtspSessions, ctx.Session)
	}
	h.svc.rtspSessionsMu.Unlock()
	if !ok {
		return
	}
	h.svc.limiter.release(rc.code) // pair the OnPlay acquire (A-1)
	if rc.ps != nil {
		rc.pollBytes(ctx.Session)
		rc.ps.close()
	}
}

// lookupStream matches a request path to a registered ServerStream.
// The SETUP path may have a track suffix (e.g. /live/foo/trackID=0), so we
// match by exact path or by prefix followed by "/". Multi-segment codes
// addressed via the non-canonical `live/<seg>/<seg>` form are matched on a
// second pass after stripping the `live/` prefix — single-segment codes
// stay rejected for bare URLs because that branch never runs when the
// path has no prefix to strip.
func (h *rtspHandler) lookupStream(path string) *gortsplib.ServerStream {
	path = strings.TrimPrefix(path, "/")
	h.svc.mu.Lock()
	defer h.svc.mu.Unlock()
	if s := matchRTSPMount(h.svc.rtspMounts, path); s != nil {
		return s
	}
	if stripped := strings.TrimPrefix(path, "live/"); stripped != path {
		return matchRTSPMount(h.svc.rtspMounts, stripped)
	}
	return nil
}

func matchRTSPMount(mounts map[string]*gortsplib.ServerStream, path string) *gortsplib.ServerStream {
	for mountPath, stream := range mounts {
		if path == mountPath || strings.HasPrefix(path, mountPath+"/") {
			return stream
		}
	}
	return nil
}

// ─── per-stream pipeline ─────────────────────────────────────────────────────

// serveRTSP is spawned by spawnOutputs and runs for the lifetime of the stream.
func (s *Service) serveRTSP(ctx context.Context, streamCode, bufID domain.StreamCode) {
	// Wait for RTSP server to start (or be disabled).
	select {
	case <-ctx.Done():
		return
	case <-s.rtspSrvReady:
	}

	s.mu.Lock()
	srv := s.rtspSrv
	s.mu.Unlock()
	if srv == nil {
		return // server disabled (port = 0)
	}

	sub, err := s.buf.Subscribe(bufID)
	if err != nil {
		slog.Warn("publisher: RTSP subscribe failed", "stream_code", streamCode, "err", err)
		return
	}
	defer s.buf.Unsubscribe(bufID, sub)

	slog.Info("publisher: RTSP session started", "stream_code", streamCode)
	defer slog.Info("publisher: RTSP session ended", "stream_code", streamCode)

	runRTSPPipeline(ctx, streamCode, srv, sub, s)
}

// runRTSPPipeline drives the per-stream rtspSession from a buffer hub
// subscriber. AVPackets are dispatched directly into the session (no
// TS round-trip); raw TS bytes (rare — only for sources that publish
// pre-muxed TS into the buffer hub) go through a fallback demuxer.
func runRTSPPipeline(
	ctx context.Context,
	streamCode domain.StreamCode,
	srv *gortsplib.Server,
	sub *buffer.Subscriber,
	svc *Service,
) {
	sess := &rtspSession{
		streamCode: streamCode,
		srv:        srv,
		svc:        svc,
		mountPath:  rtspLiveMountPath(streamCode),
		firstVideo: true,
		ctx:        ctx,
	}
	defer sess.closeStream()

	// Lazy fallback demuxer for raw-TS sources. Allocated only on the
	// first TS-bytes packet so AV-only streams don't pay for it.
	var (
		demuxBuf  *tsBuffer
		demuxDone chan error
		tsCarry   []byte
	)
	startDemux := func() {
		if demuxBuf != nil {
			return
		}
		demuxBuf = newTSBuffer(streamCode)
		demuxDone = make(chan error, 1)
		dmx := tsdemux.New()
		dmx.OnFrame = sess.onTSFrame
		go func() { demuxDone <- dmx.Input(ctx, demuxBuf) }()
	}
	defer func() {
		if demuxBuf != nil {
			demuxBuf.Close()
			<-demuxDone
		}
	}()

	// Periodic touch for the per-client play sessions. gortsplib serialises
	// outgoing RTP packets internally so the publisher never sees a
	// per-write hook to feed playSession.add() (which is what RTMP / SRT
	// use to refresh UpdatedAt). Without this ticker the idle reaper —
	// which closes any session whose UpdatedAt is older than
	// `sessions.idle_timeout_sec` (default 30s) — would close every
	// long-running RTSP session as "idle" even though they are streaming.
	// 5s cadence stays well inside the 30s window with margin for clock
	// skew between the reaper and this loop.
	touchTick := time.NewTicker(5 * time.Second)
	defer touchTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-touchTick.C:
			svc.touchRTSPSessions(streamCode)
		case pkt, ok := <-sub.Recv():
			if !ok {
				return
			}
			if pkt.SessionStart {
				sess.onSessionBoundary()
			}
			switch {
			case pkt.AV != nil:
				// Direct path. PTS / DTS / KeyFrame preserved; no scaling
				// round-trip, no demuxer latency.
				sess.handleAVPacket(pkt.AV)
			case len(pkt.TS) > 0:
				// Raw-TS fallback. Feed 188-aligned chunks into the
				// demuxer, which produces onTSFrame callbacks the same
				// shape as handleAVPacket.
				startDemux()
				tsmux.DrainTS188Aligned(&tsCarry, pkt.TS, func(aligned []byte) {
					_, _ = demuxBuf.Write(aligned)
				})
			}
		}
	}
}

// rtspClient pairs the publisher-side play-session adapter with the
// per-RTSP-session byte cursor used to incrementalise gortsplib's
// cumulative `Stats().OutboundBytes` into per-touch deltas. The cursor
// lives outside playSession because it is RTSP-specific — RTMP / SRT /
// MPEGTS already see per-write byte counts and call playSession.add()
// directly.
type rtspClient struct {
	ps           *playSession
	code         domain.StreamCode // stream code, for releasing the playback cap on close (ps may be nil when no tracker)
	gortspSess   rtspStatsSource   // captured for nil-safe pollBytes when the close ctx isn't handy
	lastOutbound atomic.Uint64     // cumulative bytes seen on the previous poll
}

// rtspStatsSource is the subset of *gortsplib.ServerSession pollBytes
// needs. Extracting an interface keeps pollBytes unit-testable without a
// running RTSP server — production code passes the concrete
// *gortsplib.ServerSession (which satisfies the interface implicitly),
// tests pass a small fake that returns scripted byte totals.
type rtspStatsSource interface {
	Stats() *gortsplib.SessionStats
}

// pollBytes reads gortsplib's cumulative outbound counter, computes the
// delta since the last poll, and feeds it through playSession.add() so
// the tracker's live record can grow mid-stream. Idempotent and safe to
// call concurrently — atomic.Swap fences the read-and-update.
//
// `ss` is passed in rather than read from rtspClient because the close
// callback already has it on the context; the touch loop falls back to
// the captured gortspSess pointer.
func (c *rtspClient) pollBytes(ss rtspStatsSource) {
	if ss == nil {
		ss = c.gortspSess
	}
	if ss == nil {
		return
	}
	if c.ps == nil {
		return // cap-only entry (no sessions tracker) — nothing to credit
	}
	stats := ss.Stats()
	if stats == nil {
		return
	}
	curr := stats.OutboundBytes
	last := c.lastOutbound.Swap(curr)
	if curr > last {
		c.ps.add(int64(curr - last)) //nolint:gosec // RTSP session totals are well under int64 max
	}
}

// touchRTSPSessions polls outbound bytes for every active per-client
// RTSP session bound to streamCode and refreshes UpdatedAt. Called from
// runRTSPPipeline on a 5s ticker so (a) the idle reaper does not
// mistake a healthy long-running RTSP stream for an abandoned one, and
// (b) the API surfaces a growing byte counter mid-stream rather than
// staying at 0 until close. Per-session throttling lives inside
// playSession.touch — calling it more often than the throttle window
// is a cheap CAS-and-skip.
func (s *Service) touchRTSPSessions(streamCode domain.StreamCode) {
	type victim struct {
		client *rtspClient
		ss     rtspStatsSource
	}
	s.rtspSessionsMu.Lock()
	victims := make([]victim, 0, len(s.rtspSessions))
	for ss, rc := range s.rtspSessions {
		if rc != nil && rc.ps != nil && rc.ps.streamCode == streamCode {
			rc.gortspSess = ss
			victims = append(victims, victim{client: rc, ss: ss})
		}
	}
	s.rtspSessionsMu.Unlock()
	for _, v := range victims {
		v.client.pollBytes(v.ss)
		v.client.ps.touch()
	}
}

// ─── rtspSession ─────────────────────────────────────────────────────────────

type rtspSession struct {
	streamCode domain.StreamCode
	srv        *gortsplib.Server
	svc        *Service
	mountPath  string

	// ctx is the per-stream pipeline context. Captured so pacing sleeps can
	// abort cleanly when the stream stops, instead of holding the read loop
	// hostage for up to rtspPaceMaxSleep.
	ctx context.Context //nolint:containedctx // ctx cancellation aborts pace sleeps; passing through every method would just be noise.

	// codec detection
	sps    []byte
	pps    []byte
	aacCfg *mpeg4audio.AudioSpecificConfig

	// pending frames buffered before the stream is initialised
	pendingVideo []rtspPendingVideo
	pendingAudio []rtspPendingAudio

	// serving state (nil until initialised)
	stream     *gortsplib.ServerStream
	videoMedia *description.Media
	audioMedia *description.Media
	videoEnc   *rtph264.Encoder
	audioEnc   *rtpmpeg4audio.Encoder

	// timing — gomedia's TS demuxer hands us PTS / DTS in milliseconds
	// (it divides PES 90 kHz ticks by 90 before invoking OnFrame). The
	// `base*` fields anchor the RTP timeline at the first frame; the
	// `last*RTP` / `last*Src` pair lets us rebase forward when the
	// upstream source has a discontinuity (loop, restart, encoder reset)
	// instead of underflowing `src - base` into a huge RTP timestamp
	// that breaks ffmpeg's H.264 frame_num tracking + DTS monotonicity.
	baseDTS      uint64 // ms — first video DTS observed
	baseDTSSet   bool
	baseAudio    uint64 // ms — first audio DTS observed
	baseAudioSet bool
	lastVideoSrc uint64 // ms — last video DTS we processed
	lastAudioSrc uint64 // ms — last audio DTS we processed
	lastVideoRTP uint32 // 90 kHz — last RTP timestamp emitted on the video track
	lastAudioRTP uint32 // sampleRate — last RTP timestamp emitted on the audio track
	firstVideo   bool

	// pacing — anchor (paceBase wallclock, paceMedia DTSms) so each packet
	// can be held until its target wallclock arrives. Shared between video
	// and audio so a single timeline drives both tracks; whichever track
	// ticks first sets the anchor.
	paceBase  time.Time
	paceMedia uint64
	paceSet   bool
}

type rtspPendingVideo struct {
	frame    []byte
	pts, dts uint64
}

type rtspPendingAudio struct {
	frame []byte
	dts   uint64
}

// onSessionBoundary is invoked at the top of the recv loop whenever a
// buffer.Packet arrives with SessionStart=true. It re-arms the firstVideo
// gate so the next IDR re-initialises downstream decoders, and clears the
// wallclock pacer anchor so the next paced packet establishes a fresh
// (paceBase, paceMedia) mapping. Audio keeps streaming — its timeline is
// independent and re-anchored by computeAudioRTP if DTS jumps.
func (sess *rtspSession) onSessionBoundary() {
	sess.firstVideo = true
	sess.paceSet = false
}

// handleAVPacket dispatches a buffer-hub AVPacket directly into the RTP
// pipeline, bypassing MPEG-TS round-trip. The RTP timeline stays
// monotonic via computeVideoRTP / computeAudioRTP, which absorb any
// backwards source DTS jump. Session-boundary resets are handled in
// onSessionBoundary at the recv-loop level, not here.
func (sess *rtspSession) handleAVPacket(av *domain.AVPacket) {
	if av == nil || len(av.Data) == 0 {
		return
	}
	switch av.Codec { //nolint:exhaustive // RTSP only carries H.264 + AAC; other codecs intentionally drop.
	case domain.AVCodecH264:
		sess.handleVideo(av.Data, av.PTSms, av.DTSms)
	case domain.AVCodecAAC:
		sess.handleAudio(av.Data, av.DTSms)
	}
}

// onTSFrame is the demuxer callback used only on the raw-TS fallback
// path (sources that publish pre-muxed TS into the buffer hub). AV
// sources reach handleVideo / handleAudio via handleAVPacket directly.
func (sess *rtspSession) onTSFrame(cid tsdemux.StreamType, frame []byte, pts, dts uint64) {
	if len(frame) == 0 {
		return
	}
	switch cid { //nolint:exhaustive // H.265 / MPEG audio intentionally drop — RTSP serve carries only H.264 + AAC in this build
	case tsdemux.StreamTypeH264:
		sess.handleVideo(frame, pts, dts)
	case tsdemux.StreamTypeAAC:
		sess.handleAudio(frame, dts)
	default:
	}
}

func (sess *rtspSession) handleVideo(frame []byte, pts, dts uint64) {
	// Drop until the first IDR so decoders can initialise.
	if sess.firstVideo {
		if !tsmux.KeyFrameH264(frame) {
			return
		}
		sess.firstVideo = false
	}

	// Extract SPS/PPS on keyframes.
	if tsmux.KeyFrameH264(frame) {
		if sps, pps := extractSPSPPS(frame); len(sps) > 0 && len(pps) > 0 {
			sess.sps = sps
			sess.pps = pps
		}
	}

	if sess.stream == nil {
		// Queue frame while waiting for codec info.
		if len(sess.sps) > 0 && len(sess.pps) > 0 {
			cp := make([]byte, len(frame))
			copy(cp, frame)
			sess.pendingVideo = append(sess.pendingVideo, rtspPendingVideo{frame: cp, pts: pts, dts: dts})

			// Init if we also have audio config, or if we've waited long enough.
			if sess.aacCfg != nil || len(sess.pendingVideo) >= maxRTSPPendingFrames {
				if err := sess.initStream(); err != nil {
					slog.Warn("publisher: RTSP init stream failed",
						"stream_code", sess.streamCode, "err", err)
					sess.pendingVideo = sess.pendingVideo[:0]
					sess.pendingAudio = sess.pendingAudio[:0]
				}
			}
		}
		return
	}

	sess.writeVideoRTP(frame, pts, dts)
}

func (sess *rtspSession) handleAudio(frame []byte, dts uint64) {
	if sess.firstVideo {
		return // hold audio until first video keyframe
	}

	// Extract AAC config from ADTS on first audio frame.
	if sess.aacCfg == nil {
		var pkts mpeg4audio.ADTSPackets
		if err := pkts.Unmarshal(frame); err == nil && len(pkts) > 0 {
			p := pkts[0]
			sess.aacCfg = &mpeg4audio.AudioSpecificConfig{
				Type:          p.Type,
				SampleRate:    p.SampleRate,
				ChannelConfig: p.ChannelConfig,
			}
		}
	}

	if sess.stream == nil {
		// Queue frame while waiting for stream init.
		cp := make([]byte, len(frame))
		copy(cp, frame)
		sess.pendingAudio = append(sess.pendingAudio, rtspPendingAudio{frame: cp, dts: dts})

		// If video codec is ready, trigger init now that we have audio config.
		if sess.aacCfg != nil && len(sess.sps) > 0 && len(sess.pps) > 0 {
			if err := sess.initStream(); err != nil {
				slog.Warn("publisher: RTSP init stream failed",
					"stream_code", sess.streamCode, "err", err)
				sess.pendingVideo = sess.pendingVideo[:0]
				sess.pendingAudio = sess.pendingAudio[:0]
			}
		}
		return
	}

	sess.writeAudioRTP(frame, dts)
}

func (sess *rtspSession) initStream() error {
	h264Format := &format.H264{
		PayloadTyp:        96,
		SPS:               append([]byte(nil), sess.sps...),
		PPS:               append([]byte(nil), sess.pps...),
		PacketizationMode: 1,
	}
	videoMedia := &description.Media{
		Type:    description.MediaTypeVideo,
		Formats: []format.Format{h264Format},
	}

	medias := []*description.Media{videoMedia}

	var aacFormat *format.MPEG4Audio
	var audioMedia *description.Media
	if sess.aacCfg != nil {
		// SizeLength / IndexLength / IndexDeltaLength / ProfileLevelID are
		// part of the MPEG4-GENERIC RTP packetisation (RFC 3640) and
		// surface in SDP as `a=fmtp:... sizelength=13;...`. gortsplib only
		// emits each field when its value is non-zero, so leaving them at
		// the zero default produces an SDP without `sizelength`, which
		// strict clients (gortsplib v4 / v5 pull, ffmpeg) reject with
		// `invalid SDP: media N is invalid: sizelength is missing`.
		// 13 / 3 / 3 are the universal AAC-hbr Mode 1 values (matches
		// FFmpeg's RTP muxer).
		aacFormat = &format.MPEG4Audio{
			PayloadTyp:       97,
			Config:           sess.aacCfg,
			SizeLength:       13,
			IndexLength:      3,
			IndexDeltaLength: 3,
			ProfileLevelID:   1,
		}
		audioMedia = &description.Media{
			Type:    description.MediaTypeAudio,
			Formats: []format.Format{aacFormat},
		}
		medias = append(medias, audioMedia)
	}

	stream := &gortsplib.ServerStream{
		Server: sess.srv,
		Desc:   &description.Session{Medias: medias},
	}
	if err := stream.Initialize(); err != nil {
		return fmt.Errorf("initialize server stream: %w", err)
	}

	videoEnc, err := h264Format.CreateEncoder()
	if err != nil {
		stream.Close()
		return fmt.Errorf("create H264 encoder: %w", err)
	}
	if err := videoEnc.Init(); err != nil {
		stream.Close()
		return fmt.Errorf("init H264 encoder: %w", err)
	}

	var audioEnc *rtpmpeg4audio.Encoder
	if aacFormat != nil {
		audioEnc, err = aacFormat.CreateEncoder()
		if err != nil {
			stream.Close()
			return fmt.Errorf("create AAC encoder: %w", err)
		}
		if err := audioEnc.Init(); err != nil {
			stream.Close()
			return fmt.Errorf("init AAC encoder: %w", err)
		}
	}

	sess.stream = stream
	sess.videoMedia = videoMedia
	sess.audioMedia = audioMedia
	sess.videoEnc = videoEnc
	sess.audioEnc = audioEnc

	// Register mount so the handler can serve DESCRIBE requests.
	sess.svc.mu.Lock()
	sess.svc.rtspMounts[sess.mountPath] = stream
	sess.svc.mu.Unlock()

	slog.Info("publisher: RTSP stream mounted",
		"stream_code", sess.streamCode,
		"path", sess.mountPath,
		"audio", audioMedia != nil,
	)

	// Replay buffered frames.
	for _, v := range sess.pendingVideo {
		sess.writeVideoRTP(v.frame, v.pts, v.dts)
	}
	for _, a := range sess.pendingAudio {
		sess.writeAudioRTP(a.frame, a.dts)
	}
	sess.pendingVideo = nil
	sess.pendingAudio = nil

	return nil
}

func (sess *rtspSession) writeVideoRTP(frame []byte, pts, dts uint64) {
	// Wallclock pacing: hold this packet until its target wallclock arrives
	// so bursty upstream delivery (HLS segments, NVENC's faster-than-realtime
	// output) reaches the wire smoothed back to realtime.
	sess.pace(dts)

	// gomedia's mpeg2 TSDemuxer divides the PES PTS/DTS by 90 before
	// invoking OnFrame (ts-demuxer.go line 211/213), so the values that
	// reach this callback are MILLISECONDS — not 90 kHz ticks. H.264 RTP
	// runs at 90 kHz; we multiply ms by 90 to land in the right clock.
	//
	// Per RFC 6184 §7.1 the RTP timestamp is the presentation time and
	// must be IDENTICAL for every RTP packet that fragments one access
	// unit (e.g. all FU-A pieces) so the receiver can re-assemble them.
	rtpTS := sess.computeVideoRTP(pts, dts)

	nalus := annexBToNALUs(frame)
	if len(nalus) == 0 {
		return
	}

	pkts, err := sess.videoEnc.Encode(nalus)
	if err != nil {
		slog.Debug("publisher: RTSP H264 encode error", "stream_code", sess.streamCode, "err", err)
		return
	}
	for _, pkt := range pkts {
		pkt.Timestamp = rtpTS
		if err := sess.stream.WritePacketRTP(sess.videoMedia, pkt); err != nil {
			slog.Debug("publisher: RTSP write video RTP", "stream_code", sess.streamCode, "err", err)
			return
		}
	}
	sess.lastVideoRTP = rtpTS
	sess.lastVideoSrc = dts
}

// computeVideoRTP maps an upstream (pts, dts) in milliseconds to a 90-kHz
// RTP timestamp, rebasing on backwards / huge-jump source DTS so the RTP
// timeline stays monotonic across upstream loops, restarts, or encoder
// resets. Without rebase, `pts - baseDTS` underflows uint64 the moment
// the source jumps back, producing an RTP timestamp near 2^32 — which
// surfaces in ffmpeg as "Non-increasing DTS" / "Frame num gap" and makes
// downstream players fail to decode any video frame at all.
//
// The trailing monotonic clamp is the second line of defence: small jitter
// (<rtspMaxBackJumpMs back) bypasses the rebase branch but can still drop
// rtpTS below the previously-emitted value. ffmpeg copy mode catches that
// as "Application provided invalid, non monotonically increasing dts to
// muxer" and VLC drops the affected frames. Forcing rtpTS > lastVideoRTP
// — even by 1 tick — costs nothing for downstream playback and makes the
// timeline strictly monotonic on the wire.
func (sess *rtspSession) computeVideoRTP(pts, dts uint64) uint32 {
	if !sess.baseDTSSet {
		sess.baseDTS = dts
		sess.baseDTSSet = true
		sess.lastVideoSrc = dts
		return 0
	}
	if pts < sess.baseDTS || dts+rtspMaxBackJumpMs < sess.lastVideoSrc {
		// Source discontinuity (loop / restart / reorder past tolerance).
		// Anchor the new base such that this frame produces the next RTP
		// tick after the last one we sent. ffmpeg / gortsplib accept a
		// 1-tick step; what they reject is going backwards.
		sess.baseDTS = pts - uint64(sess.lastVideoRTP/90) - 1
	}
	rel := pts - sess.baseDTS
	rtpTS := uint32(rel * 90)
	if rtpTS <= sess.lastVideoRTP {
		rtpTS = sess.lastVideoRTP + 1
	}
	return rtpTS
}

func (sess *rtspSession) writeAudioRTP(frame []byte, dts uint64) {
	if sess.audioMedia == nil || sess.audioEnc == nil || sess.aacCfg == nil {
		return
	}

	// Wallclock pacing — see writeVideoRTP. Shared anchor across both
	// tracks so audio and video stay in sync regardless of which one
	// established the pacer.
	sess.pace(dts)

	// gomedia's TSDemuxer hands us DTS in milliseconds (see comment in
	// writeVideoRTP). AAC RTP runs at the audio sample rate (48 kHz
	// here), so the conversion is `ms × sampleRate / 1000`. Same
	// discontinuity-aware rebase as the video path: when the upstream
	// source loops or resets, dts jumps back and `dts - baseAudio`
	// would underflow uint64 → huge RTP timestamp → ffmpeg sees DTS
	// near int64 max and rejects the packet.
	rtpTS := sess.computeAudioRTP(dts)

	var adtsPkts mpeg4audio.ADTSPackets
	if err := adtsPkts.Unmarshal(frame); err != nil || len(adtsPkts) == 0 {
		return
	}
	aus := make([][]byte, 0, len(adtsPkts))
	for _, p := range adtsPkts {
		if len(p.AU) > 0 {
			aus = append(aus, p.AU)
		}
	}
	if len(aus) == 0 {
		return
	}

	pkts, err := sess.audioEnc.Encode(aus)
	if err != nil {
		slog.Debug("publisher: RTSP AAC encode error", "stream_code", sess.streamCode, "err", err)
		return
	}
	// Encoder.Encode resets its internal timestamp to 0 on every call and
	// increments by 1024 (samples per AAC AU) for each subsequent AU in
	// the batch. So pkts come back with relative timestamps 0, 1024,
	// 2048, ... — we ADD our base (rtpTS) instead of overwriting, so
	// the second / third / Nth AU in a multi-AU ADTS frame keeps the
	// correct +1024 spacing relative to the first.
	//
	// The previous behaviour (`pkt.Timestamp = rtpTS`) collapsed every
	// AU in a batch to the same timestamp; strict consumers rebuilt
	// the implicit AU timeline (TS, TS+1024, TS+2048…) and saw
	// "non-monotonically increasing dts" once a later frame's base TS
	// fell below an earlier frame's last implied AU time, freezing
	// playback after ~1s.
	for _, pkt := range pkts {
		pkt.Timestamp += rtpTS
		if err := sess.stream.WritePacketRTP(sess.audioMedia, pkt); err != nil {
			slog.Debug("publisher: RTSP write audio RTP", "stream_code", sess.streamCode, "err", err)
			return
		}
		if pkt.Timestamp > sess.lastAudioRTP {
			sess.lastAudioRTP = pkt.Timestamp
		}
	}
	sess.lastAudioSrc = dts
}

// computeAudioRTP mirrors computeVideoRTP for the audio track, in the AAC
// sample rate clock (48 kHz here). Rebases on backwards source DTS so a
// looped / restarted upstream doesn't underflow the RTP timestamp; the
// trailing monotonic clamp guards against small in-window jitter the same
// way computeVideoRTP does — see that function for the rationale.
func (sess *rtspSession) computeAudioRTP(dts uint64) uint32 {
	rate := uint64(sess.aacCfg.SampleRate)
	if !sess.baseAudioSet {
		sess.baseAudio = dts
		sess.baseAudioSet = true
		sess.lastAudioSrc = dts
		return 0
	}
	if dts < sess.baseAudio || dts+rtspMaxBackJumpMs < sess.lastAudioSrc {
		// Source discontinuity — keep the RTP timeline moving forward.
		sess.baseAudio = dts - uint64(sess.lastAudioRTP)*1000/rate - 1
	}
	relMs := dts - sess.baseAudio
	rtpTS := uint32(relMs * rate / 1000)
	if rtpTS <= sess.lastAudioRTP {
		rtpTS = sess.lastAudioRTP + 1
	}
	return rtpTS
}

// pace blocks until the wallclock target for `dts` arrives, anchoring the
// (paceBase, paceMedia) mapping on the first call. The result is that the
// RTP wire-rate matches the source's media-time rate even when the upstream
// feeds the buffer hub in bursts (HLS chunks, NVENC's fast-than-realtime
// output, transcoder catch-up after backpressure).
//
// Anchor is shared between video and audio so the two tracks stay phase-
// locked. Re-anchors when:
//   - paceSet is false (first call, or after a session-boundary reset),
//   - the would-be sleep exceeds rtspPaceMaxSleep (source PTS jumped far
//     ahead — looped feed, fresh keyframe burst, encoder reset),
//   - we have fallen further than rtspPaceMaxLag behind realtime
//     (sustained source underrun; chasing the old anchor only adds
//     latency without smoothing anything).
//
// Sleep aborts on sess.ctx cancellation so stream teardown is not held up
// by an in-flight pace wait.
func (sess *rtspSession) pace(dts uint64) {
	if !sess.paceSet {
		sess.paceBase = time.Now()
		sess.paceMedia = dts
		sess.paceSet = true
		return
	}
	// Signed math: source DTS can dip slightly below the anchor (small
	// reorder / jitter) without crossing a session boundary. Treat any
	// negative delta as a re-anchor rather than under-flowing into a
	// giant positive duration.
	deltaMs := int64(dts) - int64(sess.paceMedia)
	if deltaMs < 0 {
		sess.paceBase = time.Now()
		sess.paceMedia = dts
		return
	}
	target := sess.paceBase.Add(time.Duration(deltaMs) * time.Millisecond)
	delay := time.Until(target)
	if delay > rtspPaceMaxSleep {
		// PTS jumped far ahead of wallclock — re-anchor instead of
		// sleeping a long time on a stale mapping.
		sess.paceBase = time.Now()
		sess.paceMedia = dts
		return
	}
	if delay < -rtspPaceMaxLag {
		// We have fallen more than rtspPaceMaxLag behind realtime;
		// re-anchor at "now" so future packets pace from a fresh
		// reference instead of perpetually chasing the old one.
		sess.paceBase = time.Now()
		sess.paceMedia = dts
		return
	}
	if delay <= 0 {
		return
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-sess.ctx.Done():
	case <-timer.C:
	}
}

func (sess *rtspSession) closeStream() {
	if sess.stream == nil {
		return
	}
	sess.svc.mu.Lock()
	delete(sess.svc.rtspMounts, sess.mountPath)
	sess.svc.mu.Unlock()
	sess.stream.Close()
	slog.Info("publisher: RTSP stream unmounted", "stream_code", sess.streamCode)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// extractSPSPPS returns the raw SPS (type 7) and PPS (type 8) NAL units from
// an Annex-B H.264 byte stream. Returns nil slices if not found.
func extractSPSPPS(annexB []byte) (sps, pps []byte) {
	for _, nalu := range annexBToNALUs(annexB) {
		if len(nalu) == 0 {
			continue
		}
		switch nalu[0] & 0x1f {
		case 7:
			sps = append([]byte(nil), nalu...)
		case 8:
			pps = append([]byte(nil), nalu...)
		}
	}
	return sps, pps
}

// annexBToNALUs splits an Annex-B H.264 byte stream into individual raw NAL units
// (without start codes). Handles both 3-byte and 4-byte start codes.
func annexBToNALUs(annexB []byte) [][]byte {
	var result [][]byte
	start := -1
	i := 0
	for i < len(annexB) {
		if i+4 <= len(annexB) && annexB[i] == 0 && annexB[i+1] == 0 && annexB[i+2] == 0 && annexB[i+3] == 1 {
			if start >= 0 && i > start {
				result = append(result, annexB[start:i])
			}
			i += 4
			start = i
			continue
		}
		if i+3 <= len(annexB) && annexB[i] == 0 && annexB[i+1] == 0 && annexB[i+2] == 1 {
			if start >= 0 && i > start {
				result = append(result, annexB[start:i])
			}
			i += 3
			start = i
			continue
		}
		i++
	}
	if start >= 0 && start < len(annexB) {
		result = append(result, annexB[start:])
	}
	// Filter empty NALUs.
	out := result[:0]
	for _, n := range result {
		if len(n) > 0 {
			out = append(out, n)
		}
	}
	return out
}
