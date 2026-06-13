package publisher

// serve_mpegts.go — HTTP MPEG-TS output (low-latency relay).
//
// Architecture (per HTTP request):
//
//	Client GET /<code>/mpegts
//	     │
//	     ▼
//	HandleMPEGTS — subscribes the playback buffer hub for <code>
//	     │
//	     ▼
//	Buffer Hub  ─► subscriber.Recv()  ─► http.ResponseWriter (chunked)
//	                                            │
//	                                       HTTP client (e.g. another Open-Streamer
//	                                       acting as a relay, ffmpeg, VLC)
//
// Latency contributors:
//   - One buffer-hub chunk (typically 1316 bytes — single UDP datagram, ~ms at
//     >12 Mbps).
//   - One TCP RTT to flush.
// Net result: 50–200 ms on LAN, comparable to "lowest latency that doesn't
// require a custom binary protocol". Far better than HLS/DASH (4–10 s
// segments) for server-to-server relay.
//
// Backpressure policy: same as the rest of the publisher. The buffer hub
// drops packets to slow subscribers via `select default:` — never blocks the
// writer. If a particular HTTP client can't keep up the dropped packets show
// up as missing TS packets (continuity counter break) on that client only;
// other subscribers and the writer side are untouched.

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// mpegtsContentType is the IANA media type for raw MPEG-TS over HTTP. Some
// clients (FFmpeg) accept several variants; this one is the most widely
// recognised and is what common production media servers emit.
const mpegtsContentType = "video/mp2t"

// HandleMPEGTS returns an http.HandlerFunc that streams raw MPEG-TS bytes
// from the playback buffer hub for the {code} URL parameter. Used by the
// API server to mount /<code>/mpegts in the same router group as the
// HLS / DASH file routes (so the sessions middleware tracks viewers
// uniformly across protocols).
//
// Behaviour:
//   - 404 if the stream is not running, or its MPEGTS protocol flag is off.
//   - 500 if the buffer subscribe fails (transient — buffer was deleted
//     between the runtime check and Subscribe).
//   - On success, writes raw TS bytes with chunked transfer encoding until
//     the client disconnects, the stream stops, or the request context is
//     cancelled.
//
// The handler intentionally does not flush every packet — Go's net/http
// chunked writer flushes on its own buffer threshold (~4 KiB) which is a
// good latency / syscall trade-off for typical TS bitrates. Operators that
// need per-packet flushing can lower the buffer-hub chunk size at the
// source (e.g. UDP datagram size) instead of churning syscalls here.
func (s *Service) HandleMPEGTS() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		code := domain.StreamCode(chi.URLParam(r, "code"))
		if code == "" {
			http.NotFound(w, r)
			return
		}

		bufID, enabled, ok := s.mpegtsTargetFor(code)
		if !ok || !enabled {
			http.NotFound(w, r)
			return
		}

		// Cap concurrent playback connections before allocating any per-client
		// state (A-1). Released on every return path via defer.
		if !s.limiter.acquire(code) {
			slog.Warn("publisher: mpegts connection rejected — playback cap reached", "stream_code", code, "remote", r.RemoteAddr)
			http.Error(w, "too many connections", http.StatusServiceUnavailable)
			return
		}
		defer s.limiter.release(code)

		sub, err := s.buf.Subscribe(bufID)
		if err != nil {
			// Buffer was deleted between mpegtsTargetFor and Subscribe —
			// treat as transient, surface as 503 so the client retries.
			slog.Debug("publisher: mpegts subscribe failed",
				"stream_code", code, "buffer_id", bufID, "err", err)
			http.Error(w, "stream not available", http.StatusServiceUnavailable)
			return
		}
		defer s.buf.Unsubscribe(bufID, sub)

		w.Header().Set("Content-Type", mpegtsContentType)
		w.Header().Set("Cache-Control", "no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		// X-Accel-Buffering=no asks reverse proxies (nginx) not to buffer the
		// response — preserves the low-latency property end-to-end when the
		// server is behind a proxy.
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)

		flusher, _ := w.(http.Flusher)

		// Open a play session so the API surfaces this viewer like any
		// other protocol. MPEGTS uses OpenConn (UUID-based, like RTMP/SRT)
		// because the underlying HTTP request is long-lived — there is no
		// per-segment refresh path the HLS/DASH middleware could hook into.
		mpegtsSess := openMPEGTSSession(r.Context(), s.tracker, code, r)
		defer mpegtsSess.close()

		slog.Info("publisher: mpegts client connected",
			"stream_code", code, "remote", r.RemoteAddr)

		streamMPEGTSToClient(r.Context(), code, sub, w, flusher, mpegtsSess)

		slog.Info("publisher: mpegts client disconnected",
			"stream_code", code, "remote", r.RemoteAddr)
	}
}

// mpegtsTargetFor returns (playbackBufferID, mpegtsEnabled, streamRunning).
// The triple shape lets the caller distinguish "stream not running" (404)
// from "stream running but MPEGTS opted-out" (also 404 to keep external
// observers from probing internal state).
func (s *Service) mpegtsTargetFor(code domain.StreamCode) (domain.StreamCode, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss, ok := s.streams[code]
	if !ok {
		return "", false, false
	}
	return ss.mediaBuf, ss.mpegtsEnabled, true
}

// streamMPEGTSToClient pumps packets from sub into w until the request
// context is cancelled, the buffer is deleted (channel closed), or a write
// error occurs. Extracted from HandleMPEGTS so the hot loop stays linear
// and unit-testable against a fake ResponseWriter.
//
// sess is credited with each successful write's byte count and refreshed
// (throttled) so the idle reaper does not close this long-lived HTTP
// stream as "idle". Pass noopPlaySession() in tests that don't care.
func streamMPEGTSToClient(
	ctx context.Context, //nolint:revive // ctx is conventionally first; tools can't see http.Request
	code domain.StreamCode,
	sub *bufferSub,
	w http.ResponseWriter,
	flusher http.Flusher,
	sess *playSession,
) {
	// Periodic flusher backstop: at very low bitrates a single chunk may not
	// reach Go's net/http internal threshold for a while, increasing latency.
	// Flushing at most every 200 ms bounds end-to-end latency without churning
	// syscalls at high bitrates (where the threshold flushes naturally).
	flushTick := time.NewTicker(200 * time.Millisecond)
	defer flushTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushTick.C:
			if flusher != nil {
				flusher.Flush()
			}
		case pkt, ok := <-sub.Recv():
			if !ok {
				return // buffer hub closed (stream stopped)
			}
			data := pkt.TS
			if len(data) == 0 && pkt.AV != nil {
				// AV packets bypass the muxer here — the playback buffer for
				// raw-TS sources only ever carries Packet.TS, and transcoder
				// output is also Packet.TS. AV-only buffers are not a target
				// for this handler (RTMP / RTSP pull readers feed AV directly
				// into separate per-protocol pipelines).
				continue
			}
			n, err := w.Write(data)
			if err != nil {
				if !isClientGone(err) {
					slog.Warn("publisher: mpegts write failed",
						"stream_code", code, "err", err)
				}
				return
			}
			sess.add(int64(n))
		}
	}
}

// bufferSub is the local alias for *buffer.Subscriber so this file's import
// surface stays minimal. The buffer package is imported via service.go.
type bufferSub = buffer.Subscriber

// isClientGone reports whether err is a "client hung up" rather than a
// real network problem. Used to suppress a noisy warn log on every closed
// connection.
func isClientGone(err error) bool {
	return errors.Is(err, http.ErrAbortHandler) ||
		errors.Is(err, errClientDisconnect)
}

// errClientDisconnect is a sentinel; net/http returns various concrete
// errors when the client closes the TCP connection mid-write (broken pipe,
// connection reset, …). Matching by errors.Is keeps platform-specific
// strings out of the comparison.
var errClientDisconnect = errors.New("client disconnected")
