package publisher

// serve_srt.go — SRT play server (listener mode, publisher-side).
//
// Architecture:
//
//	Buffer Hub → subscriber → tsmux.FeedWirePacket → DrainTS188Aligned → srt.Conn.Write
//	                                                                            │
//	                                                                       SRT client
//
// SRT is a transport protocol — no codec conversion needed; raw MPEG-TS is
// written directly to each subscribing connection.
//
// Client URL: srt://host:port?streamid=live/<stream_code> for single-segment
// codes; srt://host:port?streamid=<seg1>/<seg2>/... for multi-segment codes
// (live/ omitted). Bare single-segment streamids without the live/ prefix
// are rejected at the buffer-lookup step (they resolve to a non-existent
// stream code).
//
// Each client gets its own goroutine and an independent Buffer Hub subscriber.
// When the server context is cancelled, all active subscriber connections are
// closed via a per-connection watcher goroutine.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	srt "github.com/datarhei/gosrt"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/sessions"
	"github.com/datvietvac-techhub/open-streamer/internal/tsmux"
)

// RunSRTPlayServer starts the SRT play listener.
// Returns nil immediately when listeners.srt is disabled.
func (s *Service) RunSRTPlayServer(ctx context.Context) error {
	srtCfg := s.currentListeners().SRT
	if !srtCfg.Enabled || srtCfg.Port == 0 {
		return nil
	}
	host := srtCfg.ListenHost
	if host == "" {
		host = domain.DefaultListenHost
	}
	addr := fmt.Sprintf("%s:%d", host, srtCfg.Port)

	latency := time.Duration(srtCfg.LatencyMS) * time.Millisecond
	if latency <= 0 {
		latency = time.Duration(domain.DefaultSRTLatencyMS) * time.Millisecond
	}
	cfg := srt.DefaultConfig()
	cfg.ReceiverLatency = latency
	cfg.PeerLatency = latency

	srv := &srt.Server{
		Addr:   addr,
		Config: &cfg,
		HandleConnect: func(req srt.ConnRequest) srt.ConnType {
			return s.srtHandleConnect(req)
		},
		HandleSubscribe: func(conn srt.Conn) {
			s.srtHandleSubscribe(ctx, conn)
		},
	}

	if err := srv.Listen(); err != nil {
		return fmt.Errorf("publisher srt: listen %q: %w", addr, err)
	}

	slog.Info("publisher: SRT play server listening", "addr", addr)

	go func() {
		<-ctx.Done()
		srv.Shutdown()
	}()

	if err := srv.Serve(); err != nil && err != srt.ErrServerClosed {
		return fmt.Errorf("publisher srt: serve: %w", err)
	}
	return nil
}

// srtHandleConnect validates an incoming SRT connection request.
// Returns SUBSCRIBE if the requested stream is active, REJECT otherwise.
func (s *Service) srtHandleConnect(req srt.ConnRequest) srt.ConnType {
	code := srtStreamCode(req.StreamId())
	if code == "" {
		slog.Warn("publisher: SRT connect rejected: empty streamid")
		return srt.REJECT
	}

	_, ok := s.mediaBufferFor(domain.StreamCode(code))
	if !ok {
		slog.Warn("publisher: SRT connect rejected: stream not active", "streamid", req.StreamId())
		return srt.REJECT
	}
	return srt.SUBSCRIBE
}

// srtHandleSubscribe serves raw MPEG-TS to one SRT subscriber.
// It is called in a goroutine by gosrt's Serve loop.
func (s *Service) srtHandleSubscribe(ctx context.Context, conn srt.Conn) {
	defer func() { _ = conn.Close() }()

	code := srtStreamCode(conn.StreamId())
	if code == "" {
		return
	}
	streamCode := domain.StreamCode(code)

	bufID, ok2 := s.mediaBufferFor(streamCode)
	if !ok2 {
		slog.Warn("publisher: SRT stream no longer active", "stream_code", streamCode)
		return
	}

	// Media-plane authorization (token / IP / country) before any state. SRT
	// carries the token in the streamid query (?streamid=live/code?token=…).
	srtTok := ""
	if sid := conn.StreamId(); sid != "" {
		if i := strings.IndexByte(sid, '?'); i >= 0 {
			srtTok = sessions.TokenFromQuery(sid[i+1:])
		}
	}
	if !s.playAllowed(streamCode, "srt", conn.RemoteAddr().String(), srtTok, "", "") {
		return
	}

	// Cap concurrent playback connections before allocating the subscriber +
	// muxer state (A-1). The conn is closed by the deferred conn.Close above.
	if !s.limiter.acquire(streamCode) {
		slog.Warn("publisher: SRT play rejected — playback cap reached", "stream_code", streamCode, "remote", conn.RemoteAddr())
		return
	}
	defer s.limiter.release(streamCode)

	sub, err := s.buf.Subscribe(bufID)
	if err != nil {
		slog.Warn("publisher: SRT subscribe failed", "stream_code", streamCode, "err", err)
		return
	}
	defer s.buf.Unsubscribe(bufID, sub)

	slog.Info("publisher: SRT play session started", "stream_code", streamCode, "remote", conn.RemoteAddr())
	defer slog.Info("publisher: SRT play session ended", "stream_code", streamCode)

	// Track the session for the API. The tracker is nil-safe; openSRTSession
	// returns the running byte counter and a closer the deferred cleanup
	// invokes with the final transferred byte count.
	srtSess := openSRTSession(ctx, s.tracker, streamCode, conn)
	defer srtSess.close()

	// Close the connection when the server context is cancelled so the write
	// loop below unblocks.
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-watchDone:
		}
	}()

	var tsCarry []byte
	var avMux *tsmux.FromAV

	// SRT carries MPEG-TS by convention as 7×188-byte chunks per UDP
	// payload (1316 bytes — matches SRT's default packet size minus
	// header, see SRT-spec / Haivision recommendations). Writing one
	// 188-byte packet per conn.Write would otherwise produce ~2000
	// Write calls/sec at 3 Mbps; gosrt's per-write overhead pushes
	// the receiver's recovery buffer past `latency_ms` and the link
	// is declared dead — observed as "SRT write error EOF" within
	// hundreds of ms of session start. Batching here keeps the call
	// rate to ~250/sec at the same bitrate.
	const srtBatchPackets = 7
	const srtBatchSize = srtBatchPackets * 188
	batch := make([]byte, 0, srtBatchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := conn.Write(batch)
		if err != nil {
			return err
		}
		srtSess.add(int64(n))
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return
		case pkt, ok := <-sub.Recv():
			if !ok {
				return
			}
			if pkt.SessionStart {
				// Source switched. Drop stale muxer state, TS carry, and
				// the half-filled batch so the new session's first bytes
				// don't merge with the previous source's tail.
				avMux = nil
				tsCarry = nil
				batch = batch[:0]
			}
			var writeErr error
			tsmux.FeedWirePacket(ctx, pkt.TS, pkt.AV, &avMux, func(b []byte) {
				if writeErr != nil {
					return
				}
				tsmux.DrainTS188Aligned(&tsCarry, b, func(aligned []byte) {
					if writeErr != nil {
						return
					}
					batch = append(batch, aligned...)
					if len(batch) >= srtBatchSize {
						if err := flush(); err != nil {
							writeErr = err
						}
					}
				})
			})
			if writeErr == nil {
				// Flush whatever's left so the receiver doesn't sit on a
				// partial batch waiting for the next packet — at low
				// bitrates the wait can exceed the latency window.
				if err := flush(); err != nil {
					writeErr = err
				}
			}
			if writeErr != nil {
				slog.Debug("publisher: SRT write error", "stream_code", streamCode, "err", writeErr)
				return
			}
		}
	}
}

// srtStreamCode extracts the stream code from an SRT streamid. The `live/`
// prefix is always stripped when present, so single-segment codes are
// reached via "live/<code>" and multi-segment codes via "<seg1>/<seg2>/..."
// directly. A bare single-segment streamid (no `live/` prefix and no '/')
// is rejected — returning "" — so single-segment streams cannot be hit
// by accident from a half-typed URL.
func srtStreamCode(streamid string) string {
	streamid = strings.TrimSpace(streamid)
	// Drop a playback-token query (streamid=live/code?token=…) before deriving
	// the code, so the token doesn't bleed into the stream-code lookup.
	if i := strings.IndexByte(streamid, '?'); i >= 0 {
		streamid = streamid[:i]
	}
	hadLivePrefix := strings.HasPrefix(streamid, "live/")
	code := strings.TrimPrefix(streamid, "live/")
	if code == "" {
		return ""
	}
	if !hadLivePrefix && !strings.Contains(code, "/") {
		return ""
	}
	return code
}
