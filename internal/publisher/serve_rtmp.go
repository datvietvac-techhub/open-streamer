package publisher

// serve_rtmp.go — RTMP play handler (publisher-side).
//
// The RTMP listener is owned by the ingestor and bound on the shared port
// (listeners.rtmp). When an external client issues a "play" command, the
// ingestor's RTMP server delegates to HandleRTMPPlay below. Wired in
// runtime.Manager via ing.SetRTMPPlayHandler(pub.HandleRTMPPlay).
//
// Client URL: rtmp://host:port/live/<stream_code> for single-segment codes;
// rtmp://host:port/<seg1>/<seg2>/... for multi-segment codes (live/ omitted).
//
// Data flow per session:
//
//	Buffer Hub → subscriber → tsmux.FeedWirePacket → tsBuffer → mpeg2.TSDemuxer
//	                                                                   │
//	                                                    onTSFrame (H.264 Annex-B, AAC ADTS)
//	                                                                   │
//	                                                  writeFrame callback → RTMP client

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/q191201771/lal/pkg/rtmp"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor/push"
	"github.com/datvietvac-techhub/open-streamer/internal/tsdemux"
	"github.com/datvietvac-techhub/open-streamer/internal/tsmux"
)

// HandleRTMPPlay is the play handler registered with the ingestor's RTMP server.
// It subscribes to the Buffer Hub for the given stream key, demuxes the TS stream,
// and writes H.264 / AAC frames into the lal RTMP session until ctx is cancelled.
// Returns an error when the stream is not active (caller closes the connection).
//
// Signature matches push.PlayFunc — the info argument carries remote-peer
// metadata captured by the RTMP server at OnPlay handshake time and is
// forwarded into the play-sessions tracker for the API.
func (s *Service) HandleRTMPPlay(
	ctx context.Context,
	key string,
	info push.PlayInfo,
	session *rtmp.ServerSession,
) error {
	code := domain.StreamCode(key)

	bufID, ok := s.mediaBufferFor(code)
	if !ok {
		return fmt.Errorf("stream %q not active", key)
	}

	sub, err := s.buf.Subscribe(bufID)
	if err != nil {
		return fmt.Errorf("subscribe %q: %w", key, err)
	}
	defer s.buf.Unsubscribe(bufID, sub)

	slog.Info("publisher: RTMP play session started", "stream_code", code, "remote", info.RemoteAddr)
	defer slog.Info("publisher: RTMP play session ended", "stream_code", code)

	rtmpSess := openRTMPSession(ctx, s.tracker, code, info.RemoteAddr, "")
	defer rtmpSess.close()

	writer := push.NewRTMPFrameWriter(session)
	writeFrame := func(kind push.FrameKind, data []byte, pts, dts uint32) error {
		return writer.WriteFrame(kind, data, pts, dts)
	}
	preloadSeq := func(sps, pps []byte) error {
		return writer.PreloadAvcSeqHeader(sps, pps)
	}

	runRTMPPlayPipeline(ctx, code, sub, rtmpSess, writeFrame, preloadSeq)
	return nil
}

// ─── pipeline ────────────────────────────────────────────────────────────────

// runRTMPPlayPipeline feeds TS from sub into a TSDemuxer and writes H.264/AAC
// frames via writeFrame until ctx is cancelled or sub closes. The sess argument
// is credited with each frame's payload bytes for the play-sessions API.
//
// preloadSeq is invoked once per pipeline run when the producer goroutine first
// observes a complete SPS+PPS pair in the raw TS bytes. The downstream RTMP
// frame writer uses this to send the AVC sequence header tag without waiting
// for SPS+PPS to appear in a per-frame Annex-B payload — necessary because
// gomedia's TSDemuxer can deliver access-unit bytes that don't include
// parameter-set NAL units, leaving the writer's "wait for SPS in frame"
// path stuck and pulling clients (test5) without a parsed
// AVCDecoderConfigurationRecord, so their own IDR keyframes never carry
// SPS/PPS downstream. nil is acceptable for tests that don't exercise the
// preload path.
func runRTMPPlayPipeline(
	ctx context.Context,
	streamCode domain.StreamCode,
	sub *buffer.Subscriber,
	sess *playSession,
	writeFrame func(kind push.FrameKind, data []byte, pts, dts uint32) error,
	preloadSeq func(sps, pps []byte) error,
) {
	tb := newTSBuffer(streamCode)

	go func() {
		defer tb.Close()
		var tsCarry []byte
		var avMux *tsmux.FromAV
		// SPS/PPS scanner state. We keep scanning every TS chunk until both
		// are found, then stop and call preloadSeq once. The writer is
		// idempotent past the first preload, so a stray re-trigger (which
		// can't happen here because of the cachedSeqSent flag) would be
		// harmless anyway.
		var cachedSPS, cachedPPS []byte
		cachedSeqSent := false

		for {
			select {
			case <-ctx.Done():
				return
			case pkt, ok := <-sub.Recv():
				if !ok {
					return
				}
				if pkt.SessionStart {
					// Source switched (failover / reconnect / mixer cycle).
					// Drop stale muxer state so the new session gets fresh
					// PAT/PMT tables and clean continuity counters; flush
					// the TS carry so a half-formed packet from the old
					// session can't merge with new-session bytes.
					avMux = nil
					tsCarry = nil
				}
				tsmux.FeedWirePacket(ctx, pkt.TS, pkt.AV, &avMux, func(b []byte) {
					// Scan b for SPS/PPS until both cached, then preload
					// the writer once. Defensive against gomedia's
					// TSDemuxer occasionally delivering access units
					// without parameter-set NAL units (observed in
					// production for RTMP-loopback streams: the writer's
					// extract-from-frame path then drops every frame
					// silently, no seq header tag is sent, and pulling
					// clients can't decode video).
					if !cachedSeqSent {
						if cachedSPS == nil || cachedPPS == nil {
							sps, pps := tsmux.FindH264ParameterSets(b)
							if sps != nil && cachedSPS == nil {
								cachedSPS = sps
							}
							if pps != nil && cachedPPS == nil {
								cachedPPS = pps
							}
						}
						if cachedSPS != nil && cachedPPS != nil && preloadSeq != nil {
							if err := preloadSeq(cachedSPS, cachedPPS); err != nil {
								slog.Warn("publisher: RTMP preload seq header FAILED",
									"stream_code", streamCode, "err", err,
									"sps_size", len(cachedSPS), "pps_size", len(cachedPPS))
							} else {
								slog.Info("publisher: RTMP seq header PRELOADED from raw TS",
									"stream_code", streamCode,
									"sps_size", len(cachedSPS),
									"pps_size", len(cachedPPS))
							}
							cachedSeqSent = true
						}
					}
					tsCarry = append(tsCarry, b...)
					for len(tsCarry) >= 188 {
						if tsCarry[0] != 0x47 {
							idx := bytes.IndexByte(tsCarry, 0x47)
							if idx < 0 {
								if len(tsCarry) > 187 {
									tsCarry = tsCarry[len(tsCarry)-187:]
								}
								return
							}
							tsCarry = tsCarry[idx:]
							if len(tsCarry) < 188 {
								return
							}
						}
						if len(tsCarry) >= 376 && tsCarry[188] != 0x47 {
							tsCarry = tsCarry[1:]
							continue
						}
						if _, err := tb.Write(tsCarry[:188]); err != nil {
							return
						}
						tsCarry = tsCarry[188:]
					}
				})
			}
		}
	}()

	// Wrap writeFrame so each successful payload increments the tracker's
	// per-session byte counter. Approximation: we count the H.264 / AAC
	// payload size, ignoring the few-byte RTMP chunk header — bytes total
	// is for analytics, not billing.
	trackedWrite := func(kind push.FrameKind, data []byte, pts, dts uint32) error {
		if err := writeFrame(kind, data, pts, dts); err != nil {
			return err
		}
		sess.add(int64(len(data)))
		return nil
	}

	ps := &rtmpPlaySession{
		streamCode: streamCode,
		writeFrame: trackedWrite,
		firstVideo: true,
	}

	dmx := tsdemux.New()
	dmx.OnFrame = ps.onTSFrame

	demuxDone := make(chan error, 1)
	go func() { demuxDone <- dmx.Input(ctx, tb) }()

	select {
	case <-ctx.Done():
		tb.Close()
		<-demuxDone
	case err := <-demuxDone:
		if err != nil {
			slog.Debug("publisher: RTMP play demux ended",
				"stream_code", streamCode, "err", err)
		}
	}
}

// ─── rtmpPlaySession ─────────────────────────────────────────────────────────

type rtmpPlaySession struct {
	streamCode  domain.StreamCode
	writeFrame  func(kind push.FrameKind, data []byte, pts, dts uint32) error
	baseDTS     uint64
	baseDTSSet  bool
	baseADTS    uint64
	baseADTSSet bool
	firstVideo  bool
}

func (ps *rtmpPlaySession) onTSFrame(cid tsdemux.StreamType, frame []byte, pts, dts uint64) {
	if len(frame) == 0 {
		return
	}
	switch cid { //nolint:exhaustive // H.265 / MPEG audio intentionally drop — standard RTMP play carries only H.264 + AAC
	case tsdemux.StreamTypeH264:
		ps.writeVideo(frame, pts, dts)
	case tsdemux.StreamTypeAAC:
		ps.writeAudio(frame, dts)
	default:
		return
	}
}

func (ps *rtmpPlaySession) writeVideo(frame []byte, pts, dts uint64) {
	if ps.firstVideo {
		if !tsmux.KeyFrameH264(frame) {
			return
		}
		ps.firstVideo = false
		ps.baseDTS = dts
		ps.baseDTSSet = true
	}
	if !ps.baseDTSSet {
		ps.baseDTS = dts
		ps.baseDTSSet = true
	}
	relDTS := uint32(dts - ps.baseDTS)
	relPTS := uint32(pts - ps.baseDTS)
	if err := ps.writeFrame(push.FrameKindH264, frame, relPTS, relDTS); err != nil {
		slog.Debug("publisher: RTMP play write video error",
			"stream_code", ps.streamCode, "err", err)
	}
}

func (ps *rtmpPlaySession) writeAudio(frame []byte, dts uint64) {
	if ps.firstVideo {
		return // hold audio until first video keyframe
	}
	if !ps.baseADTSSet {
		ps.baseADTS = dts
		ps.baseADTSSet = true
	}
	relDTS := uint32(dts - ps.baseADTS)
	if err := ps.writeFrame(push.FrameKindAAC, frame, relDTS, relDTS); err != nil {
		slog.Debug("publisher: RTMP play write audio error",
			"stream_code", ps.streamCode, "err", err)
	}
}
