package pull

// rtsp.go — RTSP pull ingestor using gortsplib (bluenviron/gortsplib/v5).
//
// gortsplib is chosen over the legacy joy4 RTSP client because it:
//   - Implements RTCP Sender Report based A/V clock synchronisation so that
//     audio and video timestamps are aligned correctly regardless of their
//     independent RTP clock sources.
//   - Has a built-in RTP re-ordering buffer (eliminates out-of-order jitter).
//   - Actively maintained; joy4 is abandoned.
//
// Codec support:
//   - Video: H.264 (preferred) or H.265 (fallback when no H.264 track exists).
//   - Audio: MPEG-4 AAC (MPEG4Audio RTP format).
//
// Timestamp handling:
//   - c.PacketPTS returns the media-timeline PTS in clock-rate units, already
//     synchronised across tracks via the GlobalDecoder / RTCP SR path.
//   - For H.264/H.265 video (90 kHz RTP clock): ptsMS = pts / 90.
//   - For AAC audio (sample-rate clock): ptsMS = pts * 1000 / clockRate.
//   - DTS for H.264/H.265 is extracted by DTSExtractor (mediacommon) which
//     reads POC/VUI timing info from the bitstream — necessary for streams
//     with B-frames.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtph264"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtph265"
	mch264 "github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	mch265 "github.com/bluenviron/mediacommon/v2/pkg/codecs/h265"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	"github.com/pion/rtp"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/pkg/version"
)

const rtspChanSize = 16384

// RTSPReader connects to a remote RTSP server in play (pull) mode and emits domain.AVPacket.
// It uses gortsplib for proper RTCP-based A/V synchronisation and RTP jitter buffering.
type RTSPReader struct {
	input domain.Input

	mu   sync.Mutex
	conn *gortsplib.Client
	pkts chan domain.AVPacket
	done chan struct{}
}

// NewRTSPReader constructs an RTSPReader without opening a connection.
func NewRTSPReader(input domain.Input) *RTSPReader {
	return &RTSPReader{input: input}
}

// Open dials the RTSP source, negotiates tracks, and starts the background read loop.
// It blocks until DESCRIBE + SETUP + PLAY complete.
func (r *RTSPReader) Open(ctx context.Context) error {
	_ = ctx

	r.mu.Lock()
	if r.conn != nil {
		r.mu.Unlock()
		return fmt.Errorf("rtsp reader: already open")
	}
	pkts := make(chan domain.AVPacket, rtspChanSize)
	done := make(chan struct{})
	r.pkts = pkts
	r.done = done
	r.mu.Unlock()

	u, err := base.ParseURL(r.input.URL)
	if err != nil {
		r.abortOpen(pkts, done)
		return fmt.Errorf("rtsp reader: parse URL %q: %w", r.input.URL, err)
	}

	connectTimeout := time.Duration(r.input.Net.TimeoutSec) * time.Second
	if connectTimeout == 0 {
		connectTimeout = time.Duration(domain.DefaultRTSPTimeoutSec) * time.Second
	}

	c := &gortsplib.Client{
		Scheme:      u.Scheme,
		Host:        u.Host,
		ReadTimeout: connectTimeout,
		UserAgent:   version.UserAgent(),
	}
	if err := c.Start(); err != nil {
		r.abortOpen(pkts, done)
		return fmt.Errorf("rtsp reader: start %q: %w", r.input.URL, err)
	}

	desc, _, err := c.Describe(u)
	if err != nil {
		c.Close()
		r.abortOpen(pkts, done)
		return fmt.Errorf("rtsp reader: describe %q: %w", r.input.URL, err)
	}

	setupCount := 0

	// --- H.264 video ---
	var h264Forma *format.H264
	var h264Media *description.Media
	h264Media = desc.FindFormat(&h264Forma)
	if h264Media != nil {
		if _, err := c.Setup(desc.BaseURL, h264Media, 0, 0); err != nil {
			slog.Warn("rtsp reader: setup H264 track failed", "url", r.input.URL, "err", err)
			h264Media = nil
			h264Forma = nil
		} else {
			setupCount++
		}
	}

	// --- H.265 video (fallback when no H.264 track) ---
	var h265Forma *format.H265
	var h265Media *description.Media
	if h264Media == nil {
		h265Media = desc.FindFormat(&h265Forma)
		if h265Media != nil {
			if _, err := c.Setup(desc.BaseURL, h265Media, 0, 0); err != nil {
				slog.Warn("rtsp reader: setup H265 track failed", "url", r.input.URL, "err", err)
				h265Media = nil
				h265Forma = nil
			} else {
				setupCount++
			}
		}
	}

	// --- AAC audio ---
	var aacForma *format.MPEG4Audio
	var aacMedia *description.Media
	aacMedia = desc.FindFormat(&aacForma)
	if aacMedia != nil {
		if _, err := c.Setup(desc.BaseURL, aacMedia, 0, 0); err != nil {
			slog.Warn("rtsp reader: setup AAC track failed", "url", r.input.URL, "err", err)
			aacMedia = nil
			aacForma = nil
		} else {
			setupCount++
		}
	}

	if setupCount == 0 {
		c.Close()
		r.abortOpen(pkts, done)
		return fmt.Errorf("rtsp reader: no supported tracks in %q (need H264, H265, or AAC)", r.input.URL)
	}

	// Register callbacks before Play.
	r.registerH264Callback(c, h264Media, h264Forma, pkts, done)
	r.registerH265Callback(c, h265Media, h265Forma, pkts, done)
	r.registerAACCallback(c, aacMedia, aacForma, pkts, done)

	if _, err := c.Play(nil); err != nil {
		c.Close()
		r.abortOpen(pkts, done)
		return fmt.Errorf("rtsp reader: play %q: %w", r.input.URL, err)
	}

	r.mu.Lock()
	r.conn = c
	r.mu.Unlock()

	slog.Info("rtsp reader: connected",
		"url", r.input.URL,
		"h264", h264Media != nil,
		"h265", h265Media != nil,
		"aac", aacMedia != nil,
	)

	go r.waitLoop(c, pkts, done)
	return nil
}

// registerH264Callback sets up OnPacketRTP for an H.264 track.
func (r *RTSPReader) registerH264Callback(
	c *gortsplib.Client,
	medi *description.Media,
	forma *format.H264,
	pkts chan domain.AVPacket,
	done chan struct{},
) {
	if medi == nil || forma == nil {
		return
	}

	rtpDec, err := forma.CreateDecoder()
	if err != nil {
		slog.Warn("rtsp reader: create H264 RTP decoder failed", "url", r.input.URL, "err", err)
		return
	}

	// Codec-config cache. Seeded from SDP `sprop-parameter-sets`; refreshed
	// every time the source emits inline SPS/PPS in an IDR. Mirrors the
	// RTMP guard (RTMPMsgConverter.ensureKeyFrameHasParamSets) so IDRs
	// without inline params still reach the buffer hub init-able, and
	// IDRs with neither inline NOR cached params get dropped instead of
	// poisoning downstream muxers with un-decodable keyframes.
	cachedSPS := append([]byte(nil), forma.SPS...)
	cachedPPS := append([]byte(nil), forma.PPS...)
	var droppedCount int

	var dtsEx mch264.DTSExtractor
	dtsEx.Initialize()

	c.OnPacketRTP(medi, forma, func(pkt *rtp.Packet) {
		pts, ok := c.PacketPTS(medi, pkt)
		if !ok {
			return
		}

		au, err := rtpDec.Decode(pkt)
		if err != nil {
			if !errors.Is(err, rtph264.ErrNonStartingPacketAndNoPrevious) &&
				!errors.Is(err, rtph264.ErrMorePacketsNeeded) {
				slog.Debug("rtsp reader: H264 RTP decode", "url", r.input.URL, "err", err)
			}
			return
		}

		isIDR := mch264.IsRandomAccess(au)
		dts, err := dtsEx.Extract(au, pts)
		if err != nil {
			return
		}

		// Refresh cache on inline SPS/PPS — handles upstream resolution
		// changes that update the param sets mid-stream.
		inlineSPS, inlinePPS := findH264SPSPPSInNALUs(au)
		if len(inlineSPS) > 0 {
			cachedSPS = append(cachedSPS[:0], inlineSPS...)
		}
		if len(inlinePPS) > 0 {
			cachedPPS = append(cachedPPS[:0], inlinePPS...)
		}

		annexB := nalusToAnnexB(au)
		var data []byte
		switch {
		case !isIDR:
			data = annexB
		case len(inlineSPS) > 0 && len(inlinePPS) > 0:
			// IDR carries its own SPS+PPS — emit as-is, no double prefix.
			data = annexB
		case len(cachedSPS) > 0 && len(cachedPPS) > 0:
			// IDR is slice-only; prepend cached prefix so the muxer sees
			// a self-contained init-able keyframe.
			prefix := buildH264SPSPrefix(cachedSPS, cachedPPS)
			data = make([]byte, 0, len(prefix)+len(annexB))
			data = append(data, prefix...)
			data = append(data, annexB...)
		default:
			// No inline SPS/PPS and no cached prefix — emitting this IDR
			// would poison downstream muxers with an un-init-able
			// keyframe. Drop and wait for a usable IDR.
			droppedCount++
			if droppedCount == 1 || droppedCount%50 == 0 {
				slog.Warn("rtsp reader: dropping H264 IDR — no inline SPS/PPS and no cached prefix",
					"url", r.input.URL, "dropped_count", droppedCount)
			}
			return
		}
		if len(data) == 0 {
			return
		}

		select {
		case pkts <- domain.AVPacket{
			Codec:    domain.AVCodecH264,
			Data:     data,
			PTSms:    uint64(pts / 90),
			DTSms:    uint64(dts / 90),
			KeyFrame: isIDR,
		}:
		case <-done:
		}
	})
}

// registerH265Callback sets up OnPacketRTP for an H.265 track.
func (r *RTSPReader) registerH265Callback(
	c *gortsplib.Client,
	medi *description.Media,
	forma *format.H265,
	pkts chan domain.AVPacket,
	done chan struct{},
) {
	if medi == nil || forma == nil {
		return
	}

	rtpDec, err := forma.CreateDecoder()
	if err != nil {
		slog.Warn("rtsp reader: create H265 RTP decoder failed", "url", r.input.URL, "err", err)
		return
	}

	// Codec-config cache. Same rationale as the H264 path — see comment
	// in registerH264Callback. H.265 needs VPS as well as SPS+PPS.
	cachedVPS := append([]byte(nil), forma.VPS...)
	cachedSPS := append([]byte(nil), forma.SPS...)
	cachedPPS := append([]byte(nil), forma.PPS...)
	var droppedCount int

	var dtsEx mch265.DTSExtractor
	dtsEx.Initialize()

	c.OnPacketRTP(medi, forma, func(pkt *rtp.Packet) {
		pts, ok := c.PacketPTS(medi, pkt)
		if !ok {
			return
		}

		au, err := rtpDec.Decode(pkt)
		if err != nil {
			if !errors.Is(err, rtph265.ErrNonStartingPacketAndNoPrevious) &&
				!errors.Is(err, rtph265.ErrMorePacketsNeeded) {
				slog.Debug("rtsp reader: H265 RTP decode", "url", r.input.URL, "err", err)
			}
			return
		}

		isIDR := mch265.IsRandomAccess(au)
		dts, err := dtsEx.Extract(au, pts)
		if err != nil {
			return
		}

		// Refresh cache on inline VPS/SPS/PPS.
		inlineVPS, inlineSPS, inlinePPS := findH265VPSSPSPPSInNALUs(au)
		if len(inlineVPS) > 0 {
			cachedVPS = append(cachedVPS[:0], inlineVPS...)
		}
		if len(inlineSPS) > 0 {
			cachedSPS = append(cachedSPS[:0], inlineSPS...)
		}
		if len(inlinePPS) > 0 {
			cachedPPS = append(cachedPPS[:0], inlinePPS...)
		}

		switch {
		case !isIDR:
			// pass through
		case len(inlineVPS) > 0 && len(inlineSPS) > 0 && len(inlinePPS) > 0:
			// IDR carries its own VPS+SPS+PPS — keep AU as-is.
		case len(cachedVPS) > 0 && len(cachedSPS) > 0 && len(cachedPPS) > 0:
			// IDR is slice-only — prepend cached params.
			au = append([][]byte{cachedVPS, cachedSPS, cachedPPS}, au...)
		default:
			droppedCount++
			if droppedCount == 1 || droppedCount%50 == 0 {
				slog.Warn("rtsp reader: dropping H265 IDR — no inline VPS/SPS/PPS and no cached prefix",
					"url", r.input.URL, "dropped_count", droppedCount)
			}
			return
		}

		data := nalusToAnnexB(au)
		if len(data) == 0 {
			return
		}

		select {
		case pkts <- domain.AVPacket{
			Codec:    domain.AVCodecH265,
			Data:     data,
			PTSms:    uint64(pts / 90),
			DTSms:    uint64(dts / 90),
			KeyFrame: isIDR,
		}:
		case <-done:
		}
	})
}

// registerAACCallback sets up OnPacketRTP for an AAC track.
func (r *RTSPReader) registerAACCallback(
	c *gortsplib.Client,
	medi *description.Media,
	forma *format.MPEG4Audio,
	pkts chan domain.AVPacket,
	done chan struct{},
) {
	if medi == nil || forma == nil {
		return
	}

	rtpDec, err := forma.CreateDecoder()
	if err != nil {
		slog.Warn("rtsp reader: create AAC RTP decoder failed", "url", r.input.URL, "err", err)
		return
	}

	aacCfg := forma.Config
	clockRate := int64(forma.ClockRate())

	c.OnPacketRTP(medi, forma, func(pkt *rtp.Packet) {
		pts, ok := c.PacketPTS(medi, pkt)
		if !ok {
			return
		}

		aus, err := rtpDec.Decode(pkt)
		if err != nil {
			slog.Debug("rtsp reader: AAC RTP decode", "url", r.input.URL, "err", err)
			return
		}

		ptsMS := pts * 1000 / clockRate

		for _, au := range aus {
			data := aacRawToADTS(aacCfg, au)
			if len(data) == 0 {
				continue
			}
			select {
			case pkts <- domain.AVPacket{
				Codec: domain.AVCodecAAC,
				Data:  data,
				PTSms: uint64(ptsMS),
				DTSms: uint64(ptsMS),
			}:
			case <-done:
				return
			}
		}
	})
}

// waitLoop blocks on c.Wait() and tears down channels when the connection ends.
func (r *RTSPReader) waitLoop(c *gortsplib.Client, pkts chan domain.AVPacket, done chan struct{}) {
	err := c.Wait()
	if err != nil {
		slog.Warn("rtsp reader: connection ended", "url", r.input.URL, "err", err)
	}
	r.mu.Lock()
	if r.conn == c {
		r.conn = nil
	}
	r.mu.Unlock()
	// Signal done before draining pkts so ReadPackets sees EOF.
	select {
	case <-done:
	default:
		close(done)
	}
	close(pkts)
}

func (r *RTSPReader) abortOpen(pkts chan domain.AVPacket, done chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	close(done)
	close(pkts)
	r.pkts = nil
	r.done = nil
}

// ReadPackets blocks until at least one AVPacket is available or the source ends.
func (r *RTSPReader) ReadPackets(ctx context.Context) ([]domain.AVPacket, error) {
	r.mu.Lock()
	ch := r.pkts
	r.mu.Unlock()
	if ch == nil {
		return nil, io.EOF
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case p, ok := <-ch:
		if !ok {
			return nil, io.EOF
		}
		batch := []domain.AVPacket{p}
		for len(batch) < 256 {
			select {
			case p2, ok2 := <-ch:
				if !ok2 {
					return batch, nil
				}
				batch = append(batch, p2)
			default:
				return batch, nil
			}
		}
		return batch, nil
	}
}

// Close closes the RTSP connection and waits for the read loop to stop.
func (r *RTSPReader) Close() error {
	r.mu.Lock()
	c := r.conn
	r.conn = nil
	r.mu.Unlock()
	if c != nil {
		c.Close()
	}
	return nil
}

// --- helpers ---

// buildH264SPSPrefix returns a 4-byte-SC+SPS+4-byte-SC+PPS Annex-B blob.
func buildH264SPSPrefix(sps, pps []byte) []byte {
	if len(sps) == 0 || len(pps) == 0 {
		return nil
	}
	b := make([]byte, 0, 8+len(sps)+len(pps))
	b = append(b, 0, 0, 0, 1)
	b = append(b, sps...)
	b = append(b, 0, 0, 0, 1)
	b = append(b, pps...)
	return b
}

// nalusToAnnexB converts a slice of raw NAL units (no startcodes) to Annex-B
// by prepending a 4-byte start code (0x00 0x00 0x00 0x01) before each NALU.
func nalusToAnnexB(au [][]byte) []byte {
	size := 0
	for _, nalu := range au {
		size += 4 + len(nalu)
	}
	if size == 0 {
		return nil
	}
	b := make([]byte, 0, size)
	for _, nalu := range au {
		b = append(b, 0, 0, 0, 1)
		b = append(b, nalu...)
	}
	return b
}

// aacRawToADTS wraps a raw MPEG-4 AAC access unit with a 7-byte ADTS header.
// Frames that already have an ADTS sync word (0xFFF) are returned unchanged.
func aacRawToADTS(cfg *mpeg4audio.AudioSpecificConfig, raw []byte) []byte {
	if len(raw) == 0 {
		return nil
	}
	// Already has ADTS header.
	if len(raw) >= 2 && raw[0] == 0xff && raw[1]&0xf0 == 0xf0 {
		return raw
	}
	if cfg == nil {
		return raw
	}
	pkts := mpeg4audio.ADTSPackets{{
		Type:          cfg.Type,
		SampleRate:    cfg.SampleRate,
		ChannelConfig: cfg.ChannelConfig,
		AU:            raw,
	}}
	out, err := pkts.Marshal()
	if err != nil {
		return raw
	}
	return out
}
