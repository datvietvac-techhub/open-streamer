package pull

// rtmp_msg_converter.go — translates lal `base.RtmpMsg` values into
// `domain.AVPacket` values, applying the byte-level conversions required
// for downstream consumers (HLS / DASH / MPEG-TS muxer):
//
//   - H.264 video sequence header → Annex-B SPS/PPS prefix (stored)
//   - H.264 NALU                  → AVCC → Annex-B, ensure SPS/PPS in front
//   - H.265 video sequence header → Annex-B VPS/SPS/PPS prefix (stored)
//   - H.265 NALU                  → AVCC → Annex-B, ensure VPS/SPS/PPS in front
//   - AAC sequence header         → AscContext (stored for ADTS)
//   - AAC raw frame               → ADTS-prefixed payload
//
// "Ensure" above means: when the cached prefix is still empty (sequence
// header not yet seen) the converter scans the IDR's Annex-B for inline
// parameter set NALUs and caches them, so subsequent IDRs always carry
// SPS/PPS by the time they reach downstream muxers. This unblocks sources
// whose RTMP server delivers AVCDecoderConfigurationRecord lazily — see
// ensureKeyFrameHasParamSets for details.
//
// The converter is shared by:
//
//   - the pull reader (rtmp.go), which feeds it `RtmpMsg`s from a remote
//     RTMP source and writes the resulting AVPackets onto a channel; and
//   - the push server (push/rtmp_server.go), which feeds it `RtmpMsg`s
//     from a local encoder and writes AVPackets directly into the buffer
//     hub. Both consumers need the same byte-level conversion guarantees.
//
// Codec config state (avcAnnexbPrefix, hevcAnnexbPrefix, aacCtx) is per-
// instance — instantiate one converter per RTMP stream/session, never
// share across streams. Each instance is single-goroutine; the calling
// session's read loop owns it for its lifetime.

import (
	"log/slog"

	"github.com/q191201771/lal/pkg/aac"
	"github.com/q191201771/lal/pkg/avc"
	"github.com/q191201771/lal/pkg/base"
	"github.com/q191201771/lal/pkg/hevc"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// RTMPMsgConverter holds the per-stream codec configuration state and
// converts incoming RTMP messages to domain AVPackets.
type RTMPMsgConverter struct {
	// avcAnnexbPrefix is the SPS+PPS bytes in Annex-B form, parsed once
	// from the AVC sequence header. Prepended to every IDR NALU so a
	// fresh decoder can initialise from a single packet.
	avcAnnexbPrefix []byte

	// hevcAnnexbPrefix is the VPS+SPS+PPS bytes in Annex-B form for HEVC,
	// same role as avcAnnexbPrefix.
	hevcAnnexbPrefix []byte

	// idrEmitted tracks how many IDR NALUs have been emitted with
	// the cached prefix prepended. droppedIDRs counts IDRs we had to
	// drop because neither inline scan nor cached prefix yielded
	// parameter sets. Used by Log* helpers to give an at-a-glance
	// health signal for streams that mysteriously have no video
	// downstream — see test5 incident where every IDR after the
	// first was being dropped silently.
	idrEmitted, droppedIDRs uint64

	// aacCtx wraps the AudioSpecificConfig parsed from the AAC sequence
	// header. Used to synthesise the ADTS header on each raw frame.
	aacCtx *aac.AscContext
}

// NewRTMPMsgConverter constructs an empty converter ready to receive
// sequence headers. IDR access units arriving before any codec config
// (sequence header msg or inline parameter set NALU) is observed are
// dropped — they are undecodable in isolation and would only pollute
// the buffer hub with un-init-able frames.
func NewRTMPMsgConverter() *RTMPMsgConverter {
	return &RTMPMsgConverter{}
}

// Convert processes one RtmpMsg and returns the resulting AVPacket(s).
// Sequence-header messages update internal state and emit no packet
// (returns nil). Unsupported codecs (legacy RTMP audio: PCM/G.711/Speex,
// non-AVC/HEVC video) are silently dropped.
func (c *RTMPMsgConverter) Convert(msg base.RtmpMsg) []domain.AVPacket {
	switch msg.Header.MsgTypeId {
	case base.RtmpTypeIdVideo:
		return c.convertVideo(msg)
	case base.RtmpTypeIdAudio:
		return c.convertAudio(msg)
	}
	return nil
}

// convertVideo handles RtmpTypeIdVideo messages.
func (c *RTMPMsgConverter) convertVideo(msg base.RtmpMsg) []domain.AVPacket {
	if len(msg.Payload) < 5 {
		return nil
	}
	codecID := msg.VideoCodecId()

	if msg.IsVideoKeySeqHeader() {
		c.captureVideoSeqHeader(msg, codecID)
		return nil
	}

	avccData := videoMsgAvccPayload(msg)
	if len(avccData) == 0 {
		return nil
	}

	var codec domain.AVCodec
	switch codecID {
	case base.RtmpCodecIdAvc:
		codec = domain.AVCodecH264
	case base.RtmpCodecIdHevc:
		codec = domain.AVCodecH265
	default:
		return nil // unsupported video codec (Sorenson, VP6, …)
	}

	annexB, err := avc.Avcc2Annexb(avccData)
	if err != nil || len(annexB) == 0 {
		return nil
	}

	isKey := msg.IsVideoKeyNalu()
	data := annexB
	if isKey {
		data = c.ensureKeyFrameHasParamSets(codecID, annexB)
		if data == nil {
			return nil // undecodable IDR — no cached prefix and no inline param sets
		}
	}

	dts := uint64(msg.Header.TimestampAbs)
	pts := dts + uint64(msg.Cts())
	return []domain.AVPacket{{
		Codec:    codec,
		Data:     data,
		PTSms:    pts,
		DTSms:    dts,
		KeyFrame: isKey,
	}}
}

// ensureKeyFrameHasParamSets guarantees the IDR access unit returned to
// downstream consumers begins with the codec parameter sets — SPS/PPS for
// AVC, VPS/SPS/PPS for HEVC. The fallback exists because some RTMP servers
// (including Open-Streamer's own raw-TS-to-RTMP republish path) deliver
// the AVCDecoderConfigurationRecord lazily after the first IDR has already
// been served — without it, downstream DASH/HLS muxers can't initialise
// their decoder and the stream is unplayable until restart.
//
// Resolution order:
//  1. Inline scan: if `annexB` already carries SPS/PPS (or VPS/SPS/PPS)
//     NALUs in-band, cache them as the prefix for future IDRs and return
//     `annexB` unchanged.
//  2. Cached prefix: if a prior call (this method or captureVideoSeqHeader)
//     populated the prefix, prepend it.
//  3. Otherwise: return nil to signal the caller to drop the packet —
//     decoders cannot initialise and emitting it would only pollute the
//     buffer hub with un-decodable data.
//
// The first time inline param sets are captured, log INFO once; later
// IDRs hit branch 1 silently because the prefix is already populated.
func (c *RTMPMsgConverter) ensureKeyFrameHasParamSets(codecID uint8, annexB []byte) []byte {
	switch codecID {
	case base.RtmpCodecIdAvc:
		return c.ensureAVCKeyFrameHasParamSets(annexB)
	case base.RtmpCodecIdHevc:
		return c.ensureHEVCKeyFrameHasParamSets(annexB)
	}
	return nil
}

// ensureAVCKeyFrameHasParamSets implements the resolution order for H.264.
// Returns annexB unchanged when SPS/PPS are inline, the prefix-prepended
// access unit when a cached prefix exists, or nil to drop.
func (c *RTMPMsgConverter) ensureAVCKeyFrameHasParamSets(annexB []byte) []byte {
	sps, pps := extractH264ParamSetsAnnexB(annexB)
	if len(sps) > 0 && len(pps) > 0 {
		if len(c.avcAnnexbPrefix) == 0 {
			c.avcAnnexbPrefix = avc.BuildSpsPps2Annexb(sps, pps)
			slog.Info("rtmp msg converter: captured H264 SPS/PPS from inline IDR NALUs",
				"sps_size", len(sps), "pps_size", len(pps))
		}
		c.idrEmitted++
		return annexB
	}
	if len(c.avcAnnexbPrefix) > 0 {
		c.idrEmitted++
		// Log once at first prepend so operators can see the path is active.
		if c.idrEmitted == 1 {
			slog.Info("rtmp msg converter: prepending cached H264 SPS/PPS to IDR (slice-only source)",
				"prefix_size", len(c.avcAnnexbPrefix), "frame_size", len(annexB))
		}
		return prependPrefix(c.avcAnnexbPrefix, annexB)
	}
	c.droppedIDRs++
	if c.droppedIDRs == 1 || c.droppedIDRs%30 == 0 {
		slog.Warn("rtmp msg converter: dropping H264 IDR — no inline SPS/PPS and no cached prefix",
			"dropped_count", c.droppedIDRs, "frame_size", len(annexB))
	}
	return nil
}

// ensureHEVCKeyFrameHasParamSets is the H.265 counterpart, requiring the
// full VPS+SPS+PPS triple for inline capture.
func (c *RTMPMsgConverter) ensureHEVCKeyFrameHasParamSets(annexB []byte) []byte {
	vps, sps, pps := extractH265ParamSetsAnnexB(annexB)
	if len(vps) > 0 && len(sps) > 0 && len(pps) > 0 {
		if len(c.hevcAnnexbPrefix) == 0 {
			c.hevcAnnexbPrefix = buildHEVCAnnexBPrefix(vps, sps, pps)
			slog.Info("rtmp msg converter: captured H265 VPS/SPS/PPS from inline IDR NALUs",
				"vps_size", len(vps), "sps_size", len(sps), "pps_size", len(pps))
		}
		c.idrEmitted++
		return annexB
	}
	if len(c.hevcAnnexbPrefix) > 0 {
		c.idrEmitted++
		if c.idrEmitted == 1 {
			slog.Info("rtmp msg converter: prepending cached H265 VPS/SPS/PPS to IDR (slice-only source)",
				"prefix_size", len(c.hevcAnnexbPrefix), "frame_size", len(annexB))
		}
		return prependPrefix(c.hevcAnnexbPrefix, annexB)
	}
	c.droppedIDRs++
	if c.droppedIDRs == 1 || c.droppedIDRs%30 == 0 {
		slog.Warn("rtmp msg converter: dropping H265 IDR — no inline VPS/SPS/PPS and no cached prefix",
			"dropped_count", c.droppedIDRs, "frame_size", len(annexB))
	}
	return nil
}

// prependPrefix returns prefix||annexB as a fresh slice, never aliasing
// either input. Callers pass the cached parameter-set prefix as `prefix`
// and the IDR access unit as `annexB`.
func prependPrefix(prefix, annexB []byte) []byte {
	out := make([]byte, 0, len(prefix)+len(annexB))
	out = append(out, prefix...)
	out = append(out, annexB...)
	return out
}

// extractH264ParamSetsAnnexB scans an Annex-B byte stream for the first
// inline SPS (NAL type 7) and PPS (NAL type 8) and returns copies of the
// raw NAL bytes (no start code). Returns nil for either output when not
// found. Used as a fallback when the source RTMP stream did not deliver
// an AVCDecoderConfigurationRecord before the first IDR.
func extractH264ParamSetsAnnexB(annexB []byte) (sps, pps []byte) {
	_ = avc.IterateNaluAnnexb(annexB, func(nal []byte) {
		if len(nal) == 0 {
			return
		}
		switch avc.ParseNaluType(nal[0]) {
		case avc.NaluTypeSps:
			if sps == nil {
				sps = append([]byte(nil), nal...)
			}
		case avc.NaluTypePps:
			if pps == nil {
				pps = append([]byte(nil), nal...)
			}
		}
	})
	return
}

// extractH265ParamSetsAnnexB scans an Annex-B byte stream for the first
// inline VPS (NAL type 32), SPS (33), and PPS (34) and returns copies of
// the raw NAL bytes. Annex-B start-code framing is codec-agnostic so we
// reuse avc.IterateNaluAnnexb here — only the NAL type extraction is
// HEVC-specific.
func extractH265ParamSetsAnnexB(annexB []byte) (vps, sps, pps []byte) {
	_ = avc.IterateNaluAnnexb(annexB, func(nal []byte) {
		if len(nal) == 0 {
			return
		}
		switch hevc.ParseNaluType(nal[0]) {
		case hevc.NaluTypeVps:
			if vps == nil {
				vps = append([]byte(nil), nal...)
			}
		case hevc.NaluTypeSps:
			if sps == nil {
				sps = append([]byte(nil), nal...)
			}
		case hevc.NaluTypePps:
			if pps == nil {
				pps = append([]byte(nil), nal...)
			}
		}
	})
	return
}

// buildHEVCAnnexBPrefix concatenates VPS, SPS, PPS each preceded by a
// 4-byte Annex-B start code. We construct the prefix manually rather than
// calling hevc.BuildVpsSpsPps2Annexb because that helper validates the
// parameter sets via ParseVps / ParseSps and fails on inputs that decoders
// would still accept; for inline-extracted NALUs we trust the source bytes
// and let the downstream decoder reject them if malformed.
func buildHEVCAnnexBPrefix(vps, sps, pps []byte) []byte {
	const startCode = "\x00\x00\x00\x01"
	out := make([]byte, 0, 12+len(vps)+len(sps)+len(pps))
	out = append(out, startCode...)
	out = append(out, vps...)
	out = append(out, startCode...)
	out = append(out, sps...)
	out = append(out, startCode...)
	out = append(out, pps...)
	return out
}

// captureVideoSeqHeader parses the codec configuration record from a
// video sequence-header message and stores the corresponding Annex-B
// prefix (SPS/PPS for AVC, VPS/SPS/PPS for HEVC). Both classic and
// Enhanced RTMP framing are recognised.
//
// IMPORTANT: lal's `SpsPpsSeqHeader2Annexb` and (non-enhanced)
// `VpsSpsPpsSeqHeader2Annexb` require the FULL FLV tag payload — they
// internally verify that payload[:5] equals the FLV header
// (`{0x17, 0, 0, 0, 0}` for AVC, `{0x1c, 0, 0, 0, 0}` for HEVC) and
// then skip 10 bytes from start (5 FLV header + first 5 of the
// AVCDecoderConfigurationRecord). Passing `msg.Payload[5:]` cuts off
// the FLV header and the function's first sanity check fails with
// `ErrAvc` / `ErrHevc`, returning silently and leaving the prefix
// uncached — which used to leave every IDR downstream relying on the
// inline-scan fallback (or being dropped) regardless of whether the
// source delivered a proper sequence header. Pass the unmodified
// `msg.Payload` so the lal helper finds the bytes where it expects.
func (c *RTMPMsgConverter) captureVideoSeqHeader(msg base.RtmpMsg, codecID uint8) {
	switch codecID {
	case base.RtmpCodecIdAvc:
		annexB, err := avc.SpsPpsSeqHeader2Annexb(msg.Payload)
		if err == nil {
			c.avcAnnexbPrefix = annexB
			slog.Info("rtmp msg converter: cached H264 SPS/PPS from AVC seq header tag",
				"prefix_size", len(annexB))
		} else {
			slog.Warn("rtmp msg converter: failed to parse AVC seq header tag",
				"err", err, "payload_size", len(msg.Payload))
		}
	case base.RtmpCodecIdHevc:
		var annexB []byte
		var err error
		if msg.IsEnhanced() {
			annexB, err = hevc.VpsSpsPpsEnhancedSeqHeader2Annexb(msg.Payload)
		} else {
			annexB, err = hevc.VpsSpsPpsSeqHeader2Annexb(msg.Payload)
		}
		if err == nil {
			c.hevcAnnexbPrefix = annexB
			slog.Info("rtmp msg converter: cached H265 VPS/SPS/PPS from HEVC seq header tag",
				"prefix_size", len(annexB), "enhanced", msg.IsEnhanced())
		} else {
			slog.Warn("rtmp msg converter: failed to parse HEVC seq header tag",
				"err", err, "payload_size", len(msg.Payload), "enhanced", msg.IsEnhanced())
		}
	}
}

// videoMsgAvccPayload returns the AVCC-formatted NALU bytes from a video
// message, accounting for the difference between classic and Enhanced
// RTMP framing. Returns nil when the message is not a NALU carrier.
func videoMsgAvccPayload(msg base.RtmpMsg) []byte {
	if msg.IsEnhanced() {
		idx := msg.GetEnchanedHevcNaluIndex()
		if idx == 0 || idx >= len(msg.Payload) {
			return nil
		}
		return msg.Payload[idx:]
	}
	if len(msg.Payload) <= 5 {
		return nil
	}
	return msg.Payload[5:]
}

// convertAudio handles RtmpTypeIdAudio messages. Only AAC is surfaced;
// legacy RTMP audio formats (PCM, MP3, G.711, Speex, Nellymoser) are
// dropped because the rest of the pipeline assumes AAC at the buffer
// hub for AV inputs (raw-TS sources go through a different path).
func (c *RTMPMsgConverter) convertAudio(msg base.RtmpMsg) []domain.AVPacket {
	if len(msg.Payload) < 2 {
		return nil
	}
	if msg.AudioCodecId() != base.RtmpSoundFormatAac {
		return nil
	}
	if msg.IsAacSeqHeader() {
		ctx, err := aac.NewAscContext(msg.Payload[2:])
		if err == nil {
			c.aacCtx = ctx
		}
		return nil
	}
	if c.aacCtx == nil {
		return nil // no codec config seen yet — drop until seq header arrives
	}
	rawAAC := msg.Payload[2:]
	if len(rawAAC) == 0 {
		return nil
	}
	adtsHeader := c.aacCtx.PackAdtsHeader(len(rawAAC))
	out := make([]byte, 0, len(adtsHeader)+len(rawAAC))
	out = append(out, adtsHeader...)
	out = append(out, rawAAC...)

	dts := uint64(msg.Header.TimestampAbs)
	return []domain.AVPacket{{
		Codec: domain.AVCodecAAC,
		Data:  out,
		PTSms: dts,
		DTSms: dts,
	}}
}
