package pull

import (
	"encoding/binary"
	"testing"

	"github.com/q191201771/lal/pkg/base"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// Minimal valid H.264 NAL unit bytes for tests. The first byte encodes
// forbidden_zero_bit(0) + nal_ref_idc + nal_unit_type. We only care about
// nal_unit_type for this package's logic, so the remaining bytes are
// zeros — they're never parsed as RBSP here.
const (
	h264SPSByte = 0x67 // nal_ref_idc=3, type=7 (SPS)
	h264PPSByte = 0x68 // nal_ref_idc=3, type=8 (PPS)
	h264IDRByte = 0x65 // nal_ref_idc=3, type=5 (IDR slice)
	h264PByte   = 0x41 // nal_ref_idc=2, type=1 (non-IDR slice)
)

// H.265 NAL unit type lives in (b[0] >> 1) & 0x3F. Construct headers with
// the right type and forbidden_zero_bit cleared.
const (
	h265VPSByte = byte(32 << 1) // 0x40 — VPS_NUT
	h265SPSByte = byte(33 << 1) // 0x42 — SPS_NUT
	h265PPSByte = byte(34 << 1) // 0x44 — PPS_NUT
	h265IDRByte = byte(19 << 1) // 0x26 — IDR_W_RADL
)

// annexBStartCode is the canonical 4-byte Annex-B start code prefix.
var annexBStartCode = []byte{0, 0, 0, 1}

// avccNALU concatenates each NALU prefixed with a 4-byte big-endian length
// — the AVCC framing the converter consumes from RTMP NALU messages.
func avccNALU(nalus ...[]byte) []byte {
	var out []byte
	tmp := make([]byte, 4)
	for _, n := range nalus {
		binary.BigEndian.PutUint32(tmp, uint32(len(n)))
		out = append(out, tmp...)
		out = append(out, n...)
	}
	return out
}

// flvAVCNALUMessage builds the RtmpMsg.Payload for an AVC NALU tag —
// classic (non-Enhanced) FLV framing: 1 byte FrameType+CodecId, 1 byte
// PacketType=NALU, 3 bytes CompositionTime, then the AVCC payload.
func flvAVCNALUMessage(isKey bool, avccPayload []byte) []byte {
	frameByte := byte(base.RtmpAvcKeyFrame) // 0x17 = key + AVC
	if !isKey {
		frameByte = byte(base.RtmpAvcInterFrame) // 0x27 = inter + AVC
	}
	out := make([]byte, 0, 5+len(avccPayload))
	out = append(out, frameByte, base.RtmpAvcPacketTypeNalu, 0, 0, 0)
	return append(out, avccPayload...)
}

func newRTMPVideoMsg(payload []byte, dts uint32) base.RtmpMsg {
	return base.RtmpMsg{
		Header: base.RtmpHeader{
			MsgTypeId:    base.RtmpTypeIdVideo,
			TimestampAbs: dts,
		},
		Payload: payload,
	}
}

// ─── extractH264ParamSetsAnnexB ─────────────────────────────────────────

func TestExtractH264ParamSetsAnnexB_FindsSPSAndPPS(t *testing.T) {
	t.Parallel()
	sps := []byte{h264SPSByte, 0x42, 0x00, 0x1F}
	pps := []byte{h264PPSByte, 0xCE, 0x3C, 0x80}
	idr := []byte{h264IDRByte, 0x88, 0x00}
	annexB := concat(annexBStartCode, sps, annexBStartCode, pps, annexBStartCode, idr)

	gotSPS, gotPPS := extractH264ParamSetsAnnexB(annexB)
	require.Equal(t, sps, gotSPS)
	require.Equal(t, pps, gotPPS)
}

func TestExtractH264ParamSetsAnnexB_NoParamSets(t *testing.T) {
	t.Parallel()
	idr := []byte{h264IDRByte, 0x88, 0x00}
	annexB := concat(annexBStartCode, idr)

	gotSPS, gotPPS := extractH264ParamSetsAnnexB(annexB)
	require.Nil(t, gotSPS)
	require.Nil(t, gotPPS)
}

func TestExtractH264ParamSetsAnnexB_OnlySPS(t *testing.T) {
	t.Parallel()
	// Avoid trailing 0x00 in NAL bytes — Annex-B parsing is ambiguous
	// when a start code follows a 0x00, which real encoders avoid via
	// emulation-prevention bytes; test fixtures sidestep it by ending
	// each NAL on a non-zero byte.
	sps := []byte{h264SPSByte, 0x42, 0x1F}
	idr := []byte{h264IDRByte, 0x88}
	annexB := concat(annexBStartCode, sps, annexBStartCode, idr)

	gotSPS, gotPPS := extractH264ParamSetsAnnexB(annexB)
	require.Equal(t, sps, gotSPS)
	require.Nil(t, gotPPS, "PPS must be nil when missing — caller treats this as no-param-set")
}

// ─── extractH265ParamSetsAnnexB ─────────────────────────────────────────

func TestExtractH265ParamSetsAnnexB_FindsAllThree(t *testing.T) {
	t.Parallel()
	// NAL bytes intentionally end on non-zero values — see the H.264
	// OnlySPS test for why trailing 0x00 is problematic for Annex-B
	// parsers without emulation prevention.
	vps := []byte{h265VPSByte, 0x01, 0x0C, 0x01}
	sps := []byte{h265SPSByte, 0x01, 0x60, 0x33}
	pps := []byte{h265PPSByte, 0xC1, 0x72, 0xB4}
	idr := []byte{h265IDRByte, 0x55, 0xAA}
	annexB := concat(annexBStartCode, vps, annexBStartCode, sps, annexBStartCode, pps, annexBStartCode, idr)

	gotVPS, gotSPS, gotPPS := extractH265ParamSetsAnnexB(annexB)
	require.Equal(t, vps, gotVPS)
	require.Equal(t, sps, gotSPS)
	require.Equal(t, pps, gotPPS)
}

func TestExtractH265ParamSetsAnnexB_MissingVPS(t *testing.T) {
	t.Parallel()
	sps := []byte{h265SPSByte, 0x01, 0x60}
	pps := []byte{h265PPSByte, 0xC1, 0x72}
	idr := []byte{h265IDRByte, 0x00}
	annexB := concat(annexBStartCode, sps, annexBStartCode, pps, annexBStartCode, idr)

	gotVPS, gotSPS, gotPPS := extractH265ParamSetsAnnexB(annexB)
	require.Nil(t, gotVPS, "VPS missing — caller must treat as no-param-set")
	require.Equal(t, sps, gotSPS)
	require.Equal(t, pps, gotPPS)
}

// ─── convertVideo end-to-end ────────────────────────────────────────────

// IDR with inline SPS+PPS arriving BEFORE any AVCDecoderConfigurationRecord
// must (a) be emitted as an AVPacket (not dropped), (b) populate the
// converter's cached prefix so the next IDR can rely on it, (c) carry
// KeyFrame=true.
func TestConvertVideo_AVC_InlineSPSPPS_FirstIDREmittedAndPrefixCached(t *testing.T) {
	t.Parallel()
	c := NewRTMPMsgConverter()

	sps := []byte{h264SPSByte, 0x42, 0x00, 0x1F}
	pps := []byte{h264PPSByte, 0xCE, 0x3C}
	idr := []byte{h264IDRByte, 0x88}
	avcc := avccNALU(sps, pps, idr)
	msg := newRTMPVideoMsg(flvAVCNALUMessage(true, avcc), 1000)

	pkts := c.Convert(msg)
	require.Len(t, pkts, 1, "IDR with inline SPS/PPS must be emitted")
	require.Equal(t, domain.AVCodecH264, pkts[0].Codec)
	require.True(t, pkts[0].KeyFrame)
	require.Greater(t, len(c.avcAnnexbPrefix), 0, "prefix must be cached after inline capture")
}

// IDR arriving WITHOUT any prior sequence header AND no inline param sets
// must be dropped — emitting it would push an undecodable IDR into the
// buffer hub. This is the stream_a / test5 root-cause case.
func TestConvertVideo_AVC_NoParamSetsAtAll_IDRDropped(t *testing.T) {
	t.Parallel()
	c := NewRTMPMsgConverter()

	idr := []byte{h264IDRByte, 0x88, 0x00}
	avcc := avccNALU(idr)
	msg := newRTMPVideoMsg(flvAVCNALUMessage(true, avcc), 1000)

	pkts := c.Convert(msg)
	require.Empty(t, pkts, "IDR without SPS/PPS (inline or cached) must be dropped")
	require.Empty(t, c.avcAnnexbPrefix, "no prefix must be cached when none was found")
}

// Once a sequence header has populated the prefix, plain IDRs (without
// inline SPS/PPS) must be emitted with the prefix prepended — preserves
// the original behaviour for well-behaved sources.
func TestConvertVideo_AVC_CachedPrefix_IDRGetsPrepended(t *testing.T) {
	t.Parallel()
	c := NewRTMPMsgConverter()
	c.avcAnnexbPrefix = []byte{0, 0, 0, 1, h264SPSByte, 0x42, 0, 0, 0, 0, 0, 0, 1, h264PPSByte, 0xCE}

	idr := []byte{h264IDRByte, 0x88, 0x00}
	avcc := avccNALU(idr)
	msg := newRTMPVideoMsg(flvAVCNALUMessage(true, avcc), 1000)

	pkts := c.Convert(msg)
	require.Len(t, pkts, 1)
	require.True(t, pkts[0].KeyFrame)
	// Annex-B output must start with the cached SPS/PPS bytes — this is
	// the contract every downstream muxer relies on.
	require.Greater(t, len(pkts[0].Data), len(c.avcAnnexbPrefix))
	require.Equal(t, c.avcAnnexbPrefix, pkts[0].Data[:len(c.avcAnnexbPrefix)])
}

// Non-key (P) frames must always be emitted unchanged — the inline-scan
// fallback only applies to keyframes (P-frames carry no parameter sets).
func TestConvertVideo_AVC_NonKey_AlwaysEmitted(t *testing.T) {
	t.Parallel()
	c := NewRTMPMsgConverter()

	pSlice := []byte{h264PByte, 0x9A, 0x00}
	avcc := avccNALU(pSlice)
	msg := newRTMPVideoMsg(flvAVCNALUMessage(false, avcc), 2000)

	pkts := c.Convert(msg)
	require.Len(t, pkts, 1)
	require.False(t, pkts[0].KeyFrame)
}

// HEVC counterpart: full VPS+SPS+PPS inline must be captured and the IDR
// emitted on first sight, even without a prior sequence header msg.
func TestConvertVideo_HEVC_InlineVPSSPSPPS_PrefixCached(t *testing.T) {
	t.Parallel()
	c := NewRTMPMsgConverter()

	vps := []byte{h265VPSByte, 0x00, 0x0C, 0x01}
	sps := []byte{h265SPSByte, 0x01, 0x60}
	pps := []byte{h265PPSByte, 0xC1, 0x72}
	idr := []byte{h265IDRByte, 0x00}
	avcc := avccNALU(vps, sps, pps, idr)
	// HEVC NALU msg uses CodecId=12 — frame byte 0x1C (key) / 0x2C (inter).
	frameByte := byte(base.RtmpHevcKeyFrame)
	payload := append([]byte{frameByte, base.RtmpHevcPacketTypeNalu, 0, 0, 0}, avcc...)
	msg := newRTMPVideoMsg(payload, 1000)

	pkts := c.Convert(msg)
	require.Len(t, pkts, 1, "HEVC IDR with inline VPS/SPS/PPS must be emitted")
	require.Equal(t, domain.AVCodecH265, pkts[0].Codec)
	require.True(t, pkts[0].KeyFrame)
	require.Greater(t, len(c.hevcAnnexbPrefix), 0)
}
