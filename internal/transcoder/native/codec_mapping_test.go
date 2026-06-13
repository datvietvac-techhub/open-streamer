package native

import (
	"testing"

	pb "github.com/datvietvac-techhub/open-streamer/internal/transcoder/native/proto"
)

// TestProtoCodec_MapsEveryESCodec locks the wire-enum mapping for the
// supervisor's downstream AVPacket routing. A silent drop of any value
// here would land the wrong codec on the buffer hub and break HLS
// segmentation in non-obvious ways (publisher's tsmux.FromAV treats
// an UNSPECIFIED packet as raw bytes), so the table is verified
// exhaustively rather than spot-checked.
func TestProtoCodec_MapsEveryESCodec(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		in   esFrameCodec
		want pb.Codec
	}{
		{esCodecUnknown, pb.Codec_CODEC_UNSPECIFIED},
		{esCodecH264, pb.Codec_CODEC_H264},
		{esCodecH265, pb.Codec_CODEC_H265},
		{esCodecAAC, pb.Codec_CODEC_AAC},
	}
	for _, tc := range tcs {
		if got := protoCodec(tc.in); got != tc.want {
			t.Fatalf("protoCodec(%d) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestEncoderCodecToES_KnownNames covers the H.264 vs H.265 split the
// pipeline relies on when wrapping encoder output as OutputFrames.
// Mis-tagging here means the supervisor writes the wrong domain.AVCodec
// and tsmux.FromAV picks the wrong PES wrapper.
func TestEncoderCodecToES_KnownNames(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name string
		want esFrameCodec
	}{
		{"libx264", esCodecH264},
		{"h264_nvenc", esCodecH264},
		{"h264_videotoolbox", esCodecH264},
		{"", esCodecH264}, // default
		{"libx265", esCodecH265},
		{"hevc", esCodecH265},
		{"hevc_nvenc", esCodecH265},
		{"hevc_videotoolbox", esCodecH265},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := encoderCodecToES(tc.name); got != tc.want {
				t.Fatalf("encoderCodecToES(%q) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

// TestDecoderCodecFamily covers A-3's codec-mismatch detection: the
// pipeline rebuilds the decoder when the active decoder's family differs
// from the incoming elementary-stream codec. cuvid and CPU variants share
// a family (no rebuild on a GPU↔CPU fallback). An unknown decoder name
// maps to esCodecUnknown so any real video codec is treated as a mismatch
// and forces a rebuild rather than crash-looping.
func TestDecoderCodecFamily(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name string
		want esFrameCodec
	}{
		{"h264", esCodecH264},
		{"h264_cuvid", esCodecH264},
		{"", esCodecH264}, // NewDecoder default
		{"hevc", esCodecH265},
		{"hevc_cuvid", esCodecH265},
		{"av1", esCodecUnknown},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := decoderCodecFamily(tc.name); got != tc.want {
				t.Fatalf("decoderCodecFamily(%q) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

// TestVideoDecoderNameForCodec covers A-3's decoder selection: an HEVC
// source must pick hevc / hevc_cuvid, and the choice must preserve the
// GPU (cuvid) vs CPU lane of the currently-active decoder — a GPU host
// (h264_cuvid startup) seeing HEVC stays on NVDEC (hevc_cuvid), a CPU host
// gets the software hevc decoder.
func TestVideoDecoderNameForCodec(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		current string
		codec   esFrameCodec
		want    string
	}{
		{"h264", esCodecH265, "hevc"},
		{"h264_cuvid", esCodecH265, "hevc_cuvid"},
		{"hevc", esCodecH264, "h264"},
		{"hevc_cuvid", esCodecH264, "h264_cuvid"},
		{"h264_cuvid", esCodecH264, "h264_cuvid"}, // idempotent on GPU
		{"h264", esCodecH264, "h264"},             // idempotent on CPU
	}
	for _, tc := range tcs {
		t.Run(tc.current+"_to_"+tc.want, func(t *testing.T) {
			t.Parallel()
			if got := videoDecoderNameForCodec(tc.current, tc.codec); got != tc.want {
				t.Fatalf("videoDecoderNameForCodec(%q, %v) = %q, want %q", tc.current, tc.codec, got, tc.want)
			}
		})
	}
}

// TestEnsureVideoDecoder_NoRebuildPaths verifies the cheap early returns of
// ensureVideoDecoder that run on the hot decode path: a non-video codec, or
// a decoder that already matches the source, must NOT touch the decoder
// (these literal pipelines carry a nil *Decoder, so a wrong rebuild attempt
// would panic). The actual rebuild branch is exercised end-to-end on real
// HEVC sources; here we lock down the no-op decisions.
func TestEnsureVideoDecoder_NoRebuildPaths(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name         string
		decoderCodec string
		in           esFrameCodec
	}{
		{"aac_is_not_video", "h264", esCodecAAC},
		{"unknown_is_not_video", "h264", esCodecUnknown},
		{"h264_decoder_matches_h264", "h264", esCodecH264},
		{"cuvid_decoder_matches_h264", "h264_cuvid", esCodecH264},
		{"hevc_decoder_matches_h265", "hevc", esCodecH265},
		{"hevc_cuvid_decoder_matches_h265", "hevc_cuvid", esCodecH265},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := &StreamPipeline{cfg: PipelineConfig{Decoder: DecoderConfig{Codec: tc.decoderCodec}}}
			out, err := p.ensureVideoDecoder(tc.in)
			if err != nil {
				t.Fatalf("ensureVideoDecoder(%v) error = %v, want nil", tc.in, err)
			}
			if out != nil {
				t.Fatalf("ensureVideoDecoder(%v) out = %v, want nil (no rebuild expected)", tc.in, out)
			}
		})
	}
}

// TestIsVideoKeyframeAnnexB covers the codec-aware AV-path keyframe gate:
// H.264 IDR detection routes to the H.264 detector, HEVC to the H.265 one.
// The dispatch divergence is the key property — the same H.264 IDR bytes
// must NOT be read as an HEVC IRAP (NAL type 5 is an HEVC P-slice family),
// so feeding the wrong codec can't accidentally open the keyframe gate.
func TestIsVideoKeyframeAnnexB(t *testing.T) {
	t.Parallel()
	startCode := []byte{0x00, 0x00, 0x00, 0x01}
	// H.264 IDR: NAL type 5 → first byte 0x65.
	h264IDR := append(append([]byte{}, startCode...), 0x65, 0x88, 0x80, 0x00)
	// H.264 non-IDR slice: NAL type 1 → 0x41.
	h264NonIDR := append(append([]byte{}, startCode...), 0x41, 0x9a, 0x00)
	// HEVC IDR_W_RADL: NAL type 19 → header byte0 = 19<<1 = 0x26, byte1 = 0x01.
	h265IDR := append(append([]byte{}, startCode...), 0x26, 0x01, 0x00, 0x00)

	if !isVideoKeyframeAnnexB(esCodecH264, h264IDR) {
		t.Error("H.264 IDR not detected as keyframe")
	}
	if isVideoKeyframeAnnexB(esCodecH264, h264NonIDR) {
		t.Error("H.264 non-IDR slice wrongly detected as keyframe")
	}
	if !isVideoKeyframeAnnexB(esCodecH265, h265IDR) {
		t.Error("HEVC IRAP not detected as keyframe")
	}
	// Dispatch divergence: H.264 IDR bytes must not read as an HEVC IRAP.
	if isVideoKeyframeAnnexB(esCodecH265, h264IDR) {
		t.Error("H.264 IDR bytes wrongly detected as HEVC IRAP — gate dispatched to wrong codec")
	}
}

// TestTSInput_DrainAudioReadyEmpty — symmetry with the video drain
// test: the pipeline's lazy-drain model calls DrainReadyAudio eagerly
// and must not block on an empty queue.
func TestTSInput_DrainAudioReadyEmpty(t *testing.T) {
	t.Parallel()
	in := newTSInput(t.Context())
	defer in.Close()
	if got := in.DrainReadyAudio(); got != nil {
		t.Fatalf("DrainReadyAudio on empty queue = %v, want nil", got)
	}
}
