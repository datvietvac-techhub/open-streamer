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
