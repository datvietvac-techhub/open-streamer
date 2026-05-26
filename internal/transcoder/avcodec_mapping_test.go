package transcoder

import (
	"testing"

	"github.com/ntt0601zcoder/open-streamer/internal/domain"
	pb "github.com/ntt0601zcoder/open-streamer/internal/transcoder/native/proto"
)

// TestAVCodecFromProto_MapsKnownCodecs ensures every wire codec the
// subprocess can emit lands on the matching domain.AVCodec value. The
// supervisor's buffer-hub write uses this to populate
// domain.AVPacket.Codec; a silent mismatch sends the bytes through
// publisher's tsmux.FromAV with the wrong PES wrapper and breaks
// HLS / DASH segmentation.
func TestAVCodecFromProto_MapsKnownCodecs(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		in     pb.Codec
		want   domain.AVCodec
		wantOK bool
	}{
		{pb.Codec_CODEC_H264, domain.AVCodecH264, true},
		{pb.Codec_CODEC_H265, domain.AVCodecH265, true},
		{pb.Codec_CODEC_AAC, domain.AVCodecAAC, true},
	}
	for _, tc := range tcs {
		got, ok := avCodecFromProto(tc.in)
		if ok != tc.wantOK || got != tc.want {
			t.Fatalf("avCodecFromProto(%v) = (%v, %v), want (%v, %v)",
				tc.in, got, ok, tc.want, tc.wantOK)
		}
	}
}

// TestAVCodecFromProto_UnspecifiedDrops — explicit coverage of the
// drop-path the supervisor uses when an older subprocess (or a bug)
// fails to set the codec field. Returning ok=false is the contract
// supervisor.writeOutputPacket relies on to warn-and-drop instead of
// misrouting bytes.
func TestAVCodecFromProto_UnspecifiedDrops(t *testing.T) {
	t.Parallel()
	got, ok := avCodecFromProto(pb.Codec_CODEC_UNSPECIFIED)
	if ok {
		t.Fatalf("expected drop for CODEC_UNSPECIFIED, got ok=true")
	}
	if got != domain.AVCodecUnknown {
		t.Fatalf("expected AVCodecUnknown sentinel, got %v", got)
	}
}
