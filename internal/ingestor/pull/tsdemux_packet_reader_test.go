package pull

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// MPEG audio frame header layer-detection table. Reference vectors from the
// ISO/IEC 11172-3 / 13818-3 spec — second header byte's middle two bits
// encode the Layer (00=reserved, 01=Layer III, 10=Layer II, 11=Layer I).
//
// The TS container's stream_type (0x03 / 0x04) doesn't carry Layer info,
// so without this helper an MP3 broadcast appears as the generic "mp2a" in
// the UI even when the data path forwards correctly.
func TestMpegAudioCodecFromFrame(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		frame []byte
		want  domain.AVCodec
	}{
		{
			name:  "MPEG1 Layer III (MP3) — sync 0xFFFB",
			frame: []byte{0xFF, 0xFB, 0x90, 0x00},
			want:  domain.AVCodecMP3,
		},
		{
			name:  "MPEG2 Layer III — sync 0xFFF3",
			frame: []byte{0xFF, 0xF3, 0x40, 0x00},
			want:  domain.AVCodecMP3,
		},
		{
			name:  "MPEG2.5 Layer III — sync 0xFFE3",
			frame: []byte{0xFF, 0xE3, 0x40, 0x00},
			want:  domain.AVCodecMP3,
		},
		{
			name:  "MPEG1 Layer II (MP2) — sync 0xFFFD",
			frame: []byte{0xFF, 0xFD, 0x40, 0x04},
			want:  domain.AVCodecMP2,
		},
		{
			name:  "MPEG1 Layer I — sync 0xFFFF",
			frame: []byte{0xFF, 0xFF, 0x60, 0x00},
			want:  domain.AVCodecMP2,
		},
		{
			name:  "missing sync byte 1 → fallback to MP2",
			frame: []byte{0x00, 0xFB, 0x90, 0x00},
			want:  domain.AVCodecMP2,
		},
		{
			name:  "missing sync continuation bits → fallback to MP2",
			frame: []byte{0xFF, 0x00, 0x00, 0x00},
			want:  domain.AVCodecMP2,
		},
		{
			name:  "too-short frame (1 byte) → fallback to MP2",
			frame: []byte{0xFF},
			want:  domain.AVCodecMP2,
		},
		{
			name:  "empty frame → fallback to MP2",
			frame: []byte{},
			want:  domain.AVCodecMP2,
		},
		{
			name:  "reserved Layer field (00) → fallback to MP2",
			frame: []byte{0xFF, 0xF1, 0x00, 0x00}, // V=11, L=00
			want:  domain.AVCodecMP2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := mpegAudioCodecFromFrame(tc.frame)
			assert.Equal(t, tc.want, got)
		})
	}
}
