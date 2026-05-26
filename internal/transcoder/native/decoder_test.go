package native

import (
	"testing"

	"github.com/asticode/go-astiav"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Decoder must reject codec names not built into the linked libavcodec
// — otherwise the failure surfaces later as a confusing nil-context
// panic on the first SendPacket.
func TestNewDecoder_UnknownCodecRejected(t *testing.T) {
	t.Parallel()
	_, err := NewDecoder(DecoderConfig{Codec: "not_a_real_decoder_xyz"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not available")
}

func TestNewDecoder_DefaultsToH264(t *testing.T) {
	t.Parallel()
	dec, err := NewDecoder(DecoderConfig{})
	require.NoError(t, err)
	defer dec.Close()
	assert.NotNil(t, dec.codecCtx)
}

// Roundtrip: synthetic NV12 → encode (libx264 ultrafast) → decode → match.
// Validates the full encoder ↔ decoder loop. Frame counts may not match
// exactly because the decoder buffers B-frames; we assert at least ONE
// frame round-trips and the decoded dimensions match the encoded ones.
func TestDecoder_RoundtripsLibx264Output(t *testing.T) {
	t.Parallel()
	const (
		width     = 320
		height    = 240
		framerate = 25
		nFrames   = 30
	)
	enc, err := NewEncoder(EncoderConfig{
		Width:       width,
		Height:      height,
		Framerate:   framerate,
		BitrateKbps: 256,
		GOPSize:     framerate,
		MaxBFrames:  0,
		Options:     map[string]string{"preset": "ultrafast", "tune": "zerolatency"},
	})
	require.NoError(t, err)
	defer enc.Close()

	dec, err := NewDecoder(DecoderConfig{Codec: "h264"})
	require.NoError(t, err)
	defer dec.Close()

	var decoded int
	for i := 0; i < nFrames; i++ {
		frame := allocTestNV12Frame(t, width, height, astiav.PixelFormatYuv420P, int64(i))
		encPkts, err := enc.Encode(frame)
		frame.Free()
		require.NoError(t, err)
		for _, pkt := range encPkts {
			frames, err := dec.Decode(pkt.Data, int64(i), int64(i))
			require.NoError(t, err)
			for _, f := range frames {
				assert.Equal(t, width, f.Width())
				assert.Equal(t, height, f.Height())
				assert.Equal(t, astiav.PixelFormatYuv420P, f.PixelFormat())
				f.Free()
				decoded++
			}
		}
	}
	// Flush both sides — encoder for any held packets, then decoder for
	// the matching held frames.
	flushPkts, err := enc.Flush()
	require.NoError(t, err)
	for _, pkt := range flushPkts {
		frames, err := dec.Decode(pkt.Data, 0, 0)
		require.NoError(t, err)
		for _, f := range frames {
			f.Free()
			decoded++
		}
	}
	flushFrames, err := dec.Flush()
	require.NoError(t, err)
	for _, f := range flushFrames {
		f.Free()
		decoded++
	}

	require.Positive(t, decoded, "no frames survived encode → decode round-trip")
}

func TestDecoder_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	dec, err := NewDecoder(DecoderConfig{})
	require.NoError(t, err)
	dec.Close()
	dec.Close()
}

func TestDecoder_DecodeAfterCloseReturnsError(t *testing.T) {
	t.Parallel()
	dec, err := NewDecoder(DecoderConfig{})
	require.NoError(t, err)
	dec.Close()
	_, err = dec.Decode([]byte{0x00}, 0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}
