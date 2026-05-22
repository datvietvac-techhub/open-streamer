package native

import (
	"bytes"
	"testing"

	"github.com/asticode/go-astiav"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Constructor must reject codec names that the linked libavcodec build
// doesn't carry. Operators running a stripped libav must see a clear
// error at pipeline-spawn time instead of a confusing crash later.
func TestNewEncoder_UnknownCodecNameRejected(t *testing.T) {
	t.Parallel()
	_, err := NewEncoder(EncoderConfig{
		Codec:     "not_a_real_encoder_xyz",
		Width:     320,
		Height:    240,
		Framerate: 25,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not available")
}

// Empty Codec defaults to libx264 — every libav build we ship has it
// (CPU codec, no external HW deps).
func TestNewEncoder_DefaultsToLibx264(t *testing.T) {
	t.Parallel()
	enc, err := NewEncoder(EncoderConfig{
		Width:       320,
		Height:      240,
		Framerate:   25,
		BitrateKbps: 256,
		GOPSize:     25,
		// libx264 needs preset to avoid the noisy "preset" dictionary
		// warning at Open(); ultrafast keeps the test fast on CI.
		Options: map[string]string{"preset": "ultrafast", "tune": "zerolatency"},
	})
	require.NoError(t, err)
	defer enc.Close()
	assert.NotNil(t, enc.codecCtx)
}

// End-to-end smoke: feed N synthetic YUV frames through libx264, expect
// the encoder to emit at least one packet whose first bytes look like
// a NAL start-code (0x00 0x00 0x00 0x01) — proves the pipeline produced
// real H.264 bitstream and not just empty buffers.
func TestEncoder_EncodesSyntheticFrames(t *testing.T) {
	t.Parallel()
	const (
		width     = 320
		height    = 240
		framerate = 25
		nFrames   = 30 // 1.2s of video at 25 fps — enough for at least one keyframe
	)
	enc, err := NewEncoder(EncoderConfig{
		Width:       width,
		Height:      height,
		Framerate:   framerate,
		BitrateKbps: 256,
		GOPSize:     framerate, // one keyframe per second
		MaxBFrames:  0,         // disable B-frames for deterministic single-pass emit
		Options:     map[string]string{"preset": "ultrafast", "tune": "zerolatency"},
	})
	require.NoError(t, err)
	defer enc.Close()

	var totalPackets int
	var firstPacketBytes []byte
	for i := 0; i < nFrames; i++ {
		frame := allocTestNV12Frame(t, width, height, astiav.PixelFormatYuv420P, int64(i))
		pkts, err := enc.Encode(frame)
		frame.Free()
		require.NoError(t, err)
		totalPackets += len(pkts)
		if firstPacketBytes == nil && len(pkts) > 0 {
			firstPacketBytes = pkts[0]
		}
	}

	flushPkts, err := enc.Flush()
	require.NoError(t, err)
	totalPackets += len(flushPkts)
	if firstPacketBytes == nil && len(flushPkts) > 0 {
		firstPacketBytes = flushPkts[0]
	}

	require.Positive(t, totalPackets, "encoder produced no packets across %d frames + flush", nFrames)
	require.NotEmpty(t, firstPacketBytes, "first packet has no bytes")

	// libx264 emits Annex-B by default (sps/pps + IDR). First packet
	// should start with a NAL start-code prefix.
	assert.True(t,
		bytes.HasPrefix(firstPacketBytes, []byte{0x00, 0x00, 0x00, 0x01}) ||
			bytes.HasPrefix(firstPacketBytes, []byte{0x00, 0x00, 0x01}),
		"first packet missing NAL start-code prefix (got % x...)", firstPacketBytes[:minInt(8, len(firstPacketBytes))])
}

// Close is idempotent — calling it twice must not panic or double-free
// the underlying libav contexts.
func TestEncoder_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	enc, err := NewEncoder(EncoderConfig{
		Width:     320,
		Height:    240,
		Framerate: 25,
		Options:   map[string]string{"preset": "ultrafast"},
	})
	require.NoError(t, err)
	enc.Close()
	enc.Close() // second call must be no-op, not panic
}

// Encode after Close must return an error rather than crashing inside
// libav with a nil-context dereference.
func TestEncoder_EncodeAfterCloseReturnsError(t *testing.T) {
	t.Parallel()
	enc, err := NewEncoder(EncoderConfig{
		Width:     320,
		Height:    240,
		Framerate: 25,
		Options:   map[string]string{"preset": "ultrafast"},
	})
	require.NoError(t, err)
	enc.Close()
	_, err = enc.Encode(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// PTS counter monotonicity — caller-facing helper for pipelines that
// derive PTS from frame index. Three calls give 0, 1, 2 in order.
func TestEncoder_NextPTSMonotonic(t *testing.T) {
	t.Parallel()
	enc, err := NewEncoder(EncoderConfig{
		Width:     320,
		Height:    240,
		Framerate: 25,
		Options:   map[string]string{"preset": "ultrafast"},
	})
	require.NoError(t, err)
	defer enc.Close()

	a := enc.NextPTS()
	b := enc.NextPTS()
	c := enc.NextPTS()
	assert.Equal(t, int64(0), a)
	assert.Equal(t, int64(1), b)
	assert.Equal(t, int64(2), c)
}

// allocTestNV12Frame builds a YUV420P frame populated with a solid-grey
// luma plane and chroma centred at 128 (effectively grey). Pts is set to
// the supplied index in encoder time-base units.
//
// Despite the name, pix_fmt is YUV420P (planar) not NV12 (semi-planar)
// — kept "NV12" in the function name because the production frame
// holder uses NV12 from NVDEC and we want grep-continuity. P3 will
// switch the helper to NV12 once the scaler delivers them.
//
//nolint:unparam // pixFmt fixed to PixelFormatYuv420P today; will accept NV12 in P3.
func allocTestNV12Frame(t *testing.T, width, height int, pixFmt astiav.PixelFormat, pts int64) *astiav.Frame {
	t.Helper()
	f := astiav.AllocFrame()
	f.SetWidth(width)
	f.SetHeight(height)
	f.SetPixelFormat(pixFmt)
	f.SetPts(pts)
	require.NoError(t, f.AllocBuffer(0))

	// Populate luma + chroma with the same mid-grey value so the
	// encoded picture is decodable but content-free. Stride may be >
	// width due to libav's alignment; FrameData.SetBytes handles the
	// padding for us when we pass a width*height*1.5 buffer.
	ySize := width * height
	uvSize := (width / 2) * (height / 2)
	buf := make([]byte, ySize+2*uvSize)
	for i := 0; i < ySize; i++ {
		buf[i] = 128 // mid-luma
	}
	for i := ySize; i < ySize+2*uvSize; i++ {
		buf[i] = 128 // neutral chroma
	}
	require.NoError(t, f.Data().SetBytes(buf, 1))
	return f
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
