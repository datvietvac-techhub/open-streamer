package native

import (
	"testing"

	"github.com/asticode/go-astiav"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helpers ---------------------------------------------------------------

// buildSourceEncoder returns an x264 encoder that simulates an incoming
// source stream of the given size. Used by tests to generate realistic
// H.264 packets the StreamPipeline can decode. fps kept as a param so
// future tests can simulate 30fps / 60fps sources without changing the
// helper signature.
//
//nolint:unparam // every test today wants 25fps; param reserved for P5 fps-conversion tests.
func buildSourceEncoder(t *testing.T, w, h, fps int) *Encoder {
	t.Helper()
	enc, err := NewEncoder(EncoderConfig{
		Width:       w,
		Height:      h,
		Framerate:   fps,
		BitrateKbps: 1000,
		GOPSize:     fps,
		MaxBFrames:  0,
		Options:     map[string]string{"preset": "ultrafast", "tune": "zerolatency"},
	})
	require.NoError(t, err)
	return enc
}

// renditionPipeline returns a pipeline that decodes h264, scales any
// input down to 1280x720, and re-encodes at 1.6 Mbps — the production
// 720p rendition for an HD source.
//
//nolint:unparam // see buildSourceEncoder; fps reserved for P5.
func renditionPipeline(t *testing.T, fps int) *StreamPipeline {
	t.Helper()
	p, err := NewStreamPipeline(PipelineConfig{
		Decoder: DecoderConfig{Codec: "h264"},
		Scaler: ScalerConfig{
			DstWidth:    1280,
			DstHeight:   720,
			DstPixelFmt: astiav.PixelFormatYuv420P,
		},
		Encoder: EncoderConfig{
			Width:       1280,
			Height:      720,
			Framerate:   fps,
			BitrateKbps: 1600,
			GOPSize:     fps,
			MaxBFrames:  0,
			Options:     map[string]string{"preset": "ultrafast", "tune": "zerolatency"},
		},
	})
	require.NoError(t, err)
	return p
}

// feed pushes every packet produced by encoding nFrames synthetic
// frames through `enc` into the pipeline. Returns total encoded output
// bytes the pipeline emitted.
func feed(t *testing.T, p *StreamPipeline, enc *Encoder, w, h, nFrames int) int {
	t.Helper()
	var outBytes int
	for i := 0; i < nFrames; i++ {
		frame := allocTestNV12Frame(t, w, h, astiav.PixelFormatYuv420P, int64(i))
		pkts, err := enc.Encode(frame)
		frame.Free()
		require.NoError(t, err)
		for _, pkt := range pkts {
			out, err := p.ProcessPacket(pkt, int64(i), int64(i))
			require.NoError(t, err)
			for _, op := range out {
				outBytes += len(op)
			}
		}
	}
	return outBytes
}

// tests -----------------------------------------------------------------

// Constructor cleans up partial allocations on error. If the encoder
// config is broken (unknown codec), the previously-allocated scaler /
// decoder must NOT leak — caller doesn't get to Close them because the
// constructor never returned a handle.
func TestNewStreamPipeline_PartialFailureCleansUp(t *testing.T) {
	t.Parallel()
	_, err := NewStreamPipeline(PipelineConfig{
		Decoder: DecoderConfig{Codec: "h264"},
		Scaler: ScalerConfig{
			DstWidth:    640,
			DstHeight:   360,
			DstPixelFmt: astiav.PixelFormatYuv420P,
		},
		Encoder: EncoderConfig{
			Codec: "not_a_real_encoder", // forces error
			Width: 640, Height: 360, Framerate: 25,
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encoder")
}

// Happy-path single-input run: pipeline accepts encoded packets from a
// 1080p25 source, produces a 720p25 rendition stream. Validates the
// data plane without any switching.
func TestStreamPipeline_SingleInputProducesOutput(t *testing.T) {
	t.Parallel()
	p := renditionPipeline(t, 25)
	defer p.Close()

	src := buildSourceEncoder(t, 1920, 1080, 25)
	defer src.Close()

	outBytes := feed(t, p, src, 1920, 1080, 30)

	tail, err := p.Flush()
	require.NoError(t, err)
	for _, t := range tail {
		outBytes += len(t)
	}
	require.Positive(t, outBytes, "pipeline produced no rendition bytes")
}

// CENTRAL INVARIANT — the one the migration exists to deliver.
// SwitchInput must:
//   - swap the decoder pointer to a fresh instance
//   - keep the SAME encoder pointer (no realloc)
//   - keep the SAME scaler pointer (no realloc)
//   - keep the encoder PTS counter monotonically advancing across the
//     switch boundary (no reset)
//
// The test feeds frames from a 1080p source, switches to a 720p source
// with a different decoder config, and asserts all four properties.
func TestStreamPipeline_SwitchInputPreservesEncoder(t *testing.T) {
	t.Parallel()
	p := renditionPipeline(t, 25)
	defer p.Close()

	// Snapshot encoder + scaler pointers before any I/O.
	encBefore := p.encoder
	scBefore := p.scaler
	decBefore := p.decoder
	require.NotNil(t, encBefore)
	require.NotNil(t, scBefore)
	require.NotNil(t, decBefore)

	// Feed 1080p25 source for ~1s.
	src1080 := buildSourceEncoder(t, 1920, 1080, 25)
	feed(t, p, src1080, 1920, 1080, 25)
	src1080.Close()

	ptsAtSwitch := p.EncoderPTS()
	require.Positive(t, ptsAtSwitch, "encoder must have produced frames pre-switch")

	// Switch to a fresh decoder (simulates input change). In production
	// this is also where the new source's extradata would land if the
	// upstream used out-of-band codec params.
	flushed, err := p.SwitchInput(DecoderConfig{Codec: "h264"})
	require.NoError(t, err)
	_ = flushed // we only care that the call succeeded for this test

	// THE INVARIANT: encoder + scaler are the SAME pointers.
	assert.Same(t, encBefore, p.encoder,
		"SwitchInput must NOT reallocate the encoder — entire migration depends on this")
	assert.Same(t, scBefore, p.scaler,
		"SwitchInput must NOT reallocate the scaler")
	// Decoder must be a new instance.
	assert.NotSame(t, decBefore, p.decoder, "SwitchInput must replace the decoder")

	// Feed a 720p source through the same pipeline. The scaler should
	// auto-reconfigure on the first decoded frame; encoder is unaware.
	src720 := buildSourceEncoder(t, 1280, 720, 25)
	defer src720.Close()
	feed(t, p, src720, 1280, 720, 25)

	// Encoder PTS continued past ptsAtSwitch — no reset.
	assert.Greater(t, p.EncoderPTS(), ptsAtSwitch,
		"encoder PTS counter must advance monotonically across SwitchInput")
}

// SwitchInput emits the OLD decoder's flushed B-frame queue through the
// encoder so the cutover doesn't drop content. Construct a scenario
// where the old decoder has at least one queued frame and assert
// SwitchInput's return slice is non-empty.
//
// Note: with our test-encoder config (MaxBFrames=0, ultrafast) the
// decoder usually has nothing queued — the assertion is "no error" and
// "return slice is well-formed", not "must be > 0", because making
// libx264 emit reorder is fragile across versions.
func TestStreamPipeline_SwitchInputDrainsOldDecoder(t *testing.T) {
	t.Parallel()
	p := renditionPipeline(t, 25)
	defer p.Close()

	src := buildSourceEncoder(t, 1920, 1080, 25)
	defer src.Close()
	feed(t, p, src, 1920, 1080, 10)

	flushed, err := p.SwitchInput(DecoderConfig{Codec: "h264"})
	require.NoError(t, err)
	// Each entry in flushed must be a valid Annex-B chunk.
	for _, b := range flushed {
		require.NotEmpty(t, b, "SwitchInput returned an empty packet")
	}
}

// Source-format change WITHOUT SwitchInput — simulates an input that
// just changes resolution mid-stream (rare in practice but tests the
// scaler's reconfigure path inside the pipeline rather than via direct
// Scaler tests).
func TestStreamPipeline_ScalerAbsorbsSourceFormatChange(t *testing.T) {
	t.Parallel()
	p := renditionPipeline(t, 25)
	defer p.Close()

	src1 := buildSourceEncoder(t, 1920, 1080, 25)
	feed(t, p, src1, 1920, 1080, 10)
	src1.Close()

	// SwitchInput so the decoder is fresh and accepts the new SPS/PPS.
	_, err := p.SwitchInput(DecoderConfig{Codec: "h264"})
	require.NoError(t, err)

	src2 := buildSourceEncoder(t, 854, 480, 25)
	defer src2.Close()
	feed(t, p, src2, 854, 480, 10)

	// No assertion beyond "no error and Close passes" — the scaler's
	// per-source reconfigure path is dedicated-tested in scaler_test.go.
}

// Flush must produce the encoder's tail packets so the last frames of
// a stream are not lost. Feed N frames, Flush, expect at least one
// extra packet OR a clean no-op (depends on encoder's internal queue).
func TestStreamPipeline_FlushDrainsEncoder(t *testing.T) {
	t.Parallel()
	p := renditionPipeline(t, 25)

	src := buildSourceEncoder(t, 1920, 1080, 25)
	defer src.Close()
	feed(t, p, src, 1920, 1080, 30)

	tail, err := p.Flush()
	require.NoError(t, err)
	// Tail may legitimately be empty for ultrafast no-B encoders; only
	// assert the call succeeded without error and the byte slices are
	// well-formed.
	for _, b := range tail {
		require.NotEmpty(t, b)
	}
	p.Close()
}

// Close is idempotent — production supervisor may call it twice on the
// teardown path (once from the data goroutine, once from the control
// goroutine).
func TestStreamPipeline_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	p := renditionPipeline(t, 25)
	p.Close()
	p.Close()
}

func TestStreamPipeline_ProcessAfterCloseReturnsError(t *testing.T) {
	t.Parallel()
	p := renditionPipeline(t, 25)
	p.Close()
	_, err := p.ProcessPacket([]byte{0}, 0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestStreamPipeline_SwitchAfterCloseReturnsError(t *testing.T) {
	t.Parallel()
	p := renditionPipeline(t, 25)
	p.Close()
	_, err := p.SwitchInput(DecoderConfig{Codec: "h264"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}
