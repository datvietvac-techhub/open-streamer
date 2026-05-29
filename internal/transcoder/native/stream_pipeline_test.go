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

// renditionPipeline returns a single-rendition pipeline (decode h264,
// scale to 1280x720, re-encode at 1.6 Mbps). Single rendition is the
// minimum the pipeline accepts and is what most legacy tests assume;
// multi-rendition coverage lives in dedicated tests below.
//
//nolint:unparam // see buildSourceEncoder; fps reserved for P5.
func renditionPipeline(t *testing.T, fps int) *StreamPipeline {
	t.Helper()
	p, err := NewStreamPipeline(PipelineConfig{
		Decoder: DecoderConfig{Codec: "h264"},
		Renditions: []RenditionConfig{
			{
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
			},
		},
		// Passthrough audio: these tests exercise the video + switch
		// path, where the discontinuity signal is expected.
		Audio: AudioConfig{Copy: true},
	})
	require.NoError(t, err)
	return p
}

// multiRenditionPipeline returns a 3-rendition (1080p/720p/480p) ABR
// pipeline used by P7 tests to exercise the per-rendition encode
// loop and the OutputFrame.TargetIndex routing.
func multiRenditionPipeline(t *testing.T, fps int) *StreamPipeline {
	t.Helper()
	mk := func(w, h, kbps int) RenditionConfig {
		return RenditionConfig{
			Scaler: ScalerConfig{
				DstWidth:    w,
				DstHeight:   h,
				DstPixelFmt: astiav.PixelFormatYuv420P,
			},
			Encoder: EncoderConfig{
				Width:       w,
				Height:      h,
				Framerate:   fps,
				BitrateKbps: kbps,
				GOPSize:     fps,
				MaxBFrames:  0,
				Options:     map[string]string{"preset": "ultrafast", "tune": "zerolatency"},
			},
		}
	}
	p, err := NewStreamPipeline(PipelineConfig{
		Decoder: DecoderConfig{Codec: "h264"},
		Renditions: []RenditionConfig{
			mk(1920, 1080, 3500),
			mk(1280, 720, 1600),
			mk(854, 480, 800),
		},
		Audio: AudioConfig{Copy: true},
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
			out, err := p.ProcessPacket(pkt.Data, int64(i), int64(i))
			require.NoError(t, err)
			for _, op := range out {
				outBytes += len(op.Data)
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
		Renditions: []RenditionConfig{
			{
				Scaler: ScalerConfig{
					DstWidth:    640,
					DstHeight:   360,
					DstPixelFmt: astiav.PixelFormatYuv420P,
				},
				Encoder: EncoderConfig{
					Codec: "not_a_real_encoder", // forces error
					Width: 640, Height: 360, Framerate: 25,
				},
			},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encoder")
}

// Empty Renditions is rejected — a pipeline with no output target
// would silently drop every frame.
func TestNewStreamPipeline_NoRenditionsRejected(t *testing.T) {
	t.Parallel()
	_, err := NewStreamPipeline(PipelineConfig{
		Decoder: DecoderConfig{Codec: "h264"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rendition")
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
	for _, f := range tail {
		outBytes += len(f.Data)
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

	// Snapshot encoder + scaler pointers before any I/O. Single-rendition
	// pipeline, so index 0 is the only one to assert against.
	require.Len(t, p.encoders, 1)
	require.Len(t, p.scalers, 1)
	encBefore := p.encoders[0]
	scBefore := p.scalers[0]
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

	// THE INVARIANT: every rendition's encoder + scaler keep their
	// identity. Multi-rendition pipelines must preserve ALL of them,
	// not just the first one.
	assert.Same(t, encBefore, p.encoders[0],
		"SwitchInput must NOT reallocate the encoder — entire migration depends on this")
	assert.Same(t, scBefore, p.scalers[0],
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

// Multi-rendition pipeline emits exactly N output frames per decoded
// input frame, each tagged with a distinct TargetIndex (0..N-1) so
// the supervisor routes each one to the matching rendition buffer.
// Without that fan-out, only track_1 would ever see data — exactly
// the bug the previous single-target hardcode produced.
func TestStreamPipeline_MultiRenditionFansOutPerTarget(t *testing.T) {
	t.Parallel()
	p := multiRenditionPipeline(t, 25)
	defer p.Close()

	src := buildSourceEncoder(t, 1920, 1080, 25)
	defer src.Close()

	perTarget := map[int32]int{}
	for i := 0; i < 30; i++ {
		frame := allocTestNV12Frame(t, 1920, 1080, astiav.PixelFormatYuv420P, int64(i))
		pkts, err := src.Encode(frame)
		frame.Free()
		require.NoError(t, err)
		for _, pkt := range pkts {
			out, err := p.ProcessPacket(pkt.Data, int64(i), int64(i))
			require.NoError(t, err)
			for _, op := range out {
				perTarget[op.TargetIndex]++
			}
		}
	}
	tail, err := p.Flush()
	require.NoError(t, err)
	for _, op := range tail {
		perTarget[op.TargetIndex]++
	}

	// Every rendition must have produced at least one packet.
	for idx := int32(0); idx < 3; idx++ {
		assert.Positivef(t, perTarget[idx],
			"rendition %d produced zero output packets — fan-out broken", idx)
	}
	// No frame should have leaked into a non-existent target.
	for idx := range perTarget {
		assert.GreaterOrEqualf(t, idx, int32(0), "unexpected negative TargetIndex %d", idx)
		assert.Lessf(t, idx, int32(3), "TargetIndex %d outside rendition count", idx)
	}
}

// TestStreamPipeline_SwitchInputLatchesSessionStartAndForcedIDR locks
// the discontinuity contract: after SwitchInput, the next decoded
// frame's encoded output (from every rendition) carries
// SessionStart=true exactly once, and the encoder was asked for an
// IDR via the frame's picture-type hint. Without these, the player
// downstream keeps the old source's MSE init context and decodes
// new-source frames into permanent garbage that requires a reload.
func TestStreamPipeline_SwitchInputLatchesSessionStartAndForcedIDR(t *testing.T) {
	t.Parallel()
	p := multiRenditionPipeline(t, 25)
	defer p.Close()

	src1 := buildSourceEncoder(t, 1920, 1080, 25)
	feed(t, p, src1, 1920, 1080, 10)
	src1.Close()

	_, err := p.SwitchInput(DecoderConfig{Codec: "h264"})
	require.NoError(t, err)
	assert.True(t, p.pendingSessionStart, "SwitchInput must latch pendingSessionStart")
	assert.True(t, p.pendingForceKeyframe, "SwitchInput must latch pendingForceKeyframe")

	src2 := buildSourceEncoder(t, 1920, 1080, 25)
	defer src2.Close()

	// Push exactly one frame; the latches must be consumed by the
	// first OutputFrame batch emitted and not carry into subsequent
	// frames.
	var firstBatch []OutputFrame
	for i := 0; i < 5 && len(firstBatch) == 0; i++ {
		frame := allocTestNV12Frame(t, 1920, 1080, astiav.PixelFormatYuv420P, int64(i))
		pkts, err := src2.Encode(frame)
		frame.Free()
		require.NoError(t, err)
		for _, pkt := range pkts {
			out, err := p.ProcessPacket(pkt.Data, int64(i), int64(i))
			require.NoError(t, err)
			if len(out) > 0 {
				firstBatch = out
				break
			}
		}
	}
	require.NotEmpty(t, firstBatch, "no output produced after SwitchInput")

	for _, f := range firstBatch {
		assert.True(t, f.SessionStart,
			"target %d: first OutputFrame after SwitchInput must carry SessionStart=true", f.TargetIndex)
	}
	assert.False(t, p.pendingSessionStart,
		"pendingSessionStart must be consumed by the first emitted batch")
	assert.False(t, p.pendingForceKeyframe,
		"pendingForceKeyframe must be consumed by the first emitted batch")

	// Drive more frames; their OutputFrames must NOT carry SessionStart.
	frame := allocTestNV12Frame(t, 1920, 1080, astiav.PixelFormatYuv420P, 1000)
	pkts, err := src2.Encode(frame)
	frame.Free()
	require.NoError(t, err)
	for _, pkt := range pkts {
		out, err := p.ProcessPacket(pkt.Data, 1000, 1000)
		require.NoError(t, err)
		for _, f := range out {
			assert.False(t, f.SessionStart,
				"SessionStart leaked into a non-first OutputFrame batch (target %d)", f.TargetIndex)
		}
	}
}

// TestStreamPipeline_SwitchSuppressesDiscontinuityWhenReencoding locks
// the seamless-switch contract: when audio is re-encoded (Audio.Copy=
// false) the output is fully continuous (same encoder SPS/PPS,
// rebased PTS, fixed audio format), so SwitchInput must NOT latch
// pendingSessionStart — emitting EXT-X-DISCONTINUITY there would make
// the player re-buffer at every scene change. forceKeyframe is still
// latched so the boundary segment starts at a clean IDR.
func TestStreamPipeline_SwitchSuppressesDiscontinuityWhenReencoding(t *testing.T) {
	t.Parallel()
	p, err := NewStreamPipeline(PipelineConfig{
		Decoder: DecoderConfig{Codec: "h264"},
		Renditions: []RenditionConfig{
			{
				Scaler:  ScalerConfig{DstWidth: 1280, DstHeight: 720, DstPixelFmt: astiav.PixelFormatYuv420P},
				Encoder: EncoderConfig{Width: 1280, Height: 720, Framerate: 25, BitrateKbps: 1600, GOPSize: 25, Options: map[string]string{"preset": "ultrafast", "tune": "zerolatency"}},
			},
		},
		Audio: AudioConfig{Codec: "aac", SampleRate: 44100, Channels: 2, BitrateK: 128}, // Copy=false → re-encode
	})
	if err != nil {
		t.Skipf("aac encoder unavailable: %v", err)
	}
	defer p.Close()
	require.NotNil(t, p.audioReenc, "re-encode config must build an audioReencoder")

	_, err = p.SwitchInput(DecoderConfig{Codec: "h264"})
	require.NoError(t, err)
	assert.False(t, p.pendingSessionStart,
		"re-encoded audio → continuous output → must NOT signal discontinuity (would force player re-buffer)")
	assert.True(t, p.pendingForceKeyframe,
		"boundary segment must still start at a forced IDR")
}

// SwitchInput on a multi-rendition pipeline must preserve identity of
// EVERY scaler + encoder, not just the first. A regression here would
// drop frames for renditions 1..N on every input switch.
func TestStreamPipeline_SwitchInputPreservesAllRenditions(t *testing.T) {
	t.Parallel()
	p := multiRenditionPipeline(t, 25)
	defer p.Close()

	encsBefore := append([]*Encoder(nil), p.encoders...)
	scsBefore := append([]*Scaler(nil), p.scalers...)

	src := buildSourceEncoder(t, 1920, 1080, 25)
	feed(t, p, src, 1920, 1080, 25)
	src.Close()

	_, err := p.SwitchInput(DecoderConfig{Codec: "h264"})
	require.NoError(t, err)

	require.Len(t, p.encoders, len(encsBefore))
	require.Len(t, p.scalers, len(scsBefore))
	for i := range encsBefore {
		assert.Samef(t, encsBefore[i], p.encoders[i],
			"SwitchInput reallocated rendition %d encoder", i)
		assert.Samef(t, scsBefore[i], p.scalers[i],
			"SwitchInput reallocated rendition %d scaler", i)
	}
}

// TestRebaseVideoPTS_StalledSourcePacesAtFrameRate reproduces the
// "buffer grows, won't play after input switch" failure. When the active
// decoder mishandles a source's timestamps (e.g. an HLS-pull input whose
// pkt_timebase the NVDEC binding can't set) it emits frames with stalled
// / non-monotonic PTS. The legacy monotonic guard advanced the output by
// +1 ms per frame, collapsing the timeline (~1 ms/frame) so the publisher
// never reached segDur in PTS and force-flushed off-IDR every wallclock
// max_dur. The output must instead advance by one nominal frame interval.
func TestRebaseVideoPTS_StalledSourcePacesAtFrameRate(t *testing.T) {
	p := &StreamPipeline{videoFrameDurMs: 40, pendingRebase: true} // 25 fps

	const stuck = 90_000 // a decoder emitting the SAME source PTS every frame
	got := make([]int64, 0, 5)
	for range 5 {
		got = append(got, p.rebaseVideoPTS(stuck))
	}

	// First frame anchors the output clock to 1 ms; every subsequent frame
	// advances by the 40 ms frame interval — NOT the legacy +1 ms.
	assert.Equal(t, []int64{1, 41, 81, 121, 161}, got)
}

// TestRebaseVideoPTS_MonotonicSourceTracksSource confirms the fix is inert
// on healthy input: a source PTS advancing normally is passed through
// (offset-rebased) without the frame-interval pacing kicking in. The
// source steps by 50 ms (≠ the 40 ms frame interval) so the assertion
// proves the output follows the SOURCE, not the pacing fallback.
func TestRebaseVideoPTS_MonotonicSourceTracksSource(t *testing.T) {
	p := &StreamPipeline{videoFrameDurMs: 40, pendingRebase: true}

	got := make([]int64, 0, 4)
	for _, s := range []int64{1000, 1050, 1100, 1150} {
		got = append(got, p.rebaseVideoPTS(s))
	}
	assert.Equal(t, []int64{1, 51, 101, 151}, got)
}
