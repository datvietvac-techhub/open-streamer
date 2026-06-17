package native

import (
	"testing"
	"unsafe"

	"github.com/asticode/go-astiav"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newFLTPFrame builds an 8-sample planar-float stereo frame whose every sample
// equals val.
func newFLTPFrame(t *testing.T, val float32) *astiav.Frame {
	t.Helper()
	f := astiav.AllocFrame()
	f.SetNbSamples(8)
	f.SetSampleRate(48000)
	f.SetSampleFormat(astiav.SampleFormatFltp)
	f.SetChannelLayout(astiav.ChannelLayoutStereo)
	require.NoError(t, f.AllocBuffer(0))
	require.NoError(t, f.SamplesFillSilence())
	b, err := f.Data().Bytes(1)
	require.NoError(t, err)
	s := unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), len(b)/4)
	for i := range s {
		s[i] = val
	}
	require.NoError(t, f.Data().SetBytes(b, 1))
	return f
}

func frameSamples(t *testing.T, f *astiav.Frame) []float32 {
	t.Helper()
	b, err := f.Data().Bytes(1)
	require.NoError(t, err)
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), len(b)/4)
}

// applyGain scales every sample by volFactor and clamps to [-1, 1].
func TestApplyGain_ScalesAndClamps(t *testing.T) {
	t.Parallel()

	f := newFLTPFrame(t, 0.4)
	(&audioReencoder{volFactor: 2}).applyGain(f) // ×2
	for _, s := range frameSamples(t, f) {
		assert.InDelta(t, 0.8, float64(s), 1e-6) // 0.4 × 2 = 0.8, no clamp
	}
	f.Free()

	// Boost that overshoots full scale is clamped to 1.0 (no wrap/distortion).
	f = newFLTPFrame(t, 0.8)
	(&audioReencoder{volFactor: 2}).applyGain(f) // 0.8×2=1.6 → clamp 1.0
	for _, s := range frameSamples(t, f) {
		assert.InDelta(t, 1.0, float64(s), 1e-6)
	}
	f.Free()

	// Attenuation.
	f = newFLTPFrame(t, 0.6)
	(&audioReencoder{volFactor: 0.5}).applyGain(f)
	for _, s := range frameSamples(t, f) {
		assert.InDelta(t, 0.3, float64(s), 1e-6)
	}
	f.Free()
}

// Unity gain (1.0) is a no-op — the common path must not touch samples.
func TestApplyGain_UnityNoOp(t *testing.T) {
	t.Parallel()
	f := newFLTPFrame(t, 0.37)
	(&audioReencoder{volFactor: 1}).applyGain(f)
	for _, s := range frameSamples(t, f) {
		assert.InDelta(t, 0.37, float64(s), 1e-6)
	}
	f.Free()
}
