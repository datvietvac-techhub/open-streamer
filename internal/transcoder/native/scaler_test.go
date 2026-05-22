package native

import (
	"testing"

	"github.com/asticode/go-astiav"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewScaler_RejectsZeroDstDims(t *testing.T) {
	t.Parallel()
	_, err := NewScaler(ScalerConfig{DstWidth: 0, DstHeight: 480, DstPixelFmt: astiav.PixelFormatYuv420P})
	require.Error(t, err)
	_, err = NewScaler(ScalerConfig{DstWidth: 640, DstHeight: 0, DstPixelFmt: astiav.PixelFormatYuv420P})
	require.Error(t, err)
}

// Same-format Scale is essentially a memcpy through libswscale —
// validates the happy path: a 320×240 YUV420P frame in, a 320×240
// YUV420P frame out, same PTS.
func TestScaler_PassThroughSameFormat(t *testing.T) {
	t.Parallel()
	s, err := NewScaler(ScalerConfig{
		DstWidth:    320,
		DstHeight:   240,
		DstPixelFmt: astiav.PixelFormatYuv420P,
	})
	require.NoError(t, err)
	defer s.Close()

	src := allocTestNV12Frame(t, 320, 240, astiav.PixelFormatYuv420P, 42)
	defer src.Free()

	dst, err := s.Scale(src)
	require.NoError(t, err)
	defer dst.Free()

	assert.Equal(t, 320, dst.Width())
	assert.Equal(t, 240, dst.Height())
	assert.Equal(t, astiav.PixelFormatYuv420P, dst.PixelFormat())
	assert.Equal(t, int64(42), dst.Pts(), "scaler must forward source PTS")
}

// Downscale 1920×1080 → 1280×720 — the production ABR rung. Verify
// the dst frame has the expected dst dims, not src dims.
func TestScaler_Downscale1080pTo720p(t *testing.T) {
	t.Parallel()
	s, err := NewScaler(ScalerConfig{
		DstWidth:    1280,
		DstHeight:   720,
		DstPixelFmt: astiav.PixelFormatYuv420P,
	})
	require.NoError(t, err)
	defer s.Close()

	src := allocTestNV12Frame(t, 1920, 1080, astiav.PixelFormatYuv420P, 0)
	defer src.Free()
	dst, err := s.Scale(src)
	require.NoError(t, err)
	defer dst.Free()
	assert.Equal(t, 1280, dst.Width())
	assert.Equal(t, 720, dst.Height())
}

// The whole reason this layer exists separate from the encoder: when
// source dims change mid-stream (typical on input switch from one
// resolution to another), the scaler must rebuild its sws context
// transparently. Verify by feeding two frames of different sizes and
// observing both Scale calls succeed.
func TestScaler_ReconfiguresOnSourceFormatChange(t *testing.T) {
	t.Parallel()
	s, err := NewScaler(ScalerConfig{
		DstWidth:    640,
		DstHeight:   360,
		DstPixelFmt: astiav.PixelFormatYuv420P,
	})
	require.NoError(t, err)
	defer s.Close()

	// First call seeds sws @ 1920×1080.
	src1 := allocTestNV12Frame(t, 1920, 1080, astiav.PixelFormatYuv420P, 0)
	dst1, err := s.Scale(src1)
	require.NoError(t, err)
	src1.Free()
	dst1.Free()
	w, h, _ := s.SourceFormat()
	assert.Equal(t, 1920, w)
	assert.Equal(t, 1080, h)

	// Second call with 1280×720 — sws ctx must be rebuilt silently.
	src2 := allocTestNV12Frame(t, 1280, 720, astiav.PixelFormatYuv420P, 0)
	dst2, err := s.Scale(src2)
	require.NoError(t, err, "scaler must absorb mid-stream source-dim change")
	src2.Free()
	dst2.Free()
	w, h, _ = s.SourceFormat()
	assert.Equal(t, 1280, w)
	assert.Equal(t, 720, h)
}

func TestScaler_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	s, err := NewScaler(ScalerConfig{
		DstWidth:    320,
		DstHeight:   240,
		DstPixelFmt: astiav.PixelFormatYuv420P,
	})
	require.NoError(t, err)
	s.Close()
	s.Close()
}

func TestScaler_ScaleAfterCloseReturnsError(t *testing.T) {
	t.Parallel()
	s, err := NewScaler(ScalerConfig{
		DstWidth:    320,
		DstHeight:   240,
		DstPixelFmt: astiav.PixelFormatYuv420P,
	})
	require.NoError(t, err)
	s.Close()
	src := allocTestNV12Frame(t, 320, 240, astiav.PixelFormatYuv420P, 0)
	defer src.Free()
	_, err = s.Scale(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// End-to-end pipeline: synthetic 1920×1080 source → encode → decode →
// scale to 1280×720 → encode at the rendition bitrate. Validates the
// full chain decoder → scaler → encoder works as the StreamPipeline
// (P3) will wire it.
func TestPipeline_RoundtripEncodeDecodeScaleEncode(t *testing.T) {
	t.Parallel()
	const (
		srcWidth  = 1920
		srcHeight = 1080
		dstWidth  = 1280
		dstHeight = 720
		framerate = 25
		nFrames   = 25
	)

	// Source encoder (simulates incoming H.264 from an ingest source).
	srcEnc, err := NewEncoder(EncoderConfig{
		Width:       srcWidth,
		Height:      srcHeight,
		Framerate:   framerate,
		BitrateKbps: 3500,
		GOPSize:     framerate,
		MaxBFrames:  0,
		Options:     map[string]string{"preset": "ultrafast", "tune": "zerolatency"},
	})
	require.NoError(t, err)
	defer srcEnc.Close()

	dec, err := NewDecoder(DecoderConfig{Codec: "h264"})
	require.NoError(t, err)
	defer dec.Close()

	sc, err := NewScaler(ScalerConfig{
		DstWidth:    dstWidth,
		DstHeight:   dstHeight,
		DstPixelFmt: astiav.PixelFormatYuv420P,
	})
	require.NoError(t, err)
	defer sc.Close()

	rendEnc, err := NewEncoder(EncoderConfig{
		Width:       dstWidth,
		Height:      dstHeight,
		Framerate:   framerate,
		BitrateKbps: 1600,
		GOPSize:     framerate,
		MaxBFrames:  0,
		Options:     map[string]string{"preset": "ultrafast", "tune": "zerolatency"},
	})
	require.NoError(t, err)
	defer rendEnc.Close()

	var rendBytes int
	for i := 0; i < nFrames; i++ {
		// 1) Generate a synthetic source frame.
		srcFrame := allocTestNV12Frame(t, srcWidth, srcHeight, astiav.PixelFormatYuv420P, int64(i))
		srcPkts, err := srcEnc.Encode(srcFrame)
		srcFrame.Free()
		require.NoError(t, err)

		// 2) Feed each encoded packet into the decoder.
		for _, pkt := range srcPkts {
			frames, err := dec.Decode(pkt, int64(i), int64(i))
			require.NoError(t, err)

			// 3) Scale each decoded frame and 4) re-encode at the
			// rendition bitrate.
			for _, df := range frames {
				scaled, err := sc.Scale(df)
				df.Free()
				require.NoError(t, err)
				scaled.SetPts(rendEnc.NextPTS())
				rendPkts, err := rendEnc.Encode(scaled)
				scaled.Free()
				require.NoError(t, err)
				for _, rp := range rendPkts {
					rendBytes += len(rp)
				}
			}
		}
	}
	require.Positive(t, rendBytes,
		"end-to-end pipeline produced no rendition bytes across %d frames", nFrames)
}
