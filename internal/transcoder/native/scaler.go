package native

import (
	"errors"
	"fmt"

	"github.com/asticode/go-astiav"
)

// ScalerConfig defines a destination format. Source dimensions / pixel
// format are inferred from the first frame and re-detected on every
// Scale call so a mid-stream resolution change auto-reconfigures the
// underlying sws context — that's the main reason this exists separate
// from the encoder.
type ScalerConfig struct {
	DstWidth    int
	DstHeight   int
	DstPixelFmt astiav.PixelFormat // typically PixelFormatYuv420P for libx264 / NVENC input
	// Flags is passed to libswscale; 0 = SWS_FAST_BILINEAR-like default.
	// Use astiav.NewSoftwareScaleContextFlags(astiav.SoftwareScaleContextFlagBicubic)
	// for quality-leaning resamples.
	Flags astiav.SoftwareScaleContextFlags
}

// Scaler wraps libswscale. It is constructed once per stream lifetime
// and reconfigured transparently when the source frame's dimensions or
// pixel format change — that lets the upstream decoder be torn down and
// replaced on input switch without anyone downstream noticing.
//
// Not safe for concurrent use; serialise via the owning goroutine.
type Scaler struct {
	cfg ScalerConfig

	// sws is the active swscale context. nil = no input observed yet,
	// so the first Scale call seeds it.
	sws *astiav.SoftwareScaleContext

	// Source params from the most recent Scale call. Reconfigure when
	// any of them changes vs incoming frame.
	srcW   int
	srcH   int
	srcFmt astiav.PixelFormat

	closed bool
}

// NewScaler allocates a scaler with the given destination format. The
// underlying sws context is allocated lazily on the first Scale call so
// callers don't need to know the source dims upfront — the decoder
// hasn't produced a frame yet at construction time.
func NewScaler(cfg ScalerConfig) (*Scaler, error) {
	if cfg.DstWidth <= 0 || cfg.DstHeight <= 0 {
		return nil, fmt.Errorf("native: scaler dst dims must be positive (got %dx%d)", cfg.DstWidth, cfg.DstHeight)
	}
	return &Scaler{cfg: cfg}, nil
}

// Scale converts src to a newly-allocated destination frame matching
// the scaler's configured dst format. The caller owns the returned
// frame and MUST call Free on it.
//
// On source-format change (different srcW / srcH / pix_fmt vs prior
// call), the underlying sws context is silently rebuilt — that's the
// whole point of having Scaler stand between an ephemeral decoder and a
// long-lived encoder.
func (s *Scaler) Scale(src *astiav.Frame) (*astiav.Frame, error) {
	if s.closed {
		return nil, errors.New("native: scale on closed scaler")
	}
	if src == nil {
		return nil, errors.New("native: scale nil frame")
	}

	if err := s.ensureContext(src.Width(), src.Height(), src.PixelFormat()); err != nil {
		return nil, err
	}

	dst := astiav.AllocFrame()
	dst.SetWidth(s.cfg.DstWidth)
	dst.SetHeight(s.cfg.DstHeight)
	dst.SetPixelFormat(s.cfg.DstPixelFmt)
	if err := dst.AllocBuffer(0); err != nil {
		dst.Free()
		return nil, fmt.Errorf("native: scaler dst AllocBuffer: %w", err)
	}

	if err := s.sws.ScaleFrame(src, dst); err != nil {
		dst.Free()
		return nil, fmt.Errorf("native: ScaleFrame: %w", err)
	}

	// Forward source PTS so the downstream encoder can stamp the
	// encoded packet with the original frame timing. Callers can
	// override via dst.SetPts after Scale returns if they want a
	// different time base.
	dst.SetPts(src.Pts())
	return dst, nil
}

// ensureContext re-creates the sws context when source params have
// changed since the previous call (or on the first ever call).
func (s *Scaler) ensureContext(srcW, srcH int, srcFmt astiav.PixelFormat) error {
	if s.sws != nil && srcW == s.srcW && srcH == s.srcH && srcFmt == s.srcFmt {
		return nil
	}
	if s.sws != nil {
		s.sws.Free()
		s.sws = nil
	}
	sws, err := astiav.CreateSoftwareScaleContext(
		srcW, srcH, srcFmt,
		s.cfg.DstWidth, s.cfg.DstHeight, s.cfg.DstPixelFmt,
		s.cfg.Flags,
	)
	if err != nil {
		return fmt.Errorf("native: CreateSoftwareScaleContext %dx%d %s → %dx%d %s: %w",
			srcW, srcH, srcFmt, s.cfg.DstWidth, s.cfg.DstHeight, s.cfg.DstPixelFmt, err)
	}
	s.sws = sws
	s.srcW = srcW
	s.srcH = srcH
	s.srcFmt = srcFmt
	return nil
}

// SourceFormat returns the dimensions and pixel format the scaler is
// currently configured for, or (0, 0, PixelFormatNone) if no Scale call
// has happened yet.
func (s *Scaler) SourceFormat() (w, h int, fmt astiav.PixelFormat) {
	return s.srcW, s.srcH, s.srcFmt
}

// Close releases the underlying sws context. Safe to call once;
// subsequent calls are no-ops.
func (s *Scaler) Close() {
	if s.closed {
		return
	}
	s.closed = true
	if s.sws != nil {
		s.sws.Free()
		s.sws = nil
	}
}
