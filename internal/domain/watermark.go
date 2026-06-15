package domain

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// WatermarkType determines whether the overlay is text or an image.
type WatermarkType string

// WatermarkType values select overlay content kind.
const (
	WatermarkTypeText  WatermarkType = "text"
	WatermarkTypeImage WatermarkType = "image"
)

// WatermarkPosition controls where the overlay is placed in the frame.
type WatermarkPosition string

// WatermarkPosition values name corners, center, or `custom` for overlay
// placement. `custom` activates the X / Y fields below — operators set
// raw FFmpeg expressions ("100", "main_w-overlay_w-50", "if(gt(t,5),10,-100)")
// for full positional flexibility while the corners stay convenient defaults.
const (
	WatermarkTopLeft     WatermarkPosition = "top_left"
	WatermarkTopRight    WatermarkPosition = "top_right"
	WatermarkBottomLeft  WatermarkPosition = "bottom_left"
	WatermarkBottomRight WatermarkPosition = "bottom_right"
	WatermarkCenter      WatermarkPosition = "center"
	WatermarkCustom      WatermarkPosition = "custom"
)

// WatermarkConfig defines an overlay applied to the video before encoding.
// Applied via FFmpeg drawtext (text) or overlay (image) filter.
type WatermarkConfig struct {
	Enabled bool          `json:"enabled" yaml:"enabled"`
	Type    WatermarkType `json:"type" yaml:"type"`

	// --- Text overlay ---

	// Text is the string to render. Supports strftime directives for live timestamps.
	// E.g. "LIVE %{localtime:%H:%M:%S}"
	Text string `json:"text" yaml:"text"`

	// FontFile is the path to a .ttf/.otf font file.
	// "" = FFmpeg default font.
	FontFile string `json:"font_file" yaml:"font_file"`

	// FontSize in pixels. Default: 24.
	FontSize int `json:"font_size" yaml:"font_size"`

	// FontColor in FFmpeg color syntax. E.g. "white", "#FFFFFF", "white@0.8".
	FontColor string `json:"font_color" yaml:"font_color"`

	// --- Image overlay ---

	// Filename references a WatermarkAsset by its on-disk name in the
	// /watermarks library (eg. `vtv1_logo.png`). Required when Type ==
	// WatermarkTypeImage. The coordinator resolves this into ImagePath
	// (private field) before passing the config to the transcoder.
	Filename WatermarkFilename `json:"filename,omitempty" yaml:"filename,omitempty"`

	// ImagePath is the absolute path the coordinator writes after looking
	// Filename up in the watermarks service. NOT user-settable — `json:"-"`
	// keeps it off the wire so operators can only configure assets that
	// live in the managed library. Transcoder reads ImagePath downstream
	// because it has no view of the library; passing the resolved path
	// keeps the filter graph code library-agnostic.
	ImagePath string `json:"-" yaml:"-"`

	// --- Common ---

	// Opacity controls transparency: 0.0 = fully transparent, 1.0 = fully opaque.
	Opacity float64 `json:"opacity" yaml:"opacity"`

	// Position selects how the (X, Y) of the overlay are computed:
	//   - top_left/top_right/bottom_left/bottom_right/center: convenience
	//     anchors. OffsetX/Y act as inward padding from the chosen edge
	//     (Center ignores offsets).
	//   - custom: X / Y below are used as raw FFmpeg expressions —
	//     pixel ints ("100"), expressions ("main_w-overlay_w-50"), or
	//     time-aware fades ("if(gt(t,5),10,-100)") all work.
	Position WatermarkPosition `json:"position" yaml:"position"`

	// OffsetX and OffsetY are pixel offsets from the chosen position edge.
	// Ignored when Position == custom.
	OffsetX int `json:"offset_x" yaml:"offset_x"`
	OffsetY int `json:"offset_y" yaml:"offset_y"`

	// X / Y are raw FFmpeg coordinate expressions used only when Position
	// == custom. Empty string defaults to "0". The exact variables exposed
	// depend on the filter:
	//   - drawtext (text watermark): w/h = frame size, tw/th = text size
	//   - overlay (image watermark):  W/H = main video size, w/h = overlay size
	X string `json:"x,omitempty" yaml:"x,omitempty"`
	Y string `json:"y,omitempty" yaml:"y,omitempty"`

	// Resize, when true, makes the watermark render at a consistent on-screen
	// ratio across every rendition in an ABR ladder. The largest profile
	// renders the asset at its NATIVE pixel size (operators design at the top
	// rendition); smaller profiles shrink the asset by the ratio of their
	// width to the largest profile's width. Pixel-scale fields (FontSize for
	// text, OffsetX/OffsetY for both) shrink with the same factor so corner
	// padding and glyph height stay visually proportional.
	//
	// When false (default), the watermark uses native pixel dimensions and
	// fixed offsets on every profile — appearing larger on lower-resolution
	// renditions because they cover fewer pixels of frame.
	Resize bool `json:"resize,omitempty" yaml:"resize,omitempty"`
}

// Defaults that fill in when the operator leaves a field empty / zero.
const (
	defaultWatermarkFontSize  = 24
	defaultWatermarkFontColor = "white"
	defaultWatermarkOpacity   = 1.0
	defaultWatermarkPosition  = WatermarkBottomRight
	defaultWatermarkOffset    = 10
)

// IsActive reports whether the watermark should actually be applied.
// Returns false for nil, disabled, or fully-transparent configs (any of
// which would emit a no-op filter — easier to just skip).
//
// For image watermarks, the check accepts EITHER Filename (pre-resolve)
// OR ImagePath (post-coordinator-resolve), so the same predicate is
// valid before the coordinator translates user input and at the time the
// transcoder builds the filter graph.
func (w *WatermarkConfig) IsActive() bool {
	if w == nil || !w.Enabled {
		return false
	}
	if w.Opacity != 0 && w.Opacity < 0.01 {
		return false
	}
	switch w.Type {
	case WatermarkTypeText:
		return strings.TrimSpace(w.Text) != ""
	case WatermarkTypeImage:
		return strings.TrimSpace(string(w.Filename)) != "" || strings.TrimSpace(w.ImagePath) != ""
	default:
		return false
	}
}

// Validate enforces invariants the FFmpeg filter graph relies on.
// Called from the API layer at save time so misconfigured streams never
// reach the transcoder. Disabled watermarks (Enabled=false) skip validation
// entirely so an operator can park a draft config without tripping errors.
func (w *WatermarkConfig) Validate() error {
	if w == nil || !w.Enabled {
		return nil
	}
	switch w.Type {
	case WatermarkTypeText:
		if strings.TrimSpace(w.Text) == "" {
			return fmt.Errorf("watermark: text is required when type=text")
		}
		if w.FontFile != "" {
			if err := assertReadableFile(w.FontFile); err != nil {
				return fmt.Errorf("watermark: font_file: %w", err)
			}
		}
		if err := ValidateFontColor(w.FontColor); err != nil {
			return fmt.Errorf("watermark: font_color: %w", err)
		}
	case WatermarkTypeImage:
		if strings.TrimSpace(string(w.Filename)) == "" {
			return fmt.Errorf("watermark: filename is required when type=image")
		}
		if err := ValidateWatermarkFilename(string(w.Filename)); err != nil {
			return fmt.Errorf("watermark: filename: %w", err)
		}
	default:
		return fmt.Errorf("watermark: unknown type %q (want text|image)", w.Type)
	}
	if w.Opacity < 0 || w.Opacity > 1 {
		return fmt.Errorf("watermark: opacity must be in [0,1], got %.2f", w.Opacity)
	}
	if w.FontSize < 0 {
		return fmt.Errorf("watermark: font_size must be >= 0, got %d", w.FontSize)
	}
	switch w.Position {
	case "", WatermarkTopLeft, WatermarkTopRight,
		WatermarkBottomLeft, WatermarkBottomRight, WatermarkCenter:
		// ok
	case WatermarkCustom:
		if strings.TrimSpace(w.X) == "" && strings.TrimSpace(w.Y) == "" {
			return fmt.Errorf("watermark: position=custom requires non-empty x and/or y")
		}
	default:
		return fmt.Errorf("watermark: unknown position %q", w.Position)
	}
	return nil
}

// fontColorPattern accepts a plain color literal only: a named color (letters),
// #RRGGBB[AA] or 0xRRGGBB[AA] hex, with an optional @opacity in [0,1]. It
// deliberately excludes every lavfi metacharacter (colon, comma, quote,
// bracket, backslash) so a font_color can't chain a second drawtext/movie
// filter into the graph (S-3).
var fontColorPattern = regexp.MustCompile(`^([A-Za-z]+|#[0-9A-Fa-f]{6}|#[0-9A-Fa-f]{8}|0x[0-9A-Fa-f]{6}|0x[0-9A-Fa-f]{8})(@(0(\.[0-9]+)?|1(\.0+)?|\.[0-9]+))?$`)

// ValidateFontColor rejects a watermark font_color that is not a plain color
// literal. Empty is allowed (the transcoder defaults it to "white").
func ValidateFontColor(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	if !fontColorPattern.MatchString(s) {
		return fmt.Errorf("invalid color %q; want a named color, #RRGGBB[AA], or 0xRRGGBB[AA] (optionally @<0..1>)", s)
	}
	return nil
}

// Resolved returns a copy with empty / zero fields replaced by defaults.
// Caller treats the returned value as fully populated. Returns nil when
// the input is nil so callers can chain safely.
func (w *WatermarkConfig) Resolved() *WatermarkConfig {
	if w == nil {
		return nil
	}
	out := *w
	if out.FontSize == 0 {
		out.FontSize = defaultWatermarkFontSize
	}
	if strings.TrimSpace(out.FontColor) == "" {
		out.FontColor = defaultWatermarkFontColor
	}
	if out.Opacity == 0 {
		out.Opacity = defaultWatermarkOpacity
	}
	if out.Position == "" {
		out.Position = defaultWatermarkPosition
	}
	// Offsets default to 10px from the chosen anchor (matches drawtext
	// "comfortable padding" rule). Center position ignores offsets.
	if out.OffsetX == 0 && out.Position != WatermarkCenter {
		out.OffsetX = defaultWatermarkOffset
	}
	if out.OffsetY == 0 && out.Position != WatermarkCenter {
		out.OffsetY = defaultWatermarkOffset
	}
	return &out
}

// assertReadableFile is the file-existence check Validate uses for both
// font_file and image_path. Wraps the os error so the caller receives
// "<file>: not found" instead of the bare syscall message.
func assertReadableFile(p string) error {
	if !filepath.IsAbs(p) {
		return fmt.Errorf("path must be absolute, got %q", p)
	}
	st, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file not found: %s", p)
		}
		return fmt.Errorf("stat %s: %w", p, err)
	}
	if st.IsDir() {
		return fmt.Errorf("expected file, got directory: %s", p)
	}
	return nil
}

// ThumbnailConfig controls periodic screenshot generation for stream preview.
// Thumbnails are written as JPEG files alongside HLS segments.
type ThumbnailConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled"`

	// IntervalSec generates one thumbnail every N seconds.
	IntervalSec int `json:"interval_sec" yaml:"interval_sec"`

	// Width and Height of the output thumbnail in pixels.
	// 0 = match source resolution.
	Width  int `json:"width" yaml:"width"`
	Height int `json:"height" yaml:"height"`

	// Quality is the JPEG quality (1–31, lower = better). Default: 5.
	Quality int `json:"quality" yaml:"quality"`

	// OutputDir is relative to the publisher HLS directory.
	// E.g. "thumbnails" → written to {hls_dir}/{stream_code}/thumbnails/thumb.jpg
	OutputDir string `json:"output_dir" yaml:"output_dir"`
}
