package native

import (
	"strings"
	"testing"
)

// TestNewWatermarker_DisabledReturnsNil locks the no-op contract the
// pipeline relies on: a disabled config must NOT allocate a filter
// graph, so encodeOneRendition can skip the filter pass entirely
// instead of routing every frame through a passthrough graph.
func TestNewWatermarker_DisabledReturnsNil(t *testing.T) {
	t.Parallel()
	w, err := NewWatermarker(WatermarkConfig{Enabled: false}, 1920, 1080, 25)
	if err != nil {
		t.Fatalf("NewWatermarker(disabled) = err %v, want nil", err)
	}
	if w != nil {
		t.Fatalf("NewWatermarker(disabled) returned non-nil instance — pipeline would still pay filter cost")
	}
}

// TestNewWatermarker_RejectsInvalidCanvas — building a graph for a
// zero-sized canvas would fail later inside libavfilter with an
// opaque message; reject at construction so callers see the bad
// rendition dims clearly.
func TestNewWatermarker_RejectsInvalidCanvas(t *testing.T) {
	t.Parallel()
	cfg := WatermarkConfig{Enabled: true, Type: "text", Text: "hi"}
	if _, err := NewWatermarker(cfg, 0, 1080, 25); err == nil {
		t.Fatal("expected error for width=0")
	}
	if _, err := NewWatermarker(cfg, 1920, 0, 25); err == nil {
		t.Fatal("expected error for height=0")
	}
}

// TestBuildWatermarkFilter_TextDefaults verifies the drawtext string
// has every parameter set even when caller omitted font-related
// fields — the defaults are what production operators see when they
// only fill in Text + Position via the REST API.
func TestBuildWatermarkFilter_TextDefaults(t *testing.T) {
	t.Parallel()
	got, err := BuildWatermarkFilter(WatermarkConfig{
		Enabled:  true,
		Type:     "text",
		Text:     "Hello",
		Position: "top_right",
		OffsetX:  16,
		OffsetY:  16,
	}, 1920, 1080)
	if err != nil {
		t.Fatalf("BuildWatermarkFilter: %v", err)
	}
	requires := []string{
		"drawtext=",
		"text='Hello'",
		"fontsize=24",
		"fontcolor=white",
		"x=w-text_w-16",
		"y=16",
		"fontfile=",
	}
	for _, w := range requires {
		if !strings.Contains(got, w) {
			t.Errorf("filter missing %q in %q", w, got)
		}
	}
}

// TestBuildWatermarkFilter_TextOpacityFormat — opacity must be
// emitted as fontcolor@N to keep drawtext happy; emitting it as a
// separate option silently produces an opaque overlay (drawtext
// drops unknown args).
func TestBuildWatermarkFilter_TextOpacityFormat(t *testing.T) {
	t.Parallel()
	got, err := BuildWatermarkFilter(WatermarkConfig{
		Enabled: true, Type: "text", Text: "T", Opacity: 0.7, FontColor: "white",
	}, 640, 360)
	if err != nil {
		t.Fatalf("BuildWatermarkFilter: %v", err)
	}
	if !strings.Contains(got, "fontcolor=white@0.7") {
		t.Errorf("opacity not embedded in fontcolor: %s", got)
	}
}

// TestBuildWatermarkFilter_TextEscapesQuotes — single quotes in user
// text break the lavfi quoted-string grammar. Without escaping a
// "don't" string aborts filter parsing and the subprocess fails
// to start.
func TestBuildWatermarkFilter_TextEscapesQuotes(t *testing.T) {
	t.Parallel()
	got, err := BuildWatermarkFilter(WatermarkConfig{
		Enabled: true, Type: "text", Text: "don't",
	}, 1920, 1080)
	if err != nil {
		t.Fatalf("BuildWatermarkFilter: %v", err)
	}
	if !strings.Contains(got, `text='don'\''t'`) {
		t.Errorf("apostrophe not lavfi-escaped: %s", got)
	}
}

// TestBuildWatermarkFilter_ImageRequiresAsset — the asset_path
// validation runs before NewWatermarker tries to allocate a graph,
// so the operator gets a clear error instead of an "asset not
// found" surfaced from inside libavfilter.
func TestBuildWatermarkFilter_ImageRequiresAsset(t *testing.T) {
	t.Parallel()
	_, err := BuildWatermarkFilter(WatermarkConfig{
		Enabled: true, Type: "image",
	}, 1920, 1080)
	if err == nil {
		t.Fatal("expected error for image watermark without asset_path")
	}
}

// TestBuildWatermarkFilter_ImageOpacity — opacity for image overlay
// rides through colorchannelmixer rather than fontcolor.
func TestBuildWatermarkFilter_ImageOpacity(t *testing.T) {
	t.Parallel()
	got, err := BuildWatermarkFilter(WatermarkConfig{
		Enabled: true, Type: "image", AssetPath: "/tmp/wm.png", Opacity: 0.5,
	}, 1920, 1080)
	if err != nil {
		t.Fatalf("BuildWatermarkFilter: %v", err)
	}
	if !strings.Contains(got, "colorchannelmixer=aa=0.5") {
		t.Errorf("opacity not threaded through colorchannelmixer: %s", got)
	}
	if !strings.Contains(got, "overlay=") {
		t.Errorf("overlay filter missing: %s", got)
	}
}

// TestBuildWatermarkFilter_UnknownType is a guardrail: an unknown
// type from the proto would otherwise reach NewWatermarker and
// produce a half-built graph or panic. Reject explicitly.
func TestBuildWatermarkFilter_UnknownType(t *testing.T) {
	t.Parallel()
	_, err := BuildWatermarkFilter(WatermarkConfig{
		Enabled: true, Type: "gif",
	}, 1920, 1080)
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

// TestResolveTextPosition — drawtext position math is sign- and
// expression-sensitive (text_w / text_h must subtract for right /
// bottom anchors). Locking the four canonical anchors keeps the
// player-visible alignment stable across refactors.
func TestResolveTextPosition(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		pos  string
		offX int
		offY int
		x    string
		y    string
	}{
		{"top_left", 16, 16, "16", "16"},
		{"top_right", 16, 16, "w-text_w-16", "16"},
		{"bottom_left", 16, 16, "16", "h-text_h-16"},
		{"bottom_right", 16, 16, "w-text_w-16", "h-text_h-16"},
		{"center", 0, 0, "(w-text_w)/2+0", "(h-text_h)/2+0"},
		{"unknown_falls_back_to_top_left", 8, 8, "8", "8"},
	}
	for _, tc := range tcs {
		t.Run(tc.pos, func(t *testing.T) {
			t.Parallel()
			x, y := resolveTextPosition(tc.pos, tc.offX, tc.offY)
			if x != tc.x || y != tc.y {
				t.Fatalf("pos=%s: got (%s, %s), want (%s, %s)", tc.pos, x, y, tc.x, tc.y)
			}
		})
	}
}

// TestResolveOverlayPosition — image overlay uses uppercase canvas
// dims (W/H) and lowercase overlay dims (w/h) per lavfi
// convention; mixing them up would push the image off-canvas.
func TestResolveOverlayPosition(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		pos string
		x   string
		y   string
	}{
		{"top_left", "0", "0"},
		{"top_right", "W-w-0", "0"},
		{"bottom_left", "0", "H-h-0"},
		{"bottom_right", "W-w-0", "H-h-0"},
		{"center", "(W-w)/2+0", "(H-h)/2+0"},
	}
	for _, tc := range tcs {
		t.Run(tc.pos, func(t *testing.T) {
			t.Parallel()
			x, y := resolveOverlayPosition(tc.pos, 0, 0)
			if x != tc.x || y != tc.y {
				t.Fatalf("pos=%s: got (%s, %s), want (%s, %s)", tc.pos, x, y, tc.x, tc.y)
			}
		})
	}
}

// TestEscapeLavfiArg — path arguments with colons (drive-letters,
// directory separators on some FS layouts) must be escaped so the
// lavfi parser doesn't treat them as filter-option separators.
func TestEscapeLavfiArg(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		in, want string
	}{
		{"/usr/share/fonts/dejavu.ttf", `/usr/share/fonts/dejavu.ttf`},
		{`C:\fonts\dejavu.ttf`, `C\:\\fonts\\dejavu.ttf`},
	}
	for _, tc := range tcs {
		got := escapeLavfiArg(tc.in)
		if got != tc.want {
			t.Errorf("escapeLavfiArg(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestWatermarker_FilterRoundTripText runs a small text watermark
// through a real filter graph on a synthetic 1080p NV12 frame and
// asserts an output frame is produced. Catches build-time issues
// where the libavfilter version on the host rejects the drawtext
// expressions (e.g., missing libfreetype, no fontconfig).
func TestWatermarker_FilterRoundTripText(t *testing.T) {
	t.Parallel()
	w, err := NewWatermarker(WatermarkConfig{
		Enabled:   true,
		Type:      "text",
		Text:      "LIVE",
		Position:  "top_right",
		OffsetX:   24,
		OffsetY:   24,
		FontSize:  32,
		FontColor: "red",
	}, 640, 360, 25)
	if err != nil {
		t.Skipf("drawtext unavailable in linked libavfilter (likely no freetype): %v", err)
	}
	defer w.Close()

	in := allocTestNV12Frame(t, 640, 360, 0 /*YUV420P*/, 0)
	defer in.Free()
	in.SetPts(0)
	out, err := w.Filter(in)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	defer out.Free()
	if out.Width() != 640 || out.Height() != 360 {
		t.Fatalf("watermark resized canvas: got %dx%d, want 640x360", out.Width(), out.Height())
	}
}

// TestWatermarker_CloseIsIdempotent — both pipeline.Close and the
// constructor's failure-path cleanup may invoke Close on the same
// watermarker; a panic-on-double-close would crash the subprocess.
func TestWatermarker_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	w, err := NewWatermarker(WatermarkConfig{
		Enabled: true, Type: "text", Text: "x", FontSize: 12,
	}, 320, 180, 25)
	if err != nil {
		t.Skipf("drawtext unavailable: %v", err)
	}
	w.Close()
	w.Close()
}
