package native

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/asticode/go-astiav"
)

// WatermarkConfig parametrises an overlay applied to every decoded
// frame after the scaler and before the encoder. The same struct
// carries both text and image overlays — Type selects which.
//
// Position resolves a human label (top_left / top_right / ...) into
// concrete x/y expressions in the lavfi grammar that drawtext / overlay
// accept. OffsetX / OffsetY shift the anchor inward from the chosen
// corner, mirroring the operator-facing config in the REST API.
type WatermarkConfig struct {
	Enabled bool
	Type    string // "text" or "image"

	Text      string
	AssetPath string

	Position string // top_left | top_right | bottom_left | bottom_right | center
	OffsetX  int
	OffsetY  int
	Opacity  float64

	FontSize  int
	FontColor string
	FontFile  string
}

// defaultFontFile is the typical DejaVu install path on the target
// hosts. Falling back to it when FontFile is empty lets text
// watermarks work out-of-the-box without forcing operators to know
// where the system fonts live.
const defaultFontFile = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

// Watermarker wraps a libavfilter graph that overlays a single
// drawtext / image filter onto every decoded frame the pipeline
// pushes through. One Watermarker per rendition — different
// renditions resolve different x/y because the input frame size
// changes, so they need their own filter graph instance.
//
// Not safe for concurrent use; the pipeline serialises Filter calls
// on its main goroutine.
type Watermarker struct {
	cfg    WatermarkConfig
	graph  *astiav.FilterGraph
	src    *astiav.BuffersrcFilterContext
	sink   *astiav.BuffersinkFilterContext
	out    *astiav.Frame
	closed bool
}

// NewWatermarker builds a filter graph for the given watermark over a
// canvas of width x height pixels at the given framerate. Returns
// (nil, nil) when cfg.Enabled is false so callers can unconditionally
// invoke it and skip the filter slot when no watermark is configured.
func NewWatermarker(cfg WatermarkConfig, width, height, framerate int) (*Watermarker, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if width <= 0 || height <= 0 {
		return nil, fmt.Errorf("native: watermark: invalid canvas %dx%d", width, height)
	}
	if framerate <= 0 {
		framerate = 25
	}

	graph := astiav.AllocFilterGraph()
	if graph == nil {
		return nil, errors.New("native: watermark: AllocFilterGraph returned nil")
	}

	buffersrc := astiav.FindFilterByName("buffer")
	buffersink := astiav.FindFilterByName("buffersink")
	if buffersrc == nil || buffersink == nil {
		graph.Free()
		return nil, errors.New("native: watermark: buffer/buffersink filter missing from linked libavfilter")
	}

	src, err := graph.NewBuffersrcFilterContext(buffersrc, "in")
	if err != nil {
		graph.Free()
		return nil, fmt.Errorf("native: watermark: buffersrc ctx: %w", err)
	}
	sink, err := graph.NewBuffersinkFilterContext(buffersink, "out")
	if err != nil {
		graph.Free()
		return nil, fmt.Errorf("native: watermark: buffersink ctx: %w", err)
	}

	params := astiav.AllocBuffersrcFilterContextParameters()
	defer params.Free()
	params.SetWidth(width)
	params.SetHeight(height)
	params.SetPixelFormat(astiav.PixelFormatYuv420P)
	params.SetTimeBase(astiav.NewRational(1, 1000)) // ms — matches encoder time base
	params.SetSampleAspectRatio(astiav.NewRational(1, 1))
	if err := src.SetParameters(params); err != nil {
		graph.Free()
		return nil, fmt.Errorf("native: watermark: src SetParameters: %w", err)
	}
	if err := src.Initialize(nil); err != nil {
		graph.Free()
		return nil, fmt.Errorf("native: watermark: src Initialize: %w", err)
	}

	filterDesc, err := BuildWatermarkFilter(cfg, width, height)
	if err != nil {
		graph.Free()
		return nil, fmt.Errorf("native: watermark: build filter: %w", err)
	}

	inputs := astiav.AllocFilterInOut()
	defer inputs.Free()
	outputs := astiav.AllocFilterInOut()
	defer outputs.Free()

	outputs.SetName("in")
	outputs.SetFilterContext(src.FilterContext())
	outputs.SetPadIdx(0)
	outputs.SetNext(nil)

	inputs.SetName("out")
	inputs.SetFilterContext(sink.FilterContext())
	inputs.SetPadIdx(0)
	inputs.SetNext(nil)

	if err := graph.Parse(filterDesc, inputs, outputs); err != nil {
		graph.Free()
		return nil, fmt.Errorf("native: watermark: parse %q: %w", filterDesc, err)
	}
	if err := graph.Configure(); err != nil {
		graph.Free()
		return nil, fmt.Errorf("native: watermark: configure: %w", err)
	}

	return &Watermarker{
		cfg:   cfg,
		graph: graph,
		src:   src,
		sink:  sink,
		out:   astiav.AllocFrame(),
	}, nil
}

// Filter pushes one frame through the graph and returns the filtered
// frame. The returned frame is caller-owned; Free it when done.
//
// The graph runs synchronously — every input frame produces exactly
// one output frame for our single-output overlays. AddFrame uses
// KeepRef so the caller's frame isn't consumed, letting the pipeline
// pass the same scaled frame into N watermarkers if needed (it
// doesn't today, but the contract leaves room).
func (w *Watermarker) Filter(in *astiav.Frame) (*astiav.Frame, error) {
	if w.closed {
		return nil, errors.New("native: watermark: filter on closed watermarker")
	}
	if err := w.src.AddFrame(in, astiav.NewBuffersrcFlags(astiav.BuffersrcFlagKeepRef)); err != nil {
		return nil, fmt.Errorf("native: watermark: AddFrame: %w", err)
	}
	if err := w.sink.GetFrame(w.out, astiav.NewBuffersinkFlags()); err != nil {
		return nil, fmt.Errorf("native: watermark: GetFrame: %w", err)
	}
	clone := w.out.Clone()
	w.out.Unref()
	return clone, nil
}

// Close releases the filter graph and scratch frame. Idempotent.
func (w *Watermarker) Close() {
	if w.closed {
		return
	}
	w.closed = true
	if w.out != nil {
		w.out.Free()
		w.out = nil
	}
	if w.graph != nil {
		w.graph.Free()
		w.graph = nil
	}
}

// BuildWatermarkFilter renders the lavfi filter description for the
// given config + canvas. Exported for tests; the production path
// calls it indirectly via NewWatermarker.
//
// Text overlay uses drawtext with all parameters quoted so colons,
// commas, and quotes in user text don't escape into the filter
// grammar. Image overlay uses movie= as a second source (no
// extra -i flag needed) feeding into overlay.
func BuildWatermarkFilter(cfg WatermarkConfig, canvasW, canvasH int) (string, error) {
	switch cfg.Type {
	case "text":
		return buildTextFilter(cfg, canvasW, canvasH), nil
	case "image":
		return buildImageFilter(cfg, canvasW, canvasH)
	default:
		return "", fmt.Errorf("unknown watermark type %q (want text|image)", cfg.Type)
	}
}

func buildTextFilter(cfg WatermarkConfig, canvasW, canvasH int) string {
	fontFile := cfg.FontFile
	if fontFile == "" {
		fontFile = defaultFontFile
	}
	fontSize := cfg.FontSize
	if fontSize <= 0 {
		fontSize = 24
	}
	fontColor := cfg.FontColor
	if fontColor == "" {
		fontColor = "white"
	}
	if cfg.Opacity > 0 && cfg.Opacity < 1 {
		fontColor = fmt.Sprintf("%s@%g", fontColor, cfg.Opacity)
	}

	x, y := resolveTextPosition(cfg.Position, cfg.OffsetX, cfg.OffsetY)

	// Escape single quotes in the user-supplied text per lavfi rules
	// (single quote inside a quoted string is written as '\'').
	text := strings.ReplaceAll(cfg.Text, "'", `'\''`)

	parts := []string{
		"text='" + text + "'",
		"fontfile='" + escapeLavfiArg(fontFile) + "'",
		"fontsize=" + strconv.Itoa(fontSize),
		"fontcolor=" + fontColor,
		"x=" + x,
		"y=" + y,
	}
	return "drawtext=" + strings.Join(parts, ":")
}

func buildImageFilter(cfg WatermarkConfig, canvasW, canvasH int) (string, error) {
	if cfg.AssetPath == "" {
		return "", errors.New("image watermark requires asset_path")
	}
	x, y := resolveOverlayPosition(cfg.Position, cfg.OffsetX, cfg.OffsetY)
	movieSrc := "movie='" + escapeLavfiArg(cfg.AssetPath) + "':loop=0,setpts=N/(FRAME_RATE*TB)"
	if cfg.Opacity > 0 && cfg.Opacity < 1 {
		movieSrc += fmt.Sprintf(",format=rgba,colorchannelmixer=aa=%g", cfg.Opacity)
	}
	return fmt.Sprintf("%s[wm];[in][wm]overlay=%s:%s", movieSrc, x, y), nil
}

// resolveTextPosition maps a position keyword to the (x, y)
// expression pair drawtext understands. The expressions reference
// `text_w` / `text_h` (drawtext's own dims) and `w` / `h` (canvas
// dims) so the placement adapts as the font renders different
// strings.
func resolveTextPosition(pos string, offX, offY int) (string, string) {
	ox := strconv.Itoa(offX)
	oy := strconv.Itoa(offY)
	switch pos {
	case "top_right":
		return "w-text_w-" + ox, oy
	case "bottom_left":
		return ox, "h-text_h-" + oy
	case "bottom_right":
		return "w-text_w-" + ox, "h-text_h-" + oy
	case "center":
		return "(w-text_w)/2+" + ox, "(h-text_h)/2+" + oy
	default: // top_left
		return ox, oy
	}
}

// resolveOverlayPosition is the image equivalent — overlay uses
// `overlay_w` / `overlay_h` (the secondary input's dims) and W / H
// (canvas dims). Image positions reference the canvas size in
// uppercase per lavfi convention.
func resolveOverlayPosition(pos string, offX, offY int) (string, string) {
	ox := strconv.Itoa(offX)
	oy := strconv.Itoa(offY)
	switch pos {
	case "top_right":
		return "W-w-" + ox, oy
	case "bottom_left":
		return ox, "H-h-" + oy
	case "bottom_right":
		return "W-w-" + ox, "H-h-" + oy
	case "center":
		return "(W-w)/2+" + ox, "(H-h)/2+" + oy
	default: // top_left
		return ox, oy
	}
}

// escapeLavfiArg quotes a value safe to embed inside single quotes
// in a lavfi filter description. lavfi's quoting rules treat
// backslash and colon as special; full backslash-escaping is enough
// for paths / fonts which never contain single quotes in practice.
func escapeLavfiArg(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `:`, `\:`)
	return s
}
