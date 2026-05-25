package native

import (
	"errors"
	"fmt"
	"strings"

	"github.com/asticode/go-astiav"
)

// cuda.go is the GPU-resident leg of the pipeline: decode (h264_cuvid)
// outputs AV_PIX_FMT_CUDA frames in VRAM, scale_cuda resizes them on the
// GPU, and h264_nvenc encodes straight from VRAM — frames never touch
// system RAM. This is the path that actually unloads the CPU: the
// software sws_scale + the GPU→host readback (the cuvid-to-system-memory
// trap) are what pinned the host at 100 % even after NVDEC decode.
//
// All three stages SHARE one CUDA hwdevice (cudaContext). The decoder
// allocates its own per-source hw_frames_ctx (source WxH, rebuilt on
// switch); scale_cuda's buffersink owns the OUTPUT hw_frames_ctx (fixed
// WxH) that the encoder binds to.
//
// Requires an FFmpeg built with --enable-cuda-nvcc (scale_cuda kernels)
// and a host NVIDIA driver providing libcuda — see Dockerfile.builder.

// cudaContext wraps the per-pipeline CUDA hardware device. One per
// StreamPipeline, created when the configured decoder is a *_cuvid
// variant; shared by the decoder, every rendition's scale_cuda graph,
// and every NVENC encoder so all GPU work lands on the same device /
// context (no cross-context copies).
type cudaContext struct {
	dev *astiav.HardwareDeviceContext
}

// newCUDAContext creates a CUDA hwdevice on the default GPU. Returns an
// error when no CUDA device / driver is present so the caller can fall
// back to the CPU pipeline instead of crashing the stream.
func newCUDAContext() (*cudaContext, error) {
	t := astiav.FindHardwareDeviceTypeByName("cuda")
	if t == astiav.HardwareDeviceTypeNone {
		return nil, errors.New("native: cuda hwdevice type not in linked libavutil")
	}
	dev, err := astiav.CreateHardwareDeviceContext(t, "", nil, 0)
	if err != nil {
		return nil, fmt.Errorf("native: create cuda hwdevice: %w", err)
	}
	return &cudaContext{dev: dev}, nil
}

// Free releases the CUDA hardware device context. Nil-safe and idempotent.
func (c *cudaContext) Free() {
	if c == nil || c.dev == nil {
		return
	}
	c.dev.Free()
	c.dev = nil
}

// isCUVIDDecoder reports whether a decoder name is an NVDEC (cuvid)
// variant — the trigger for building the GPU-resident path.
func isCUVIDDecoder(name string) bool {
	return strings.HasSuffix(name, "_cuvid")
}

// pickCUDAPixelFormat is the decoder get_format callback for cuvid: lock
// the output to AV_PIX_FMT_CUDA so decoded frames stay in VRAM. Falls
// back to the first offered format if CUDA isn't on offer (shouldn't
// happen once a CUDA device is attached, but keeps decode alive instead
// of returning NONE and failing).
func pickCUDAPixelFormat(pfs []astiav.PixelFormat) astiav.PixelFormat {
	for _, pf := range pfs {
		if pf == astiav.PixelFormatCuda {
			return pf
		}
	}
	if len(pfs) > 0 {
		return pfs[0]
	}
	return astiav.PixelFormatNone
}

// gpuScaler resizes CUDA frames on the GPU via a scale_cuda filter graph
// (buffersrc[cuda] → scale_cuda=W:H:format=nv12 → buffersink). It is the
// VRAM-resident analogue of Scaler: input is an AV_PIX_FMT_CUDA frame
// from the decoder, output is an AV_PIX_FMT_CUDA NV12 frame at the
// rendition's target dimensions, ready for NVENC with no host copy.
//
// The underlying graph is rebuilt when the source frame's hw_frames_ctx
// changes (input switch → new decoder → new frames ctx), mirroring how
// Scaler rebuilds its sws context on a source-format change.
type gpuScaler struct {
	cuda   *cudaContext
	dstW   int
	dstH   int
	wm     WatermarkConfig // optional overlay folded into the same graph
	graph  *astiav.FilterGraph
	src    *astiav.BuffersrcFilterContext
	sink   *astiav.BuffersinkFilterContext
	out    *astiav.Frame
	srcW   int
	srcH   int
	closed bool
}

// newGPUScaler constructs a GPU scaler targeting dstW x dstH. The filter
// graph is allocated lazily on the first Scale call (the source
// dimensions + hw_frames_ctx aren't known until the decoder produces a
// frame).
func newGPUScaler(cuda *cudaContext, dstW, dstH int, wm WatermarkConfig) (*gpuScaler, error) {
	if dstW <= 0 || dstH <= 0 {
		return nil, fmt.Errorf("native: gpu scaler dst dims must be positive (got %dx%d)", dstW, dstH)
	}
	if cuda == nil || cuda.dev == nil {
		return nil, errors.New("native: gpu scaler requires a cuda context")
	}
	return &gpuScaler{cuda: cuda, dstW: dstW, dstH: dstH, wm: wm, out: astiav.AllocFrame()}, nil
}

// gpuFilterDesc builds the lavfi description for the GPU graph: scale_cuda
// on the GPU, plus — when a watermark is configured — a CPU round-trip
// (hwdownload → drawtext/overlay → hwupload_cuda) around it. drawtext and
// overlay are CPU-only filters, so the frame briefly leaves VRAM for the
// overlay and returns; this is still far cheaper than software decode +
// sws, which is what watermarked streams paid before. format=yuv420p
// feeds drawtext/overlay a planar layout they accept; hwupload_cuda
// re-uploads as nv12 for NVENC.
func gpuFilterDesc(dstW, dstH int, wm WatermarkConfig) (string, error) {
	scale := fmt.Sprintf("scale_cuda=%d:%d:format=nv12", dstW, dstH)
	if !wm.Enabled {
		return scale, nil
	}
	wmDesc, err := BuildWatermarkFilter(wm, dstW, dstH)
	if err != nil {
		return "", err
	}
	// hwdownload can only emit the CUDA frame's sw_format (nv12) — asking
	// it for yuv420p fails ("Invalid output format ... for hwframe
	// download"). Download as nv12 and let libavfilter auto-insert the
	// nv12→yuv420p conversion drawtext/overlay need; format=nv12 before
	// hwupload_cuda gives NVENC its native layout back.
	pre := scale + ",hwdownload,format=nv12"
	post := ",format=nv12,hwupload_cuda"
	if wm.Type == "image" {
		// The image desc is "movie=..[wm];[in][wm]overlay=..": label the
		// downloaded base [in] so the overlay's [in] pad connects, then
		// re-upload the overlay output to CUDA.
		return pre + "[in];" + wmDesc + post, nil
	}
	// drawtext (text) is a linear filter — splice it inline.
	return pre + "," + wmDesc + post, nil
}

// Scale pushes one CUDA frame through scale_cuda and returns the scaled
// CUDA frame. The returned frame is owned by the caller and must be
// Freed. The graph is (re)built when the input dimensions change.
func (g *gpuScaler) Scale(in *astiav.Frame) (*astiav.Frame, error) {
	if g.closed {
		return nil, errors.New("native: gpu scale on closed scaler")
	}
	if in == nil {
		return nil, errors.New("native: gpu scale nil frame")
	}
	if err := g.ensureGraph(in); err != nil {
		return nil, err
	}
	if err := g.src.AddFrame(in, astiav.NewBuffersrcFlags(astiav.BuffersrcFlagKeepRef)); err != nil {
		return nil, fmt.Errorf("native: gpu scale AddFrame: %w", err)
	}
	if err := g.sink.GetFrame(g.out, astiav.NewBuffersinkFlags()); err != nil {
		return nil, fmt.Errorf("native: gpu scale GetFrame: %w", err)
	}
	clone := g.out.Clone()
	g.out.Unref()
	return clone, nil
}

// ensureGraph (re)builds the scale_cuda graph when the source dims change
// (or on first use). The source frame carries its hw_frames_ctx from the
// decoder; we attach it to the buffersrc so scale_cuda runs on the GPU.
func (g *gpuScaler) ensureGraph(in *astiav.Frame) error {
	if g.graph != nil && in.Width() == g.srcW && in.Height() == g.srcH {
		return nil
	}
	g.freeGraph()

	graph := astiav.AllocFilterGraph()
	if graph == nil {
		return errors.New("native: gpu scaler AllocFilterGraph nil")
	}
	buffersrc := astiav.FindFilterByName("buffer")
	buffersink := astiav.FindFilterByName("buffersink")
	if buffersrc == nil || buffersink == nil {
		graph.Free()
		return errors.New("native: gpu scaler buffer/buffersink filter missing")
	}
	src, err := graph.NewBuffersrcFilterContext(buffersrc, "in")
	if err != nil {
		graph.Free()
		return fmt.Errorf("native: gpu scaler buffersrc ctx: %w", err)
	}
	sink, err := graph.NewBuffersinkFilterContext(buffersink, "out")
	if err != nil {
		graph.Free()
		return fmt.Errorf("native: gpu scaler buffersink ctx: %w", err)
	}

	params := astiav.AllocBuffersrcFilterContextParameters()
	defer params.Free()
	params.SetWidth(in.Width())
	params.SetHeight(in.Height())
	params.SetPixelFormat(astiav.PixelFormatCuda)
	params.SetTimeBase(astiav.NewRational(1, 1000))
	if hfc := in.HardwareFramesContext(); hfc != nil {
		params.SetHardwareFramesContext(hfc)
	}
	if err := src.SetParameters(params); err != nil {
		graph.Free()
		return fmt.Errorf("native: gpu scaler src SetParameters: %w", err)
	}
	if err := src.Initialize(nil); err != nil {
		graph.Free()
		return fmt.Errorf("native: gpu scaler src Initialize: %w", err)
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

	desc, err := gpuFilterDesc(g.dstW, g.dstH, g.wm)
	if err != nil {
		graph.Free()
		return fmt.Errorf("native: gpu scaler filter desc: %w", err)
	}
	if err := graph.Parse(desc, inputs, outputs); err != nil {
		graph.Free()
		return fmt.Errorf("native: gpu scaler parse %q: %w", desc, err)
	}
	if err := graph.Configure(); err != nil {
		graph.Free()
		return fmt.Errorf("native: gpu scaler configure: %w", err)
	}

	g.graph = graph
	g.src = src
	g.sink = sink
	g.srcW = in.Width()
	g.srcH = in.Height()
	return nil
}

func (g *gpuScaler) freeGraph() {
	if g.graph != nil {
		g.graph.Free()
		g.graph = nil
		g.src = nil
		g.sink = nil
	}
}

// Close releases the filter graph and scratch frame. Idempotent.
func (g *gpuScaler) Close() {
	if g.closed {
		return
	}
	g.closed = true
	g.freeGraph()
	if g.out != nil {
		g.out.Free()
		g.out = nil
	}
}
