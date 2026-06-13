package native

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/asticode/go-astiav"
)

// DecoderConfig parametrises a video decoder.
//
// One Decoder per ACTIVE input — recreated when the input switches and
// the new source advertises different codec params (different resolution,
// SAR, profile). The encoder downstream is NOT recreated; that's the
// migration's central design choice.
type DecoderConfig struct {
	// Codec is the libavcodec decoder name. Common values:
	//   "h264"            CPU H.264 (universal fallback)
	//   "h264_cuvid"      NVIDIA NVDEC for H.264
	//   "hevc"            CPU H.265
	//   "hevc_cuvid"      NVIDIA NVDEC for H.265
	// Empty defaults to "h264" so unit tests work without HW.
	Codec string

	// ExtraData is the AVCC / Annex-B initialisation blob the codec needs
	// before it can decode the first packet (SPS / PPS, or sequence
	// header for HEVC). Pass nil when the bitstream is Annex-B with
	// in-band SPS/PPS (libx264 / NVENC default), which is the common
	// case from our P1 encoder.
	ExtraData []byte

	// cuda is the shared CUDA device for GPU-resident decode. Set by the
	// pipeline at runtime (not from the wire) when Codec is a *_cuvid
	// variant; nil keeps the decoder on the CPU / system-memory path.
	// Unexported so the proto adapter can't accidentally populate it.
	cuda *cudaContext
}

// Decoder wraps a libavcodec decoder context. Construct once per active
// input via NewDecoder, feed encoded packets via Decode, drain leftover
// frames via Flush before closing, and Close to release libav state.
//
// Not safe for concurrent use; serialise via the owning goroutine.
type Decoder struct {
	cfg      DecoderConfig
	codec    *astiav.Codec
	codecCtx *astiav.CodecContext
	pkt      *astiav.Packet
	frame    *astiav.Frame // scratch frame; cloned out before yielding to caller
	closed   bool
}

// NewDecoder allocates and opens a video decoder for the given config.
// Returns an error if the codec name is unknown to the linked libavcodec.
func NewDecoder(cfg DecoderConfig) (*Decoder, error) {
	name := cfg.Codec
	if name == "" {
		name = "h264"
	}
	codec := astiav.FindDecoderByName(name)
	if codec == nil {
		return nil, fmt.Errorf("native: decoder %q not available in linked libavcodec", name)
	}
	cc := astiav.AllocCodecContext(codec)
	if cc == nil {
		return nil, fmt.Errorf("native: AllocCodecContext returned nil for %q", name)
	}

	// GPU-resident decode: attach the shared CUDA device so an NVDEC
	// (cuvid) decoder emits AV_PIX_FMT_CUDA frames in VRAM instead of
	// copying NV12 back to host RAM (the readback that kept the CPU
	// pinned even after NVDEC). The pixel-format callback locks the
	// output to CUDA so libavcodec doesn't negotiate a system-memory
	// format. CPU decoders leave cfg.cuda nil and decode to RAM.
	if cfg.cuda != nil && cfg.cuda.dev != nil && isCUVIDDecoder(name) {
		cc.SetHardwareDeviceContext(cfg.cuda.dev)
		cc.SetPixelFormatCallback(pickCUDAPixelFormat)
		// The decoder auto-allocates its own CUDA frames pool sized to the
		// SOURCE resolution. It's ephemeral — rebuilt on every input switch
		// so a source of any resolution decodes cleanly (pinning the pool to
		// the first source's dims breaks a switch to a differently-sized
		// input). Seamlessness across the switch comes from the LONG-LIVED
		// encoder, not a shared pool: scale_cuda always outputs the fixed
		// rendition dims, and NVENC re-registers each CUDA frame by device
		// pointer, so the encoder survives the decoder + scale-graph rebuild.
	}

	// ExtraData (codec_par.extradata equivalent) is mandatory for some
	// streams where SPS/PPS live out-of-band — set BEFORE Open so the
	// decoder picks it up at init. Annex-B sources (our encoder, most
	// CDN HLS) leave it nil and rely on in-band parsing.
	if len(cfg.ExtraData) > 0 {
		if err := cc.SetExtraData(cfg.ExtraData); err != nil {
			cc.Free()
			return nil, fmt.Errorf("native: decoder SetExtraData: %w", err)
		}
	}

	if err := cc.Open(codec, nil); err != nil {
		cc.Free()
		return nil, fmt.Errorf("native: open decoder %q: %w", name, err)
	}

	return &Decoder{
		cfg:      cfg,
		codec:    codec,
		codecCtx: cc,
		pkt:      astiav.AllocPacket(),
		frame:    astiav.AllocFrame(),
	}, nil
}

// newDecoderWithFallback builds a decoder, degrading a GPU (cuvid)
// decoder to its CPU equivalent if the hardware path fails to open
// (missing / mismatched driver, NVDEC unavailable). Keeps the stream
// alive on CPU instead of escalating to a terminal subprocess error and
// respawn-looping when GPU decode can't initialise.
func newDecoderWithFallback(cfg DecoderConfig) (*Decoder, error) {
	dec, err := NewDecoder(cfg)
	if err == nil {
		return dec, nil
	}
	cpu := cpuDecoderFallback(cfg.Codec)
	if cpu == cfg.Codec {
		return nil, err // already a CPU decoder; nothing to fall back to
	}
	slog.Warn("native: GPU decoder unavailable, falling back to CPU",
		"requested", cfg.Codec, "fallback", cpu, "err", err)
	cfg.Codec = cpu
	return NewDecoder(cfg)
}

// cpuDecoderFallback maps a GPU decoder name to its CPU equivalent, or
// returns the name unchanged when it is already a CPU decoder.
func cpuDecoderFallback(name string) string {
	switch name {
	case "h264_cuvid":
		return "h264"
	case "hevc_cuvid":
		return "hevc"
	default:
		return name
	}
}

// decoderCodecFamily maps a libavcodec decoder name to the elementary-
// stream codec it decodes. The cuvid (NVDEC) and CPU variants of the same
// codec share a family. Used to detect when an incoming video frame's
// codec no longer matches the active decoder so the pipeline can rebuild
// it instead of crash-looping on AVERROR_INVALIDDATA (A-3). An empty name
// defaults to H.264 (NewDecoder's default). Unknown names map to
// esCodecUnknown so a mismatch is reported (forcing a rebuild) rather than
// silently treated as matching.
func decoderCodecFamily(name string) esFrameCodec {
	switch name {
	case "hevc", "hevc_cuvid":
		return esCodecH265
	case "h264", "h264_cuvid", "":
		return esCodecH264
	default:
		return esCodecUnknown
	}
}

// videoDecoderNameForCodec resolves the libavcodec decoder name for an
// incoming elementary-stream codec, preserving the GPU (cuvid) vs CPU lane
// of the currently-active decoder. A raw-TS or AV source whose video is
// HEVC must decode with hevc / hevc_cuvid; feeding H.265 NAL units to an
// H.264 decoder returns AVERROR_INVALIDDATA on every packet (A-3).
// newDecoderWithFallback degrades the cuvid choice to CPU if NVDEC can't
// open it on this host.
func videoDecoderNameForCodec(current string, codec esFrameCodec) string {
	gpu := isCUVIDDecoder(current)
	switch codec { //nolint:exhaustive // default covers esCodecH264 + non-video codecs routed defensively as H.264
	case esCodecH265:
		if gpu {
			return "hevc_cuvid"
		}
		return "hevc"
	default: // esCodecH264 (and, defensively, anything else routed as video)
		if gpu {
			return "h264_cuvid"
		}
		return "h264"
	}
}

// Decode submits one encoded packet and drains any frames the decoder
// is ready to release. Returns CLONED frames — the caller takes
// ownership and MUST call Free on each one to avoid leaking libav
// buffers.
//
// data carries the compressed bytes (Annex-B NAL units for H.264, or
// the AVCC sample for stored MP4). pts / dts are in the caller's time
// base; pass 0 when the source is raw bitstream without timing.
//
// An empty return slice with nil error means the decoder accepted the
// packet but has not yet emitted a frame — normal during pipeline
// warmup or when the decoder is buffering B-frame reorder.
//
// Passing nil data flushes the decoder. Use Flush() for clarity.
func (d *Decoder) Decode(data []byte, pts, dts int64) ([]*astiav.Frame, error) {
	if d.closed {
		return nil, errors.New("native: decode on closed decoder")
	}

	if data == nil {
		// Flush mode: pass nil packet to SendPacket.
		if err := d.codecCtx.SendPacket(nil); err != nil {
			return nil, fmt.Errorf("native: SendPacket(nil): %w", err)
		}
	} else {
		// Bind data into the scratch packet without copying — the
		// underlying CodecContext refs the buffer until ReceiveFrame
		// returns. Subsequent FromData / Unref calls release.
		if err := d.pkt.FromData(data); err != nil {
			return nil, fmt.Errorf("native: packet FromData: %w", err)
		}
		d.pkt.SetPts(pts)
		d.pkt.SetDts(dts)
		if err := d.codecCtx.SendPacket(d.pkt); err != nil {
			d.pkt.Unref()
			return nil, fmt.Errorf("native: SendPacket: %w", err)
		}
		d.pkt.Unref()
	}

	return d.drainFrames()
}

// Flush signals end-of-stream so the decoder emits any reordered
// frames it was holding for B-frame back-references. After Flush the
// decoder is unusable; Close to release.
func (d *Decoder) Flush() ([]*astiav.Frame, error) {
	if d.closed {
		return nil, errors.New("native: flush on closed decoder")
	}
	return d.Decode(nil, 0, 0)
}

// drainFrames loops ReceiveFrame until EAGAIN (decoder needs more
// input) or EOF (after flush), cloning each yielded frame so the
// caller's lifetime is independent of the scratch slot.
func (d *Decoder) drainFrames() ([]*astiav.Frame, error) {
	var out []*astiav.Frame
	for {
		err := d.codecCtx.ReceiveFrame(d.frame)
		if err != nil {
			if errors.Is(err, astiav.ErrEagain) || errors.Is(err, astiav.ErrEof) {
				return out, nil
			}
			return out, fmt.Errorf("native: ReceiveFrame: %w", err)
		}
		// Clone copies the AVFrame metadata + Refs the underlying
		// buffer — fast (no pixel copy) and gives the caller a frame
		// whose Free is independent of our scratch.frame.Unref below.
		clone := d.frame.Clone()
		out = append(out, clone)
		d.frame.Unref()
	}
}

// CodecContext exposes the underlying libavcodec context for callers
// that need to read decoded params (width / height / pix_fmt) once the
// first frame has been produced. Returns nil after Close.
func (d *Decoder) CodecContext() *astiav.CodecContext {
	if d.closed {
		return nil
	}
	return d.codecCtx
}

// Close releases the decoder, scratch packet, and scratch frame. Safe
// to call once; subsequent calls are no-ops.
func (d *Decoder) Close() {
	if d.closed {
		return
	}
	d.closed = true
	if d.frame != nil {
		d.frame.Free()
		d.frame = nil
	}
	if d.pkt != nil {
		d.pkt.Free()
		d.pkt = nil
	}
	if d.codecCtx != nil {
		d.codecCtx.Free()
		d.codecCtx = nil
	}
}
