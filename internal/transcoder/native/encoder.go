package native

import (
	"errors"
	"fmt"

	"github.com/asticode/go-astiav"
)

// EncoderConfig parametrises a single rendition's H.264 encoder.
//
// The encoder is constructed once per stream lifetime and survives every
// input switch (that's the whole point of the native pipeline — see
// doc.go). Width / Height / Framerate are fixed at construction; if the
// upstream source format differs, the scaler in front of the encoder
// (see scaler.go in a follow-up phase) normalises every decoded frame to
// these dimensions before it reaches Encode().
type EncoderConfig struct {
	// Codec is the libavcodec encoder name. Common values:
	//   "libx264"           CPU baseline, every platform
	//   "h264_nvenc"        NVIDIA NVENC (Linux + Windows)
	//   "h264_videotoolbox" Apple Silicon / Intel macOS
	//   "h264_vaapi"        Intel / AMD on Linux with VA-API
	//   "h264_qsv"          Intel Quick Sync
	// Empty defaults to libx264 so unit tests run anywhere.
	Codec string

	Width       int
	Height      int
	Framerate   int // frames per second; encoder time base derived as 1/Framerate
	BitrateKbps int // target bitrate; 0 = codec default (rarely useful)
	MaxBitrate  int // peak bitrate cap in kbps; 0 = no cap (encoder rate-control still applies)
	GOPSize     int // keyframe interval in FRAMES (typical = Framerate × 2 for 2s GOPs)

	// Options are passed verbatim to avcodec_open2 via an AVDictionary.
	// Use this for encoder-specific knobs that don't have a typed setter
	// (preset, tune, profile, level, cq, rc, etc.). Keys are codec-
	// specific; consult the encoder's docs.
	Options map[string]string

	// MaxBFrames controls how many consecutive B-frames the encoder may
	// emit. -1 = encoder default; 0 = no B-frames (low-latency live);
	// 2-3 = typical VOD. NVENC enables HW B-ref pyramid automatically
	// when >0.
	MaxBFrames int

	// cuda is the shared CUDA device for GPU-resident encode. Set by the
	// pipeline at runtime (not from the wire) when this is an NVENC
	// encoder fed by a cuvid decoder; nil keeps the encoder on the CPU
	// (YUV420P system-memory) input path.
	cuda *cudaContext
}

// Encoder wraps a libavcodec encoder context. Construct once via
// NewEncoder, feed normalised YUV frames via Encode, drain pending
// packets via Flush at shutdown, and Close to free libav resources.
//
// Encoder is NOT safe for concurrent use; the caller (StreamPipeline)
// serialises Encode calls on its main goroutine.
type Encoder struct {
	cfg      EncoderConfig
	codec    *astiav.Codec
	codecCtx *astiav.CodecContext
	pkt      *astiav.Packet
	frameIdx int64 // monotonically increasing PTS counter (in time-base units)
	closed   bool

	// GPU-resident input. cudaInput defers Open until the first CUDA
	// frame supplies the hw_frames_ctx NVENC binds to; pendingDict holds
	// the options dict across that deferral; opened guards the one-time
	// lazy Open.
	cudaInput   bool
	opened      bool
	pendingDict *astiav.Dictionary
}

// EncodedPacket is one output AVPacket the encoder produced, copied
// out so the caller's lifetime is independent of the encoder's scratch
// packet. PTS and DTS are in the encoder's time base (1/Framerate by
// default). Keyframe is true for IDR packets, used by the downstream
// AV-path supervisor to set domain.AVPacket.KeyFrame correctly so
// muxers and player playlist generators see proper keyframe metadata.
type EncodedPacket struct {
	Data     []byte
	PTS      int64
	DTS      int64
	Keyframe bool
}

// NewEncoder allocates and opens an H.264 encoder for the given config.
// Returns an error if the encoder name isn't compiled into the linked
// libavcodec (e.g. asking for h264_nvenc on a macOS build) or if any
// codec option is rejected at Open() time.
func NewEncoder(cfg EncoderConfig) (*Encoder, error) {
	codecName := cfg.Codec
	if codecName == "" {
		codecName = "libx264"
	}
	codec := astiav.FindEncoderByName(codecName)
	if codec == nil {
		return nil, fmt.Errorf("native: encoder %q not available in linked libavcodec", codecName)
	}
	cc := astiav.AllocCodecContext(codec)
	if cc == nil {
		return nil, fmt.Errorf("native: AllocCodecContext returned nil for %q", codecName)
	}

	// Mandatory video parameters. Pixel format is fixed at YUV420P (the
	// universal H.264 input format) so callers don't need to negotiate
	// per encoder — the scaler in front (P2) converts whatever the
	// decoder emits.
	cc.SetWidth(cfg.Width)
	cc.SetHeight(cfg.Height)
	gpuInput := cfg.cuda != nil && cfg.cuda.dev != nil && isNVENCEncoder(codecName)
	if gpuInput {
		cc.SetPixelFormat(astiav.PixelFormatCuda)
	} else {
		cc.SetPixelFormat(astiav.PixelFormatYuv420P)
	}
	cc.SetFramerate(astiav.NewRational(cfg.Framerate, 1))
	// Time-base = 1/1000 (millisecond). The pipeline feeds frames
	// stamped with their source-stream PTS (in ms) so the encoder's
	// output PTS / DTS land in the same timeline as the AAC audio
	// passthrough — without that shared origin, the player gets
	// video at t≈0 and audio at t≈ several minutes (source wallclock),
	// and they drift out of sync immediately.
	//
	// NVENC and libx264 both accept ms time-base; framerate is still
	// communicated separately via SetFramerate so rate control and
	// GOP timing stay correct.
	cc.SetTimeBase(astiav.NewRational(1, 1000))
	applyEncoderRateControl(cc, cfg)

	// Codec-specific options (preset / profile / level / tune / cq / …).
	dict, err := buildEncoderOptionsDict(cfg.Options)
	if err != nil {
		cc.Free()
		return nil, err
	}

	// GPU-resident encode: NVENC needs its input hw_frames_ctx bound
	// before Open, but that context only exists once scale_cuda has
	// produced its first frame. Defer Open to the first Encode call that
	// supplies a CUDA frame (openForCUDAFrame); hold the dict until then.
	if gpuInput {
		return &Encoder{
			cfg:         cfg,
			codec:       codec,
			codecCtx:    cc,
			pkt:         astiav.AllocPacket(),
			cudaInput:   true,
			pendingDict: dict,
		}, nil
	}

	if err := cc.Open(codec, dict); err != nil {
		if dict != nil {
			dict.Free()
		}
		cc.Free()
		return nil, fmt.Errorf("native: open encoder %q: %w", codecName, err)
	}
	if dict != nil {
		dict.Free()
	}

	return &Encoder{
		cfg:      cfg,
		codec:    codec,
		codecCtx: cc,
		pkt:      astiav.AllocPacket(),
		opened:   true,
	}, nil
}

// buildEncoderOptionsDict packs the codec-specific option map into an
// AVDictionary for avcodec_open2. Returns (nil, nil) for an empty map.
// Keeps encoder-specific knobs (preset / rc / cq / …) untyped at this
// layer so adding one doesn't force a code change here.
func buildEncoderOptionsDict(opts map[string]string) (*astiav.Dictionary, error) {
	if len(opts) == 0 {
		return nil, nil
	}
	dict := astiav.NewDictionary()
	for k, v := range opts {
		if err := dict.Set(k, v, 0); err != nil {
			dict.Free()
			return nil, fmt.Errorf("native: encoder option %s=%s: %w", k, v, err)
		}
	}
	return dict, nil
}

// applyEncoderRateControl sets the optional bitrate / peak-rate / GOP /
// B-frame knobs, each of which defaults to "leave encoder default" when
// unset (zero, or -1 for B-frames).
func applyEncoderRateControl(cc *astiav.CodecContext, cfg EncoderConfig) {
	if cfg.BitrateKbps > 0 {
		cc.SetBitRate(int64(cfg.BitrateKbps) * 1000)
	}
	if cfg.MaxBitrate > 0 {
		cc.SetRateControlMaxRate(int64(cfg.MaxBitrate) * 1000)
		// Buffer size = 2× peak is the common heuristic; without it the
		// encoder may exceed the cap on bursty content.
		cc.SetRateControlBufferSize(cfg.MaxBitrate * 1000 * 2)
	}
	if cfg.GOPSize > 0 {
		cc.SetGopSize(cfg.GOPSize)
	}
	if cfg.MaxBFrames >= 0 {
		cc.SetMaxBFrames(cfg.MaxBFrames)
	}
}

// Encode submits one YUV frame to the encoder and drains any output
// packets the encoder is ready to release.
//
// The caller is responsible for setting frame.SetPts to a strictly
// monotonically increasing value in the encoder's time base (1/Framerate
// by default). Passing nil flushes the encoder — equivalent to Flush().
//
// Returns a slice of newly-allocated byte slices, one per output packet.
// The encoder's internal packet buffer is reused, so callers must NOT
// retain references to the returned slices beyond the next Encode call.
// (Concretely: the returned slices are copies, so this is safe; the
// rule exists so a future zero-copy refactor isn't a breaking change.)
//
// Returns (nil, nil) when the encoder accepted the frame but has not yet
// produced output (typical during pipeline warmup with B-frames).
func (e *Encoder) Encode(frame *astiav.Frame) ([]EncodedPacket, error) {
	if e.closed {
		return nil, errors.New("native: encode on closed encoder")
	}
	if e.cudaInput && !e.opened {
		if frame == nil {
			// Flush before any frame ever arrived — nothing to encode.
			return nil, nil
		}
		if err := e.openForCUDAFrame(frame); err != nil {
			return nil, err
		}
	}
	if err := e.codecCtx.SendFrame(frame); err != nil {
		return nil, fmt.Errorf("native: SendFrame: %w", err)
	}
	return e.drainPackets()
}

// openForCUDAFrame performs the deferred NVENC Open, binding the encoder
// to the CUDA frame's hw_frames_ctx so it encodes straight from VRAM.
// Called once, on the first CUDA frame the scale_cuda graph produces.
func (e *Encoder) openForCUDAFrame(frame *astiav.Frame) error {
	hfc := frame.HardwareFramesContext()
	if hfc == nil {
		return errors.New("native: cuda encoder: first frame carries no hw_frames_ctx")
	}
	e.codecCtx.SetHardwareFramesContext(hfc)
	if err := e.codecCtx.Open(e.codec, e.pendingDict); err != nil {
		return fmt.Errorf("native: open cuda encoder %q: %w", e.cfg.Codec, err)
	}
	if e.pendingDict != nil {
		e.pendingDict.Free()
		e.pendingDict = nil
	}
	e.opened = true
	return nil
}

// Flush signals end-of-stream to the encoder and drains any remaining
// packets in its internal queue (B-frames waiting for a future P-frame
// reference, for example). Must be called before Close to capture the
// last frames; after Flush, the encoder is unusable.
func (e *Encoder) Flush() ([]EncodedPacket, error) {
	if e.closed {
		return nil, errors.New("native: flush on closed encoder")
	}
	return e.Encode(nil)
}

// drainPackets loops ReceivePacket until the encoder reports EAGAIN
// (needs more input) or EOF (after flush). Returns all packets it
// pulled out in order, with PTS / DTS / keyframe metadata copied off
// the scratch AVPacket before Unref.
func (e *Encoder) drainPackets() ([]EncodedPacket, error) {
	var out []EncodedPacket
	for {
		err := e.codecCtx.ReceivePacket(e.pkt)
		if err != nil {
			// EAGAIN = encoder needs more input frames; EOF = encoder
			// drained after flush. Both are normal control-flow signals,
			// not failures.
			if errors.Is(err, astiav.ErrEagain) || errors.Is(err, astiav.ErrEof) {
				return out, nil
			}
			return out, fmt.Errorf("native: ReceivePacket: %w", err)
		}
		out = append(out, EncodedPacket{
			Data:     append([]byte(nil), e.pkt.Data()...),
			PTS:      e.pkt.Pts(),
			DTS:      e.pkt.Dts(),
			Keyframe: e.pkt.Flags().Has(astiav.PacketFlagKey),
		})
		e.pkt.Unref()
	}
}

// Close releases the encoder and its packet buffer. Safe to call once;
// subsequent calls are no-ops.
func (e *Encoder) Close() {
	if e.closed {
		return
	}
	e.closed = true
	if e.pendingDict != nil {
		e.pendingDict.Free()
		e.pendingDict = nil
	}
	if e.pkt != nil {
		e.pkt.Free()
		e.pkt = nil
	}
	if e.codecCtx != nil {
		e.codecCtx.Free()
		e.codecCtx = nil
	}
}

// NextPTS allocates the next monotonic PTS for a frame in the encoder's
// time base (1 unit per frame at the configured framerate). Helper for
// pipelines that drive the encoder at a constant rate; callers with
// real wallclock-derived PTS may bypass this and set frame.SetPts
// directly.
func (e *Encoder) NextPTS() int64 {
	pts := e.frameIdx
	e.frameIdx++
	return pts
}
