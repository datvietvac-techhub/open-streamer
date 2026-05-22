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
	cc.SetPixelFormat(astiav.PixelFormatYuv420P)
	cc.SetFramerate(astiav.NewRational(cfg.Framerate, 1))
	// Time-base is the unit each PTS / DTS is expressed in. 1/Framerate
	// gives integer-per-frame timestamps that NVENC and libx264 both
	// accept without warnings about "timestamps are inconsistent".
	cc.SetTimeBase(astiav.NewRational(1, cfg.Framerate))
	if cfg.BitrateKbps > 0 {
		cc.SetBitRate(int64(cfg.BitrateKbps) * 1000)
	}
	if cfg.MaxBitrate > 0 {
		cc.SetRateControlMaxRate(int64(cfg.MaxBitrate) * 1000)
		// Buffer size = 2× peak is the common heuristic; without it the
		// encoder may exceed the cap on bursty content while still
		// reporting compliance.
		cc.SetRateControlBufferSize(cfg.MaxBitrate * 1000 * 2)
	}
	if cfg.GOPSize > 0 {
		cc.SetGopSize(cfg.GOPSize)
	}
	if cfg.MaxBFrames >= 0 {
		cc.SetMaxBFrames(cfg.MaxBFrames)
	}

	// Codec-specific options (preset / profile / level / tune / cq / …).
	// Bundle through avcodec_open2's dictionary so encoder-specific knobs
	// stay untyped at this layer — adding NVENC's "rc=vbr" doesn't force
	// a code change here.
	var dict *astiav.Dictionary
	if len(cfg.Options) > 0 {
		dict = astiav.NewDictionary()
		for k, v := range cfg.Options {
			if err := dict.Set(k, v, 0); err != nil {
				dict.Free()
				cc.Free()
				return nil, fmt.Errorf("native: encoder option %s=%s: %w", k, v, err)
			}
		}
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
	}, nil
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
func (e *Encoder) Encode(frame *astiav.Frame) ([][]byte, error) {
	if e.closed {
		return nil, errors.New("native: encode on closed encoder")
	}
	if err := e.codecCtx.SendFrame(frame); err != nil {
		return nil, fmt.Errorf("native: SendFrame: %w", err)
	}
	return e.drainPackets()
}

// Flush signals end-of-stream to the encoder and drains any remaining
// packets in its internal queue (B-frames waiting for a future P-frame
// reference, for example). Must be called before Close to capture the
// last frames; after Flush, the encoder is unusable.
func (e *Encoder) Flush() ([][]byte, error) {
	if e.closed {
		return nil, errors.New("native: flush on closed encoder")
	}
	return e.Encode(nil)
}

// drainPackets loops ReceivePacket until the encoder reports EAGAIN
// (needs more input) or EOF (after flush). Returns all packets it
// pulled out in order.
func (e *Encoder) drainPackets() ([][]byte, error) {
	var out [][]byte
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
		// Copy the packet data so the caller can outlive the next
		// ReceivePacket / Unref cycle. Future optimisation: return a
		// reusable *Packet with explicit Release, but the copy is
		// negligible compared to encode cost and simplifies the API.
		data := append([]byte(nil), e.pkt.Data()...)
		out = append(out, data)
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
