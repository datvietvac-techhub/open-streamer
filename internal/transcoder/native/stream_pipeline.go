package native

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/asticode/go-astiav"
)

// PipelineConfig bundles the three constituent configs for a single
// transcoded rendition's worth of pipeline. P5 will replace this with
// a per-stream PipelineConfig carrying N rendition encoders sharing
// one decoder; P3's job is just to prove the encoder survives a
// decoder swap.
type PipelineConfig struct {
	Encoder EncoderConfig
	Scaler  ScalerConfig
	Decoder DecoderConfig
}

// OutputFrame is one elementary-stream access unit ready to leave the
// subprocess: transcoded video from the encoder, or AAC passed
// straight through from the demuxer. Codec / PTS / DTS / Keyframe let
// the supervisor build the right domain.AVPacket on the receive side
// without re-inspecting bytes.
//
// PTS / DTS are in milliseconds — converted to the publisher's
// expected time base at the pipeline edge so downstream consumers
// (tsmux.FromAV, segmenters, players) all see a uniform clock.
type OutputFrame struct {
	Data     []byte
	Codec    esFrameCodec
	PTS      int64
	DTS      int64
	Keyframe bool
}

// StreamPipeline composes decoder → scaler → encoder into the single
// data-flow the StreamRunner subprocess will execute per stream. Its
// central design invariant is: SwitchInput tears down and recreates the
// Decoder while the Scaler + Encoder stay alive. That is the property
// the FFmpeg subprocess implementation could not provide and is the
// whole reason for the migration.
//
// Not safe for concurrent use across data and control plane — the
// caller (StreamRunner in P4) drives ProcessPacket on one goroutine
// and signals SwitchInput on the same goroutine via channel selects.
// The mutex here exists only to make SwitchInput observable in tests
// and to back the assertion in production unit-tests that the encoder
// pointer is unchanged across switch.
type StreamPipeline struct {
	cfg PipelineConfig

	// Long-lived across switch.
	encoder *Encoder
	scaler  *Scaler

	// Decoder lifecycle is per-input. Recreated by SwitchInput.
	mu      sync.Mutex
	decoder *Decoder
	closed  bool

	// tsInput is lazily created on the first ProcessPacket call when
	// the bytes look like raw MPEG-TS (UDP / HLS-pull / file-TS path).
	// Annex-B inputs (RTMP / RTSP / mixer / copy) leave it nil and use
	// the direct decoder path. The detection is one-shot per pipeline
	// lifetime; SwitchInput tears the demuxer down so a fresh PSI/PMT
	// can be parsed for the new source.
	tsInput      *tsInput
	inputCtx     context.Context
	inputCancel  context.CancelFunc
	formatProbed bool

	// sawKeyframe gates the decoder until the first IDR access unit
	// arrives. Subscribing to the buffer hub mid-GOP gives us P/B
	// slices first that reference parameter sets the decoder has
	// never been initialised with — libavcodec emits "non-existing
	// PPS X referenced" warnings and then returns
	// "Invalid data found when processing input" from SendPacket,
	// which the supervisor escalates to a terminal subprocess error
	// and respawn-loops the stream. Dropping pre-IDR frames keeps
	// the decoder happy without losing recoverable input.
	//
	// Reset in SwitchInput so the new source's pre-IDR period is
	// gated too.
	sawKeyframe bool
}

// NewStreamPipeline constructs all three stages. On error any
// partially-allocated stages are torn down before returning so the
// caller does not need defensive Close calls.
func NewStreamPipeline(cfg PipelineConfig) (*StreamPipeline, error) {
	enc, err := NewEncoder(cfg.Encoder)
	if err != nil {
		return nil, fmt.Errorf("pipeline: encoder: %w", err)
	}
	sc, err := NewScaler(cfg.Scaler)
	if err != nil {
		enc.Close()
		return nil, fmt.Errorf("pipeline: scaler: %w", err)
	}
	dec, err := NewDecoder(cfg.Decoder)
	if err != nil {
		sc.Close()
		enc.Close()
		return nil, fmt.Errorf("pipeline: decoder: %w", err)
	}
	return &StreamPipeline{
		cfg:     cfg,
		encoder: enc,
		scaler:  sc,
		decoder: dec,
	}, nil
}

// ProcessPacket feeds one compressed packet from the active input into
// the decoder → scaler → encoder chain and returns the encoded packets
// ready to ship to the output buffer. Returns (nil, nil) when the
// decoder accepted the packet but produced no output yet (B-frame
// reorder buffering or pipeline warmup).
//
// PTS / DTS are forwarded to the decoder's input packet but the encoder
// derives its own monotonic PTS via NextPTS — that's the simplest way
// to keep the encoder's output timing stable across input switches
// where the source PTS bases differ.
func (p *StreamPipeline) ProcessPacket(data []byte, pts, dts int64) ([]OutputFrame, error) {
	if p.isClosed() {
		return nil, errors.New("native: process on closed pipeline")
	}

	if !p.formatProbed {
		p.formatProbed = true
		if looksLikeTS(data) {
			p.inputCtx, p.inputCancel = context.WithCancel(context.Background())
			p.tsInput = newTSInput(p.inputCtx)
		}
	}

	if p.tsInput != nil {
		if err := p.tsInput.Feed(data); err != nil {
			return nil, fmt.Errorf("pipeline: ts feed: %w", err)
		}
		video, err := p.decodeAndEncodeESFrames(p.tsInput.DrainReady())
		if err != nil {
			return video, err
		}
		audio := p.passthroughAudio(p.tsInput.DrainReadyAudio())
		return append(video, audio...), nil
	}

	if !p.sawKeyframe {
		if !isH264KeyframeAnnexB(data) {
			return nil, nil
		}
		p.sawKeyframe = true
	}

	p.mu.Lock()
	dec := p.decoder
	p.mu.Unlock()

	frames, err := dec.Decode(data, pts, dts)
	if err != nil {
		return nil, fmt.Errorf("pipeline: decode: %w", err)
	}
	return p.runFramesThroughEncoder(frames)
}

// decodeAndEncodeESFrames feeds a batch of demuxed video ES access
// units through the decoder + encoder. Used by the TS-input path
// where one gRPC chunk yields multiple frames the demuxer assembled
// from PES. Pre-IDR frames are dropped silently — see sawKeyframe.
func (p *StreamPipeline) decodeAndEncodeESFrames(frames []esFrame) ([]OutputFrame, error) {
	if len(frames) == 0 {
		return nil, nil
	}
	p.mu.Lock()
	dec := p.decoder
	p.mu.Unlock()

	var out []OutputFrame
	for _, f := range frames {
		if !p.sawKeyframe {
			if !f.keyframe {
				continue
			}
			p.sawKeyframe = true
		}
		decoded, err := dec.Decode(f.data, f.pts, f.dts)
		if err != nil {
			return out, fmt.Errorf("pipeline: decode: %w", err)
		}
		encoded, err := p.runFramesThroughEncoder(decoded)
		if err != nil {
			return out, err
		}
		out = append(out, encoded...)
	}
	return out, nil
}

// passthroughAudio wraps demuxed AAC frames as OutputFrames with PTS
// taken straight from the demuxer (millisecond-domain). No decode, no
// re-encode — the original AAC bytes ship as-is. The supervisor's AV
// write path picks them up and feeds publisher's tsmux.FromAV, which
// muxes them into the output TS alongside the transcoded video.
func (p *StreamPipeline) passthroughAudio(frames []esFrame) []OutputFrame {
	if len(frames) == 0 {
		return nil
	}
	out := make([]OutputFrame, 0, len(frames))
	for _, f := range frames {
		out = append(out, OutputFrame{
			Data:  f.data,
			Codec: f.codec,
			PTS:   f.pts,
			DTS:   f.dts,
		})
	}
	return out
}

// isH264KeyframeAnnexB reports whether data contains an H.264 IDR
// NAL unit (type 5) — the AV path's equivalent of esFrame.keyframe.
// The buffer hub guarantees IDR access units carry SPS / PPS inline
// (see RTMPMsgConverter.ensureKeyFrameHasParamSets), so an IDR-bearing
// chunk is self-sufficient init for the decoder.
//
// Walks Annex-B start codes (00 00 01 or 00 00 00 01); stops at the
// first NAL whose type is 5. Returns true even if other NAL types
// (SPS/PPS/SEI) sit ahead of the IDR in the same access unit.
func isH264KeyframeAnnexB(data []byte) bool {
	for i := 0; i+3 < len(data); {
		if data[i] != 0 || data[i+1] != 0 {
			i++
			continue
		}
		startLen := 0
		switch {
		case data[i+2] == 1:
			startLen = 3
		case data[i+2] == 0 && data[i+3] == 1:
			startLen = 4
		default:
			i++
			continue
		}
		naluStart := i + startLen
		if naluStart >= len(data) {
			return false
		}
		if data[naluStart]&0x1F == 5 {
			return true
		}
		i = naluStart + 1
	}
	return false
}

// SwitchInput swaps the decoder for one matching newCfg. Before the
// swap, the OLD decoder is flushed so any in-flight reordered frames
// still reach the encoder; those flushed frames are encoded under the
// CURRENT encoder PTS sequence so output timing stays monotonic.
//
// The encoder + scaler are NEVER touched here — that's the property
// the whole migration is about. Tests assert encoder pointer identity
// before / after to lock the invariant in.
//
// The TS demuxer (if any) is also torn down so the next ProcessPacket
// re-probes and parses fresh PSI / PMT from the new source.
func (p *StreamPipeline) SwitchInput(newCfg DecoderConfig) ([]OutputFrame, error) {
	if p.isClosed() {
		return nil, errors.New("native: switch on closed pipeline")
	}

	p.mu.Lock()
	oldDec := p.decoder
	p.mu.Unlock()

	// Drain anything the old decoder still has in its B-frame queue
	// so the cutover is clean in encoder PTS space.
	flushFrames, err := oldDec.Flush()
	if err != nil {
		return nil, fmt.Errorf("pipeline: flush old decoder: %w", err)
	}
	out, err := p.runFramesThroughEncoder(flushFrames)
	if err != nil {
		return nil, err
	}

	oldDec.Close()

	if p.tsInput != nil {
		p.inputCancel()
		p.tsInput.Close()
		p.tsInput = nil
		p.inputCtx = nil
		p.inputCancel = nil
		p.formatProbed = false
	}
	p.sawKeyframe = false

	newDec, err := NewDecoder(newCfg)
	if err != nil {
		return out, fmt.Errorf("pipeline: alloc new decoder: %w", err)
	}

	p.mu.Lock()
	p.decoder = newDec
	p.cfg.Decoder = newCfg
	p.mu.Unlock()
	return out, nil
}

// Flush drains the full chain (decoder → encoder) on stream stop.
// After Flush the pipeline must be Closed; further Process / Switch
// calls will fail.
//
// Audio passthrough is drained too so AAC frames still queued in the
// TS demuxer get emitted before shutdown — without that the publisher
// would close out a partial last segment that the player can't seek
// past.
func (p *StreamPipeline) Flush() ([]OutputFrame, error) {
	if p.isClosed() {
		return nil, errors.New("native: flush on closed pipeline")
	}

	// Drain any ES frames the TS demuxer assembled but the pipeline
	// hasn't decoded yet (the post-Feed DrainReady call only picks up
	// what was ready at that instant; trailing PES fragments land
	// after the chunk that completed them).
	var head []OutputFrame
	if p.tsInput != nil {
		readyV, err := p.decodeAndEncodeESFrames(p.tsInput.DrainReady())
		head = readyV
		if err != nil {
			return head, err
		}
		head = append(head, p.passthroughAudio(p.tsInput.DrainReadyAudio())...)
	}

	p.mu.Lock()
	dec := p.decoder
	p.mu.Unlock()

	flushFrames, err := dec.Flush()
	if err != nil {
		return head, fmt.Errorf("pipeline: flush decoder: %w", err)
	}
	out, err := p.runFramesThroughEncoder(flushFrames)
	if err != nil {
		return append(head, out...), err
	}
	tailPkts, err := p.encoder.Flush()
	if err != nil {
		return append(head, out...), fmt.Errorf("pipeline: flush encoder: %w", err)
	}
	codec := encoderCodecToES(p.cfg.Encoder.Codec)
	tail := make([]OutputFrame, 0, len(tailPkts))
	for _, pkt := range tailPkts {
		tail = append(tail, OutputFrame{
			Data:     pkt.Data,
			Codec:    codec,
			PTS:      pkt.PTS,
			DTS:      pkt.DTS,
			Keyframe: pkt.Keyframe,
		})
	}
	return append(append(head, out...), tail...), nil
}

// Close releases all three stages. Idempotent.
func (p *StreamPipeline) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	dec := p.decoder
	p.decoder = nil
	p.mu.Unlock()

	if p.tsInput != nil {
		p.inputCancel()
		p.tsInput.Close()
		p.tsInput = nil
	}
	if dec != nil {
		dec.Close()
	}
	if p.scaler != nil {
		p.scaler.Close()
	}
	if p.encoder != nil {
		p.encoder.Close()
	}
}

// EncoderPTS returns the next PTS counter value WITHOUT consuming it.
// Test-only inspection used to assert PTS continuity across SwitchInput
// and to verify the encoder was not reallocated.
//
// Production code derives PTS implicitly via Encoder.NextPTS inside
// runFramesThroughEncoder; callers don't need this hook for normal
// operation.
func (p *StreamPipeline) EncoderPTS() int64 {
	if p.isClosed() {
		return -1
	}
	return p.encoder.frameIdx
}

// runFramesThroughEncoder scales + encodes every frame in the slice
// and returns the resulting OutputFrames. Frees each input frame
// after use — the caller's decoder yielded them as caller-owned and
// the lifetime ends right here.
//
// Encoder PTS / DTS arrive in the encoder's time base (1/Framerate);
// we convert to milliseconds at the edge so downstream consumers see
// a uniform clock.
func (p *StreamPipeline) runFramesThroughEncoder(frames []*astiav.Frame) ([]OutputFrame, error) {
	var out []OutputFrame
	for i, f := range frames {
		pkts, err := p.encodeOne(f)
		f.Free()
		if err != nil {
			for _, leftover := range frames[i+1:] {
				leftover.Free()
			}
			return out, err
		}
		out = append(out, pkts...)
	}
	return out, nil
}

// encodeOne scales then encodes a single frame, wrapping each emitted
// EncodedPacket as a video OutputFrame in millisecond time base.
//
// PTS source: the decoded frame inherits PTS from the input packet
// (set by dec.Decode(data, pts, dts)). Forwarding it to the encoder
// keeps the output video on the SAME timeline as the AAC passthrough
// — both expressed in source-stream milliseconds. Without this the
// encoder would emit PTS starting at zero while audio still rides
// the source wallclock, and the player gets A/V drift the moment a
// segment lands.
//
// NextPTS fallback covers the Annex-B AV-path (RTMP / RTSP) which
// today passes pts=0 through ProcessPacket; in that mode there's no
// audio so monotonic frame indices are still self-consistent.
func (p *StreamPipeline) encodeOne(f *astiav.Frame) ([]OutputFrame, error) {
	scaled, err := p.scaler.Scale(f)
	if err != nil {
		return nil, fmt.Errorf("pipeline: scale: %w", err)
	}
	pts := f.Pts()
	if pts <= 0 {
		pts = p.encoder.NextPTS()
	}
	scaled.SetPts(pts)
	pkts, err := p.encoder.Encode(scaled)
	scaled.Free()
	if err != nil {
		return nil, fmt.Errorf("pipeline: encode: %w", err)
	}
	if len(pkts) == 0 {
		return nil, nil
	}
	codec := encoderCodecToES(p.cfg.Encoder.Codec)
	out := make([]OutputFrame, 0, len(pkts))
	for _, pkt := range pkts {
		out = append(out, OutputFrame{
			Data:     pkt.Data,
			Codec:    codec,
			PTS:      pkt.PTS,
			DTS:      pkt.DTS,
			Keyframe: pkt.Keyframe,
		})
	}
	return out, nil
}

// encoderCodecToES maps an EncoderConfig.Codec libavcodec name to the
// pipeline's local codec enum. Defaults to H.264 because libx264 /
// h264_* are the only encoders configurable today; H.265 / AV1 land
// when their encoders are wired in a future phase.
func encoderCodecToES(name string) esFrameCodec {
	switch name {
	case "hevc", "libx265", "hevc_nvenc", "hevc_videotoolbox", "hevc_vaapi", "hevc_qsv":
		return esCodecH265
	default:
		return esCodecH264
	}
}

func (p *StreamPipeline) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}
