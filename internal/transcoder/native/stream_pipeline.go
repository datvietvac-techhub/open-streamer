package native

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/asticode/go-astiav"
)

// PipelineConfig describes one stream's full transcode topology: a
// single decoder feeding N rendition pipelines (scaler + encoder)
// that share its decoded frames. One decoder is the right choice
// because decoding is the expensive half — running it once for N
// renditions saves ~N× the GPU memory and CPU cost vs spinning a
// decoder per rendition.
type PipelineConfig struct {
	Decoder    DecoderConfig
	Renditions []RenditionConfig
}

// RenditionConfig groups the scaler + encoder + optional watermark for
// one output rendition (e.g., 1080p@3500k, 720p@1600k, 480p@800k).
// The pipeline runs N of these from one decoded frame so the buffer
// hub gets one output per rendition target.
//
// Watermark is applied AFTER scale so the overlay position is in the
// rendition's pixel space — a top_right offset of 16 px stays
// readable on 480p without disappearing off-canvas, which it would
// if we baked the overlay before scaling.
type RenditionConfig struct {
	Encoder   EncoderConfig
	Scaler    ScalerConfig
	Watermark WatermarkConfig
}

// BroadcastTargetIndex marks an OutputFrame as "write to every
// rendition buffer", not just one. The supervisor's writeOutputPacket
// path fans audio passthrough across every target so each variant
// segment contains both the rendition's video AND the shared audio.
const BroadcastTargetIndex int32 = -1

// OutputFrame is one elementary-stream access unit ready to leave the
// subprocess: transcoded video from one rendition encoder, or AAC
// passed straight through from the demuxer. Codec / PTS / DTS /
// Keyframe let the supervisor build the right domain.AVPacket on the
// receive side without re-inspecting bytes.
//
// TargetIndex routes the frame to a specific rendition buffer; the
// sentinel BroadcastTargetIndex (-1) means write to every rendition
// (used for the shared audio passthrough).
//
// PTS / DTS are in milliseconds — converted to the publisher's
// expected time base at the pipeline edge so downstream consumers
// (tsmux.FromAV, segmenters, players) all see a uniform clock.
type OutputFrame struct {
	Data        []byte
	Codec       esFrameCodec
	PTS         int64
	DTS         int64
	Keyframe    bool
	TargetIndex int32
}

// StreamPipeline composes decoder → N renditions (scaler + encoder)
// into the single data-flow the StreamRunner subprocess will execute
// per stream. Its central design invariant is: SwitchInput tears down
// and recreates the Decoder while every Scaler + Encoder stays alive.
// That is the property the FFmpeg subprocess implementation could not
// provide and is the whole reason for the migration.
//
// N renditions sharing one decoder is the multi-rendition design
// choice: decoding once and fanning out to N scaler/encoder pairs
// costs roughly 1×decode + N×(scale+encode) instead of N×(decode+
// scale+encode). For a typical ABR ladder (1080p/720p/480p) this
// saves ~2× the GPU memory and decoder cycles.
//
// Not safe for concurrent use across data and control plane — the
// caller (StreamRunner in P4) drives ProcessPacket on one goroutine
// and signals SwitchInput on the same goroutine via channel selects.
// The mutex here exists only to make SwitchInput observable in tests
// and to back the assertion in production unit-tests that the encoder
// pointer is unchanged across switch.
type StreamPipeline struct {
	cfg PipelineConfig

	// Long-lived across switch. One entry per rendition; encoders[i]
	// pairs with scalers[i] (+ optional watermarkers[i]) and emits
	// OutputFrames with TargetIndex=i. watermarkers[i] is nil when
	// the rendition has no watermark configured, so the encode loop
	// can skip a no-op filter pass instead of paying the graph cost.
	encoders     []*Encoder
	scalers      []*Scaler
	watermarkers []*Watermarker

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

// NewStreamPipeline constructs the decoder + every rendition's
// scaler/encoder pair. On error every partially-allocated stage is
// torn down before returning so the caller does not need defensive
// Close calls. Empty Renditions is rejected — a pipeline with no
// output target has nothing to do and would silently drop frames.
func NewStreamPipeline(cfg PipelineConfig) (*StreamPipeline, error) {
	if len(cfg.Renditions) == 0 {
		return nil, errors.New("pipeline: at least one rendition required")
	}

	encoders := make([]*Encoder, 0, len(cfg.Renditions))
	scalers := make([]*Scaler, 0, len(cfg.Renditions))
	watermarkers := make([]*Watermarker, 0, len(cfg.Renditions))
	closeAll := func() {
		for _, e := range encoders {
			e.Close()
		}
		for _, s := range scalers {
			s.Close()
		}
		for _, w := range watermarkers {
			if w != nil {
				w.Close()
			}
		}
	}

	for i, r := range cfg.Renditions {
		enc, sc, wm, err := buildRendition(r)
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("pipeline: rendition %d: %w", i, err)
		}
		encoders = append(encoders, enc)
		scalers = append(scalers, sc)
		watermarkers = append(watermarkers, wm)
	}

	dec, err := NewDecoder(cfg.Decoder)
	if err != nil {
		closeAll()
		return nil, fmt.Errorf("pipeline: decoder: %w", err)
	}
	return &StreamPipeline{
		cfg:          cfg,
		encoders:     encoders,
		scalers:      scalers,
		watermarkers: watermarkers,
		decoder:      dec,
	}, nil
}

// buildRendition allocates one rendition's encoder + scaler +
// optional watermarker, freeing the earlier allocations if any later
// step fails. Pulling this out of NewStreamPipeline keeps the
// constructor's branching shallow enough to stay under the cognitive-
// complexity bar.
func buildRendition(r RenditionConfig) (*Encoder, *Scaler, *Watermarker, error) {
	enc, err := NewEncoder(r.Encoder)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encoder: %w", err)
	}
	sc, err := NewScaler(r.Scaler)
	if err != nil {
		enc.Close()
		return nil, nil, nil, fmt.Errorf("scaler: %w", err)
	}
	wm, err := NewWatermarker(r.Watermark, r.Encoder.Width, r.Encoder.Height, r.Encoder.Framerate)
	if err != nil {
		sc.Close()
		enc.Close()
		return nil, nil, nil, fmt.Errorf("watermark: %w", err)
	}
	return enc, sc, wm, nil
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

// passthroughAudio wraps demuxed AAC frames as OutputFrames tagged
// with the BroadcastTargetIndex sentinel so the supervisor fans each
// frame across every rendition's buffer. PTS comes straight from the
// demuxer (millisecond-domain); no decode / re-encode — the original
// AAC bytes ship as-is. Each rendition publisher then muxes the same
// audio with its own transcoded video via tsmux.FromAV, so all
// variants stay A/V-aligned on the same source timeline.
func (p *StreamPipeline) passthroughAudio(frames []esFrame) []OutputFrame {
	if len(frames) == 0 {
		return nil
	}
	out := make([]OutputFrame, 0, len(frames))
	for _, f := range frames {
		out = append(out, OutputFrame{
			Data:        f.data,
			Codec:       f.codec,
			PTS:         f.pts,
			DTS:         f.dts,
			TargetIndex: BroadcastTargetIndex,
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

	// Drain every rendition's encoder tail (B-frames still held).
	var tail []OutputFrame
	for i, enc := range p.encoders {
		pkts, err := enc.Flush()
		if err != nil {
			return append(head, out...), fmt.Errorf("pipeline: rendition %d flush encoder: %w", i, err)
		}
		codec := encoderCodecToES(p.cfg.Renditions[i].Encoder.Codec)
		for _, pkt := range pkts {
			tail = append(tail, OutputFrame{
				Data:        pkt.Data,
				Codec:       codec,
				PTS:         pkt.PTS,
				DTS:         pkt.DTS,
				Keyframe:    pkt.Keyframe,
				TargetIndex: int32(i), //nolint:gosec // i bounded by len(encoders)
			})
		}
	}
	return append(append(head, out...), tail...), nil
}

// Close releases the decoder, demuxer, and every rendition's scaler +
// watermark + encoder. Idempotent.
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
	for _, wm := range p.watermarkers {
		if wm != nil {
			wm.Close()
		}
	}
	for _, sc := range p.scalers {
		sc.Close()
	}
	for _, enc := range p.encoders {
		enc.Close()
	}
}

// EncoderPTS returns the FIRST rendition encoder's PTS counter
// WITHOUT consuming it. Test-only inspection used to assert PTS
// continuity across SwitchInput and to verify the encoder was not
// reallocated. Renditions are PTS-aligned (encodeOne uses the same
// pts for every rendition), so any single encoder reflects the
// pipeline's PTS state.
func (p *StreamPipeline) EncoderPTS() int64 {
	if p.isClosed() {
		return -1
	}
	return p.encoders[0].frameIdx
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

// encodeOne fans one decoded frame across every rendition: each
// rendition's scaler resizes the frame to its target dims, then its
// encoder produces packets tagged with TargetIndex=i so the
// supervisor knows which rendition buffer to write each packet into.
//
// PTS source: the decoded frame inherits PTS from the input packet
// (set by dec.Decode(data, pts, dts)). Forwarding it to every
// encoder keeps the output video on the SAME timeline as the AAC
// passthrough — both expressed in source-stream milliseconds.
// Without this the encoder would emit PTS starting at zero while
// audio still rides the source wallclock, and the player gets A/V
// drift the moment a segment lands.
//
// NextPTS fallback covers the Annex-B AV-path (RTMP / RTSP) which
// today passes pts=0 through ProcessPacket; in that mode there's no
// audio so monotonic frame indices are still self-consistent. The
// fallback is taken from the FIRST encoder so renditions stay PTS-
// aligned across the ladder.
func (p *StreamPipeline) encodeOne(f *astiav.Frame) ([]OutputFrame, error) {
	pts := f.Pts()
	if pts <= 0 {
		pts = p.encoders[0].NextPTS()
	}
	var out []OutputFrame
	for i := range p.encoders {
		pkts, err := p.encodeOneRendition(i, f, pts)
		if err != nil {
			return out, err
		}
		out = append(out, pkts...)
	}
	return out, nil
}

// encodeOneRendition runs one frame through rendition i's scaler +
// optional watermark + encoder and returns the encoded packets
// tagged with TargetIndex=i. PTS is set on the frame the encoder
// actually consumes — for the watermark path that's the filtered
// frame, since the filter graph passes PTS through and the encoder
// reads it back from there.
func (p *StreamPipeline) encodeOneRendition(i int, f *astiav.Frame, pts int64) ([]OutputFrame, error) {
	scaled, err := p.scalers[i].Scale(f)
	if err != nil {
		return nil, fmt.Errorf("pipeline: rendition %d scale: %w", i, err)
	}
	scaled.SetPts(pts)

	toEncode := scaled
	if wm := p.watermarkers[i]; wm != nil {
		filtered, err := wm.Filter(scaled)
		scaled.Free()
		if err != nil {
			return nil, fmt.Errorf("pipeline: rendition %d watermark: %w", i, err)
		}
		filtered.SetPts(pts)
		toEncode = filtered
	}

	pkts, err := p.encoders[i].Encode(toEncode)
	toEncode.Free()
	if err != nil {
		return nil, fmt.Errorf("pipeline: rendition %d encode: %w", i, err)
	}
	if len(pkts) == 0 {
		return nil, nil
	}
	codec := encoderCodecToES(p.cfg.Renditions[i].Encoder.Codec)
	out := make([]OutputFrame, 0, len(pkts))
	for _, pkt := range pkts {
		out = append(out, OutputFrame{
			Data:        pkt.Data,
			Codec:       codec,
			PTS:         pkt.PTS,
			DTS:         pkt.DTS,
			Keyframe:    pkt.Keyframe,
			TargetIndex: int32(i), //nolint:gosec // i bounded by len(encoders)
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
