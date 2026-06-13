package native

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/asticode/go-astiav"
	gocodec "github.com/yapingcat/gomedia/go-codec"
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
	// Audio drives the audio path: when Audio.Copy is true (or the
	// zero value) AAC frames pass straight through; otherwise they are
	// decoded, resampled, and re-encoded to Audio's codec / bitrate /
	// sample rate / channels.
	Audio AudioConfig
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
// SessionStart marks the first packet emitted after a SwitchInput
// so the supervisor sets buffer.Packet.SessionStart and the
// publisher emits EXT-X-DISCONTINUITY on the next HLS segment.
// Without this signal players keep the previous source's MSE init
// context and corrupt-decode the new source's frames.
//
// PTS / DTS are in milliseconds — converted to the publisher's
// expected time base at the pipeline edge so downstream consumers
// (tsmux.FromAV, segmenters, players) all see a uniform clock.
type OutputFrame struct {
	Data         []byte
	Codec        esFrameCodec
	PTS          int64
	DTS          int64
	Keyframe     bool
	TargetIndex  int32
	SessionStart bool
}

// StreamPipeline composes decoder → N renditions (scaler + encoder)
// into the single data-flow the StreamRunner subprocess will execute
// per stream. Its central design invariant is: SwitchInput tears down
// and recreates the Decoder while every Scaler + Encoder stays alive.
// Keeping the encoder alive across the switch is what makes the output
// seamless: stable SPS/PPS and a continuous rebased PTS.
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

	// GPU-resident path. When useGPU is true the renditions run
	// decode(cuvid)→scale_cuda→nvenc entirely in VRAM: gpuScalers[i]
	// replaces scalers[i] (which is nil), watermarkers[i] is nil, and
	// cuda is the shared device. Chosen only for NVENC + cuvid streams
	// with NO watermark (GPU drawtext lands in a later phase); every
	// other stream stays on the CPU path. The two index-aligned slices
	// keep encodeOneRendition's per-rendition lookup uniform.
	useGPU     bool
	cuda       *cudaContext
	gpuScalers []*gpuScaler

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

	// pendingSessionStart is set on every SwitchInput and consumed
	// by the first OutputFrame emitted afterwards (drained tail of
	// the old decoder doesn't count — only the first frame coming
	// from the NEW input). The supervisor forwards it as
	// buffer.Packet.SessionStart so the publisher emits an
	// EXT-X-DISCONTINUITY tag on the next HLS segment, telling
	// players to re-init their MSE decoder for the new source.
	//
	// pendingForceKeyframe goes hand-in-hand: the first frame from
	// the new input must be encoded as an IDR so the new segment
	// is independently decodable. Without this the encoder may
	// emit P-frames first that reference reordered buffers from
	// the OLD input — invalid output that decodes as macroblock
	// garbage.
	pendingSessionStart  bool
	pendingForceKeyframe bool

	// Output-PTS rebasing keeps the emitted timeline continuous and
	// monotonic across input switches. Raw source PTS jumps on every
	// switch (each input has its own wallclock anchor and framerate),
	// so feeding it straight to the encoder produces a chaotic
	// timeline that players can't assemble even with an
	// EXT-X-DISCONTINUITY tag — the live edge stalls. Rebasing onto a
	// single continuous clock is what makes switching actually work
	// (the standard design for seamless live input switching).
	//
	// ptsOffset is added to every source PTS; recomputed on the first
	// frame after each switch so the new input continues right after
	// the last emitted output instead of jumping. lastVideoOut /
	// lastAudioOut track per-track monotonic ceilings (video and
	// audio are independent streams that interleave, so a single
	// shared ceiling would corrupt their relative timing). The shared
	// ptsOffset keeps A/V in sync because both ride the same source
	// timeline.
	ptsOffset     int64
	lastVideoOut  int64
	lastAudioOut  int64
	pendingRebase bool

	// videoFrameDurMs is the nominal inter-frame interval (1000/framerate
	// ms) used to pace the video output clock when a source's decoded PTS
	// stalls or regresses — e.g. an HLS-pull input whose timestamps the
	// NVDEC decoder mishandles (the linked go-astiav binding exposes no
	// pkt_timebase setter) emits frames with clustered / non-monotonic
	// PTS. Advancing the output by one frame interval there — instead of
	// the legacy +1 ms — keeps the emitted timeline tracking real time, so
	// segment-duration math stays correct and the publisher cuts at the
	// encoder's IDRs instead of force-flushing on its wallclock safety net
	// (the "buffer grows, won't play after input switch" failure).
	videoFrameDurMs int64

	// videoSrcIntervalMs is the last OBSERVED source frame interval (src PTS
	// delta between consecutive video frames). rebaseVideoPTS paces its
	// monotonic-guard advance by this, not the static videoFrameDurMs: a
	// configured-fps interval that exceeds the real source interval (e.g.
	// 40 ms/25 fps applied to a 30 fps / 33 ms source) makes the guard
	// over-advance every frame and self-perpetuate, re-timing video to the
	// configured fps and drifting it ~(1 − srcFps/cfgFps) off the audio clock.
	// Relearned per source; falls back to videoFrameDurMs until first observed.
	videoSrcIntervalMs int64
	lastVideoSrc       int64 // previous video source PTS (for the interval)
	hasVideoSrc        bool
	lastAudioSrc       int64 // previous audio source PTS (regress-guard conjunct)
	hasAudioSrc        bool

	// audioReenc is non-nil when Audio.Copy is false — AAC frames are
	// decoded → resampled → re-encoded instead of passed through. nil
	// keeps the cheap passthrough path.
	audioReenc *audioReencoder

	// pendingAudio holds audio OutputFrames whose output PTS has run ahead
	// of the video output clock (lastVideoOut) by more than audioLeadCapMs.
	// They are released as video catches up so downstream segments keep A/V
	// within ~audioLeadCapMs. Needed because the cheap audio passthrough
	// emits a whole demuxed chunk instantly while the heavy video encode
	// (decode + scale + N×NVENC) drains it over real time — on a bursty
	// input (HLS-pull delivering a full playlist window at a switch) audio
	// would otherwise outrun video by seconds, baking a permanent lip-sync
	// offset into the output. PTS-ordered (rebaseAudioPTS is monotonic).
	pendingAudio []OutputFrame
}

const (
	// audioLeadCapMs bounds how far an audio output PTS may lead the video
	// output clock before the frame is held in pendingAudio. Set above the
	// largest legitimate intrinsic source A/V offset (~0.4 s observed) so
	// normal real-time inputs (UDP) are never throttled — only a bursty
	// input that lets audio outrun video triggers holding.
	audioLeadCapMs int64 = 500

	// audioHoldMaxMs is a safety valve: if video stalls and held audio spans
	// more than this, the oldest held frames are released anyway so the audio
	// track never goes silent (A/V desync returns until video recovers, which
	// is preferable to dead audio).
	audioHoldMaxMs int64 = 5000

	// maxSaneFrameIntervalMs caps what rebaseVideoPTS will accept as an
	// observed source frame interval (≈ down to 5 fps). Anything larger is a
	// switch/loop jump or a stall, not a real inter-frame gap, so it must not
	// be learned as the pacing step.
	maxSaneFrameIntervalMs int64 = 200
)

// NewStreamPipeline constructs the decoder + every rendition's
// scaler/encoder pair. On error every partially-allocated stage is
// torn down before returning so the caller does not need defensive
// Close calls. Empty Renditions is rejected — a pipeline with no
// output target has nothing to do and would silently drop frames.
func NewStreamPipeline(cfg PipelineConfig) (*StreamPipeline, error) {
	if len(cfg.Renditions) == 0 {
		return nil, errors.New("pipeline: at least one rendition required")
	}

	// Decide the GPU-resident path: NVENC + cuvid decode. Watermarks run
	// on the GPU graph too (scale_cuda + a hwdownload/drawtext/hwupload
	// round-trip). A CUDA init failure falls back to CPU.
	useGPU := isCUVIDDecoder(cfg.Decoder.Codec)
	var cuda *cudaContext
	if useGPU {
		c, err := newCUDAContext()
		if err != nil {
			slog.Warn("native: CUDA unavailable, falling back to CPU pipeline", "err", err)
			useGPU = false
		} else {
			cuda = c
		}
	}
	// A cuvid decoder only makes sense feeding the GPU path — its CUDA
	// frames can't enter the CPU sws scaler. When we're not taking the
	// GPU path (watermark present, or CUDA init failed), downgrade decode
	// to the CPU equivalent so the pipeline stays coherent.
	if !useGPU && isCUVIDDecoder(cfg.Decoder.Codec) {
		cfg.Decoder.Codec = cpuDecoderFallback(cfg.Decoder.Codec)
	}
	if useGPU {
		cfg.Decoder.cuda = cuda
	}

	encoders := make([]*Encoder, 0, len(cfg.Renditions))
	scalers := make([]*Scaler, 0, len(cfg.Renditions))
	gpuScalers := make([]*gpuScaler, 0, len(cfg.Renditions))
	watermarkers := make([]*Watermarker, 0, len(cfg.Renditions))
	closeAll := func() {
		for _, e := range encoders {
			e.Close()
		}
		for _, s := range scalers {
			if s != nil {
				s.Close()
			}
		}
		for _, g := range gpuScalers {
			if g != nil {
				g.Close()
			}
		}
		for _, w := range watermarkers {
			if w != nil {
				w.Close()
			}
		}
		cuda.Free() // nil-safe
	}

	for i, r := range cfg.Renditions {
		if useGPU {
			r.Encoder.cuda = cuda
			enc, gs, err := buildGPURendition(r, cuda)
			if err != nil {
				closeAll()
				return nil, fmt.Errorf("pipeline: gpu rendition %d: %w", i, err)
			}
			encoders = append(encoders, enc)
			gpuScalers = append(gpuScalers, gs)
			scalers = append(scalers, nil)
			watermarkers = append(watermarkers, nil)
			continue
		}
		enc, sc, wm, err := buildRendition(r)
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("pipeline: rendition %d: %w", i, err)
		}
		encoders = append(encoders, enc)
		scalers = append(scalers, sc)
		gpuScalers = append(gpuScalers, nil)
		watermarkers = append(watermarkers, wm)
	}

	dec, err := newDecoderWithFallback(cfg.Decoder)
	if err != nil {
		closeAll()
		return nil, fmt.Errorf("pipeline: decoder: %w", err)
	}
	audioReenc, err := buildAudioReencoder(cfg.Audio)
	if err != nil {
		dec.Close()
		closeAll()
		return nil, fmt.Errorf("pipeline: %w", err)
	}

	return &StreamPipeline{
		cfg:             cfg,
		encoders:        encoders,
		scalers:         scalers,
		gpuScalers:      gpuScalers,
		watermarkers:    watermarkers,
		useGPU:          useGPU,
		cuda:            cuda,
		decoder:         dec,
		pendingRebase:   true, // first-ever frame anchors the output clock
		videoFrameDurMs: videoFrameDurationMs(cfg.Renditions[0].Encoder.Framerate),
		audioReenc:      audioReenc,
	}, nil
}

// buildGPURendition builds the VRAM-resident encoder + scale_cuda scaler
// for one rendition. The encoder defers its NVENC Open until the first
// CUDA frame supplies the hw_frames_ctx (see Encoder.openForCUDAFrame).
func buildGPURendition(r RenditionConfig, cuda *cudaContext) (*Encoder, *gpuScaler, error) {
	enc, err := NewEncoder(r.Encoder)
	if err != nil {
		return nil, nil, fmt.Errorf("encoder: %w", err)
	}
	gs, err := newGPUScaler(cuda, r.Scaler.DstWidth, r.Scaler.DstHeight, r.Watermark)
	if err != nil {
		enc.Close()
		return nil, nil, fmt.Errorf("gpu scaler: %w", err)
	}
	return enc, gs, nil
}

// buildAudioReencoder returns a re-encoder when Audio.Copy is false,
// or (nil, nil) for the passthrough path. Source audio is always AAC
// by the time it reaches the pipeline — the TS demuxer only queues
// StreamTypeAAC into the audio path.
func buildAudioReencoder(cfg AudioConfig) (*audioReencoder, error) {
	if cfg.Copy {
		return nil, nil
	}
	ar, err := newAudioReencoder(cfg, "aac")
	if err != nil {
		return nil, fmt.Errorf("audio reencoder: %w", err)
	}
	return ar, nil
}

// handleAudio routes demuxed AAC frames either through the
// re-encoder (Audio.Copy=false) or straight to passthrough. Both
// paths emit broadcast OutputFrames whose PTS the caller rebases onto
// the continuous output clock.
//
// Audio is gated on sawKeyframe (the first video IDR), same as video.
// Without this, after a switch the audio (which has no keyframe
// concept) starts flowing immediately and anchors the shared rebase
// offset BEFORE the gated video does — leaving audio ~1 GOP (≈4 s)
// ahead of video as a constant skew. Gating audio to the first video
// IDR makes both tracks start from the same point so they share the
// offset cleanly.
func (p *StreamPipeline) handleAudio(frames []esFrame) ([]OutputFrame, error) {
	if !p.sawKeyframe {
		return nil, nil
	}
	if len(frames) > 0 {
		if err := p.bufferAudio(frames); err != nil {
			return p.releaseAudio(), err
		}
	}
	return p.releaseAudio(), nil
}

// bufferAudio rebases the demuxed AAC frames onto the output clock and queues
// them in pendingAudio (passthrough copies as-is; otherwise re-encodes). The
// A/V-sync gate in releaseAudio decides when they actually leave the pipeline.
func (p *StreamPipeline) bufferAudio(frames []esFrame) error {
	if p.audioReenc == nil {
		p.pendingAudio = append(p.pendingAudio, p.passthroughAudio(frames)...)
		return nil
	}
	for _, f := range frames {
		enc, err := p.audioReenc.Process(f.data, f.pts)
		if err != nil {
			return fmt.Errorf("pipeline: audio reencode: %w", err)
		}
		for i := range enc {
			enc[i].PTS = p.rebaseAudioPTS(enc[i].PTS)
			enc[i].DTS = enc[i].PTS
		}
		p.pendingAudio = append(p.pendingAudio, enc...)
	}
	return nil
}

// releaseAudio pops pending audio frames that no longer lead the video output
// clock by more than audioLeadCapMs, keeping the emitted audio paced with
// video so downstream segments stay A/V-aligned. pendingAudio is PTS-ordered,
// so the first frame still leading bounds the release. The audioHoldMaxMs
// safety valve force-releases the oldest frames when video has stalled, so the
// audio track never goes silent.
func (p *StreamPipeline) releaseAudio() []OutputFrame {
	if len(p.pendingAudio) == 0 {
		return nil
	}
	limit := p.lastVideoOut + audioLeadCapMs
	n := 0
	for n < len(p.pendingAudio) && p.pendingAudio[n].PTS <= limit {
		n++
	}
	if n < len(p.pendingAudio) {
		newest := p.pendingAudio[len(p.pendingAudio)-1].PTS
		for n < len(p.pendingAudio) && newest-p.pendingAudio[n].PTS > audioHoldMaxMs {
			n++
		}
	}
	if n == 0 {
		return nil
	}
	out := make([]OutputFrame, n)
	copy(out, p.pendingAudio[:n])
	p.pendingAudio = p.pendingAudio[n:]
	return out
}

// rebaseRegressLimitMs bounds how far the SOURCE PTS may jump backwards —
// with the rebased value equally far behind the emitted output — before the
// pipeline treats the source timeline as broken (a 33-bit PTS wrap that
// slipped past the demuxer's unwrap after a mid-session demuxer rebuild, or
// a genuine upstream reset) and re-anchors the shared A/V offset. Both
// conjuncts are required: the output-behind condition alone would also
// match a long stall whose pacing pushed the output ahead of a FROZEN
// source PTS (the NVDEC mis-timestamp class) — re-anchoring there would
// yank the shared offset forward ~30 s every 30 s and permanently desync
// audio. A frozen source (delta 0) never trips the source-regression
// conjunct; a wrap (source drops ~95 443 717 ms) always does. Anything
// smaller — B-frame reordering, ms truncation, HLS-pull burst skew — is
// handled by the per-track monotonic clamps below. Without this guard a
// huge backward jump strands `src+offset` permanently below the output
// line, where the +1 ms tie-break compresses the emitted timeline ~40×
// forever (the failure that took every transcoded stream down at the
// first 26.5 h wrap).
const rebaseRegressLimitMs = 30_000

// reanchorOnRegress recomputes the shared A/V offset when a rebased value
// falls hopelessly behind the output line. The break is common-mode (both
// tracks ride one source clock), so a single re-anchor heals both tracks
// while preserving their relative A/V offset.
func (p *StreamPipeline) reanchorOnRegress(srcPTS int64, track string) {
	slog.Warn("native: source timeline broke backwards, re-anchoring",
		"track", track, "src_pts_ms", srcPTS, "pts_offset_ms", p.ptsOffset,
		"last_video_out_ms", p.lastVideoOut, "last_audio_out_ms", p.lastAudioOut)
	p.pendingRebase = true
	p.anchorRebase(srcPTS)
}

// rebaseVideoPTS / rebaseAudioPTS map a source-stream PTS (ms) onto
// the pipeline's continuous output timeline. The offset is recomputed
// lazily on the first frame after a switch (pendingRebase) so the new
// input continues one tick past the last emitted output rather than
// jumping to its own wallclock anchor. Per-track monotonic clamps
// guard against intra-track regressions without coupling video and
// audio ordering.
func (p *StreamPipeline) rebaseVideoPTS(srcPTS int64) int64 {
	p.anchorRebase(srcPTS)
	if p.hasVideoSrc && p.lastVideoSrc-srcPTS > rebaseRegressLimitMs &&
		p.lastVideoOut-(srcPTS+p.ptsOffset) > rebaseRegressLimitMs {
		p.reanchorOnRegress(srcPTS, "video")
	}
	// Whether the SOURCE advanced since the previous frame — captured BEFORE
	// updating lastVideoSrc. <= 0 means a genuine stall/regress (decoder
	// repeating or rewinding PTS); > 0 means the source is moving forward and
	// any dip of the rebased value to/under the last output is mere sub-frame
	// jitter / ms-truncation, not a stall.
	advancing := p.hasVideoSrc && srcPTS > p.lastVideoSrc
	if advancing {
		// Learn the real frame interval from forward deltas (paces the stall
		// fallback at the source rate, not a static configured fps).
		if d := srcPTS - p.lastVideoSrc; d <= maxSaneFrameIntervalMs {
			p.videoSrcIntervalMs = d
		}
	}
	p.lastVideoSrc, p.hasVideoSrc = srcPTS, true

	out := srcPTS + p.ptsOffset
	if out <= p.lastVideoOut {
		if advancing {
			// Advancing source whose rebased value merely landed at/under the
			// last output: strict-monotonic tie-break ONLY (+1). The next
			// forward frame whose src+offset clears lastVideoOut returns the
			// output to the true source line, so a single collision cannot
			// self-perpetuate. The previous code added a full frame interval
			// here and then ratcheted lastVideoOut forward unconditionally —
			// once tripped it leaked (step − trueDelta) ms/frame, re-timing
			// video off the audio clock (~0.05 s/h slow A/V drift).
			out = p.lastVideoOut + 1
		} else {
			// Genuine stall (source PTS not advancing — clustered / repeated
			// PTS from a decoder that mishandles timestamps). Pace by the
			// observed frame interval so the output framerate doesn't collapse
			// to ~1 ms steps and stall the segmenter (the 98cbcbc case).
			step := p.videoSrcIntervalMs
			if step <= 0 {
				step = p.videoFrameDurMs
			}
			out = p.lastVideoOut + step
		}
	}
	p.lastVideoOut = out
	return out
}

func (p *StreamPipeline) rebaseAudioPTS(srcPTS int64) int64 {
	p.anchorRebase(srcPTS)
	if p.hasAudioSrc && p.lastAudioSrc-srcPTS > rebaseRegressLimitMs &&
		p.lastAudioOut-(srcPTS+p.ptsOffset) > rebaseRegressLimitMs {
		p.reanchorOnRegress(srcPTS, "audio")
	}
	p.lastAudioSrc, p.hasAudioSrc = srcPTS, true
	out := srcPTS + p.ptsOffset
	if out <= p.lastAudioOut {
		out = p.lastAudioOut + 1
	}
	p.lastAudioOut = out
	return out
}

// anchorRebase recomputes ptsOffset from the first frame seen after a
// switch so the new input's output continues just past whichever
// track last emitted. Computed from whichever track (video or audio)
// arrives first; the shared offset then keeps both in their original
// A/V relationship.
func (p *StreamPipeline) anchorRebase(srcPTS int64) {
	if !p.pendingRebase {
		return
	}
	p.pendingRebase = false
	base := p.lastVideoOut
	if p.lastAudioOut > base {
		base = p.lastAudioOut
	}
	// base==0 on the first-ever frame → output starts at 1 ms.
	p.ptsOffset = base + 1 - srcPTS
}

// videoFrameDurationMs is the nominal inter-frame interval in
// milliseconds for the configured output framerate. rebaseVideoPTS uses
// it to pace the output clock when a source's decoded PTS stalls.
// Defaults to 40 ms (25 fps) when the framerate is unknown.
func videoFrameDurationMs(framerate int) int64 {
	if framerate <= 0 {
		return 40
	}
	if d := int64(1000 / framerate); d >= 1 {
		return d
	}
	return 1
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

// ensureVideoDecoder rebuilds the video decoder when the incoming
// elementary-stream codec differs from the codec the active decoder was
// opened for. The startup decoder is always H.264 (decoderCodecForBackend
// assumes H.264 sources), but a raw-TS or AV source can carry HEVC — from
// the very first frame, or after an upstream mux switches H.264→HEVC on an
// established stream. Feeding H.265 NAL units to an H.264 decoder returns
// AVERROR_INVALIDDATA on every packet, which the supervisor escalates to a
// terminal error → respawn → same first frame → permanent crash loop with
// restart_count climbing (A-3). HEVC decode infrastructure already exists
// (hevc / hevc_cuvid via newDecoderWithFallback); this just selects it on
// demand.
//
// On a rebuild the old decoder is flushed through the surviving encoder so
// the cutover stays monotonic in encoder PTS space; the keyframe gate is
// reset so decode resumes on the new codec's first IRAP; and on the GPU
// path the scale graphs rebind to the new decoder's freshly-allocated CUDA
// pool (same reason as switchInputGPU). The encoder is never rebuilt — it
// operates on decoded YUV/CUDA frames and is codec-agnostic — so the output
// stream stays continuous. Returns any frames flushed from the old decoder
// (nil in the common start-of-stream case where it has decoded nothing).
//
// Non-video codecs (AAC, unknown) and a decoder that already matches the
// source return immediately, so the per-frame call on the hot path is a
// cheap family compare.
func (p *StreamPipeline) ensureVideoDecoder(codec esFrameCodec) ([]OutputFrame, error) {
	if codec != esCodecH264 && codec != esCodecH265 {
		return nil, nil // not a video codec routed to the decoder
	}
	p.mu.Lock()
	current := p.cfg.Decoder.Codec
	p.mu.Unlock()
	if decoderCodecFamily(current) == codec {
		return nil, nil // decoder already matches the source codec
	}

	newName := videoDecoderNameForCodec(current, codec)
	slog.Warn("native: source video codec differs from decoder — rebuilding",
		"decoder", current, "rebuild_to", newName)

	p.mu.Lock()
	oldDec := p.decoder
	p.mu.Unlock()

	flushed, err := oldDec.Flush()
	if err != nil {
		return nil, fmt.Errorf("pipeline: flush decoder on codec change: %w", err)
	}
	out, err := p.runFramesThroughEncoder(flushed)
	if err != nil {
		return out, err
	}
	oldDec.Close()

	cfg := p.cfg.Decoder
	cfg.Codec = newName
	if p.useGPU {
		cfg.cuda = p.cuda // new decoder emits CUDA frames into a fresh auto-pool
		for _, gs := range p.gpuScalers {
			if gs != nil {
				gs.MarkSourceChanged() // rebind scale_cuda to the new pool
			}
		}
	}
	newDec, err := newDecoderWithFallback(cfg)
	if err != nil {
		return out, fmt.Errorf("pipeline: alloc %s decoder for codec change: %w", newName, err)
	}

	p.mu.Lock()
	p.decoder = newDec
	p.cfg.Decoder = cfg
	p.mu.Unlock()
	p.sawKeyframe = false
	p.pendingForceKeyframe = true
	return out, nil
}

// isVideoKeyframeAnnexB reports whether an Annex-B access unit carries a
// random-access point for its codec — an H.264 IDR (NAL type 5) or an HEVC
// IRAP. The AV-path keyframe gate uses it so decode resumes on a clean SAP
// regardless of codec (the raw-TS path already tags f.keyframe per codec at
// demux time).
func isVideoKeyframeAnnexB(codec esFrameCodec, data []byte) bool {
	if codec == esCodecH265 {
		return gocodec.IsH265IDRFrame(data)
	}
	return isH264KeyframeAnnexB(data)
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
func (p *StreamPipeline) ProcessPacket(data []byte, codec esFrameCodec, pts, dts int64) ([]OutputFrame, error) {
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
		audio, err := p.handleAudio(p.tsInput.DrainReadyAudio())
		if err != nil {
			return append(video, audio...), err
		}
		return append(video, audio...), nil
	}

	// AV-path (RTSP / RTMP / copy / mixer): one ES access unit per call.
	// Route AAC to the audio path — feeding it to the H.264 decoder was
	// B-1 (audio silently dropped / decoder crash-loop). Video falls
	// through to the keyframe-gated decode below, now carrying the real
	// source PTS so the output timeline tracks the source (B-2).
	if codec == esCodecAAC {
		return p.handleAudio([]esFrame{{data: data, pts: pts, dts: dts, codec: esCodecAAC}})
	}

	// Rebuild the decoder if this source's video codec differs from the
	// active decoder (HEVC source, or a mid-stream H.264→HEVC switch) — see
	// ensureVideoDecoder (A-3). Any frames flushed from the old decoder are
	// carried through and prepended to this call's output.
	rebuilt, err := p.ensureVideoDecoder(codec)
	if err != nil {
		return rebuilt, err
	}

	if !p.sawKeyframe {
		if !isVideoKeyframeAnnexB(codec, data) {
			return rebuilt, nil
		}
		p.sawKeyframe = true
	}

	p.mu.Lock()
	dec := p.decoder
	p.mu.Unlock()

	frames, err := dec.Decode(data, pts, dts)
	if err != nil {
		return rebuilt, fmt.Errorf("pipeline: decode: %w", err)
	}
	encoded, err := p.runFramesThroughEncoder(frames)
	return append(rebuilt, encoded...), err
}

// decodeAndEncodeESFrames feeds a batch of demuxed video ES access
// units through the decoder + encoder. Used by the TS-input path
// where one gRPC chunk yields multiple frames the demuxer assembled
// from PES. Pre-IDR frames are dropped silently — see sawKeyframe.
func (p *StreamPipeline) decodeAndEncodeESFrames(frames []esFrame) ([]OutputFrame, error) {
	if len(frames) == 0 {
		return nil, nil
	}

	var out []OutputFrame
	for _, f := range frames {
		// Rebuild the decoder if the demuxed codec differs from the active
		// one — a raw-TS source whose video is HEVC (esCodecH265) would
		// otherwise feed H.265 into the H.264 decoder and crash-loop the
		// subprocess (A-3). No-op once the decoder matches.
		rebuilt, err := p.ensureVideoDecoder(f.codec)
		if err != nil {
			return out, err
		}
		out = append(out, rebuilt...)

		if !p.sawKeyframe {
			if !f.keyframe {
				continue
			}
			p.sawKeyframe = true
		}
		p.mu.Lock()
		dec := p.decoder
		p.mu.Unlock()
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
		// Rebase onto the same continuous clock as video (shared
		// ptsOffset) so audio stays in A/V sync across switches and
		// the output timeline never jumps backward.
		pts := p.rebaseAudioPTS(f.pts)
		out = append(out, OutputFrame{
			Data:        f.data,
			Codec:       f.codec,
			PTS:         pts,
			DTS:         pts,
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

// DecoderCodec returns the libavcodec decoder name the pipeline was
// configured with (e.g. "h264_cuvid" on NVENC hosts). SwitchInput reuses
// it so the hardware decode path is preserved across input switches
// instead of silently dropping back to CPU.
func (p *StreamPipeline) DecoderCodec() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfg.Decoder.Codec
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

	// GPU path rebuilds only the decoder (into a stable pipeline-owned CUDA
	// pool), keeping the scale_cuda graph + NVENC encoder bound to one
	// frames ctx so the output stays seamless (see switchInputGPU). The CPU
	// rebuild below applies only to the software path.
	if p.useGPU {
		return p.switchInputGPU()
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
	p.pendingForceKeyframe = true
	p.pendingRebase = true
	p.pendingAudio = nil                           // drop the old input's held audio; new input re-anchors
	p.hasVideoSrc, p.videoSrcIntervalMs = false, 0 // relearn the new source's frame interval
	p.hasAudioSrc = false                          // new source = new audio clock for the regress guard
	// EXT-X-DISCONTINUITY is only needed when the OUTPUT stream's shape
	// can change across the switch. With audio re-encode the output is
	// fully continuous — same encoder (identical SPS/PPS), continuous
	// rebased PTS, and a FIXED audio format (the configured rate
	// regardless of source) — so signalling a discontinuity would force
	// the player to needlessly re-init its MSE decoder and re-buffer
	// (the "slight loading" seen at every scene change). Suppressing it
	// makes the switch seamless. Passthrough audio still needs it: the
	// source audio sample rate (44.1 vs 48 kHz) changes mid-stream,
	// which a player can only absorb at a discontinuity boundary.
	if p.audioReenc != nil {
		// Drop the audio decoder + resampler so they rebuild against
		// the new source's sample rate / channels; encoder survives so
		// the output AAC stream stays continuous.
		p.audioReenc.SwitchInput()
	} else {
		p.pendingSessionStart = true
	}

	// This path is CPU-only: SwitchInput dispatches to switchInputGPU above
	// when useGPU, so p.cuda is nil here and the new decoder is a software
	// codec.
	newDec, err := newDecoderWithFallback(newCfg)
	if err != nil {
		return out, fmt.Errorf("pipeline: alloc new decoder: %w", err)
	}

	p.mu.Lock()
	p.decoder = newDec
	p.cfg.Decoder = newCfg
	p.mu.Unlock()
	return out, nil
}

// switchInputGPU performs a seamless GPU input switch by rebuilding ONLY
// the decoder; the scale_cuda graphs and NVENC encoders are left untouched.
// The new decoder auto-allocates a CUDA frames pool sized to the new
// source (so a switch to a differently-sized input — e.g. 720p after 1080p
// — decodes cleanly), then scale_cuda rebuilds its graph on the first frame
// whose dims changed and resizes back to the fixed rendition dims. The
// encoder never restarts: it was opened at the rendition dims and
// re-registers each incoming CUDA frame by device pointer, so a fresh
// output pool at the same dims flows straight through. Same encoder =
// identical SPS/PPS, continuous rebased PTS, no player MSE re-init.
//
// The TS demuxer is rebuilt (new source = new PSI) and the keyframe gate +
// PTS rebase reset so the cutover lands on the new source's IDR.
func (p *StreamPipeline) switchInputGPU() ([]OutputFrame, error) {
	p.mu.Lock()
	oldDec := p.decoder
	p.mu.Unlock()

	// Drain the old decoder's reorder queue through the surviving encoder
	// so the cutover stays monotonic in encoder PTS space.
	flushFrames, err := oldDec.Flush()
	if err != nil {
		return nil, fmt.Errorf("pipeline: flush old decoder: %w", err)
	}
	out, err := p.runFramesThroughEncoder(flushFrames)
	if err != nil {
		return out, err
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
	p.pendingForceKeyframe = true
	p.pendingRebase = true
	p.pendingAudio = nil                           // drop the old input's held audio; new input re-anchors
	p.hasVideoSrc, p.videoSrcIntervalMs = false, 0 // relearn the new source's frame interval
	p.hasAudioSrc = false                          // new source = new audio clock for the regress guard
	if p.audioReenc != nil {
		p.audioReenc.SwitchInput()
	} else {
		p.pendingSessionStart = true
	}

	// The new decoder allocates a fresh CUDA frames pool, so each rendition's
	// scale_cuda graph must rebind to it on the next frame — even when the new
	// source is the same resolution. A same-res switch otherwise keeps the old
	// pool's hw_frames_ctx and crashes with "gpu scale GetFrame: Invalid argument".
	for _, gs := range p.gpuScalers {
		if gs != nil {
			gs.MarkSourceChanged()
		}
	}

	cfg := p.cfg.Decoder
	cfg.cuda = p.cuda // new decoder must emit CUDA frames (auto-pool, new source dims)

	newDec, err := newDecoderWithFallback(cfg)
	if err != nil {
		return out, fmt.Errorf("pipeline: alloc new decoder on switch: %w", err)
	}
	p.mu.Lock()
	p.decoder = newDec
	p.cfg.Decoder = cfg
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
// flushTSDemuxTail drains the TS demuxer's leftover video + audio
// frames and the audio encoder's held tail before the pipeline shuts
// down — without it the publisher would close a partial last segment
// the player can't seek past.
func (p *StreamPipeline) flushTSDemuxTail() ([]OutputFrame, error) {
	var head []OutputFrame
	if p.tsInput != nil {
		readyV, err := p.decodeAndEncodeESFrames(p.tsInput.DrainReady())
		head = readyV
		if err != nil {
			return head, err
		}
		audio, err := p.handleAudio(p.tsInput.DrainReadyAudio())
		head = append(head, audio...)
		if err != nil {
			return head, err
		}
	}
	if p.audioReenc != nil {
		tailA, err := p.audioReenc.Flush()
		for i := range tailA {
			tailA[i].PTS = p.rebaseAudioPTS(tailA[i].PTS)
			tailA[i].DTS = tailA[i].PTS
		}
		head = append(head, tailA...)
		if err != nil {
			return head, err
		}
	}
	// Shutdown: force-drain any audio still held by the A/V-sync gate so the
	// final audio tail isn't lost.
	if len(p.pendingAudio) > 0 {
		head = append(head, p.pendingAudio...)
		p.pendingAudio = nil
	}
	return head, nil
}

// Flush drains the TS-demux tail, decoder, encoder, and (when active) the
// audio re-encoder at end-of-stream, returning every remaining OutputFrame
// so the final GOP and audio tail aren't dropped on shutdown.
func (p *StreamPipeline) Flush() ([]OutputFrame, error) {
	if p.isClosed() {
		return nil, errors.New("native: flush on closed pipeline")
	}

	head, err := p.flushTSDemuxTail()
	if err != nil {
		return head, err
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
	if p.audioReenc != nil {
		p.audioReenc.Close()
		p.audioReenc = nil
	}
	for _, wm := range p.watermarkers {
		if wm != nil {
			wm.Close()
		}
	}
	for _, sc := range p.scalers {
		if sc != nil {
			sc.Close()
		}
	}
	// Encoders before gpuScalers: an NVENC encoder holds an av_buffer_ref
	// on its scale_cuda graph's output CUDA frames pool, so it must
	// release that ref before the graph tears the pool down (otherwise
	// avcodec_free_context faults on a dangling CUDA hwframe). cuda device
	// last, once nothing references it.
	for _, enc := range p.encoders {
		enc.Close()
	}
	for _, g := range p.gpuScalers {
		if g != nil {
			g.Close()
		}
	}
	p.cuda.Free() // nil-safe
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
//
// SessionStart + forceKeyframe latch flags get consumed here on
// the first frame after SwitchInput so the next emitted OutputFrame
// from every rendition carries the discontinuity marker and rides
// an IDR keyframe.
func (p *StreamPipeline) encodeOne(f *astiav.Frame) ([]OutputFrame, error) {
	srcPTS := f.Pts()
	if srcPTS <= 0 {
		// AV-path frames now carry the real source PTS (B-2); this fallback
		// only fires for genuinely PTS-less frames. Space them by the
		// nominal frame duration, not the bare frame index, so the timeline
		// doesn't collapse to 1 ms/frame (~1000 fps).
		srcPTS = p.encoders[0].NextPTS() * p.videoFrameDurMs
	}
	// Rebase onto the continuous output clock so the encoder emits a
	// monotonic timeline that survives input switches — the source
	// PTS jumps on every switch and would otherwise stall the player.
	pts := p.rebaseVideoPTS(srcPTS)
	// forceIDR is consumed atomically with the per-rendition encode
	// loop — every encoder gets the I-frame hint on the SAME source
	// frame so all renditions land an IDR together. The hint is set
	// on the SCALED frame in encodeOneRendition (PictType doesn't
	// transit through the scaler).
	forceIDR := p.pendingForceKeyframe
	if forceIDR {
		p.pendingForceKeyframe = false
	}
	var out []OutputFrame
	for i := range p.encoders {
		pkts, err := p.encodeOneRendition(i, f, pts, forceIDR)
		if err != nil {
			return out, err
		}
		out = append(out, pkts...)
	}
	// Latch consumption deferred until at least one rendition emits a
	// packet — the first frame after SwitchInput often produces zero
	// output (encoder warm-up / B-frame reorder hold), and consuming
	// the flag eagerly would lose the discontinuity marker entirely.
	if p.pendingSessionStart && len(out) > 0 {
		p.pendingSessionStart = false
		for j := range out {
			out[j].SessionStart = true
		}
	}
	return out, nil
}

// encodeOneRendition runs one frame through rendition i's scaler +
// optional watermark + encoder and returns the encoded packets
// tagged with TargetIndex=i. PTS is set on the frame the encoder
// actually consumes — for the watermark path that's the filtered
// frame, since the filter graph passes PTS through and the encoder
// reads it back from there.
//
// forceIDR, when true, marks the encoder's input frame with
// PictureType=I AND its pkt_flags with PacketFlagKey so NVENC and
// libx264 both emit an IDR (libx264 honours pict_type directly;
// NVENC requires the forced-idr encoder option, set at Open time).
// The hint MUST land on the frame the encoder actually consumes —
// applying it to the pre-scaler frame loses the flag when sws_scale
// allocates a fresh output frame.
func (p *StreamPipeline) encodeOneRendition(i int, f *astiav.Frame, pts int64, forceIDR bool) ([]OutputFrame, error) {
	var (
		scaled *astiav.Frame
		err    error
	)
	if p.useGPU {
		scaled, err = p.gpuScalers[i].Scale(f) // VRAM-resident scale_cuda
	} else {
		scaled, err = p.scalers[i].Scale(f)
	}
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
	if forceIDR {
		toEncode.SetPictureType(astiav.PictureTypeI)
		toEncode.SetFlags(toEncode.Flags().Add(astiav.FrameFlagKey))
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
