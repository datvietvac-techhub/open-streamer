package dash

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Eyevinn/mp4ff/aac"
	"github.com/Eyevinn/mp4ff/avc"
	"github.com/Eyevinn/mp4ff/hevc"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/tsdemux"
	"github.com/datvietvac-techhub/open-streamer/internal/tsmux"
)

// packager.go — public Packager + Run loop.
//
// Lifecycle:
//
//  1. publisher.Service constructs a Packager via NewPackager and a
//     buffer.Subscriber for the stream.
//  2. Caller invokes Run(ctx) on its own goroutine. Run reads packets
//     from the subscriber, demuxes AV packets / TS chunks, fills the
//     frame queue, and on each cut writes fmp4 fragments + manifest.
//  3. ctx cancellation drains in-progress fragments and returns.
//
// One Packager instance serves either a single-rendition stream OR one
// shard of an ABR ladder. ABR coordination happens via the optional
// abrMaster; the master writes the root MPD, the per-shard packager
// writes ONLY media + (optionally) a per-shard MPD.

// Default constants — used when the caller passes zero in Config.
const (
	defaultPairingTimeout = 3 * time.Second
	defaultMaxSegFactor   = 3
	dashVideoMediaPattern = "seg_v_$Number%05d$.m4s"
	dashAudioMediaPattern = "seg_a_$Number%05d$.m4s"
	dashVideoInitFile     = "init_v.mp4"
	dashAudioInitFile     = "init_a.mp4"

	// videoPSAccumCap bounds the accumulator that holds VPS/SPS/PPS
	// NAL units (Annex-B framed) until BuildH264Init or BuildH265Init
	// succeeds. Sized to comfortably hold the relevant NAL units for
	// HEVC (VPS ~30 B + SPS ~80 B + PPS ~10 B) plus a 16× safety
	// margin — accumulator only ever stores SPS/PPS bytes after the
	// per-frame extractor was introduced; the cap is now a sanity
	// guard, never a normal limit. Before the extractor, every frame's
	// FULL Annex-B bytes were appended, so a single 1080p IDR
	// (100-200 KB) plus a few P-frames would burst past 1 MiB and the
	// packager would latch videoPSGivenUp, breaking 1080p ABR shards.
	videoPSAccumCap = 4 * 1024
)

// trackLossTimeout is how long a track may be silent (no frame arrival AND
// an empty queue) once the stream is Live before the segmenter treats it as
// dead and stops waiting for it. Without this, the init-presence flags
// (videoInit/audioInit) latch true for the session lifetime, so when one
// track dies mid-session the V/A duration coupling in buildCutDecision holds
// every cut forever (Ok=false) and the DASH live edge freezes. 6 s ≈ 2× the
// default pairing window — long enough to ride out a brief source stall, short
// enough that a genuine track death un-freezes the surviving track quickly.
// A package-level var (not const) so tests can shrink it. See liveTrackPresence.
var trackLossTimeout = 6 * time.Second

// Config carries the per-stream packager configuration.
type Config struct {
	// StreamID is used for log fields. Free-form; not used in any file path.
	StreamID string

	// StreamDir is the directory where media segments and (for single-
	// rendition streams) the per-stream manifest are written. The
	// directory is created with mkdir -p semantics if missing.
	StreamDir string

	// ManifestPath is the absolute path for the per-stream MPD file.
	// Leave empty for ABR shards — the ABR master writes the root MPD.
	ManifestPath string

	// SegDur is the target segment duration.
	SegDur time.Duration

	// Window is the number of segments held in the sliding manifest.
	Window int

	// History is the extra segments kept on disk past the manifest
	// window (0 = manifest window only).
	History int

	// Ephemeral controls whether old segments are deleted from disk
	// when they age past Window + History. False = keep forever
	// (DVR-style); true = delete (live-style).
	Ephemeral bool

	// PairingTimeout is the WaitingForPairing → Live timeout. Default 3 s.
	PairingTimeout time.Duration

	// ABR (optional): when set, the packager is one shard of a ladder.
	// ManifestPath becomes optional in this mode; the master writes the
	// root MPD via the snapshot pushed at every flush.
	ABRMaster *ABRMaster
	ABRSlug   string

	// PackAudio controls whether this shard emits audio segments.
	// In an ABR ladder, exactly ONE shard packs audio (typically the
	// best rendition); others set PackAudio=false. For single-rendition
	// streams pass true.
	PackAudio bool

	// OverrideBandwidth, when non-zero, replaces the per-rep bandwidth
	// value emitted in the manifest. Used by ABR ladders to declare
	// the transcoder's target bitrate rather than a measured one.
	OverrideBandwidth int

	// Metrics (all optional / nil-safe). Pre-bound by publisher.Service so the
	// dash package stays agnostic of the Prometheus registry.
	//   VideoFrames / AudioFrames — open_streamer_transcoder_frames_total per kind.
	//   SegCounter                — open_streamer_publisher_segments_total{format="dash"}.
	//   SegWriteDur               — open_streamer_publisher_segment_write_duration_seconds{format="dash"}.
	//   SegWriteErr               — open_streamer_publisher_segment_write_errors_total{format="dash"}.
	VideoFrames prometheus.Counter
	AudioFrames prometheus.Counter
	SegCounter  prometheus.Counter
	SegWriteDur prometheus.Observer
	SegWriteErr prometheus.Counter
}

// Packager is the per-stream (or per-shard) DASH publisher state.
type Packager struct {
	cfg Config

	mu        sync.Mutex
	state     *StateMachine
	queue     *FrameQueue
	seg       *Segmenter
	videoInit *VideoInit
	audioInit *AudioInit

	// videoPS accumulates the Annex-B byte stream until SPS+PPS are
	// available. Cleared once videoInit is built. videoPSGivenUp latches
	// when the accumulator exceeds videoPSAccumCap so the packager
	// drops trying to build a video init and proceeds audio-only.
	videoPS        []byte
	videoPSGivenUp bool
	isHEVC         bool

	availStart time.Time
	vSegN      uint64
	aSegN      uint64

	// lastVideoFrameAt / lastAudioFrameAt record the wallclock arrival of
	// the most recent accepted frame on each track. Once the stream is
	// Live, liveTrackPresence uses these (plus queue depth) to declare a
	// silent track dead after trackLossTimeout so the surviving track keeps
	// cutting instead of freezing on the dead track's missing frames (A-8).
	lastVideoFrameAt time.Time
	lastAudioFrameAt time.Time

	// pairingTruncated latches after truncateAtPairingLocked runs so it
	// fires exactly once per session lifetime. Without this latch, a
	// stream whose first Cut returns Ok=false (insufficient frames) would
	// stay in WaitingForPairing across ticks; truncateAtPairingLocked
	// would re-fire on every tick, re-thresholding against the now-
	// older first-frame PTSms and dropping whichever track keeps
	// arriving slower — an infinite-drop loop observed on ABR streams
	// where V and A timeline axes don't perfectly match.
	pairingTruncated bool

	// Sliding-window state for manifest.
	onDiskV     []string
	onDiskA     []string
	vSegEntries []SegmentEntry
	aSegEntries []SegmentEntry

	// Overflow-drop counters surface whenever PushVideo / PushAudio hit
	// the queue cap and shed the oldest frame to bound memory. Logged
	// at first occurrence and every 1000 drops so operators see when
	// the packager's segmenter has fallen permanently behind.
	videoOverflowDropCount int
	audioOverflowDropCount int

	// Out-of-order push counters: incremented when PushVideo / PushAudio
	// receives a frame whose PTSms is strictly less than the queue's
	// previous tail. Diagnostic for hypothesis-1 (audioCountAtOrBefore
	// sequential-stop scan misses qualifying frames after an OOO frame
	// at the front).
	videoOOOPushCount int
	audioOOOPushCount int

	// videoFrameSeen / audioFrameSeen latch true on the FIRST handleH264
	// / handleAAC call respectively. Used by tryCut to distinguish
	// "track frames flowing but init not yet built" (wait for init) vs
	// "track genuinely absent" (allow pairing-timeout single-track
	// emit). Without this, a stream whose video frames arrive seconds
	// before SPS/PPS extraction completes (file://MP4 with split init
	// header, or any source where the first IDR is delayed past the
	// 3 s pairing window) emits an audio segment alone at timeout,
	// permanently offsetting A's segN from V's segN — observed on
	// test2 as a 5.4 s lip-sync drift even after V/A duration
	// coupling fixed the per-cut math.
	videoFrameSeen bool
	audioFrameSeen bool

	// firstIDRSeen latches true on the first H.264 IDR (or HEVC IRAP)
	// pushed into the video queue. Until set, handleH264 DROPS non-key
	// frames so segment 0's first sample is a SAP — DASH manifests
	// declare startWithSAP="1" and strict players reject any segment
	// whose first sample isn't an IDR (Safari/VideoToolbox returns
	// MEDIA_ERR_DECODE -12909). SPS/PPS accumulation still runs on
	// every dropped frame so the init segment can build from
	// pre-IDR parameter sets if the source delivers them that way
	// (RTMP AVCDecoderConfigurationRecord, MP4 stsd).
	firstIDRSeen bool

	// Cut-time diagnostic counters and timestamps. starvedAudioCutCount
	// fires when a Cut decision returned Ok=true but AudioCount=0 while
	// the queue has audio frames (smoking gun for hypothesis 1 / 2).
	// lastDiagLogAt rate-limits the diagnostic log to ≤ once per second
	// per packager — tryCut runs every 50 ms, raw logs would flood.
	starvedAudioCutCount int
	lastDiagLogAt        time.Time
}

// NewPackager constructs a Packager from cfg. Validates required
// fields; missing values fall to defaults.
func NewPackager(cfg Config) (*Packager, error) {
	if cfg.StreamDir == "" {
		return nil, errors.New("dash packager: StreamDir is required")
	}
	if cfg.SegDur <= 0 {
		cfg.SegDur = 2 * time.Second
	}
	if cfg.Window <= 0 {
		cfg.Window = 6
	}
	if cfg.PairingTimeout <= 0 {
		cfg.PairingTimeout = defaultPairingTimeout
	}
	if err := os.MkdirAll(cfg.StreamDir, 0o755); err != nil {
		return nil, fmt.Errorf("dash packager: mkdir stream dir: %w", err)
	}
	return &Packager{
		cfg:   cfg,
		state: NewStateMachine(cfg.PairingTimeout),
		queue: NewFrameQueue(),
		seg:   NewSegmenter(cfg.SegDur, defaultMaxSegFactor),
	}, nil
}

// Run drives the run loop. Returns on ctx cancellation, subscriber close,
// or unrecoverable I/O error. Caller invokes on its own goroutine.
//
// Raw-TS sources (UDP / HLS pull / HTTP-MPEG-TS / SRT pull / file pull /
// transcoder output) feed bytes via pkt.TS; an internal demuxer
// goroutine converts them into AV frames and dispatches through the
// SAME handleH264 / handleAAC paths the direct-AV path uses. The
// demuxer is lazily started on the first TS packet so AV-only streams
// pay no goroutine cost.
func (p *Packager) Run(ctx context.Context, sub *buffer.Subscriber) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var tsCarry []byte
	var tb *tsBuffer
	defer func() {
		if tb != nil {
			tb.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			p.finalFlush()
			return
		case pkt, ok := <-sub.Recv():
			if !ok {
				p.finalFlush()
				return
			}
			p.onPacket(ctx, pkt, &tsCarry, &tb)
		case <-ticker.C:
			p.tryCut(time.Now())
		}
	}
}

// onPacket dispatches one buffer.Packet through the appropriate path.
// tsCarry + tb belong to Run's local state — passed by reference so
// the demuxer is lazily started on the FIRST TS packet rather than
// always-on (AV-only streams pay nothing).
func (p *Packager) onPacket(ctx context.Context, pkt buffer.Packet, tsCarry *[]byte, tb **tsBuffer) {
	if pkt.SessionStart {
		p.onSessionBoundary()
	}
	switch {
	case pkt.AV != nil && len(pkt.AV.Data) > 0:
		p.onAVPacket(pkt.AV)
	case len(pkt.TS) > 0:
		if *tb == nil {
			*tb = newTSBuffer(p.cfg.StreamID)
			startDemuxer(ctx, *tb, p.onTSFrame)
		}
		aligned := alignTS(tsCarry, pkt.TS)
		if len(aligned) > 0 {
			_, _ = (*tb).Write(aligned)
		}
	}
}

// onTSFrame is invoked by the demuxer goroutine for every extracted
// access unit. Routes to the same handle* methods the direct AV-path
// uses, so init-segment building and queue management are codec-
// agnostic to which input path delivered the frame.
//
// Runs on the demuxer goroutine — handleH264 / handleAAC acquire
// p.mu inside, so concurrency with onAVPacket (run-loop goroutine) is
// safe.
//
//nolint:exhaustive // unsupported codecs (PCM / MPEG audio L1/L2 / AC-3) intentionally drop
func (p *Packager) onTSFrame(cid tsdemux.StreamType, frame []byte, pts, dts uint64) {
	if len(frame) == 0 {
		return
	}
	av := &domain.AVPacket{
		Data:     frame,
		PTSms:    pts,
		DTSms:    dts,
		KeyFrame: false,
	}
	switch cid {
	case tsdemux.StreamTypeH264:
		av.Codec = domain.AVCodecH264
		av.KeyFrame = tsmux.KeyFrameH264(frame)
		p.onAVPacket(av)
	case tsdemux.StreamTypeH265:
		av.Codec = domain.AVCodecH265
		av.KeyFrame = tsmux.KeyFrameH265(frame)
		p.onAVPacket(av)
	case tsdemux.StreamTypeAAC:
		av.Codec = domain.AVCodecAAC
		p.onAVPacket(av)
	}
}

// onAVPacket processes an AV packet from the Normaliser-anchored path
// (RTSP / RTMP / mixer source). Updates init segments + queue.
func (p *Packager) onAVPacket(av *domain.AVPacket) {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch av.Codec { //nolint:exhaustive // unsupported codecs (mp2, mp3, ac3, eac3, av1, mpeg2video, raw-TS marker, unknown) intentionally drop — DASH packager handles H.264 / H.265 / AAC only
	case domain.AVCodecH264:
		p.handleH264(av)
	case domain.AVCodecH265:
		p.isHEVC = true
		p.handleH264(av) // same code path; isHEVC flips downstream behaviour
	case domain.AVCodecAAC:
		p.handleAAC(av)
	}
}

// handleH264 enqueues an H.264 access unit and tries to build the video
// init segment if it isn't already built.
//
// IDR-startup gate: drops every non-key frame until the first IDR
// arrives. Without this, cold-start frames captured mid-GOP enter
// the queue and segment 0 ends up starting with a P/B slice — strict
// hardware decoders (Safari/VT, Chrome-on-Mac VideoToolbox) reject
// such segments with MEDIA_ERR_DECODE -12909. SPS/PPS extraction
// still runs on every dropped frame so the init segment can build
// from any in-band parameter sets that arrive before the first IDR.
func (p *Packager) handleH264(av *domain.AVPacket) {
	p.videoFrameSeen = true
	if p.videoInit == nil && !p.videoPSGivenUp {
		p.accumulateVideoPS(av.Data)
		p.tryBuildVideoInit()
	}
	if !p.firstIDRSeen {
		if !av.KeyFrame {
			return
		}
		p.firstIDRSeen = true
	}
	// Count accepted video frames at ingress (post IDR-gate) — drives
	// open_streamer_transcoder_frames_total{kind="video"}.
	if p.cfg.VideoFrames != nil {
		p.cfg.VideoFrames.Inc()
	}
	p.pushVideoWithDiag(VideoFrame{
		AnnexB: cloneBytes(av.Data),
		PTSms:  av.PTSms,
		DTSms:  av.DTSms,
		IsIDR:  av.KeyFrame,
	})
	p.lastVideoFrameAt = time.Now()
	if p.videoInit != nil {
		p.state.OpenPairingWindow(time.Now())
	}
}

// pushVideoWithDiag is the video counterpart of pushAudioWithDiag.
// Persistent overflow drops indicate the segmenter is unable to cut
// (typically the pacing gate is permanently behind because cumulative
// per-segment dur has drifted ahead of wallclock); MPD timeline
// integrity is already lost at that point and the drop is the lesser
// evil. OOO pushes signal an upstream Normaliser anomaly.
func (p *Packager) pushVideoWithDiag(f VideoFrame) {
	dropped, ooo := p.queue.PushVideo(f)
	if dropped > 0 {
		p.videoOverflowDropCount += dropped
		if p.videoOverflowDropCount == 1 || p.videoOverflowDropCount%1000 == 0 {
			slog.Warn("dash: video frame queue overflow — dropping oldest",
				"stream_id", p.cfg.StreamID,
				"total_dropped", p.videoOverflowDropCount,
				"queue_after_drop", p.queue.VideoLen(),
			)
		}
	}
	if ooo {
		p.videoOOOPushCount++
		if p.videoOOOPushCount == 1 || p.videoOOOPushCount%100 == 0 {
			slog.Warn("dash: video OOO push (PTSms regressed vs queue tail)",
				"stream_id", p.cfg.StreamID,
				"total_ooo", p.videoOOOPushCount,
				"frame_pts_ms", f.PTSms,
				"is_idr", f.IsIDR,
			)
		}
	}
}

// handleAAC enqueues AAC access units and tries to build the audio
// init segment from the first ADTS header observed.
//
// Bundled-PES splitting: a single AAC PES from gomedia's TSDemuxer
// (raw-TS path) or from a buffer.Packet carrying a multi-frame ADTS
// payload (AV path under bursty source pacing) commonly contains
// 4–8 access units. splitADTSBundle splits them and assigns
// per-frame PTS — see splitADTSBundle's docstring for why.
//
// Non-pack shards skip the entire AAC path. In ABR mode at most one
// shard has PackAudio=true; the others receive AAC frames from the
// shared buffer hub but never emit audio segments. Without this
// early return the queue would accumulate every frame indefinitely
// (Cut returns AudioCount=0 when audioSR=0, so the drain branch
// at writeSegments is unreachable), eventually firing "audio frame
// queue overflow — dropping oldest" warnings while wasting CPU on
// ADTS splitting and init-segment I/O.
func (p *Packager) handleAAC(av *domain.AVPacket) {
	if !p.cfg.PackAudio {
		return
	}
	p.audioFrameSeen = true
	frames := splitADTSBundle(av.Data, av.PTSms)
	if len(frames) == 0 {
		return
	}
	if p.audioInit == nil {
		if hdr := parseADTS(av.Data); hdr != nil {
			ai, err := BuildAACInit(hdr)
			if err == nil {
				p.audioInit = ai
				if data, encErr := EncodeInit(ai.Init); encErr == nil {
					_ = writeFileAtomic(filepath.Join(p.cfg.StreamDir, dashAudioInitFile), data)
				}
			} else {
				slog.Warn("dash: BuildAACInit", "stream_id", p.cfg.StreamID, "err", err)
			}
		}
	}
	for _, f := range frames {
		p.pushAudioWithDiag(f)
	}
	p.lastAudioFrameAt = time.Now()
	if p.audioInit != nil {
		p.state.OpenPairingWindow(time.Now())
	}
}

// pushAudioWithDiag pushes one audio frame to the queue and updates
// the overflow + OOO diagnostic counters. Extracted from handleAAC so
// the parent stays under the cognitive-complexity ceiling.
func (p *Packager) pushAudioWithDiag(f AudioFrame) {
	// Count each split AAC access unit — open_streamer_transcoder_frames_total{kind="audio"}.
	if p.cfg.AudioFrames != nil {
		p.cfg.AudioFrames.Inc()
	}
	dropped, ooo := p.queue.PushAudio(f)
	if dropped > 0 {
		p.audioOverflowDropCount += dropped
		if p.audioOverflowDropCount == 1 || p.audioOverflowDropCount%1000 == 0 {
			slog.Warn("dash: audio frame queue overflow — dropping oldest",
				"stream_id", p.cfg.StreamID,
				"total_dropped", p.audioOverflowDropCount,
				"queue_after_drop", p.queue.AudioLen(),
			)
		}
	}
	if ooo {
		p.audioOOOPushCount++
		if p.audioOOOPushCount == 1 || p.audioOOOPushCount%100 == 0 {
			slog.Warn("dash: audio OOO push (PTSms regressed vs queue tail)",
				"stream_id", p.cfg.StreamID,
				"total_ooo", p.audioOOOPushCount,
				"frame_pts_ms", f.PTSms,
			)
		}
	}
}

// accumulateVideoPS scans this frame's Annex-B bytes for parameter-set
// NAL units (SPS+PPS for H.264; VPS+SPS+PPS for H.265) and appends
// only THOSE NAL units to p.videoPS — not the full frame bytes.
//
// Rationale: a 1080p H.264 IDR is 100-200 KB; a few P-frames between
// IDR delivery can push the accumulator past the cap before SPS/PPS
// arrive (root cause of the stream_a/track_1 + test_copy/track_1
// videoPSGivenUp failure observed on production — same source, same
// transcoder, only the 1080p shard failed because its frame bytes
// were larger). The extractor keeps videoPS tiny (~hundreds of bytes
// of NAL data) so cap pressure never fires under normal operation.
//
// Cap is still enforced as an OOM safety net for a degenerate stream
// that emits SPS/PPS-shaped garbage every frame.
func (p *Packager) accumulateVideoPS(annexB []byte) {
	psBuf := annexB4To3(annexB)
	var nals [][]byte
	if p.isHEVC {
		vpss, spss, ppss := hevc.GetParameterSetsFromByteStream(psBuf)
		nals = append(nals, vpss...)
		nals = append(nals, spss...)
		nals = append(nals, ppss...)
	} else {
		spss := avc.ExtractNalusOfTypeFromByteStream(avc.NALU_SPS, psBuf, false)
		ppss := avc.ExtractNalusOfTypeFromByteStream(avc.NALU_PPS, psBuf, false)
		nals = append(nals, spss...)
		nals = append(nals, ppss...)
	}
	for _, n := range nals {
		// Append in 4-byte Annex-B framed form so ExtractParameterSets
		// (which expects an Annex-B byte stream) finds them later.
		p.videoPS = append(p.videoPS, 0x00, 0x00, 0x00, 0x01)
		p.videoPS = append(p.videoPS, n...)
	}
	if len(p.videoPS) > videoPSAccumCap {
		p.videoPSGivenUp = true
		slog.Warn("dash: videoPS cap exceeded without init — giving up video",
			"stream_id", p.cfg.StreamID,
			"cap_bytes", videoPSAccumCap,
			"accumulator_len", len(p.videoPS),
		)
	}
}

// tryBuildVideoInit attempts to build the H.264 or H.265 init segment
// from the accumulated SPS/PPS buffer. Writes init_v.mp4 to disk on
// success and clears the accumulator.
func (p *Packager) tryBuildVideoInit() {
	if p.isHEVC {
		vpss, spss, ppss := ExtractHEVCParameterSets(p.videoPS)
		if len(spss) == 0 || len(ppss) == 0 {
			return
		}
		vi, err := BuildH265Init(vpss, spss, ppss)
		if err != nil {
			slog.Warn("dash: BuildH265Init", "stream_id", p.cfg.StreamID, "err", err)
			return
		}
		p.installVideoInit(vi)
		return
	}
	spss, ppss := ExtractParameterSets(p.videoPS)
	if len(spss) == 0 || len(ppss) == 0 {
		return
	}
	vi, err := BuildH264Init(spss, ppss)
	if err != nil {
		slog.Warn("dash: BuildH264Init", "stream_id", p.cfg.StreamID, "err", err)
		return
	}
	p.installVideoInit(vi)
}

func (p *Packager) installVideoInit(vi *VideoInit) {
	p.videoInit = vi
	p.videoPS = nil
	data, err := EncodeInit(vi.Init)
	if err != nil {
		slog.Warn("dash: EncodeInit video", "stream_id", p.cfg.StreamID, "err", err)
		return
	}
	_ = writeFileAtomic(filepath.Join(p.cfg.StreamDir, dashVideoInitFile), data)
}

// onSessionBoundary handles a buffer.Packet with SessionStart=true.
// Flushes whatever's queued, drops the queue, signals the state machine.
//
// Per-track decode counters are preserved across the boundary so the
// MPD timeline stays monotonic — players keep advancing through the
// same SegmentTimeline without seeking back. Segment numbers also
// survive for the same reason.
//
// pairingTruncated is NOT reset — a session boundary mid-stream isn't a
// "fresh pairing window"; both tracks are already advancing and the
// post-boundary content joins their existing timelines. Resetting the
// latch would re-engage pairing-truncate and drop incoming frames
// while waiting for the pairing condition to re-fire.
func (p *Packager) onSessionBoundary() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.OnSessionStart()
	// Drop queues — old session content shouldn't bleed into new session
	// segments. Init segments + AST + segment counters + nextDecode
	// counters survive.
	_ = p.queue.PopVideo(p.queue.VideoLen())
	_ = p.queue.PopAudio(p.queue.AudioLen())
	p.seg.Reset()
	p.state.OnSessionBoundaryHandled()
}

// liveTrackPresence reports which tracks the segmenter should still wait
// for. The base answer is init-presence (a track exists once its init
// segment is built), but a track that has built its init can later die
// mid-session — the upstream stops delivering one elementary stream while
// the other keeps flowing. The init pointer never clears, so without this
// the V/A coupling in buildCutDecision would hold every cut forever and
// freeze the live edge (A-8).
//
// We only down-grade once the stream is Live: during WaitingForPairing the
// init-presence flags drive the pairing handshake (and queues are still
// filling), and during a SessionBoundary the queue was just reset so the
// arrival timestamps are intentionally stale. A track counts as alive while
// it has queued frames OR delivered one within trackLossTimeout; past that,
// with an empty queue, it is declared dead and the surviving track resumes
// cutting (audio death → video-only cuts; video death → cutAudioOnly). The
// flag flips back automatically when the track resumes (handlers re-stamp
// lastXFrameAt), and tfdt stays wallclock-anchored so the resumed segment
// lands at the right live-edge position with no extra re-anchoring.
func (p *Packager) liveTrackPresence(now time.Time) (haveVideo, haveAudio bool) {
	haveVideo = p.videoInit != nil
	haveAudio = p.audioInit != nil
	if p.state.State() != StateLive {
		return haveVideo, haveAudio
	}
	if haveVideo && p.queue.VideoLen() == 0 && now.Sub(p.lastVideoFrameAt) >= trackLossTimeout {
		haveVideo = false
	}
	if haveAudio && p.queue.AudioLen() == 0 && now.Sub(p.lastAudioFrameAt) >= trackLossTimeout {
		haveAudio = false
	}
	return haveVideo, haveAudio
}

// tryCut checks the segmenter at every tick. Holds the mutex throughout
// — segmenter decisions + writes are short and locking the entire path
// avoids state-tearing under concurrent onPacket.
func (p *Packager) tryCut(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.videoInit == nil && p.audioInit == nil {
		return
	}

	haveVideo, haveAudio := p.liveTrackPresence(now)

	videoReady := haveVideo && p.queue.VideoLen() > 0
	// A shard that doesn't pack audio (an ABR non-primary rendition) has no
	// audio init and never will, so it must not wait out the pairing window for
	// audio that can't arrive — it pairs on video alone and emits at the first
	// IDR, in lockstep with the audio shard. Without this, non-audio shards only
	// flush at the pairing deadline, anchoring their availStart ~1 s later than
	// the audio shard and skewing the ladder AST + per-shard pacing (B-4).
	audioReady := !p.cfg.PackAudio || (haveAudio && p.queue.AudioLen() > 0)

	if !p.state.CanEmitFirstSegment(now, videoReady, audioReady) {
		return
	}

	// Defer pairing-timeout single-track emission while a track is
	// still being received but its init hasn't built yet. Without this,
	// for a V+A stream whose video frames arrive but SPS/PPS extraction
	// takes longer than the 3 s pairing window, the timeout fallback
	// would emit an audio-only segment first — A's segN advances, V's
	// doesn't, and the segN divergence baked into the master MPD
	// causes browser players to play A and V from different media-times
	// for the rest of the session (test2 saw 5.4 s lip-sync drift here).
	//
	// videoPSGivenUp latches when SPS/PPS extraction permanently fails;
	// only then do we accept the stream as "video genuinely absent" and
	// allow audio-only emission.
	if p.state.State() == StateWaitingForPairing {
		if p.videoFrameSeen && !haveVideo && !p.videoPSGivenUp {
			return
		}
		if p.audioFrameSeen && !haveAudio {
			return
		}
	}

	// First-segment moment: align queues so V and A start at the LATER
	// of the two firsts. This drops the earlier-arrived track's pre-
	// pairing accumulation (e.g. 4 s of audio that flowed while the
	// NVENC pipeline warmed up, or 16 s of mixer-drained V frames). The
	// pre-pairing content is lost, but media-time anchors stay aligned
	// — no perpetual A/V drift in the published timeline.
	//
	// Latch via pairingTruncated so this runs exactly once per session.
	// Otherwise, when Cut returns Ok=false on the post-truncate queue
	// (insufficient video), we'd loop and re-truncate every tick,
	// dropping each newly-arrived V frame because A's first PTS stays
	// ahead — observed on ABR streams as audio outrunning video by
	// tens of seconds in the live-edge MPD timeline.
	if !p.pairingTruncated && p.state.State() == StateWaitingForPairing && haveVideo && haveAudio {
		p.truncateAtPairingLocked()
		p.pairingTruncated = true
	}

	// DASH timeline-pace gate. tfdt is wallclock-anchored
	// (wallclockTicks(now, AST)) but per-segment dur is the frame-PTS
	// span. The two stay coherent ONLY when wallclock-since-last-emit
	// matches the media span emitted into that segment. Raw-TS sources
	// bypass the ingest rebaser, so they deliver chunks in bursts —
	// without this gate, two consecutive cuts within a chunk burst
	// produce <S> entries whose [t, t+d] ranges overlap (strict players
	// reject the MPD). Holding here lets the queue absorb the burst;
	// emit resumes once wallclock catches up to the previous segment's
	// end, draining the queue at wallclock pace across many cuts.
	if p.behindPrevSegEnd(now) {
		return
	}

	// Pass audioSR to the segmenter so V/A duration coupling works:
	// the segmenter computes targetA = round(V_dur_ms × sr / 1024 / 1000)
	// and either pops exactly that many audio frames (= V dur matches
	// A dur per cut) or holds the cut until audio catches up. Skip the
	// coupling (audioSR=0) when this shard doesn't pack audio — those
	// shards' audio frames are popped & discarded, so the count-by-PTS
	// legacy path is fine and we don't want to hold V cuts on missing
	// audio.
	var audioSR uint64
	if p.audioInit != nil && p.cfg.PackAudio {
		audioSR = uint64(p.audioInit.SampleRate) //nolint:gosec // SampleRate > 0
	}
	d := p.seg.Cut(now, p.queue, haveVideo, haveAudio, audioSR)
	if !d.Ok {
		return
	}

	p.diagAudioStarvation(now, d, haveAudio)

	flushed := p.writeSegments(now, d)
	if flushed {
		p.seg.MarkCut(now)
		p.state.OnFirstSegmentFlushed()
		if p.availStart.IsZero() {
			p.availStart = now
			// For an ABR ladder, anchor every shard to ONE shared
			// availabilityStart (set by whichever shard flushes first) so the
			// published AST is correct for all renditions, behindPrevSegEnd
			// paces them against the same origin, and the dynamic-MPD AST never
			// changes mid-stream regardless of flush order (B-4). Single-
			// rendition streams have no master and keep their own `now`.
			if p.cfg.ABRMaster != nil {
				p.availStart = p.cfg.ABRMaster.SharedAST(now)
			}
		}
		// Safety-net cuts drain the entire video queue without landing
		// on an IDR boundary, so the next pushed frame could be a P/B
		// slice that would re-contaminate segment N+1. Re-arm the
		// startup gate so handleH264 drops non-key frames until the
		// next IDR arrives, restoring SAP alignment at the cost of a
		// few hundred ms of skipped frames.
		if !d.IsIDRAligned {
			p.firstIDRSeen = false
		}
		p.trimWindow()
		p.publishManifest(now)
	}
}

// diagAudioStarvation logs a queue snapshot whenever a Cut decision
// included video but excluded all audio despite frames being queued —
// the smoking gun for the stream_a_raw / test1 / test5 audio-dead bug.
// Rate-limited to ≤ 1 log/sec/packager so the 50 ms tryCut cadence
// can't flood the journal. Caller holds p.mu.
//
// Outputs include FirstAudio.PTSms, AudioTail.PTSms, FirstVideo.PTSms,
// VideoTail.PTSms, plus skippedQualifying — the count of audio frames
// AFTER the cut boundary whose PTSms also satisfied the threshold but
// were missed by audioCountAtOrBefore's sequential-stop scan. When
// skippedQualifying > 0, hypothesis 1 (queue non-monotonic) is
// confirmed; when 0 with FirstAudio.PTSms > FirstVideo.PTSms+segDur,
// hypothesis 2 (video PTS regression) is more likely.
func (p *Packager) diagAudioStarvation(now time.Time, d CutDecision, haveAudio bool) {
	if !haveAudio || d.AudioCount > 0 || p.queue.AudioLen() == 0 {
		return
	}
	p.starvedAudioCutCount++
	if !p.lastDiagLogAt.IsZero() && now.Sub(p.lastDiagLogAt) < time.Second {
		return
	}
	p.lastDiagLogAt = now
	aFirst, _ := p.queue.FirstAudio()
	aTail, _ := p.queue.AudioTailPTSms()
	vFirst, _ := p.queue.FirstVideo()
	vTail, _ := p.queue.VideoTailPTSms()
	// Re-derive a lower-bound cutPTSms_video without re-running the
	// segmenter: the SegDur target sits at vFirst+SegDur, and Cut's
	// audio-count uses the PTS of the chosen IDR which is ≥ vFirst+SegDur.
	threshold := vFirst.PTSms + uint64(p.cfg.SegDur.Milliseconds()) //nolint:gosec
	_, skipped := p.queue.audioCountAtOrBeforeWithSkip(threshold)
	slog.Warn("dash: cut excluded all audio despite queue",
		"stream_id", p.cfg.StreamID,
		"starved_count", p.starvedAudioCutCount,
		"audio_len", p.queue.AudioLen(),
		"audio_first_ms", aFirst.PTSms,
		"audio_tail_ms", aTail,
		"video_len", p.queue.VideoLen(),
		"video_first_ms", vFirst.PTSms,
		"video_tail_ms", vTail,
		"video_count_in_decision", d.VideoCount,
		"audio_skipped_qualifying", skipped,
		"pairing_truncated", p.pairingTruncated,
	)
}

// truncateAtPairingLocked drops frames whose PTSms is earlier than the
// later track's first frame. Called once at WaitingForPairing → Live
// transition so the first emitted segment for V and A starts at the
// SAME media-time anchor. Caller must hold p.mu.
func (p *Packager) truncateAtPairingLocked() {
	vFirst, ok1 := p.queue.FirstVideo()
	aFirst, ok2 := p.queue.FirstAudio()
	if !ok1 || !ok2 {
		return
	}
	threshold := vFirst.PTSms
	if aFirst.PTSms > threshold {
		threshold = aFirst.PTSms
	}
	if vd, ad := p.queue.TruncateBefore(threshold); vd > 0 || ad > 0 {
		slog.Info("dash: pairing-truncate",
			"stream_id", p.cfg.StreamID,
			"threshold_ms", threshold,
			"video_dropped", vd,
			"audio_dropped", ad,
		)
	}
}

// writeSegments drains the cut decision's frames, builds fmp4 fragments,
// and writes them to disk. Returns true if at least one segment was
// written (used by caller to gate AST/state advancement).
func (p *Packager) writeSegments(now time.Time, d CutDecision) bool {
	flushed := false

	if d.VideoCount > 0 && p.videoInit != nil {
		// Peek the frame immediately after the last drained one so dur
		// math can cover [first.PTS, next.PTS] instead of [first.PTS,
		// last.PTS] — the latter under-reports by one inter-frame
		// interval and bakes a per-segment gap into the MPD timeline
		// that players render as a 1-frame stutter at every boundary
		// (observed on staging: video dur 5.96 s with gap +0.04 s,
		// matching the 40 ms inter-frame interval at 25 fps).
		nextPTSms, hasNext := p.queue.videoPTSAt(d.VideoCount)
		frames := p.queue.PopVideo(d.VideoCount)
		if len(frames) > 0 {
			if p.writeVideoSegment(now, frames, nextPTSms, hasNext) {
				flushed = true
			}
		}
	}
	if d.AudioCount > 0 && p.audioInit != nil && p.cfg.PackAudio {
		frames := p.queue.PopAudio(d.AudioCount)
		if len(frames) > 0 {
			if p.writeAudioSegment(now, frames) {
				flushed = true
			}
		} else if d.AudioCount > 0 {
			// d said drain N but the queue had nothing (e.g. audio-only
			// stream emptied between Cut and Pop). Still a benign case.
			_ = frames
		}
	} else if d.AudioCount > 0 && !p.cfg.PackAudio {
		// Non-primary ABR shard: drop the audio frames since the
		// primary shard packs audio for the whole ladder.
		_ = p.queue.PopAudio(d.AudioCount)
	}

	return flushed
}

// writeVideoSegment serialises one .m4s file and updates window state.
// Returns true on success.
//
// tfdt anchors:
//
//   - Segment 0: tfdt = wallclockTicks(now, AST) — pins the MPD timeline
//     onto the wallclock at the very first emit so DASH live-edge math
//     (live_edge_media_time = (now − AST) − liveDelay) has a meaningful
//     starting point.
//   - Segment N>0: tfdt = previous segment's end = prev.StartTicks +
//     prev.DurTicks. Sequential on media-time. This keeps adjacent
//     segments contiguous in MPD timeline (no inter-segment gap from
//     the 50 ms run-loop tick granularity that wallclock-anchored tfdt
//     would inherit — that gap rendered as a per-segment 1-frame
//     stutter on player). The pacing gate in tryCut already ensures
//     emits never run ahead of wallclock, so sequential tfdt can only
//     LAG wallclock (when source < realtime), never overshoot.
//
// The earlier "sequential accumulator" approach the package tried,
// which interacted catastrophically with audio under-emission, set
// tfdt = sum of all previous durs INDEPENDENTLY for each track —
// audio-frame loss caused audio dur sums to fall behind wallclock and
// the audio MPD timeline drifted >100 s behind video. The fix in this
// commit pairs sequential tfdt with splitADTSBundle (audio frames now
// emit at full source rate) and the dur-via-next-PTS math (no
// per-segment under-report), so the chronic divergence that motivated
// the wallclock-anchor revert is gone.
func (p *Packager) writeVideoSegment(now time.Time, frames []VideoFrame, nextPTSms uint64, hasNext bool) bool {
	p.vSegN++
	name := fmt.Sprintf("seg_v_%05d.m4s", p.vSegN)

	tfdt := videoTfdtForSegment(p.vSegEntries, now, p.availStart)
	segDurTicks := computeVideoSegDurTicks(frames, nextPTSms, hasNext)

	data, err := BuildVideoFragment(uint32(p.vSegN), p.videoInit.TrackID, tfdt, frames, p.isHEVC, segDurTicks) //nolint:gosec // segN fits uint32 for the stream's lifetime
	if err != nil {
		slog.Warn("dash: BuildVideoFragment", "stream_id", p.cfg.StreamID, "err", err)
		p.vSegN--
		return false
	}
	start := time.Now()
	werr := writeFileAtomic(filepath.Join(p.cfg.StreamDir, name), data)
	p.recordSegWrite(werr == nil, time.Since(start))
	if werr != nil {
		slog.Warn("dash: write video segment", "stream_id", p.cfg.StreamID, "err", werr)
		p.vSegN--
		return false
	}
	p.onDiskV = append(p.onDiskV, name)
	p.vSegEntries = append(p.vSegEntries, SegmentEntry{StartTicks: tfdt, DurTicks: segDurTicks})
	return true
}

// writeAudioSegment is the audio counterpart of writeVideoSegment.
// Uses the same sequential-after-first tfdt anchor as video; see
// writeVideoSegment's docstring for why.
func (p *Packager) writeAudioSegment(now time.Time, frames []AudioFrame) bool {
	p.aSegN++
	name := fmt.Sprintf("seg_a_%05d.m4s", p.aSegN)

	sr := uint64(p.audioInit.SampleRate) //nolint:gosec // SampleRate > 0
	tfdt := audioTfdtForSegment(p.aSegEntries, now, p.availStart, sr)
	durTicks := uint64(len(frames)) * 1024 // AAC: 1024 samples per frame

	data, err := BuildAudioFragment(uint32(p.aSegN), p.audioInit.TrackID, tfdt, frames) //nolint:gosec // segN fits uint32
	if err != nil {
		slog.Warn("dash: BuildAudioFragment", "stream_id", p.cfg.StreamID, "err", err)
		p.aSegN--
		return false
	}
	start := time.Now()
	werr := writeFileAtomic(filepath.Join(p.cfg.StreamDir, name), data)
	p.recordSegWrite(werr == nil, time.Since(start))
	if werr != nil {
		slog.Warn("dash: write audio segment", "stream_id", p.cfg.StreamID, "err", werr)
		p.aSegN--
		return false
	}
	p.onDiskA = append(p.onDiskA, name)
	p.aSegEntries = append(p.aSegEntries, SegmentEntry{StartTicks: tfdt, DurTicks: durTicks})
	return true
}

// recordSegWrite reports one segment-file write outcome to the (nil-safe)
// metrics: on success it counts the segment and observes the write latency;
// on failure it bumps the write-error counter (and does NOT pollute the
// latency histogram). Shared by the video and audio segment writers so both
// the HLS and DASH paths report symmetric publisher metrics.
func (p *Packager) recordSegWrite(ok bool, dur time.Duration) {
	if !ok {
		if p.cfg.SegWriteErr != nil {
			p.cfg.SegWriteErr.Inc()
		}
		return
	}
	if p.cfg.SegWriteDur != nil {
		p.cfg.SegWriteDur.Observe(dur.Seconds())
	}
	if p.cfg.SegCounter != nil {
		p.cfg.SegCounter.Inc()
	}
}

// behindPrevSegEnd reports whether emitting NOW would create a segment
// whose tfdt (= wallclockTicks(now, AST)) lands BEFORE the previous
// segment's end (= prev.StartTicks + prev.DurTicks). When true, the
// caller must hold the cut — emitting would bake an overlap into the
// MPD's <S t=...> entries. Checks both video and audio tracks; either
// being behind is enough to hold.
//
// availStart-zero (first cut ever) returns false: there's nothing to
// overlap with yet, and we want the first segment to anchor the timeline.
//
// Caller must hold p.mu.
func (p *Packager) behindPrevSegEnd(now time.Time) bool {
	if p.availStart.IsZero() {
		return false
	}
	if n := len(p.vSegEntries); n > 0 {
		last := p.vSegEntries[n-1]
		prevEndTicks := last.StartTicks + last.DurTicks
		nowTicks := wallclockTicks(now, p.availStart, uint64(VideoTimescale))
		if nowTicks < prevEndTicks {
			return true
		}
	}
	if n := len(p.aSegEntries); n > 0 && p.audioInit != nil {
		last := p.aSegEntries[n-1]
		prevEndTicks := last.StartTicks + last.DurTicks
		sr := uint64(p.audioInit.SampleRate) //nolint:gosec // SampleRate > 0 once audioInit is built
		nowTicks := wallclockTicks(now, p.availStart, sr)
		if nowTicks < prevEndTicks {
			return true
		}
	}
	return false
}

// videoTfdtForSegment picks the start-tick anchor for the next video
// segment. The first segment seeds onto wallclock-since-AST so the
// MPD timeline is wallclock-anchored at its origin; every subsequent
// segment uses the previous segment's end (sequential on media time)
// so adjacent segments are contiguous in the MPD's <S t=...> entries.
//
// Sequential anchoring eliminates the per-segment ≤50 ms gap that
// wallclock-anchored tfdt inherited from the run-loop tick granularity
// — see writeVideoSegment's docstring for the full rationale.
func videoTfdtForSegment(entries []SegmentEntry, now, ast time.Time) uint64 {
	if n := len(entries); n > 0 {
		last := entries[n-1]
		return last.StartTicks + last.DurTicks
	}
	return wallclockTicks(now, ast, uint64(VideoTimescale))
}

// audioTfdtForSegment is the audio counterpart of videoTfdtForSegment.
// timescale is the audio sample rate (Hz) — each tick is one sample.
func audioTfdtForSegment(entries []SegmentEntry, now, ast time.Time, timescale uint64) uint64 {
	if n := len(entries); n > 0 {
		last := entries[n-1]
		return last.StartTicks + last.DurTicks
	}
	return wallclockTicks(now, ast, timescale)
}

// wallclockTicks returns (now − ast) × timescale, clamping to 0 when
// ast is unset (the very first segment whose tfdt should be 0). Used
// to seed the FIRST segment's tfdt; subsequent segments stay on the
// media-time anchor (see videoTfdtForSegment / audioTfdtForSegment).
func wallclockTicks(now, ast time.Time, timescale uint64) uint64 {
	if ast.IsZero() {
		return 0
	}
	elapsed := now.Sub(ast).Milliseconds()
	if elapsed <= 0 {
		return 0
	}
	return uint64(elapsed) * timescale / 1000 //nolint:gosec // elapsed positive
}

// computeVideoSegDurTicks returns the total segment duration in 90 kHz
// ticks. When nextPTSms is available (peeked from the queue before
// PopVideo), dur covers [first.PTS, next.PTS] — the exact wallclock
// extent the segment occupies on the media timeline. When the segment
// drained the entire queue (safety-net cut or queue empty after the
// last frame), falls back to (last - first) plus one estimated
// per-frame interval so the segment doesn't visibly under-report by
// one frame.
//
// The next-frame anchor is what eliminates the per-segment 1-frame
// stutter that the previous `last - first` formula produced: at 25 fps
// a 150-frame segment has last.PTS = first.PTS + 5960 ms (149 inter-
// frame intervals), but the segment "covers" 6000 ms of media —
// the missing 40 ms is the last frame's own duration, which next.PTS
// captures exactly.
func computeVideoSegDurTicks(frames []VideoFrame, nextPTSms uint64, hasNext bool) uint64 {
	if len(frames) == 0 {
		// Defensive — caller shouldn't pass an empty slice, but keep
		// downstream mp4ff dur > 0 just in case.
		return uint64(VideoTimescale) * 40 / 1000
	}
	first := frames[0].PTSms
	if hasNext && nextPTSms > first {
		return (nextPTSms - first) * uint64(VideoTimescale) / 1000
	}
	// No next frame: estimate one inter-frame interval from the average
	// across drained frames so the last frame's duration isn't dropped.
	if len(frames) < 2 {
		return uint64(VideoTimescale) * 40 / 1000
	}
	last := frames[len(frames)-1].PTSms
	if last <= first {
		return uint64(VideoTimescale) * 40 / 1000
	}
	span := last - first
	perFrame := span / uint64(len(frames)-1) //nolint:gosec // len > 1 by branch above
	return (span + perFrame) * uint64(VideoTimescale) / 1000
}

// trimWindow removes old segments from disk past Window + History.
//
// Uses dropFrontN-style compaction (copy-to-front + zero-tail) rather
// than a naive `s = s[1:]` slice-forward: the latter keeps the backing
// array alive with every popped element still pinned, so over a long-
// running stream the dropped filenames + SegmentEntry structs
// accumulate inside the array until the next append-induced growth.
// Bounded but unnecessary memory; the compaction releases them
// immediately so a 24-hour stream stays at "window + history" worth
// of entries instead of growing slowly.
func (p *Packager) trimWindow() {
	maxKeep := p.cfg.Window + p.cfg.History
	if !p.cfg.Ephemeral {
		return
	}
	if len(p.onDiskV) > maxKeep {
		drop := len(p.onDiskV) - maxKeep
		for i := 0; i < drop; i++ {
			_ = os.Remove(filepath.Join(p.cfg.StreamDir, p.onDiskV[i]))
		}
		p.onDiskV = dropFrontStrings(p.onDiskV, drop)
		p.vSegEntries = dropFrontSegEntries(p.vSegEntries, drop)
	}
	if len(p.onDiskA) > maxKeep {
		drop := len(p.onDiskA) - maxKeep
		for i := 0; i < drop; i++ {
			_ = os.Remove(filepath.Join(p.cfg.StreamDir, p.onDiskA[i]))
		}
		p.onDiskA = dropFrontStrings(p.onDiskA, drop)
		p.aSegEntries = dropFrontSegEntries(p.aSegEntries, drop)
	}
}

func dropFrontStrings(s []string, n int) []string {
	if n <= 0 || len(s) == 0 {
		return s
	}
	if n >= len(s) {
		for i := range s {
			s[i] = ""
		}
		return s[:0]
	}
	remain := len(s) - n
	copy(s, s[n:])
	for i := remain; i < len(s); i++ {
		s[i] = ""
	}
	return s[:remain]
}

func dropFrontSegEntries(s []SegmentEntry, n int) []SegmentEntry {
	if n <= 0 || len(s) == 0 {
		return s
	}
	if n >= len(s) {
		return s[:0]
	}
	remain := len(s) - n
	copy(s, s[n:])
	return s[:remain]
}

// publishManifest writes the per-stream MPD (when ManifestPath is set)
// and/or notifies the ABR master with a snapshot.
func (p *Packager) publishManifest(now time.Time) {
	in := p.buildManifestInputLocked(now)
	if in == nil {
		return
	}
	if p.cfg.ManifestPath != "" {
		if data, err := BuildManifest(in); err == nil && data != nil {
			_ = writeFileAtomic(p.cfg.ManifestPath, data)
		}
	}
	if p.cfg.ABRMaster != nil {
		p.cfg.ABRMaster.UpdateShard(ShardSnapshot{
			Slug:              p.cfg.ABRSlug,
			AvailabilityStart: p.availStart,
			Video:             firstTrack(in.Video),
			Audio:             in.Audio,
		})
	}
}

// buildManifestInputLocked snapshots window state into a ManifestInput.
// Returns nil when no track has emitted any segment yet.
//
// For ABR shards (ABRSlug non-empty + ManifestPath empty), init and
// media paths are prefixed with the shard slug so the root MPD —
// served at <stream>/index.mpd by the master — can reference
// <stream>/<slug>/init_v.mp4 etc. without further URL rewriting.
// Per-stream MPDs (single-rendition mode) keep the bare filenames
// since the MPD sits next to the segments in the stream dir.
func (p *Packager) buildManifestInputLocked(now time.Time) *ManifestInput {
	if len(p.vSegEntries) == 0 && len(p.aSegEntries) == 0 {
		return nil
	}
	in := &ManifestInput{
		AvailabilityStart: p.availStart,
		PublishTime:       now,
		SegDur:            p.cfg.SegDur,
		Window:            p.cfg.Window,
	}
	pathPrefix := ""
	if p.cfg.ABRSlug != "" && p.cfg.ManifestPath == "" {
		pathPrefix = p.cfg.ABRSlug + "/"
	}
	if p.videoInit != nil && len(p.vSegEntries) > 0 {
		bw := p.cfg.OverrideBandwidth
		if bw == 0 {
			bw = 5_000_000 // sensible default for video
		}
		in.Video = []TrackManifest{{
			RepID:        repIDOr(p.cfg.ABRSlug, "v0"),
			Codec:        p.videoInit.Info.Codec,
			Bandwidth:    bw,
			Width:        p.videoInit.Info.Width,
			Height:       p.videoInit.Info.Height,
			Timescale:    VideoTimescale,
			InitFile:     pathPrefix + dashVideoInitFile,
			MediaPattern: pathPrefix + dashVideoMediaPattern,
			StartNumber:  startNumberFor(p.onDiskV, p.cfg.Window),
			Segments:     windowTail(p.vSegEntries, p.cfg.Window),
		}}
	}
	if p.audioInit != nil && p.cfg.PackAudio && len(p.aSegEntries) > 0 {
		in.Audio = &TrackManifest{
			RepID:        "a0",
			Codec:        p.audioInit.Codec,
			Bandwidth:    128_000,
			SampleRate:   p.audioInit.SampleRate,
			Timescale:    uint32(p.audioInit.SampleRate), //nolint:gosec // SampleRate fits uint32
			InitFile:     pathPrefix + dashAudioInitFile,
			MediaPattern: pathPrefix + dashAudioMediaPattern,
			StartNumber:  startNumberFor(p.onDiskA, p.cfg.Window),
			Segments:     windowTail(p.aSegEntries, p.cfg.Window),
		}
	}
	return in
}

// finalFlush is called on ctx cancel / subscriber close to write any
// in-progress segment. Best-effort.
func (p *Packager) finalFlush() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.queue.VideoLen() == 0 && p.queue.AudioLen() == 0 {
		return
	}
	d := CutDecision{
		Ok:         true,
		VideoCount: p.queue.VideoLen(),
		AudioCount: p.queue.AudioLen(),
	}
	if p.writeSegments(time.Now(), d) {
		p.trimWindow()
		p.publishManifest(time.Now())
	}
}

// ─── helpers ─────────────────────────────────────────────────────────

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func parseADTS(frame []byte) *aac.ADTSHeader {
	hdr, _, err := aac.DecodeADTSHeader(bytes.NewReader(frame))
	if err != nil {
		return nil
	}
	return hdr
}

// splitADTSBundle iterates concatenated ADTS frames in `data` and
// returns one AudioFrame per AAC access unit found, with each frame's
// raw payload (ADTS header stripped) and PTS = basePTSms + frameIdx ×
// 1024 × 1000 / sampleRate (integer ms).
//
// gomedia's TSDemuxer hands the packager a complete PES payload via
// onTSFrame, and a single AAC PES typically carries 4–8 concatenated
// ADTS frames (encoders bundle for transmit efficiency — at 48 kHz, 8
// frames is ~170 ms of audio per delivery). The DASH writer assigns a
// fixed 1024-sample duration per AudioFrame ([writeAudioSegment]'s
// `len(frames) * 1024`), so treating one bundled PES as a single
// AudioFrame under-declares segment dur by the bundling factor — the
// MPD's audio `<S d=…>` ends up 4–8× too small, audio falls behind
// video on the live edge, and players that strictly enforce
// sample-count continuity stall.
//
// When the data has no ADTS header at offset 0 (defensive: pure raw
// AAC delivered via a path that pre-strips headers), returns a single
// AudioFrame carrying the data unchanged at basePTSms.
func splitADTSBundle(data []byte, basePTSms uint64) []AudioFrame {
	if len(data) == 0 {
		return nil
	}
	hdr, _, err := aac.DecodeADTSHeader(bytes.NewReader(data))
	if err != nil || hdr == nil {
		return []AudioFrame{{Raw: cloneBytes(data), PTSms: basePTSms}}
	}
	sampleRate := int(hdr.Frequency())
	out := make([]AudioFrame, 0, 8)
	pos := 0
	frameIdx := 0
	for pos < len(data) {
		h, _, perr := aac.DecodeADTSHeader(bytes.NewReader(data[pos:]))
		if perr != nil || h == nil {
			break // garbage in the bundle tail — stop rather than mis-emit
		}
		hLen := int(h.HeaderLength)
		pLen := int(h.PayloadLength)
		if hLen <= 0 || pLen <= 0 || pos+hLen+pLen > len(data) {
			break
		}
		raw := cloneBytes(data[pos+hLen : pos+hLen+pLen])
		var ptsOffset uint64
		if sampleRate > 0 {
			ptsOffset = uint64(frameIdx) * 1024 * 1000 / uint64(sampleRate) //nolint:gosec // frameIdx bounded by bundle size
		}
		out = append(out, AudioFrame{Raw: raw, PTSms: basePTSms + ptsOffset})
		pos += hLen + pLen
		frameIdx++
	}
	return out
}

// firstTrack returns &tracks[0] or nil when empty.
func firstTrack(tracks []TrackManifest) *TrackManifest {
	if len(tracks) == 0 {
		return nil
	}
	t := tracks[0]
	return &t
}

// repIDOr returns slug when non-empty, otherwise fallback. Used to make
// ABR shard reps inherit the slug name in the manifest.
func repIDOr(slug, fallback string) string {
	if slug != "" {
		return slug
	}
	return fallback
}

// startNumberFor derives the StartNumber for SegmentTemplate from the
// filenames in the sliding window. The first filename's numeric suffix
// is the start number.
func startNumberFor(names []string, window int) uint64 {
	if len(names) == 0 {
		return 1
	}
	if len(names) > window {
		names = names[len(names)-window:]
	}
	return parseSegNum(names[0])
}

// parseSegNum extracts the numeric suffix from "seg_v_00042.m4s".
func parseSegNum(name string) uint64 {
	const minLen = len("seg_v_00001.m4s")
	if len(name) < minLen {
		return 1
	}
	// Find the underscore before the number.
	i := len(name) - len(".m4s") - 1
	j := i
	for j >= 0 && name[j] >= '0' && name[j] <= '9' {
		j--
	}
	if j == i {
		return 1
	}
	var n uint64
	for _, ch := range name[j+1 : i+1] {
		n = n*10 + uint64(ch-'0')
	}
	if n == 0 {
		return 1
	}
	return n
}

// windowTail returns the last `n` elements of segs, or all when fewer.
func windowTail(segs []SegmentEntry, n int) []SegmentEntry {
	if len(segs) <= n {
		return append([]SegmentEntry(nil), segs...)
	}
	return append([]SegmentEntry(nil), segs[len(segs)-n:]...)
}
