package publisher

// hls.go — live HLS publisher (MPEG-TS segments + sliding m3u8 manifest).
//
// Segmenting strategy:
//   - AV path (pkt.AV): flush the current segment BEFORE writing an IDR frame so
//     every new segment starts with a keyframe.
//   - TS path (pkt.TS, transcoder output): the transcoder is configured with a
//     GOP size matching segSec, so a wall-clock ticker flush at segSec intervals
//     lands close to IDR boundaries without needing a demuxer.
//
// In both modes, a 3/2 × segSec force-flush prevents runaway segments.
//
// Session boundaries (cold start, reconnect, failover, mixer cycle) are
// signalled by buffer.Packet.SessionStart=true on the first packet of the
// new session — onSessionBoundary flushes the in-progress segment and
// schedules EXT-X-DISCONTINUITY for the next one, identically across AV-
// path and raw-TS-path sources.

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ntt0601zcoder/open-streamer/internal/buffer"
	"github.com/ntt0601zcoder/open-streamer/internal/domain"
	"github.com/ntt0601zcoder/open-streamer/internal/tsmux"
	"github.com/ntt0601zcoder/open-streamer/pkg/logger"
)

// hlsRunOpts carries per-rendition metadata for ABR HLS; nil → single-rendition mode.
type hlsRunOpts struct {
	abrMaster *hlsABRMaster
	abrSlug   string
	bwBps     int
	width     int
	height    int
	// hasAudio reflects whether this rendition's segments contain an audio
	// track; surfaces in the master playlist's CODECS attribute. Derived
	// from the transcoder config at setup time so the master playlist
	// declares the correct codec list before the first segment lands.
	hasAudio bool
	// segCount is incremented once per successful segment write. Pre-bound
	// to {stream_code,format=hls,profile} so the hot path skips the
	// WithLabelValues map lookup. Nil-safe.
	segCount prometheus.Counter
	// segWriteDur observes wall-clock time spent in os.WriteFile per
	// segment. Pre-bound to {stream_code,format=hls}. Nil-safe.
	segWriteDur prometheus.Observer
}

// hlsSegEntry holds per-segment metadata kept in the sliding window.
type hlsSegEntry struct {
	name string  // e.g. "seg_000001.ts"
	dur  float64 // seconds, measured wall-clock from flush
	disc bool    // emit EXT-X-DISCONTINUITY before this segment
}

// serveHLS is the single-rendition HLS output goroutine.
func (s *Service) serveHLS(ctx context.Context, streamID domain.StreamCode) {
	hlsDir := strings.TrimSpace(s.cfg.HLS.Dir)
	if hlsDir == "" {
		slog.Error("publisher: HLS disabled — publisher.hls.dir is empty", "stream_code", streamID)
		return
	}

	sub, err := s.buf.Subscribe(streamID)
	if err != nil {
		slog.Error("publisher: HLS subscribe failed", "stream_code", streamID, "err", err)
		return
	}
	defer s.buf.Unsubscribe(streamID, sub)

	slog.Info("publisher: HLS serve started", "stream_code", streamID, "hls_dir", hlsDir)

	streamDir := filepath.Join(hlsDir, string(streamID))
	if err := os.MkdirAll(streamDir, 0o755); err != nil {
		slog.Error("publisher: HLS setup dir failed", "stream_code", streamID, "dir", streamDir, "err", err)
		return
	}

	manifest := filepath.Join(streamDir, "index.m3u8")
	opts := &hlsRunOpts{
		segCount:    s.hlsSegCounter(streamID, "main"),
		segWriteDur: s.segWriteDurObserver(streamID, "hls"),
	}
	runHLSSegmenter(
		ctx, streamID, sub, streamDir, manifest,
		s.cfg.HLS.LiveSegmentSec,
		s.cfg.HLS.LiveWindow,
		s.cfg.HLS.LiveHistory,
		s.cfg.HLS.LiveEphemeral,
		opts,
	)
}

// runHLSSegmenter is the entry-point for both single-rendition and ABR per-shard segmenters.
func runHLSSegmenter(
	ctx context.Context,
	streamID domain.StreamCode,
	sub *buffer.Subscriber,
	streamDir string,
	manifestPath string,
	segSec, window, history int,
	ephemeral bool,
	opts *hlsRunOpts,
) {
	if segSec <= 0 {
		segSec = domain.DefaultLiveSegmentSec
	}
	if window <= 0 {
		window = domain.DefaultLiveWindow
	}
	if history < 0 {
		history = domain.DefaultLiveHistory
	}

	p := &hlsSegmenter{
		streamID:     streamID,
		streamDir:    streamDir,
		manifestPath: manifestPath,
		segSec:       segSec,
		window:       window,
		history:      history,
		ephemeral:    ephemeral,
		segBuf:       make([]byte, 0, 4<<20), // 4 MiB initial capacity
	}
	if opts != nil {
		p.abrMaster = opts.abrMaster
		p.abrSlug = opts.abrSlug
		p.bwBps = opts.bwBps
		p.width = opts.width
		p.height = opts.height
		p.hasAudio = opts.hasAudio
		p.segCount = opts.segCount
		p.segWriteDur = opts.segWriteDur
	}

	p.run(ctx, sub)
}

// hlsSegmenter manages the per-stream HLS segmentation state.
type hlsSegmenter struct {
	streamID     domain.StreamCode
	streamDir    string
	manifestPath string // empty when abrMaster handles the per-shard path
	segSec       int
	window       int
	history      int
	ephemeral    bool

	mu sync.Mutex

	// Current segment accumulator (aligned 188-byte MPEG-TS packets).
	segBuf   []byte
	segStart time.Time
	discNext bool // schedule EXT-X-DISCONTINUITY for the next flushed segment

	// Media-PTS bookkeeping for AV-path segmenting. Tracks the first
	// and most recent AV packet PTS (ms) seen in the current segment
	// so cut decisions and EXTINF durations come from the source
	// timeline instead of wallclock — wallclock-based cuts skip
	// IDRs that land slightly before the segDur boundary, producing
	// segments that span every OTHER GOP (8s instead of 4s for a
	// 100-frame GOP at 25fps).
	//
	// hasMediaStart guards against using a zero segStartMediaPTS for
	// segments that haven't seen any AV packet yet (raw-TS path).
	segStartMediaPTS uint64
	lastMediaPTS     uint64
	hasMediaStart    bool

	// segBufStartOffset is the absolute byte offset (within the lifetime of
	// the raw-TS scanner) corresponding to segBuf[0]. Used to translate the
	// scanner's stream-absolute IDR offsets into segBuf-relative positions
	// when picking a keyframe-aligned split point.
	segBufStartOffset int64
	// scannedOffset is the highest scanner.LastIDROffset we have already
	// considered for splitting. Prevents re-using the same IDR boundary.
	scannedOffset int64

	// Sliding-window manifest state.
	segN   uint64
	onDisk []hlsSegEntry

	// ABR wiring.
	abrMaster *hlsABRMaster
	abrSlug   string
	bwBps     int
	width     int
	height    int
	hasAudio  bool // set at construction from hlsRunOpts

	// Pre-bound segment-write counter; nil-safe.
	segCount prometheus.Counter
	// Pre-bound histogram for os.WriteFile latency; nil-safe.
	segWriteDur prometheus.Observer

	// scanner finds IDR boundaries in raw-TS chunks (UDP/HLS/SRT/File
	// passthrough sources). Lazily allocated on the first raw-TS packet so
	// AV-only paths and transcoder-output paths pay nothing. nil → caller
	// falls back to wall-clock force-flush (acceptable for transcoder output
	// because that path is GOP-aligned by ffmpeg).
	scanner *tsKeyframeScanner

	// discardUntilIDR is set after a wall-clock force-flush on the AV path:
	// the segment we just flushed did NOT end on an IDR boundary, so any
	// non-keyframe AV packets that follow would land at the head of the
	// next segment with no SPS/PPS in scope and decoders cannot start
	// playback there. We drop those packets until the next AVPacket with
	// KeyFrame=true arrives, then resume. Only meaningful for AV path
	// (scanner == nil); raw-TS path uses tryFlushAtIDRLocked which always
	// splits at a keyframe.
	discardUntilIDR bool
}

func (p *hlsSegmenter) run(ctx context.Context, sub *buffer.Subscriber) {
	var tsCarry []byte
	var avMux *tsmux.FromAV

	segDur := time.Duration(p.segSec) * time.Second
	// maxDur is the safety deadline after which we force-flush even when no
	// IDR has arrived. The 4× multiplier is a deliberate choice over the
	// "tight" 1.5× we used before:
	//   - Live encoders commonly run GOPs of 2–3× target segment duration
	//     (e.g. 8–12s GOP for a 4s target). 1.5×segDur force-flushes every
	//     such segment mid-GOP, which then needs an EXT-X-DISCONTINUITY +
	//     a wait-for-next-IDR drop window — that drop window starves audio
	//     for several seconds (audible stutter) and the per-segment
	//     discontinuity makes browsers reset their decoder on every fetch
	//     (additional clicks/glitches).
	//   - 4×segDur tolerates GOPs up to that bound, so well-formed long-GOP
	//     sources never trip the safety net and segments end cleanly at
	//     IDR boundaries via handleAVPacket. Truly stuck streams (no IDR
	//     for ≥4×segDur) still trigger the force-flush + discardUntilIDR
	//     recovery path, but that becomes the rare edge case it's meant to
	//     be rather than the every-segment hot path.
	// Sized as a multiplier (not a constant) so it scales with operator-
	// chosen segDur — segSec=2 → 8s; segSec=4 → 16s; segSec=6 → 24s.
	maxDur := segDur * 4

	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			p.doFlush()
			return

		case pkt, ok := <-sub.Recv():
			if !ok {
				p.doFlush()
				return
			}
			p.handleIncoming(ctx, pkt, segDur, &avMux, &tsCarry)

		case <-tick.C:
			p.tickFlush(maxDur, segDur)
		}
	}
}

// handleIncoming dispatches one buffer packet through the AV / raw-TS path.
// Extracted from run() to keep that function under the cognitive-complexity
// ceiling. Mutates avMux / tsCarry via pointers — both are loop-local state.
//
// The session-boundary cue (pkt.SessionStart=true) is the single trigger
// for flushing the current segment and emitting EXT-X-DISCONTINUITY on the
// next one. It fires identically across AV-path and raw-TS-path sources,
// replacing the two parallel mechanisms used previously (in-band
// AVPacket-flag for AV, event-bus generation counter for TS — both removed
// in Phase 3 of the refactor).
func (p *hlsSegmenter) handleIncoming(
	ctx context.Context,
	pkt buffer.Packet,
	segDur time.Duration,
	avMux **tsmux.FromAV,
	tsCarry *[]byte,
) {
	if pkt.SessionStart {
		p.onSessionBoundary(avMux, tsCarry)
	}
	if pkt.AV != nil {
		// After a non-keyframe-aligned force-flush, drop AV packets until
		// the next IDR so the next segment starts at a clean keyframe.
		// See hlsSegmenter.discardUntilIDR.
		if p.shouldDropAVPacket(pkt.AV) {
			return
		}
		p.recordMediaTime(pkt.AV)
		p.maybeFlushAtKeyframe(pkt.AV, segDur)
	}
	rawTS := len(pkt.TS) > 0 && pkt.AV == nil
	tsmux.FeedWirePacket(ctx, pkt.TS, pkt.AV, avMux, func(b []byte) {
		alignedFeed(b, tsCarry, func(pkt188 []byte) bool {
			p.appendSegment188(pkt188, rawTS)
			return true
		})
	})
}

// shouldDropAVPacket returns true when the packet must be silently
// discarded because we are waiting for the next IDR after a force-flush.
// Clears the discard flag (and logs the resume) when an IDR arrives.
//
// Only video packets are subject to the drop window — audio packets pass
// through unconditionally. Without this carve-out, the 3-4s gap between
// force-flush and the next IDR would also drop every audio frame in
// that window, producing audible stutter on long-GOP sources (test5
// incident: maxDur=6s, source GOP ~9-10s, audio missing in every
// segment between successive IDRs). KeyFrame is set only on video
// keyframes in this codebase, so audio frames would all "look like"
// non-keyframes and be dropped without this guard.
func (p *hlsSegmenter) shouldDropAVPacket(av *domain.AVPacket) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.discardUntilIDR {
		return false
	}
	if !av.Codec.IsVideo() {
		return false
	}
	if av.KeyFrame {
		p.discardUntilIDR = false
		slog.Info("publisher: HLS resumed at IDR after force-flush",
			"stream_code", p.streamID)
		return false
	}
	return true
}

// appendSegment188 appends one TS packet to the active segment buffer and,
// for raw-TS sources, feeds it to the keyframe scanner.
func (p *hlsSegmenter) appendSegment188(pkt188 []byte, rawTS bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.segStart.IsZero() {
		p.segStart = time.Now()
	}
	p.segBuf = append(p.segBuf, pkt188...)
	if rawTS {
		// Lazy-init keyframe scanner for raw-TS passthrough sources only —
		// AV path doesn't need it (segments pre-aligned via handleAVPacket),
		// and transcoder TS output is GOP-aligned by ffmpeg.
		if p.scanner == nil {
			p.scanner = newTSKeyframeScanner()
		}
		p.scanner.Feed(pkt188)
	}
}

// onSessionBoundary is invoked at the top of handleIncoming whenever a
// buffer.Packet carries SessionStart=true. It flushes the in-progress
// segment so old-source data lands in its own segment, schedules the
// EXT-X-DISCONTINUITY tag for the NEXT segment, and discards stale muxer
// state so the new source gets fresh PAT/PMT tables and clean continuity
// counters (FeedWirePacket lazily re-allocates FromAV on the next call).
func (p *hlsSegmenter) onSessionBoundary(avMux **tsmux.FromAV, tsCarry *[]byte) {
	p.mu.Lock()
	if len(p.segBuf) > 0 {
		p.flushLocked()
	}
	p.discNext = true
	p.discardUntilIDR = false
	p.mu.Unlock()
	*avMux = nil
	*tsCarry = nil
}

// recordMediaTime captures the AV packet's PTS so segment age + EXTINF
// can be derived from the source timeline instead of wallclock. Called
// for every AV packet (audio and video) before maybeFlushAtKeyframe so
// segStartMediaPTS reflects the FIRST packet of the segment, regardless
// of whether it was an audio or video frame.
//
// Wallclock-based cuts skip IDRs that arrive slightly before
// time.Since(segStart) crosses segDur — for a 100-frame GOP at 25 fps
// the encoder emits an IDR exactly at the 4 s boundary, the wallclock
// check fails by a few ms, the IDR is dropped, and the segmenter
// waits for the next one ~4 s later, producing 8 s segments that
// span two GOPs each.
func (p *hlsSegmenter) recordMediaTime(av *domain.AVPacket) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.hasMediaStart {
		p.segStartMediaPTS = av.PTSms
		p.hasMediaStart = true
	}
	p.lastMediaPTS = av.PTSms
}

// maybeFlushAtKeyframe lands a fresh segment boundary on every IDR once
// the in-progress segment has reached segDur. AV-path streams use this to
// produce GOP-aligned segments; raw-TS streams rely on the keyframe
// scanner in tickFlush instead. Must not be called with p.mu held.
//
// Age is measured in the source's media PTS timeline. After cutting,
// re-anchor segStartMediaPTS to THIS keyframe's PTS so the next
// segment starts from the right point (the keyframe's TS bytes will
// be appended into segBuf moments later by tsmux.FromAV).
func (p *hlsSegmenter) maybeFlushAtKeyframe(av *domain.AVPacket, segDur time.Duration) {
	if !av.KeyFrame {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.segBuf) == 0 || !p.hasMediaStart {
		return
	}
	segDurMs := uint64(segDur.Milliseconds()) //nolint:gosec // segDur > 0 always
	if av.PTSms-p.segStartMediaPTS < segDurMs {
		return
	}
	p.flushLocked()
	p.segStartMediaPTS = av.PTSms
	p.lastMediaPTS = av.PTSms
	p.hasMediaStart = true
}

// tickFlush is called every 50 ms by the ticker; it handles:
//   - Raw-TS path: keyframe-aligned segmenting via the scanner (preferred)
//   - TS-path time-based segmenting (transcoder output, GOP-aligned)
//   - Fallback force-flush for stuck AV paths
//
// Session boundaries are signalled via buffer.Packet.SessionStart and
// handled in onSessionBoundary at packet-receive time — tickFlush only
// runs the segment-cadence safety net.
func (p *hlsSegmenter) tickFlush(maxDur, segDur time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.segBuf) == 0 || p.segStart.IsZero() {
		return
	}

	elapsed := time.Since(p.segStart)

	// Raw-TS path: prefer keyframe-aligned split. Only flush at a fresh IDR
	// boundary once we've buffered at least one segDur of wall-clock data —
	// without the wall-clock check we'd flush on every IDR (mid-GOP segments
	// far shorter than segDur).
	if p.scanner != nil && elapsed >= segDur {
		if p.tryFlushAtIDRLocked() {
			return
		}
	}

	if elapsed >= maxDur {
		// Force-flush: either TS path with no IDR seen in maxDur (degraded
		// source — better one bad segment than indefinite buffering) or an
		// AV path without a keyframe for 1.5 × segSec.
		p.flushLocked()
		// AV path: the segment we just emitted does NOT end at an IDR, so
		// the next segment would start mid-GOP with no SPS/PPS reachable.
		// Discard subsequent AV packets until the next keyframe so every
		// emitted segment begins at a clean IDR boundary. Raw-TS path uses
		// scanner-driven splitting and never lands here without an IDR.
		if p.scanner == nil {
			p.discardUntilIDR = true
			p.discNext = true // mark gap for downstream players
			slog.Info("publisher: HLS force-flush without IDR, dropping until next keyframe",
				"stream_code", p.streamID,
				"elapsed_ms", elapsed.Milliseconds(),
				"max_dur_ms", maxDur.Milliseconds(),
			)
		}
	}
}

// tryFlushAtIDRLocked splits the segment at the next IDR boundary the
// scanner has reported since the last flush. Returns true when a flush
// happened. Caller holds p.mu.
//
// The scanner reports IDR offsets in its own absolute (cumulative-fed-bytes)
// coordinate. We translate that into a segBuf-relative split point via
// segBufStartOffset, write [0:split) as the segment, then carry [split:end)
// over as the start of the next segment. p.segStart is reset so the new
// segment's wall-clock window begins at the IDR.
func (p *hlsSegmenter) tryFlushAtIDRLocked() bool {
	idrAbs := p.scanner.LastIDROffset
	if idrAbs <= p.scannedOffset {
		return false // no fresh IDR since last flush
	}
	split := int(idrAbs - p.segBufStartOffset)
	if split <= 0 || split >= len(p.segBuf) {
		// IDR is at or before segment start (already consumed) or hasn't
		// landed in segBuf yet — nothing useful to split at.
		p.scannedOffset = idrAbs
		return false
	}
	carry := append([]byte(nil), p.segBuf[split:]...)
	p.segBuf = p.segBuf[:split]
	p.flushLocked()
	// flushLocked resets segBuf + segStart; restore the carry as the new
	// segment's first bytes and re-anchor wall-clock to now.
	p.segBuf = carry
	p.segStart = time.Now()
	p.segBufStartOffset = idrAbs
	p.scannedOffset = idrAbs
	return true
}

// doFlush flushes any remaining segment data on shutdown.
func (p *hlsSegmenter) doFlush() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.segBuf) > 0 {
		p.flushLocked()
	}
}

// flushLocked writes the current segment to disk and updates the manifest.
// Caller must hold p.mu.
func (p *hlsSegmenter) flushLocked() {
	if len(p.segBuf) == 0 {
		return
	}

	// Prefer media-PTS duration when an AV packet anchored the
	// segment — wallclock drifts on bursty buffer-hub reads and
	// produces EXTINF values that disagree with the player's own
	// playback clock. Fall back to wallclock for raw-TS-only
	// segments that never recorded a media start.
	dur := time.Since(p.segStart).Seconds()
	if p.hasMediaStart && p.lastMediaPTS > p.segStartMediaPTS {
		dur = float64(p.lastMediaPTS-p.segStartMediaPTS) / 1000.0
	}
	if dur <= 0 {
		dur = float64(p.segSec)
	}

	p.segN++
	name := fmt.Sprintf("seg_%06d.ts", p.segN)
	path := filepath.Join(p.streamDir, name)

	// Snapshot and reset the accumulator without reallocating.
	data := append(make([]byte, 0, len(p.segBuf)), p.segBuf...)
	p.segBuf = p.segBuf[:0]
	p.segStart = time.Time{}
	p.hasMediaStart = false
	p.segStartMediaPTS = 0
	p.lastMediaPTS = 0
	// Re-anchor the segBuf-to-scanner offset mapping. The scanner has been
	// fed every byte we just flushed, so the next byte appended to the
	// (now-empty) segBuf will correspond to scanner.feedOffset. Without
	// this, the next IDR offset the scanner reports gets translated through
	// a stale segBufStartOffset and split lands far past len(segBuf) →
	// tryFlushAtIDRLocked silently fails forever and we fall back to the
	// non-keyframe-aligned wall-clock force-flush, breaking playback.
	if p.scanner != nil {
		p.segBufStartOffset = p.scanner.feedOffset
		p.scannedOffset = p.scanner.feedOffset
	}

	disc := p.discNext
	p.discNext = false

	writeStart := time.Now()
	// Atomic write (tmp + rename) — without this, a player that fetches
	// the segment while we're partway through writing it gets a truncated
	// file ("Invalid data found when processing input"). The manifest
	// already references the segment by name as soon as we append to
	// p.onDisk a few lines below, so the window between "playlist says
	// the file exists" and "file is fully on disk" is observable. The
	// DASH packager has used writeFileAtomic for the same reason since
	// day one — HLS uses os.WriteFile here for legacy reasons that no
	// longer apply.
	if err := writeFileAtomic(path, data); err != nil {
		slog.Warn("publisher: HLS write segment failed",
			"stream_code", p.streamID, "segment", name, "err", err)
		return
	}
	if p.segWriteDur != nil {
		p.segWriteDur.Observe(time.Since(writeStart).Seconds())
	}

	if p.segCount != nil {
		p.segCount.Inc()
	}

	logger.Trace("publisher: HLS segment flushed",
		"stream_code", p.streamID, "segment", name,
		"dur_s", fmt.Sprintf("%.3f", dur),
		"bytes", len(data),
	)

	p.onDisk = append(p.onDisk, hlsSegEntry{name: name, dur: dur, disc: disc})
	p.trimDiskLocked()

	if err := p.writeManifestLocked(); err != nil {
		slog.Warn("publisher: HLS write manifest failed", "stream_code", p.streamID, "err", err)
	}
}

// trimDiskLocked removes old segments beyond the sliding window + history.
// Caller must hold p.mu.
func (p *hlsSegmenter) trimDiskLocked() {
	maxKeep := p.window + p.history
	if maxKeep < p.window {
		maxKeep = p.window
	}
	if !p.ephemeral {
		return
	}
	for len(p.onDisk) > maxKeep {
		old := p.onDisk[0]
		p.onDisk = p.onDisk[1:]
		_ = os.Remove(filepath.Join(p.streamDir, old.name))
	}
}

// writeManifestLocked serialises the sliding-window m3u8 and writes it atomically.
// Caller must hold p.mu.
func (p *hlsSegmenter) writeManifestLocked() error {
	if p.manifestPath == "" && p.abrMaster == nil {
		return nil
	}

	win := windowTailEntries(p.onDisk, p.window)

	// EXT-X-TARGETDURATION must be >= ceil of any segment duration in the manifest.
	targetDur := p.segSec + 1
	for _, e := range win {
		c := int(e.dur) + 1
		if c > targetDur {
			targetDur = c
		}
	}

	// Media sequence = segment counter of the first segment in the window.
	mediaSeq := int(p.segN) - len(win)
	if mediaSeq < 0 {
		mediaSeq = 0
	}

	var sb strings.Builder
	sb.WriteString("#EXTM3U\n")
	sb.WriteString("#EXT-X-VERSION:3\n")
	fmt.Fprintf(&sb, "#EXT-X-TARGETDURATION:%d\n", targetDur)
	fmt.Fprintf(&sb, "#EXT-X-MEDIA-SEQUENCE:%d\n", mediaSeq)

	for _, e := range win {
		if e.disc {
			sb.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		fmt.Fprintf(&sb, "#EXTINF:%.6f,\n%s\n", e.dur, e.name)
	}

	payload := []byte(sb.String())

	if p.abrMaster != nil {
		// Notify the ABR master so it can refresh the root master playlist.
		p.abrMaster.onShardUpdated(p.abrSlug, p.bwBps, p.width, p.height, p.hasAudio)
	}

	if p.manifestPath != "" {
		return writeFileAtomic(p.manifestPath, payload)
	}
	return nil
}

// windowTailEntries returns the last n entries of the sliding window.
func windowTailEntries(entries []hlsSegEntry, n int) []hlsSegEntry {
	if n <= 0 || len(entries) == 0 {
		return entries
	}
	if len(entries) <= n {
		return entries
	}
	return entries[len(entries)-n:]
}

// hlsCodecString returns a CODECS attribute value for EXT-X-STREAM-INF based on
// the rendition resolution. These are representative H.264 profiles/levels; the
// exact string depends on the encoder configuration but this covers common setups.
//
// hasAudio appends "mp4a.40.2" (AAC-LC) so the master playlist accurately
// declares both tracks. Without this, hls.js + MSE addSourceBuffer with a
// video-only codec list and silently drop audio even though the .ts segments
// carry it — which is why "plays in the Open-Streamer console but not in the
// browser" is a recurring symptom on transcoded ABR streams.
func hlsCodecString(width, height int, hasAudio bool) string {
	pixels := width * height
	var video string
	switch {
	case pixels >= 1920*1080:
		video = "avc1.640028" // High@L4.0
	case pixels >= 1280*720:
		video = "avc1.4d401f" // Main@L3.1
	default:
		video = "avc1.42e01e" // Baseline@L3.0
	}
	if hasAudio {
		return video + ",mp4a.40.2"
	}
	return video
}
