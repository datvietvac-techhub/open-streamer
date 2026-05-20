// Package timeline owns the single, authoritative PTS/DTS anchor for
// every elementary-stream packet that reaches the buffer hub.
//
// The Normaliser unified three legacy state machines that previously
// each owned a slice of "the stream's clock":
//
//   - internal/ingestor/ptsrebaser (deleted) — AV-path wallclock anchor.
//   - internal/ingestor/pull/mixer's videoPTSBase / audioPTSBase
//     (deleted).
//   - internal/coordinator/abr_mixer's ptsRebaser (deleted) — replaced
//     by per-cycle Normaliser instances seeded from entry.t0.
//
// All three used to set a `AVPacket.Discontinuity` flag with different
// meanings. That flag is also gone: session-boundary signalling moved
// to `buffer.Packet.SessionStart` and rebaser-internal re-anchor events
// surface only via Stats / LastDiagnostic for telemetry. Consumers
// never re-normalise.
//
// The Normaliser is wallclock-anchored, session-aware, V/A-pair-aware,
// monotonic-floored, drift-capped. The OWNER is the ingestor: each
// stream has exactly one Normaliser; the buffer hub guarantees PTS/DTS
// are anchored when a consumer reads them and that `SessionStart=true`
// fronts every new lifetime.
package timeline

import (
	"log/slog"
	"sync"
	"time"

	"github.com/ntt0601zcoder/open-streamer/internal/domain"
)

// trackKeyName returns a human-readable label for log fields. Kept in
// the package so the diagnostic slog below stays self-contained.
func trackKeyName(tk trackKey) string {
	switch tk {
	case trackVideo:
		return "video"
	case trackAudio:
		return "audio"
	case numTracks:
		return "invalid"
	default:
		return "unknown"
	}
}

// Config controls Normaliser behaviour. The zero value is valid and
// produces a disabled Normaliser whose Apply is a pass-through — handy
// for tests and for the observer-disabled production default.
type Config struct {
	// Enabled gates the whole feature. False ⇒ Apply is a pass-through,
	// OnSession is a no-op.
	Enabled bool

	// JumpThresholdMs caps the burst-tolerant drift (output PTS minus
	// max(actualNow, lastOutputDts)) before a hard re-anchor fires. Same
	// semantics and units as ptsrebaser.Config.JumpThresholdMs.
	JumpThresholdMs int64

	// MaxAheadMs caps how far ahead of (now − sessionStart) the output
	// timeline may sit before incoming packets are dropped. Same as
	// ptsrebaser.Config.MaxAheadMs.
	MaxAheadMs int64

	// MaxBehindMs is the symmetric counterpart of MaxAheadMs — when the
	// proposed output sits more than MaxBehindMs behind wallclock the
	// packet is hard-re-anchored to the wallclock floor. Zero disables
	// this branch (the existing rebaser has no equivalent; the
	// monotonic-floor branch handles strict-monotonic regressions but
	// not "wallclock kept moving while source paused" cases).
	MaxBehindMs int64

	// CrossTrackSnapMs is the cross-track progression gap above which a
	// newly-seeded track snaps its output anchor onto the other track's
	// last emitted DTS. Mirrors ptsrebaser.crossTrackSnapMs (1000 ms),
	// exposed here as a knob so tests can vary it.
	CrossTrackSnapMs int64

	// SameTimebaseMaxMs is the maximum source-DTS gap at which the
	// newly-seeded track is treated as sharing a clock with the other
	// track (e.g. MPEG-TS PCR — V/A first-packet inDts within ms of each
	// other). Within this threshold the new track's outputAnchor is
	// computed as `other.outputAnchor + (inDts - other.inputOrigin)` so
	// the source-side intrinsic A/V offset survives — independent of the
	// transcoder's per-track wallclock arrival skew (B-frame decoder
	// reorder buffers video by reorder_depth × frame_dur, e.g. 160 ms
	// for 4-frame H.264 High Profile, while audio passes through
	// immediately; without this branch the Normaliser would treat that
	// arrival skew as the intrinsic offset and bake `audio_leads_video`
	// into the output PTS).
	//
	// Above this gap the two source clocks are presumed independent
	// (RTSP per-track RTP timebases differ by hours) and the new track
	// falls back to the wallclock-arrival anchor — same behaviour as
	// before this knob existed.
	SameTimebaseMaxMs int64

	// SeedHoldTimeoutMs activates the joint-seed mode. When > 0, the
	// Normaliser holds the first track's first packets for up to this
	// many milliseconds while waiting for the partner track's first
	// packet. When both first packets are in hand, the Normaliser
	// computes a JOINT anchor — the earlier-source-DTS track anchors at
	// 0, the later anchors at +|delta| — so the source-side intrinsic
	// V/A offset is preserved end-to-end regardless of network arrival
	// skew. The original lazy-seed path seeded each track independently
	// on its first Apply call, and broadcast feeds whose multicast
	// scheduler emits audio frames pre-buffered ahead of video by
	// hundreds of ms (observed empirically on certain UDP feeds) lost
	// that offset to the wallclock-arrival fallback — visible as ~500 ms
	// of audio lag in the player.
	//
	// When 0 (the default), the legacy lazy-seed path runs: each track
	// seeds immediately on its first Apply, no buffering. Set to ~1000
	// ms in production to absorb worst-case first-packet skew while
	// capping startup latency for single-track streams (audio-only,
	// video-only) which would otherwise wait forever for a partner that
	// never arrives.
	//
	// Tests that exercise single-track Normalisers should leave this at
	// 0 so packets emit immediately. Per-cycle Normalisers in
	// abr_mixer also leave it at 0 because each instance is single-
	// track by construction.
	SeedHoldTimeoutMs int64
}

// DefaultConfig matches ptsrebaser.DefaultConfig so a side-by-side run
// converges on the same outputs for well-behaved sources. Operators or
// tests that want the stricter ahead / behind cap can override per call.
func DefaultConfig() Config {
	return Config{
		Enabled:           true,
		JumpThresholdMs:   2000,
		CrossTrackSnapMs:  1000,
		SameTimebaseMaxMs: 5000,
	}
}

// trackKey buckets AVCodec into the V / A slot used for per-track
// anchor state. Codecs that classify as neither (the raw-TS marker,
// AVCodecUnknown) are skipped at the call site.
type trackKey int

const (
	trackVideo trackKey = iota
	trackAudio
	numTracks
)

// trackKeyFor maps AVCodec to its V / A slot. Returns ok=false for
// codecs the Normaliser doesn't classify (marker, unknown).
func trackKeyFor(c domain.AVCodec) (trackKey, bool) {
	switch {
	case c.IsVideo():
		return trackVideo, true
	case c.IsAudio():
		return trackAudio, true
	default:
		return 0, false
	}
}

// trackState is the per-(stream, track) anchor.
//
// inputOrigin is the source DTS observed on the first packet of the
// current session. outputAnchor is the wallclock-relative DTS to emit
// for that first packet. Subsequent packets emit
// outputAnchor + (input - inputOrigin), preserving the source's frame
// cadence within a session.
//
// lastOutputDts is the monotonicity floor for this track — every emit
// is clamped to ≥ lastOutputDts + 1 so downstream uint64 dur math
// can never underflow on backward jumps.
type trackState struct {
	seeded        bool
	inputOrigin   int64
	outputAnchor  int64
	lastOutputDts int64
}

// Normaliser is the per-stream timeline anchor. One instance per
// `buffer.StreamCode`. Safe for concurrent Apply / OnSession calls; the
// internal lock matches ptsrebaser's contract because push servers fan
// writes out across multiple goroutines.
type Normaliser struct {
	cfg Config

	mu          sync.Mutex
	sessionID   uint64
	wallOrigin  time.Time
	wallSeeded  bool
	tracks      [numTracks]trackState
	lastDiag    Diagnostic // observed outcome of the most recent Apply
	totalApply  uint64
	totalReanch uint64 // hard re-anchors
	totalDrops  uint64

	// Joint-seed buffer. Populated only when cfg.SeedHoldTimeoutMs > 0
	// AND we have observed the first packet of one track but not the
	// partner. Drained when the partner's first packet arrives (joint-
	// seed completion) or the hold window elapses (timeout fallback).
	pending             []bufferedPacket
	pendingTrack        trackKey
	pendingFirstArrival time.Time
	hasPending          bool
}

// bufferedPacket holds a value-copy of an AVPacket alongside its raw
// input PTS/DTS-derived fields so the joint-seed completion path can
// stamp the buffered packets' output PTS without re-deriving from the
// already-mutated input pointer. Caller-owned backing memory is freed
// to GC as soon as Apply returns — buffering by value detaches the
// packet from the caller's stack/local lifetime.
type bufferedPacket struct {
	pkt   domain.AVPacket
	inDts int64
	cto   int64
}

// Diagnostic captures the outcome of the most recent Apply call. Used
// by the dual-path observer to compare against ptsrebaser's output
// for the same input without exposing internal state.
type Diagnostic struct {
	Track          trackKey
	Drift          int64 // expectedDts - effActualNow, ms
	HardReanchored bool
	Dropped        bool
	OutputDts      int64
	OutputPts      int64
	SessionID      uint64
}

// New returns a Normaliser configured with cfg. An Enabled=false
// Normaliser is valid; Apply will pass packets through untouched and
// OnSession is a no-op.
func New(cfg Config) *Normaliser {
	if cfg.CrossTrackSnapMs <= 0 {
		cfg.CrossTrackSnapMs = 1000
	}
	if cfg.JumpThresholdMs <= 0 {
		cfg.JumpThresholdMs = 2000
	}
	if cfg.SameTimebaseMaxMs <= 0 {
		cfg.SameTimebaseMaxMs = 5000
	}
	if cfg.SeedHoldTimeoutMs < 0 {
		cfg.SeedHoldTimeoutMs = 0
	}
	return &Normaliser{cfg: cfg}
}

// OnSession resets per-track anchor state (each track's first packet
// re-seeds on the next Apply) but PRESERVES the wallclock origin. This
// keeps output DTS strictly monotonic across session boundaries — a
// reconnect's next packet anchors at "elapsed since the worker started"
// rather than jumping back to zero. Downstream consumers see strict
// monotonicity AND the SessionStart=true marker on the boundary packet,
// which is everything they need to reset their own derived state.
//
// To wipe wallclock state too (e.g. shutdown / test teardown), call
// Reset instead.
//
// Calling OnSession with a nil session is a no-op. Calling it before
// any Apply is valid and seeds the session ID without touching tracks
// (which are already zero-valued).
func (n *Normaliser) OnSession(sess *domain.StreamSession) {
	if n == nil || !n.cfg.Enabled || sess == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.sessionID = sess.ID
	n.tracks = [numTracks]trackState{}
	n.pending = n.pending[:0]
	n.hasPending = false
}

// SeedWallclock installs an explicit wallclock origin. Calls before any
// Apply seed the Normaliser without waiting for the first packet, so
// multiple Normaliser instances can share a common timeline (used by
// the coordinator's abr_mixer where V taps + A fan-out write to the
// same downstream rendition buffer set and must produce output DTS on
// the same wallclock axis).
//
// Calling SeedWallclock AFTER Apply has lazy-seeded wallOrigin is
// allowed and overwrites; the per-track lastOutputDts is left alone so
// the monotonic floor still holds against the OLD output sequence.
// In practice the override should happen before the first Apply, so
// this corner case is a safety net rather than an intended workflow.
func (n *Normaliser) SeedWallclock(t time.Time) {
	if n == nil || !n.cfg.Enabled {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.wallOrigin = t
	n.wallSeeded = true
}

// Reset is the unconditional reset (no session tracking). Provided so
// callers without a session boundary still have a way to discard state
// — used by tests and during shutdown.
func (n *Normaliser) Reset() {
	if n == nil {
		return
	}
	n.mu.Lock()
	n.sessionID = 0
	n.wallSeeded = false
	n.wallOrigin = time.Time{}
	n.tracks = [numTracks]trackState{}
	n.pending = n.pending[:0]
	n.hasPending = false
	n.mu.Unlock()
}

// Apply normalises p and returns the packets ready to emit downstream,
// in output-PTS order. Returns nil when the packet is either dropped
// (input running ahead of wallclock past MaxAheadMs) or held during
// the joint-seed window (see SeedHoldTimeoutMs).
//
// Common shapes of the returned slice:
//   - Pass-through (disabled, nil packet, raw-TS marker, unknown codec):
//     [p] with PTS/DTS unchanged.
//   - Post-seed steady state: [p] with PTS/DTS rewritten in place.
//   - Joint-seed completion: N ≥ 2 packets — the buffered partner-track
//     packets plus the current packet, all PTS-stamped and ordered so
//     write order matches output-PTS order.
//   - Held (joint-seed pending): nil.
//   - Dropped (MaxAheadMs overflow): nil. Distinguishable from held via
//     LastDiagnostic().Dropped.
//
// Hard re-anchor events (jump-threshold, regression, behind-cap) do NOT
// propagate via the packet — they surface only via Stats and
// LastDiagnostic. Session boundaries are signalled by
// buffer.Packet.SessionStart, not via Apply.
func (n *Normaliser) Apply(p *domain.AVPacket, now time.Time) []*domain.AVPacket {
	if n == nil || !n.cfg.Enabled {
		if p == nil {
			return nil
		}
		return []*domain.AVPacket{p}
	}
	if p == nil {
		return nil
	}
	if p.Codec == domain.AVCodecRawTSChunk {
		return []*domain.AVPacket{p}
	}
	tk, ok := trackKeyFor(p.Codec)
	if !ok {
		return []*domain.AVPacket{p}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	n.totalApply++

	if !n.wallSeeded {
		n.wallOrigin = now
		n.wallSeeded = true
	}

	inDts := int64(p.DTSms) //nolint:gosec
	inPts := int64(p.PTSms) //nolint:gosec
	cto := inPts - inDts
	if cto < 0 {
		cto = 0
	}

	actualNowMs := now.Sub(n.wallOrigin).Milliseconds()

	// Joint-seed timeout: if a partner track's first packet never
	// arrives within SeedHoldTimeoutMs, drain the buffered first-track
	// packets via the wallclock-anchor fallback so a single-track stream
	// (audio-only, video-only) is not stalled forever.
	var timeoutDrained []*domain.AVPacket
	if n.hasPending && n.cfg.SeedHoldTimeoutMs > 0 &&
		now.Sub(n.pendingFirstArrival).Milliseconds() > n.cfg.SeedHoldTimeoutMs {
		timeoutDrained = n.timeoutDrainPendingLocked()
	}

	result := n.applyOneLocked(p, tk, inDts, cto, actualNowMs, now)
	if len(timeoutDrained) == 0 {
		return result
	}
	return append(timeoutDrained, result...)
}

// applyOneLocked dispatches a single Apply call once the timeout-drain
// prelude has run. Caller has already incremented totalApply, set the
// wallclock origin, and computed inDts / cto / actualNowMs.
func (n *Normaliser) applyOneLocked(
	p *domain.AVPacket,
	tk trackKey,
	inDts, cto, actualNowMs int64,
	now time.Time,
) []*domain.AVPacket {
	track := &n.tracks[tk]
	otherTrack := &n.tracks[numTracks-1-tk]

	if track.seeded {
		return n.applySeededLocked(p, tk, track, inDts, cto, actualNowMs)
	}

	// Joint-seed window: both tracks unseeded AND joint-seed is enabled.
	// Buffer the first-track packets until the partner's first packet
	// arrives so we can compute a joint anchor that preserves source-side
	// V/A offset.
	if n.cfg.SeedHoldTimeoutMs > 0 && !otherTrack.seeded {
		switch {
		case !n.hasPending:
			n.beginPendingLocked(tk, p, inDts, cto, now)
			return nil
		case n.pendingTrack == tk:
			n.extendPendingLocked(p, inDts, cto)
			return nil
		default:
			return n.completeJointSeedLocked(p, tk, inDts, cto, actualNowMs)
		}
	}

	// Single-track seed (joint-seed disabled OR other track already
	// seeded via timeout drain).
	n.seedTrackLocked(track, tk, inDts, actualNowMs, cto, p)
	n.lastDiag = Diagnostic{
		Track:     tk,
		OutputDts: track.outputAnchor,
		OutputPts: track.outputAnchor + cto,
		SessionID: n.sessionID,
	}
	return []*domain.AVPacket{p}
}

// applySeededLocked is the post-seed Apply path: drift cap, jump
// detection, monotonic floor. Returns [p] for a kept packet, nil for a
// drift-cap drop.
func (n *Normaliser) applySeededLocked(
	p *domain.AVPacket,
	tk trackKey,
	track *trackState,
	inDts, cto, actualNowMs int64,
) []*domain.AVPacket {
	inputDelta := inDts - track.inputOrigin
	expectedDts := track.outputAnchor + inputDelta

	if n.cfg.MaxAheadMs > 0 && expectedDts-actualNowMs > n.cfg.MaxAheadMs {
		n.totalDrops++
		n.lastDiag = Diagnostic{
			Track:     tk,
			Drift:     expectedDts - actualNowMs,
			Dropped:   true,
			SessionID: n.sessionID,
		}
		return nil
	}

	effActualNow := actualNowMs
	if effActualNow < track.lastOutputDts {
		effActualNow = track.lastOutputDts
	}
	drift := expectedDts - effActualNow

	tooFarAhead := absInt64(drift) > n.cfg.JumpThresholdMs
	regressed := expectedDts < track.lastOutputDts
	tooFarBehind := n.cfg.MaxBehindMs > 0 && drift < -n.cfg.MaxBehindMs

	if tooFarAhead || regressed || tooFarBehind {
		var reason string
		switch {
		case regressed:
			reason = "monotonic_regress"
		case tooFarAhead:
			reason = "jump_threshold"
		case tooFarBehind:
			reason = "max_behind"
		}
		n.hardReanchorLocked(reanchorContext{
			pkt:         p,
			tk:          tk,
			track:       track,
			inDts:       inDts,
			cto:         cto,
			actualNowMs: actualNowMs,
			expectedDts: expectedDts,
			drift:       drift,
			reason:      reason,
		})
		return []*domain.AVPacket{p}
	}

	track.lastOutputDts = expectedDts
	assignTimes(p, expectedDts, cto)
	n.lastDiag = Diagnostic{
		Track:     tk,
		Drift:     drift,
		OutputDts: expectedDts,
		OutputPts: expectedDts + cto,
		SessionID: n.sessionID,
	}
	return []*domain.AVPacket{p}
}

// reanchorContext bundles the inputs hardReanchorLocked needs so the
// callsite stays under the parameter-count linter ceiling.
type reanchorContext struct {
	pkt         *domain.AVPacket
	tk          trackKey
	track       *trackState
	inDts       int64
	cto         int64
	actualNowMs int64
	expectedDts int64
	drift       int64
	reason      string // pre-computed by caller
}

// hardReanchorLocked performs the seeded-track re-anchor when the
// proposed output PTS overshoots the jump threshold, regresses below
// the monotonic floor, or falls past the behind-drift cap. Rate-limited
// slog so a session with sustained re-anchors does not flood the
// journal.
func (n *Normaliser) hardReanchorLocked(in reanchorContext) {
	target := in.actualNowMs
	if target <= in.track.lastOutputDts {
		target = in.track.lastOutputDts + 1
	}
	in.track.inputOrigin = in.inDts
	in.track.outputAnchor = target
	in.track.lastOutputDts = target
	assignTimes(in.pkt, target, in.cto)
	n.totalReanch++
	if n.totalReanch == 1 || n.totalReanch%50 == 0 {
		slog.Info("timeline: hard re-anchor",
			"session_id", n.sessionID,
			"track", trackKeyName(in.tk),
			"reason", in.reason,
			"drift_ms", in.drift,
			"actual_now_ms", in.actualNowMs,
			"expected_dts_ms", in.expectedDts,
			"new_anchor_ms", target,
			"total_reanch", n.totalReanch,
		)
	}
	n.lastDiag = Diagnostic{
		Track:          in.tk,
		Drift:          in.drift,
		HardReanchored: true,
		OutputDts:      target,
		OutputPts:      target + in.cto,
		SessionID:      n.sessionID,
	}
}

// beginPendingLocked starts a joint-seed hold for tk: p becomes the
// first buffered packet. Subsequent first-track packets append via
// extendPendingLocked; the partner's first packet triggers
// completeJointSeedLocked.
func (n *Normaliser) beginPendingLocked(
	tk trackKey, p *domain.AVPacket, inDts, cto int64, now time.Time,
) {
	n.pendingTrack = tk
	n.pendingFirstArrival = now
	n.hasPending = true
	n.pending = append(n.pending[:0], bufferedPacket{
		pkt:   *p,
		inDts: inDts,
		cto:   cto,
	})
	n.lastDiag = Diagnostic{
		Track:     tk,
		SessionID: n.sessionID,
	}
}

// extendPendingLocked appends another first-track packet to the hold
// buffer while we are still waiting for the partner track.
func (n *Normaliser) extendPendingLocked(p *domain.AVPacket, inDts, cto int64) {
	n.pending = append(n.pending, bufferedPacket{
		pkt:   *p,
		inDts: inDts,
		cto:   cto,
	})
}

// completeJointSeedLocked fires on the partner track's first packet.
// It computes the joint anchor (the earlier-source-DTS track at 0, the
// later at +|delta|) so the source-side V/A offset is preserved, seeds
// both tracks, drains the buffered packets with their joint-anchor PTS,
// stamps the current packet, and returns the merged emit slice in
// output-PTS order.
func (n *Normaliser) completeJointSeedLocked(
	p *domain.AVPacket, tk trackKey, inDts, cto, actualNowMs int64,
) []*domain.AVPacket {
	pendingTk := n.pendingTrack
	pendingFirst := n.pending[0]
	delta := inDts - pendingFirst.inDts

	var currentAnchor, pendingAnchor int64
	var reason string
	switch {
	case delta == 0:
		// Same source moment (rare): both at 0.
		reason = "joint_seed_simultaneous"
	case delta > 0:
		// Current is later in source → pending earlier → pending at 0.
		currentAnchor = delta
		reason = "joint_seed_pending_earlier"
	default:
		// Pending is later in source → current earlier → current at 0.
		pendingAnchor = -delta
		reason = "joint_seed_current_earlier"
	}

	pendingTrack := &n.tracks[pendingTk]
	pendingTrack.seeded = true
	pendingTrack.inputOrigin = pendingFirst.inDts
	pendingTrack.outputAnchor = pendingAnchor
	pendingTrack.lastOutputDts = pendingAnchor

	currentTrack := &n.tracks[tk]
	currentTrack.seeded = true
	currentTrack.inputOrigin = inDts
	currentTrack.outputAnchor = currentAnchor
	currentTrack.lastOutputDts = currentAnchor

	drained := make([]*domain.AVPacket, len(n.pending))
	for i := range n.pending {
		bp := n.pending[i]
		q := bp.pkt
		outDts := pendingAnchor + (bp.inDts - pendingFirst.inDts)
		assignTimes(&q, outDts, bp.cto)
		drained[i] = &q
		if outDts > pendingTrack.lastOutputDts {
			pendingTrack.lastOutputDts = outDts
		}
	}

	assignTimes(p, currentAnchor, cto)

	slog.Info("timeline: joint seed",
		"session_id", n.sessionID,
		"pending_track", trackKeyName(pendingTk),
		"current_track", trackKeyName(tk),
		"reason", reason,
		"source_delta_ms", delta,
		"pending_anchor_ms", pendingAnchor,
		"current_anchor_ms", currentAnchor,
		"pending_buffered_count", len(n.pending),
		"actual_now_ms", actualNowMs,
	)

	n.pending = n.pending[:0]
	n.hasPending = false

	n.lastDiag = Diagnostic{
		Track:     tk,
		OutputDts: currentAnchor,
		OutputPts: currentAnchor + cto,
		SessionID: n.sessionID,
	}

	// Order: the track anchored at 0 emits first so write order matches
	// output-PTS order.
	if currentAnchor == 0 {
		result := make([]*domain.AVPacket, 0, 1+len(drained))
		result = append(result, p)
		result = append(result, drained...)
		return result
	}
	result := make([]*domain.AVPacket, 0, 1+len(drained))
	result = append(result, drained...)
	result = append(result, p)
	return result
}

// timeoutDrainPendingLocked runs when the joint-seed window elapses
// without the partner track delivering its first packet. The pending
// track is seeded via the wallclock-arrival fallback (anchor=0, since
// wallOrigin was set at the first pending packet's arrival), every
// buffered packet is stamped, and the buffer is cleared. After this
// the caller proceeds with the current packet through the seeded path.
func (n *Normaliser) timeoutDrainPendingLocked() []*domain.AVPacket {
	pendingTk := n.pendingTrack
	pendingFirst := n.pending[0]

	pendingTrack := &n.tracks[pendingTk]
	pendingTrack.seeded = true
	pendingTrack.inputOrigin = pendingFirst.inDts
	pendingTrack.outputAnchor = 0
	pendingTrack.lastOutputDts = 0

	drained := make([]*domain.AVPacket, len(n.pending))
	for i := range n.pending {
		bp := n.pending[i]
		q := bp.pkt
		outDts := bp.inDts - pendingFirst.inDts
		assignTimes(&q, outDts, bp.cto)
		drained[i] = &q
		if outDts > pendingTrack.lastOutputDts {
			pendingTrack.lastOutputDts = outDts
		}
	}

	slog.Warn("timeline: joint-seed hold timed out",
		"session_id", n.sessionID,
		"pending_track", trackKeyName(pendingTk),
		"pending_buffered_count", len(n.pending),
		"hold_ms", n.cfg.SeedHoldTimeoutMs,
	)

	n.pending = n.pending[:0]
	n.hasPending = false
	return drained
}

// seedTrackLocked installs the per-track anchor on the first observed
// packet of a track within the current session. Three cases:
//
//  1. Other track has progressed > CrossTrackSnapMs (mixer / burst):
//     snap onto its lastOutputDts so the new track joins the live edge.
//     Source-DTS delta wouldn't make sense — bursty mixer sources can
//     have arbitrary intra-burst PTS span.
//  2. Other track seeded recently AND source-DTS delta fits within
//     SameTimebaseMaxMs (normal MPEG-TS startup): anchor at
//     `other.outputAnchor + (inDts - other.inputOrigin)`. This preserves
//     the encoder's intrinsic V/A offset that the wallclock-arrival
//     anchor would otherwise lose to transcoder pipeline delay
//     (decoder B-frame reorder buffers video by reorder_depth × frame_dur,
//     audio passes through immediately — a 4-frame H.264 High Profile
//     GOP injects 160 ms of audio-leading-video if we anchored both
//     tracks at their respective wallclock arrival times).
//  3. Source-DTS delta exceeds SameTimebaseMaxMs (RTSP per-track RTP
//     with hour-scale timebase offsets): fall back to the wallclock
//     arrival anchor — the source clocks are independent so the delta
//     has no meaning.
func (n *Normaliser) seedTrackLocked(
	track *trackState, tk trackKey, inDts, actualNowMs, cto int64, p *domain.AVPacket,
) {
	anchor := actualNowMs
	otherTrack := &n.tracks[numTracks-1-tk]
	anchorReason := "wallclock_first_track"
	if otherTrack.seeded {
		otherProgression := otherTrack.lastOutputDts - otherTrack.outputAnchor
		if otherProgression > n.cfg.CrossTrackSnapMs {
			anchor = otherTrack.lastOutputDts
			anchorReason = "snap_to_other_live_edge"
		} else {
			sourceDelta := inDts - otherTrack.inputOrigin
			if absInt64(sourceDelta) <= n.cfg.SameTimebaseMaxMs {
				proposed := otherTrack.outputAnchor + sourceDelta
				if proposed >= 0 {
					anchor = proposed
					anchorReason = "source_dts_delta"
				} else {
					anchorReason = "wallclock_proposed_negative"
				}
			} else {
				anchorReason = "wallclock_independent_timebase"
			}
		}
	}
	track.seeded = true
	track.inputOrigin = inDts
	track.outputAnchor = anchor
	track.lastOutputDts = anchor
	assignTimes(p, anchor, cto)

	// Operator-visible seed log so a deploy can be verified by reading
	// the journal: same source moment ⇒ V and A should land at the same
	// output anchor (delta ≈ source-side intrinsic offset, NOT the
	// pipeline arrival skew).
	slog.Info("timeline: seed track",
		"session_id", n.sessionID,
		"track", trackKeyName(tk),
		"reason", anchorReason,
		"in_dts_ms", inDts,
		"actual_now_ms", actualNowMs,
		"anchor_ms", anchor,
		"other_seeded", otherTrack.seeded,
		"other_input_origin_ms", otherTrack.inputOrigin,
		"other_output_anchor_ms", otherTrack.outputAnchor,
	)
}

// LastDiagnostic returns a copy of the most recent Apply outcome. Used
// by the dual-path observer to compare Normaliser behaviour against
// ptsrebaser packet by packet.
func (n *Normaliser) LastDiagnostic() Diagnostic {
	if n == nil {
		return Diagnostic{}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastDiag
}

// Stats returns running counters for the lifetime of this Normaliser
// instance. Used by the dual-path observer for periodic divergence
// rate reporting (e.g. "Normaliser hard-re-anchored N times while
// ptsrebaser did M times — investigate the threshold delta").
type Stats struct {
	TotalApply       uint64
	TotalReanchored  uint64
	TotalDrops       uint64
	CurrentSessionID uint64
}

// Stats returns a snapshot of the running counters.
func (n *Normaliser) Stats() Stats {
	if n == nil {
		return Stats{}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return Stats{
		TotalApply:       n.totalApply,
		TotalReanchored:  n.totalReanch,
		TotalDrops:       n.totalDrops,
		CurrentSessionID: n.sessionID,
	}
}

// assignTimes writes outDts (clamped >= 0) and outDts + cto into the
// packet. Mirrors ptsrebaser.assignTimes so dual-path comparison sees
// identical clamping behaviour for well-behaved input.
func assignTimes(p *domain.AVPacket, outDts, cto int64) {
	if outDts < 0 {
		outDts = 0
	}
	outPts := outDts + cto
	if outPts < 0 {
		outPts = 0
	}
	p.DTSms = uint64(outDts) //nolint:gosec
	p.PTSms = uint64(outPts) //nolint:gosec
}

func absInt64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
