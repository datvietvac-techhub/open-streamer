package timeline

import (
	"testing"
	"time"

	"github.com/ntt0601zcoder/open-streamer/internal/domain"
)

// startWall is the synthetic wallclock origin used across tests. Any
// fixed value works; we keep one constant so test traces are stable.
var startWall = time.Unix(1_700_000_000, 0)

// h264 / aac shorthand to keep table cases short.
func vPkt(pts, dts uint64) *domain.AVPacket {
	return &domain.AVPacket{Codec: domain.AVCodecH264, PTSms: pts, DTSms: dts}
}

func aPkt(pts, dts uint64) *domain.AVPacket {
	return &domain.AVPacket{Codec: domain.AVCodecAAC, PTSms: pts, DTSms: dts}
}

// TestPassthroughCases groups the no-op shapes: disabled, nil, raw-TS,
// unknown codec. Each must leave PTS/DTS untouched and never record a
// HardReanchored diagnostic.
func TestPassthroughCases(t *testing.T) {
	cases := []struct {
		name string
		n    *Normaliser
		p    *domain.AVPacket
	}{
		{
			name: "disabled config",
			n:    New(Config{Enabled: false, JumpThresholdMs: 2000}),
			p:    &domain.AVPacket{Codec: domain.AVCodecH264, PTSms: 12345, DTSms: 12340},
		},
		{
			name: "raw-TS marker codec",
			n:    New(DefaultConfig()),
			p:    &domain.AVPacket{Codec: domain.AVCodecRawTSChunk, PTSms: 99, DTSms: 99},
		},
		{
			name: "unknown codec",
			n:    New(DefaultConfig()),
			p:    &domain.AVPacket{Codec: domain.AVCodecUnknown, PTSms: 555, DTSms: 555},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			origPTS, origDTS := tc.p.PTSms, tc.p.DTSms
			emits := tc.n.Apply(tc.p, startWall)
			if len(emits) != 1 || emits[0] != tc.p {
				t.Fatalf("pass-through case dropped or replaced: emits=%v", emits)
			}
			if tc.p.PTSms != origPTS || tc.p.DTSms != origDTS {
				t.Fatalf("pass-through mutated packet: pts %d→%d dts %d→%d",
					origPTS, tc.p.PTSms, origDTS, tc.p.DTSms)
			}
			if tc.n.LastDiagnostic().HardReanchored {
				t.Fatalf("pass-through must not record HardReanchored")
			}
		})
	}

	t.Run("nil receiver", func(t *testing.T) {
		var n *Normaliser
		emits := n.Apply(&domain.AVPacket{}, startWall)
		if len(emits) != 1 {
			t.Fatalf("nil receiver must pass-through, not drop; emits=%v", emits)
		}
		n.OnSession(&domain.StreamSession{ID: 1})
		n.Reset()
	})

	t.Run("nil packet", func(t *testing.T) {
		emits := New(DefaultConfig()).Apply(nil, startWall)
		if len(emits) != 0 {
			t.Fatalf("nil packet must return empty slice, got %v", emits)
		}
	})
}

// TestSeed verifies the seed semantics: the very first packet of each
// track anchors at elapsed-wallclock (0 ms for the wallclock-seed track,
// >0 for tracks arriving later), CTO is preserved, and the seed never
// records a HardReanchored diagnostic.
func TestSeed(t *testing.T) {
	n := New(DefaultConfig())
	p := vPkt(12350, 12345) // CTO = 5
	n.Apply(p, startWall)

	if p.DTSms != 0 || p.PTSms != 5 {
		t.Fatalf("seed: want dts=0 pts=5, got dts=%d pts=%d", p.DTSms, p.PTSms)
	}
	if n.LastDiagnostic().HardReanchored {
		t.Fatal("seed packet must not record HardReanchored")
	}

	// Audio first packet 5 ms later — anchored at elapsed (5), independent
	// timebase preserved (no re-anchor, no shared origin clobber).
	a := aPkt(50_000_000, 50_000_000)
	n.Apply(a, startWall.Add(5*time.Millisecond))
	if a.DTSms != 5 {
		t.Fatalf("audio seed at +5ms: want dts=5, got %d", a.DTSms)
	}
	if n.LastDiagnostic().HardReanchored {
		t.Fatal("audio seed must not record HardReanchored")
	}
}

// TestSteadyState walks 25 frames at 40 ms cadence and asserts each
// emitted DTS sits at i*40 (no drift, no re-anchor).
func TestSteadyState(t *testing.T) {
	n := New(DefaultConfig())
	for i := 0; i < 25; i++ {
		p := vPkt(uint64(1_000_000+i*40), uint64(1_000_000+i*40)) //nolint:gosec
		n.Apply(p, startWall.Add(time.Duration(i*40)*time.Millisecond))
		want := uint64(i * 40) //nolint:gosec
		if p.DTSms != want {
			t.Fatalf("steady frame %d: want dts=%d, got %d", i, want, p.DTSms)
		}
		if n.LastDiagnostic().HardReanchored {
			t.Fatalf("steady frame %d: HardReanchored should not fire", i)
		}
	}
}

// TestMonotonicFloor — feed a backward-jumping input PTS that lands
// below the running output floor. Output must still be strictly
// monotonic (≥ lastOutputDts+1) and HardReanchored must record.
func TestMonotonicFloor(t *testing.T) {
	n := New(Config{Enabled: true, JumpThresholdMs: 500, CrossTrackSnapMs: 1000})

	// 25 frames at 40 ms in PTS, taking 1000 ms wallclock.
	for i := 0; i < 25; i++ {
		p := vPkt(uint64(1_000_000+i*40), uint64(1_000_000+i*40)) //nolint:gosec
		n.Apply(p, startWall.Add(time.Duration(i*40)*time.Millisecond))
	}
	// Now an upstream regression: PTS jumps 500 ms BACKWARDS.
	jump := vPkt(uint64(1_000_960-500), uint64(1_000_960-500))
	n.Apply(jump, startWall.Add(1040*time.Millisecond))

	if !n.LastDiagnostic().HardReanchored {
		t.Fatal("backward jump must record HardReanchored")
	}
	// Monotonic floor: output > 960 (the 25th frame's emit).
	if jump.DTSms <= 960 {
		t.Fatalf("backward jump must clamp ≥ lastOutputDts+1; got %d", jump.DTSms)
	}
}

// TestDriftCapAhead — sustained input running ahead of wallclock past
// MaxAheadMs must drop. Subsequent slower packets recover.
func TestDriftCapAhead(t *testing.T) {
	n := New(Config{
		Enabled:          true,
		JumpThresholdMs:  10_000, // wide so the drift cap fires before the jump branch
		MaxAheadMs:       500,
		CrossTrackSnapMs: 1000,
	})

	// Seed at t=0.
	n.Apply(vPkt(0, 0), startWall)
	// One frame "in the future" — input claims +2000 ms, wallclock at +200 ms.
	// Expected drift = 2000 - 200 = 1800 > MaxAheadMs(500) → drop.
	future := vPkt(2000, 2000)
	if emits := n.Apply(future, startWall.Add(200*time.Millisecond)); len(emits) != 0 {
		t.Fatalf("drift-cap-ahead must drop packet that races wallclock; got emits=%v dts=%d", emits, future.DTSms)
	}
	diag := n.LastDiagnostic()
	if !diag.Dropped {
		t.Fatalf("diagnostic must report Dropped=true; got %+v", diag)
	}
}

// TestDriftCapBehind — sustained input running BEHIND wallclock past
// MaxBehindMs must hard-re-anchor (the symmetric branch ptsrebaser
// lacks). This is the long-runtime drift class the refactor targets.
func TestDriftCapBehind(t *testing.T) {
	n := New(Config{
		Enabled:          true,
		JumpThresholdMs:  10_000, // wide so the |drift| branch doesn't pre-empt
		MaxBehindMs:      500,
		CrossTrackSnapMs: 1000,
	})

	// Seed at t=0; subsequent frames keep up at first.
	n.Apply(vPkt(0, 0), startWall)
	n.Apply(vPkt(40, 40), startWall.Add(40*time.Millisecond))

	// Wallclock leaps 2 s while the source delivers a single +80 ms frame.
	// Expected drift = 80 - 2080 = -2000 < -MaxBehindMs(-500) → re-anchor.
	lag := vPkt(80, 80)
	emits := n.Apply(lag, startWall.Add(2080*time.Millisecond))
	if len(emits) == 0 {
		t.Fatal("behind-drift packet must NOT be dropped (only ahead-drift drops)")
	}
	if !n.LastDiagnostic().HardReanchored {
		t.Fatal("behind-drift cap must record HardReanchored on re-anchor")
	}
	// Output anchored at actualNow = 2080 ms (≈), clamped above last emit.
	if lag.DTSms < 2000 {
		t.Fatalf("behind-drift re-anchor: want dts≈2080, got %d", lag.DTSms)
	}
}

// TestJumpAhead — input PTS leaps forward past JumpThresholdMs. Must
// hard-re-anchor to wallclock (not blindly trust the source) and record
// HardReanchored.
func TestJumpAhead(t *testing.T) {
	n := New(Config{Enabled: true, JumpThresholdMs: 500, CrossTrackSnapMs: 1000})
	n.Apply(vPkt(0, 0), startWall)
	n.Apply(vPkt(40, 40), startWall.Add(40*time.Millisecond))

	// Source jumps +10 s suddenly while wallclock only advanced 80 ms.
	jump := vPkt(10_040, 10_040)
	n.Apply(jump, startWall.Add(80*time.Millisecond))

	if !n.LastDiagnostic().HardReanchored {
		t.Fatal("forward jump past threshold must record HardReanchored")
	}
	// Anchored to actualNow ≈ 80 ms, not to the upstream's claimed 10_040.
	if jump.DTSms > 200 {
		t.Fatalf("forward jump re-anchor: want dts≈80 (wallclock), got %d", jump.DTSms)
	}
}

// TestCrossTrackSnap — when V drains a multi-second burst before A
// seeds, A snaps onto V's last emit. Mirrors ptsrebaser's progression-
// based snap.
func TestCrossTrackSnap(t *testing.T) {
	n := New(DefaultConfig())

	// 100 video frames at 40 ms PTS cadence, all delivered within ~500 µs.
	for i := 0; i < 100; i++ {
		v := vPkt(uint64(i*40), uint64(i*40)) //nolint:gosec
		n.Apply(v, startWall.Add(time.Duration(i)*5*time.Microsecond))
	}
	// V's last output ≈ 99*40 = 3960 ms.
	a := aPkt(0, 0)
	n.Apply(a, startWall.Add(600*time.Microsecond))
	if a.DTSms < 3900 {
		t.Fatalf("cross-track snap: A should anchor onto V's progression (≈3960), got %d", a.DTSms)
	}
	if n.LastDiagnostic().HardReanchored {
		t.Fatal("cross-track snap must NOT record HardReanchored (it's a clean seed, not a jump)")
	}
}

// TestIndependentVATimelines — V and A may have wildly different
// upstream timebases (RTP per-track). The Normaliser must NOT use a
// single shared origin or every other packet would ping-pong it.
func TestIndependentVATimelines(t *testing.T) {
	n := New(DefaultConfig())

	v := vPkt(1_000_000, 1_000_000)
	a := aPkt(50_000_000, 50_000_000)
	n.Apply(v, startWall)
	n.Apply(a, startWall.Add(5*time.Millisecond))

	if v.DTSms != 0 {
		t.Fatalf("V seed: want dts=0, got %d", v.DTSms)
	}
	if a.DTSms != 5 {
		t.Fatalf("A seed (5ms later): want dts=5, got %d", a.DTSms)
	}
	if n.LastDiagnostic().HardReanchored {
		t.Fatal("independent A seed must not record HardReanchored")
	}

	v2 := vPkt(1_000_040, 1_000_040)
	a2 := aPkt(50_000_021, 50_000_021)
	n.Apply(v2, startWall.Add(40*time.Millisecond))
	n.Apply(a2, startWall.Add(45*time.Millisecond))
	if v2.DTSms != 40 {
		t.Fatalf("V2: want 40, got %d", v2.DTSms)
	}
	if a2.DTSms != 26 {
		t.Fatalf("A2: want 26 (=5+21), got %d", a2.DTSms)
	}
}

// TestOnSessionResetsPerTrackButPreservesWallclock — OnSession wipes
// per-track input anchors so the next packet's source PTS becomes the
// new per-track baseline, but wallOrigin persists across session
// boundaries so output DTS stays strictly monotonic. The boundary cue
// for downstream consumers is the buffer-hub-level SessionStart=true
// marker, not a DTS reset.
func TestOnSessionResetsPerTrackButPreservesWallclock(t *testing.T) {
	n := New(DefaultConfig())

	// Session 1: V runs to ~1 s of output DTS.
	n.OnSession(&domain.StreamSession{ID: 1, Reason: domain.SessionStartFresh})
	for i := 0; i < 25; i++ {
		v := vPkt(uint64(i*40), uint64(i*40)) //nolint:gosec
		n.Apply(v, startWall.Add(time.Duration(i*40)*time.Millisecond))
	}
	if got := n.Stats().TotalApply; got != 25 {
		t.Fatalf("after S1: TotalApply=%d, want 25", got)
	}

	// Session 2: fresh upstream PTS at a totally different epoch.
	n.OnSession(&domain.StreamSession{ID: 2, Reason: domain.SessionStartFailover})
	if got := n.Stats().CurrentSessionID; got != 2 {
		t.Fatalf("CurrentSessionID after OnSession(2) = %d, want 2", got)
	}
	// 500 ms after the last S1 packet, S2's first V arrives with a
	// wildly different upstream PTS. The per-track input baseline
	// resets onto that upstream PTS, but the OUTPUT anchors at
	// elapsed-since-wallOrigin — i.e. ≈ 1500 ms (1000 ms of S1 plus
	// the 500 ms gap).
	s2start := startWall.Add(1500 * time.Millisecond)
	v := vPkt(99_999_000, 99_999_000)
	n.Apply(v, s2start)
	if v.DTSms < 1000 {
		t.Fatalf("S2 first V must anchor past S1's last output for monotonicity; got DTS=%d", v.DTSms)
	}
	if n.LastDiagnostic().HardReanchored {
		t.Fatal("S2 first packet seed must not record HardReanchored (caller sets SessionStart instead)")
	}

	// S2 second V: 40 ms later upstream, 40 ms later wallclock — must
	// advance 40 ms in output too.
	prevDTS := v.DTSms
	v2 := vPkt(99_999_040, 99_999_040)
	n.Apply(v2, s2start.Add(40*time.Millisecond))
	if v2.DTSms != prevDTS+40 {
		t.Fatalf("S2 second V: want %d, got %d", prevDTS+40, v2.DTSms)
	}
}

// TestSessionStartReasons covers the five reasons reset state
// identically — the Normaliser's reset behaviour is reason-agnostic.
func TestSessionStartReasons(t *testing.T) {
	reasons := []domain.SessionStartReason{
		domain.SessionStartFresh,
		domain.SessionStartReconnect,
		domain.SessionStartFailover,
		domain.SessionStartMixerCycle,
		domain.SessionStartConfigChange,
	}
	for _, r := range reasons {
		t.Run(r.String(), func(t *testing.T) {
			n := New(DefaultConfig())
			n.Apply(vPkt(0, 0), startWall)
			prevDTS := uint64(0)
			n.Apply(vPkt(40, 40), startWall.Add(40*time.Millisecond))

			n.OnSession(&domain.StreamSession{ID: 7, Reason: r})

			// Next packet uses fresh per-track baseline but continues
			// the wallclock-anchored output sequence (no backward jump).
			v := vPkt(99_999_000, 99_999_000)
			n.Apply(v, startWall.Add(500*time.Millisecond))
			if v.DTSms <= prevDTS+40 {
				t.Fatalf("%s: first packet after OnSession must continue monotonic output, got %d <= prev %d",
					r, v.DTSms, prevDTS+40)
			}
		})
	}
}

// TestSeedWallclockSharesTimeline — multiple Normaliser instances seeded
// with the same wallclock origin produce output on a common timeline,
// regardless of when each instance saw its first packet. Used by the
// coordinator's abr_mixer to keep V-tap and A-fan-out outputs aligned.
func TestSeedWallclockSharesTimeline(t *testing.T) {
	origin := startWall

	vNorm := New(DefaultConfig())
	aNorm := New(DefaultConfig())
	vNorm.SeedWallclock(origin)
	aNorm.SeedWallclock(origin)

	// V's first packet arrives 100 ms after the shared origin.
	v := vPkt(0, 0)
	vNorm.Apply(v, origin.Add(100*time.Millisecond))
	if v.DTSms != 100 {
		t.Fatalf("V seeded against shared origin: want DTS=100, got %d", v.DTSms)
	}

	// A's first packet arrives 600 ms after the shared origin — different
	// upstream, different timebase, but anchored to the SAME wallclock
	// origin so its output DTS is 600.
	a := aPkt(50_000_000, 50_000_000)
	aNorm.Apply(a, origin.Add(600*time.Millisecond))
	if a.DTSms != 600 {
		t.Fatalf("A seeded against shared origin: want DTS=600, got %d", a.DTSms)
	}
}

// TestDiagnosticReflectsLastApply — the dual-path observer relies on
// LastDiagnostic to compare per-packet outcomes against ptsrebaser.
func TestDiagnosticReflectsLastApply(t *testing.T) {
	n := New(DefaultConfig())
	n.OnSession(&domain.StreamSession{ID: 42})

	n.Apply(vPkt(0, 0), startWall)
	d := n.LastDiagnostic()
	if d.Track != trackVideo {
		t.Fatalf("seed diagnostic Track = %v, want trackVideo", d.Track)
	}
	if d.SessionID != 42 {
		t.Fatalf("seed diagnostic SessionID = %d, want 42", d.SessionID)
	}
	if d.HardReanchored || d.Dropped {
		t.Fatal("seed must not be marked as re-anchored or dropped")
	}

	// Force a hard re-anchor via forward jump.
	n.Apply(vPkt(10_000, 10_000), startWall.Add(50*time.Millisecond))
	d = n.LastDiagnostic()
	if !d.HardReanchored {
		t.Fatal("forward-jump diagnostic must report HardReanchored=true")
	}
}

// TestResetClearsEverything — Reset zeroes session ID + all per-track
// state, so subsequent Apply behaves like a freshly-constructed
// Normaliser.
func TestResetClearsEverything(t *testing.T) {
	n := New(DefaultConfig())
	n.OnSession(&domain.StreamSession{ID: 9})
	n.Apply(vPkt(0, 0), startWall)

	n.Reset()

	if got := n.Stats().CurrentSessionID; got != 0 {
		t.Fatalf("Reset must clear SessionID, got %d", got)
	}
	// Apply again with very different PTS — must seed at 0.
	v := vPkt(99_999_000, 99_999_000)
	n.Apply(v, startWall.Add(time.Second))
	if v.DTSms != 0 {
		t.Fatalf("Apply after Reset must re-seed at 0, got %d", v.DTSms)
	}
}

// TestCTOPreserved — Composition Time Offset (PTS − DTS) must survive
// every code path. B-frames depend on this for correct presentation
// order in the muxer downstream.
func TestCTOPreserved(t *testing.T) {
	n := New(DefaultConfig())

	cases := []struct {
		name string
		pts  uint64
		dts  uint64
		want int64
	}{
		{"non-B (PTS=DTS)", 1000, 1000, 0},
		{"B-frame with CTO=33", 1033, 1000, 33},
		{"B-frame with CTO=80", 1080, 1000, 80},
		{"clamp negative CTO (clamps to 0)", 1000, 1050, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n.Reset()
			p := vPkt(tc.pts, tc.dts)
			n.Apply(p, startWall)
			got := int64(p.PTSms) - int64(p.DTSms) //nolint:gosec
			if got != tc.want {
				t.Fatalf("CTO: want %d, got %d (pts=%d dts=%d)",
					tc.want, got, p.PTSms, p.DTSms)
			}
		})
	}
}

// TestDefaultConfigMatchesPTSRebaser — sanity-check that the default
// settings line up with ptsrebaser.DefaultConfig so the dual-path
// observer doesn't drown in trivial divergences.
func TestDefaultConfigMatchesPTSRebaser(t *testing.T) {
	c := DefaultConfig()
	if !c.Enabled {
		t.Fatal("DefaultConfig must be enabled")
	}
	if c.JumpThresholdMs != 2000 {
		t.Fatalf("DefaultConfig JumpThresholdMs = %d, want 2000 (parity with ptsrebaser)", c.JumpThresholdMs)
	}
	if c.MaxAheadMs != 0 {
		t.Fatalf("DefaultConfig MaxAheadMs = %d, want 0 (parity with ptsrebaser)", c.MaxAheadMs)
	}
	if c.CrossTrackSnapMs != 1000 {
		t.Fatalf("DefaultConfig CrossTrackSnapMs = %d, want 1000 (parity)", c.CrossTrackSnapMs)
	}
	if c.SeedHoldTimeoutMs != 0 {
		t.Fatalf("DefaultConfig SeedHoldTimeoutMs = %d, want 0 (joint-seed opt-in)", c.SeedHoldTimeoutMs)
	}
}

// jointSeedConfig returns a Normaliser config with joint-seed enabled
// and a tight hold window so timeout tests run quickly.
func jointSeedConfig(holdMs int64) Config {
	c := DefaultConfig()
	c.SeedHoldTimeoutMs = holdMs
	return c
}

// TestJointSeedAudioLeadsVideo — the user-facing bug scenario. Source
// emits the first audio packet ~500 ms BEFORE the first video packet
// in source-PTS terms (the multicast scheduler had audio frames pre-
// buffered ahead of video). The two arrive on the wire ~80 ms apart
// in WALLCLOCK. Joint seed must:
//
//   - Anchor the EARLIER-source track (audio here) at output PTS=0.
//   - Anchor the LATER-source track (video here) at output PTS=+475.
//   - Preserve that V/A offset in the output timeline, NOT replace it
//     with the network arrival skew (the bug fixed by this feature).
//
// Output PTS for same source moment X (assuming calibrated encoder
// with audio.inPTS == video.inPTS for source moment X) is:
//
//	audio_out(X) = 0 + (X − audio_origin) = X − audio_origin
//	video_out(X) = 475 + (X − video_origin) = X − video_origin + 475
//
// With audio_origin=A, video_origin=A+475, the two reduce to
// audio_out(X) == video_out(X) — lip-sync preserved.
func TestJointSeedAudioLeadsVideo(t *testing.T) {
	n := New(jointSeedConfig(1000))

	const audioOrigin uint64 = 69_579_246
	const videoOrigin uint64 = 69_579_721 // 475 ms LATER than audio in source-DTS

	// Video first packet arrives first in WALLCLOCK at t=0.
	v := vPkt(videoOrigin, videoOrigin)
	emits := n.Apply(v, startWall)
	if len(emits) != 0 {
		t.Fatalf("first video packet must be held during joint-seed; got %d emits", len(emits))
	}

	// Audio first packet arrives at t=78 ms (the observed worst-case
	// network skew that the old wallclock-arrival fallback baked into
	// the output PTS).
	a := aPkt(audioOrigin, audioOrigin)
	emits = n.Apply(a, startWall.Add(78*time.Millisecond))
	if len(emits) != 2 {
		t.Fatalf("joint-seed completion must emit drained + current = 2 packets; got %d", len(emits))
	}

	// Audio anchors at 0 (earlier in source). Audio packet emits first.
	// Buffered packets are value-copied, so identity-compare only the
	// current packet (a), not drained.
	if !emits[0].Codec.IsAudio() {
		t.Fatalf("current audio (earlier source) must emit first; got codec=%v", emits[0].Codec)
	}
	if emits[0] != a {
		t.Fatal("current packet identity must be preserved (audio passed in by caller)")
	}
	if a.DTSms != 0 {
		t.Fatalf("audio anchor: want DTS=0, got %d", a.DTSms)
	}

	// Video anchors at +475 (later in source by 475 ms). Drained packet
	// is a copy of the buffered video, not the original v pointer.
	if !emits[1].Codec.IsVideo() {
		t.Fatalf("buffered video (later source) must emit second; got codec=%v", emits[1].Codec)
	}
	if emits[1].DTSms != 475 {
		t.Fatalf("video anchor: want DTS=475 (preserving source delta), got %d", emits[1].DTSms)
	}

	// Sanity-check the post-seed steady state: subsequent video and
	// audio packets for the SAME source moment must produce equal
	// output PTS.
	v2 := vPkt(videoOrigin+40, videoOrigin+40)
	a2 := aPkt(audioOrigin+40, audioOrigin+40)
	emitsV := n.Apply(v2, startWall.Add(118*time.Millisecond))
	emitsA := n.Apply(a2, startWall.Add(119*time.Millisecond))
	if len(emitsV) != 1 || len(emitsA) != 1 {
		t.Fatalf("post-seed steady-state must emit one packet each; got v=%v a=%v", emitsV, emitsA)
	}
	// audio_origin+40 vs video_origin+40 differ by 475 in input space.
	// With audio anchor 0 and video anchor 475, video subsequent →
	// 0 + (videoOrigin+40 - videoOrigin) + 475 = 40 + 475 = 515 — wait
	// no: video.outputAnchor=475, video.inputOrigin=videoOrigin. Output
	// = 475 + (videoOrigin+40 - videoOrigin) = 515.
	if v2.DTSms != 515 {
		t.Fatalf("video subsequent: want DTS=515, got %d", v2.DTSms)
	}
	if a2.DTSms != 40 {
		t.Fatalf("audio subsequent: want DTS=40, got %d", a2.DTSms)
	}
}

// TestJointSeedBFramesLargeGap reproduces the production lip-sync report.
// The affected feeds are H.264 High profile (has_b_frames=2 ⇒ video carries a
// non-zero CTO = PTS−DTS) delivered over MPEG-TS with a LARGE inter-track
// delivery gap (~487 ms measured on the affected channels via the joint-seed
// source_delta; well-behaved channels show ~78 ms). Unlike
// TestJointSeedAudioLeadsVideo (PTS==DTS, no reorder) this exercises the
// B-frame path the affected sources actually use.
//
// The raw feed is correctly synced — a player fed the multicast directly
// (Flussonic) shows no skew — i.e. for every presentation moment X the video
// frame PTS and the audio frame PTS are equal. The Normaliser MUST preserve
// that: a V frame and an A frame for the same source moment must emit at the
// same output PTS. If they don't, the Normaliser is baking the delivery gap
// into the presentation timeline (the bug under investigation).
func TestJointSeedBFramesLargeGap(t *testing.T) {
	const cto uint64 = 80  // reorder delay, ms (≈2 frames @ 25 fps; has_b_frames=2)
	const gap uint64 = 487 // video delivered AHEAD of audio by 487 ms in source DTS

	n := New(jointSeedConfig(1000))

	// First audio packet's source PTS/DTS. The first video packet sits +gap
	// later in source DTS (video buffer fuller at connect).
	const audioOrigin uint64 = 1_000_000
	videoFirstDts := audioOrigin + gap   // 1_000_487
	videoFirstPts := videoFirstDts + cto // 1_000_567

	// Video arrives first in WALLCLOCK; held during the joint-seed window.
	v0 := vPkt(videoFirstPts, videoFirstDts)
	if emits := n.Apply(v0, startWall); len(emits) != 0 {
		t.Fatalf("first video must be held during joint-seed; got %d emits", len(emits))
	}
	// Audio arrives ~80 ms later in wallclock; completes the joint seed.
	a0 := aPkt(audioOrigin, audioOrigin)
	if emits := n.Apply(a0, startWall.Add(80*time.Millisecond)); len(emits) != 2 {
		t.Fatalf("joint-seed completion must emit 2 packets; got %d", len(emits))
	}

	// Feed a V frame and an A frame for the SAME source presentation moment X.
	// Synced source ⇒ both carry source PTS == X. Feed each at the wallclock
	// matching its expected output DTS so no drift re-anchor fires.
	const X uint64 = 1_001_000 // ~1 s in; both tracks now overlap
	vX := vPkt(X, X-cto)       // video: PTS=X, DTS=X−cto (reorder)
	aX := aPkt(X, X)           // audio: PTS=DTS=X
	emitsV := n.Apply(vX, startWall.Add(920*time.Millisecond))
	emitsA := n.Apply(aX, startWall.Add(1000*time.Millisecond))
	if len(emitsV) != 1 || len(emitsA) != 1 {
		t.Fatalf("post-seed must emit one packet each; got v=%d a=%d", len(emitsV), len(emitsA))
	}

	if vX.PTSms != aX.PTSms {
		t.Fatalf("LIP-SYNC BROKEN: same source moment X=%d → video_pts=%d, audio_pts=%d (skew=%+d ms)",
			X, vX.PTSms, aX.PTSms, int64(vX.PTSms)-int64(aX.PTSms))
	}
}

// TestPTSWrapPreservesAVSync reproduces the slow A/V drift confirmed in
// production. MPEG-TS carries a 33-bit, 90 kHz PTS that wraps every
// 2^33 / 90000 ≈ 95,443,717 ms (~26.5 h). When the source PTS wraps, each
// track's input DTS jumps backward by ~that amount; the Normaliser currently
// treats it as a regression and hard-re-anchors video and audio INDEPENDENTLY
// to wallclock (observed in the journal: monotonic_regress drift_ms=-95443xxx).
// Because the two tracks cross the wrap boundary at slightly different moments,
// the independent re-anchors land on different wallclock targets → a permanent
// A/V skew that persists until restart (the ~22 h lip-sync drift report).
//
// Correct behaviour: the wrap must be UNWRAPPED (treated as +wrap_period) so
// the timeline stays continuous and both tracks keep their relative offset.
// A same-source-moment V/A pair AFTER the wrap must emit at equal PTS.
func TestPTSWrapPreservesAVSync(t *testing.T) {
	const wrapMs uint64 = (1 << 33) / 90 // 2^33 ticks / 90 kHz, in ms ≈ 95,443,717
	n := New(jointSeedConfig(1000))

	base := wrapMs - 100 // seed just before the wrap boundary

	// Joint-seed both tracks synced at `base` (source delta 0 → both anchor 0).
	n.Apply(vPkt(base, base), startWall)                          // held
	n.Apply(aPkt(base, base), startWall.Add(20*time.Millisecond)) // completes seed

	// Two steady synced frames (output follows source, drift ~0).
	n.Apply(vPkt(base+40, base+40), startWall.Add(40*time.Millisecond))
	n.Apply(aPkt(base+40, base+40), startWall.Add(40*time.Millisecond))
	n.Apply(vPkt(base+80, base+80), startWall.Add(80*time.Millisecond))
	n.Apply(aPkt(base+80, base+80), startWall.Add(80*time.Millisecond))
	// Now at base+80 = wrapMs-20; the next frame of each track wraps to ~0.

	// Video wraps at wall=120 ms (its frame crosses the boundary): wrapped → 20.
	n.Apply(vPkt(20, 20), startWall.Add(120*time.Millisecond))
	// Audio wraps a beat later at wall=140 ms and at a slightly different value
	// (denser frames cross the boundary between video frames) → wrapped 10.
	n.Apply(aPkt(10, 10), startWall.Add(140*time.Millisecond))

	// Post-wrap same-source-moment pair (both at wrapped source DTS = 200).
	vX := vPkt(200, 200)
	aX := aPkt(200, 200)
	n.Apply(vX, startWall.Add(300*time.Millisecond))
	n.Apply(aX, startWall.Add(300*time.Millisecond))

	if vX.PTSms != aX.PTSms {
		t.Fatalf("PTS WRAP BROKE A/V SYNC: post-wrap same moment → video_pts=%d audio_pts=%d (skew=%+d ms)",
			vX.PTSms, aX.PTSms, int64(vX.PTSms)-int64(aX.PTSms))
	}
}

// TestReanchorPreservesAVSync covers the secondary desync mechanism: a hard
// re-anchor (clock drift / source stall past the jump threshold) forward-jumps
// a track to wallclock. The journal shows these firing per-track
// (jump_threshold drift_ms≈-2000). When video re-anchors but audio resumes a
// beat later (or vice versa) the two land on different wallclock targets →
// A/V skew. A re-anchor must shift the PARTNER track by the same amount so the
// relative offset survives. Here: a stall, video resumes (re-anchors forward)
// at wall=5000, audio resumes 100 ms later — a same-source-moment pair after
// must still emit at equal PTS.
func TestReanchorPreservesAVSync(t *testing.T) {
	n := New(jointSeedConfig(1000))

	// Seed both synced at source 0 (joint-seed: video held, audio completes).
	n.Apply(vPkt(0, 0), startWall)
	n.Apply(aPkt(0, 0), startWall.Add(10*time.Millisecond))
	// One steady synced frame.
	n.Apply(vPkt(40, 40), startWall.Add(40*time.Millisecond))
	n.Apply(aPkt(40, 40), startWall.Add(40*time.Millisecond))

	// Stall: wallclock advances to ~5 s while source barely moved. Video
	// resumes first → drift ≪ -jumpThreshold → forward re-anchor to wallclock.
	n.Apply(vPkt(80, 80), startWall.Add(5000*time.Millisecond))
	// Audio resumes 100 ms later in wallclock.
	n.Apply(aPkt(80, 80), startWall.Add(5100*time.Millisecond))

	// Same-source-moment pair afterward (source 120). Must emit at equal PTS.
	vX := vPkt(120, 120)
	aX := aPkt(120, 120)
	n.Apply(vX, startWall.Add(5040*time.Millisecond))
	n.Apply(aX, startWall.Add(5140*time.Millisecond))

	if vX.PTSms != aX.PTSms {
		t.Fatalf("RE-ANCHOR BROKE A/V SYNC: same source moment → video_pts=%d audio_pts=%d (skew=%+d ms)",
			vX.PTSms, aX.PTSms, int64(vX.PTSms)-int64(aX.PTSms))
	}
}

// TestJointSeedVideoLeadsAudio — symmetric to TestJointSeedAudioLeadsVideo
// but with the source emitting video first in source-DTS terms (the
// normal case). Joint-seed must anchor video at 0 and audio at +delta.
func TestJointSeedVideoLeadsAudio(t *testing.T) {
	n := New(jointSeedConfig(1000))

	const videoOrigin uint64 = 1_000_000
	const audioOrigin uint64 = 1_000_055 // 55 ms LATER than video in source-DTS

	// Audio arrives first in WALLCLOCK at t=0.
	a := aPkt(audioOrigin, audioOrigin)
	emits := n.Apply(a, startWall)
	if len(emits) != 0 {
		t.Fatalf("first audio packet must be held during joint-seed; got %d emits", len(emits))
	}

	// Video arrives at t=20 ms.
	v := vPkt(videoOrigin, videoOrigin)
	emits = n.Apply(v, startWall.Add(20*time.Millisecond))
	if len(emits) != 2 {
		t.Fatalf("joint-seed completion must emit 2 packets; got %d", len(emits))
	}

	// Video anchors at 0 (earlier in source). Video emits first.
	if !emits[0].Codec.IsVideo() {
		t.Fatalf("current video (earlier source) must emit first; got codec=%v", emits[0].Codec)
	}
	if emits[0] != v {
		t.Fatal("current packet identity must be preserved (video passed in by caller)")
	}
	if v.DTSms != 0 {
		t.Fatalf("video anchor: want DTS=0, got %d", v.DTSms)
	}
	// Audio anchors at +55. Drained packet is a copy of buffered audio.
	if !emits[1].Codec.IsAudio() {
		t.Fatalf("buffered audio (later source) must emit second; got codec=%v", emits[1].Codec)
	}
	if emits[1].DTSms != 55 {
		t.Fatalf("audio anchor: want DTS=55, got %d", emits[1].DTSms)
	}
}

// TestJointSeedTimeout — when the partner track never arrives, the
// pending buffer drains via the wallclock-anchor fallback so a single-
// track stream is not stalled forever.
func TestJointSeedTimeout(t *testing.T) {
	const holdMs int64 = 100
	n := New(jointSeedConfig(holdMs))

	// Video at t=0 — held during the joint-seed window.
	v1 := vPkt(1000, 1000)
	if emits := n.Apply(v1, startWall); len(emits) != 0 {
		t.Fatalf("first video held during joint-seed; got %d emits", len(emits))
	}
	// Another video at t=40 — still held.
	v2 := vPkt(1040, 1040)
	if emits := n.Apply(v2, startWall.Add(40*time.Millisecond)); len(emits) != 0 {
		t.Fatalf("second video held while still within hold window; got %d emits", len(emits))
	}

	// Third video at t=150 — past the 100 ms hold window. Drain triggers
	// before processing v3: 2 drained + v3 itself = 3 packets.
	v3 := vPkt(1080, 1080)
	emits := n.Apply(v3, startWall.Add(150*time.Millisecond))
	if len(emits) != 3 {
		t.Fatalf("timeout drain must emit 2 drained + current = 3 packets; got %d", len(emits))
	}
	// Drained packets are value-copies of v1 and v2; only v3 retains the
	// caller's original pointer. Stamped against pending anchor=0
	// (wallclock-arrival fallback): v1 at origin → DTS=0; v2 → DTS=40;
	// v3 (post-seed) → expected = anchor + (in − origin) = 0 + (1080 −
	// 1000) = 80.
	if emits[0].DTSms != 0 {
		t.Fatalf("drained v1 DTS: want 0, got %d", emits[0].DTSms)
	}
	if emits[1].DTSms != 40 {
		t.Fatalf("drained v2 DTS: want 40, got %d", emits[1].DTSms)
	}
	if emits[2] != v3 {
		t.Fatal("current packet (v3) identity must be preserved")
	}
	if v3.DTSms != 80 {
		t.Fatalf("v3 DTS: want 80, got %d", v3.DTSms)
	}
}

// TestJointSeedOnSessionClearsPending — OnSession must clear the
// pending buffer so a session boundary mid-hold doesn't leak stale
// packets into the next session's output.
func TestJointSeedOnSessionClearsPending(t *testing.T) {
	n := New(jointSeedConfig(1000))

	// Buffer one packet, then start a new session.
	n.Apply(vPkt(1000, 1000), startWall)
	n.OnSession(&domain.StreamSession{ID: 99})

	// New session: audio arrives first. No buffered video should leak.
	a := aPkt(50_000_000, 50_000_000)
	emits := n.Apply(a, startWall.Add(100*time.Millisecond))
	if len(emits) != 0 {
		t.Fatalf("post-OnSession audio must be held (no stale buffer); got %d emits", len(emits))
	}

	v := vPkt(2000, 2000)
	emits = n.Apply(v, startWall.Add(150*time.Millisecond))
	if len(emits) != 2 {
		t.Fatalf("post-OnSession joint-seed completion: want 2 emits, got %d", len(emits))
	}
}
