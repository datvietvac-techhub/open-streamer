package publisher

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// ─── rtspPathStreamCode / rtspLiveMountPath ──────────────────────────────────
//
// Single-segment codes are addressed via `live/<code>`; multi-segment codes
// are addressed via `<seg1>/<seg2>/...` directly with `live/` always stripped
// when present. SETUP paths carry a `/trackID=<N>` control suffix from
// gortsplib which we strip before returning the canonical code.

func TestRTSPPathStreamCode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		path string
		want domain.StreamCode
	}{
		{"single segment with live prefix", "/live/foo", "foo"},
		{"single segment without leading slash", "live/foo", "foo"},
		{"multi-segment direct", "/foo/bar", "foo/bar"},
		{"multi-segment with non-canonical live prefix", "/live/foo/bar", "foo/bar"},
		{"single segment setup trackID", "/live/foo/trackID=0", "foo"},
		{"multi-segment setup trackID", "/foo/bar/trackID=1", "foo/bar"},
		{"deep multi-segment setup trackID", "/a/b/c/trackID=2", "a/b/c"},
		{"empty path", "", ""},
		{"only slash", "/", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, rtspPathStreamCode(tc.path))
		})
	}
}

func TestRTSPLiveMountPath(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "live/foo", rtspLiveMountPath("foo"),
		"single-segment codes mount under the live/ prefix")
	assert.Equal(t, "foo/bar", rtspLiveMountPath("foo/bar"),
		"multi-segment codes mount at their raw path with no live/ prefix")
	assert.Equal(t, "a/b/c", rtspLiveMountPath("a/b/c"))
}

// matchRTSPMount must locate a mount for both the canonical path and (for
// multi-segment codes) the non-canonical `live/<code>` form. Bare
// single-segment paths must NOT match — they have nothing to strip and the
// 1-seg mount sits behind `live/`.
func TestLookupStreamMultiSegment(t *testing.T) {
	t.Parallel()

	mounts := map[string]*gortsplib.ServerStream{
		"live/foo": {}, // 1-segment stream "foo" mounted at live/foo
		"foo/bar":  {}, // 2-segment stream "foo/bar" mounted directly
	}

	// Single-segment: only `live/<code>` matches; bare path is rejected.
	require.NotNil(t, matchRTSPMount(mounts, "live/foo"), "live/foo matches mount live/foo")
	require.NotNil(t, matchRTSPMount(mounts, "live/foo/trackID=0"), "SETUP path matches mount via prefix")
	require.Nil(t, matchRTSPMount(mounts, "foo"), "bare 1-seg path must not match")

	// Multi-segment: canonical matches directly.
	require.NotNil(t, matchRTSPMount(mounts, "foo/bar"))
	require.NotNil(t, matchRTSPMount(mounts, "foo/bar/trackID=1"))
}

// computeVideoRTP must never emit an RTP timestamp ≤ the last one sent.
// Real-world trigger: HLS source jitter where DTS dips a few tens of ms
// inside the rebase window (rtspMaxBackJumpMs = 250). Without the clamp,
// the resulting rtpTS goes backwards and ffmpeg copy mode rejects every
// downstream packet with "non monotonically increasing dts to muxer".
func TestComputeVideoRTPMonotonicClampOnSmallJitter(t *testing.T) {
	sess := &rtspSession{}

	// Frame 1 anchors the timeline.
	got := sess.computeVideoRTP(1000, 1000)
	require.Equal(t, uint32(0), got, "first frame anchors at rtpTS=0")
	sess.lastVideoRTP = got
	sess.lastVideoSrc = 1000

	// Frame 2: forward by 40ms — normal 25fps tick.
	got = sess.computeVideoRTP(1040, 1040)
	require.Equal(t, uint32(40*90), got)
	sess.lastVideoRTP = got
	sess.lastVideoSrc = 1040

	// Frame 3: source jitter — pts dips 50ms back, well inside the 250ms
	// rebase threshold. Without the clamp this returns (-10ms)*90 wrapped
	// or 30ms*90 = 2700 which is < lastVideoRTP=3600. With the clamp it
	// must exceed lastVideoRTP by at least 1.
	got = sess.computeVideoRTP(1030, 1030)
	require.Greater(t, got, sess.lastVideoRTP, "jitter inside rebase window must still emit a strictly greater rtpTS")
	require.Equal(t, sess.lastVideoRTP+1, got, "clamp emits exactly +1 tick on backwards jitter")
}

// computeVideoRTP large-jump rebase still produces a monotonic value (legacy
// behaviour) — the clamp must not break the existing rebase path.
func TestComputeVideoRTPMonotonicAcrossLargeRebase(t *testing.T) {
	sess := &rtspSession{}

	_ = sess.computeVideoRTP(1000, 1000)
	sess.lastVideoRTP = 0
	sess.lastVideoSrc = 1000

	got := sess.computeVideoRTP(1040, 1040)
	require.Equal(t, uint32(40*90), got)
	sess.lastVideoRTP = got
	sess.lastVideoSrc = 1040

	// Big back-jump — looped source. dts dives below baseDTS, triggers
	// the rebase branch (not just the clamp).
	got = sess.computeVideoRTP(500, 500)
	require.Greater(t, got, sess.lastVideoRTP, "large back-jump must still produce a strictly greater rtpTS via rebase")
}

// Audio counterpart of TestComputeVideoRTPMonotonicClampOnSmallJitter.
func TestComputeAudioRTPMonotonicClampOnSmallJitter(t *testing.T) {
	sess := &rtspSession{
		aacCfg: &mpeg4audio.AudioSpecificConfig{SampleRate: 48000},
	}

	got := sess.computeAudioRTP(1000)
	require.Equal(t, uint32(0), got)
	sess.lastAudioRTP = got
	sess.lastAudioSrc = 1000

	// 21ms forward = 1 AAC AU at 48 kHz.
	got = sess.computeAudioRTP(1021)
	require.Equal(t, uint32(21*48000/1000), got)
	sess.lastAudioRTP = got
	sess.lastAudioSrc = 1021

	// Jitter back 10ms, inside the 250ms rebase threshold. Without the
	// clamp this returns 11*48 = 528 ticks which is < lastAudioRTP=1008.
	got = sess.computeAudioRTP(1011)
	require.Greater(t, got, sess.lastAudioRTP, "jitter inside rebase window must still emit a strictly greater rtpTS")
	require.Equal(t, sess.lastAudioRTP+1, got, "clamp emits exactly +1 tick on backwards jitter")
}

// pace anchors on the first call and returns immediately.
func TestPaceFirstCallAnchorsWithoutSleep(t *testing.T) {
	sess := &rtspSession{ctx: context.Background()}

	start := time.Now()
	sess.pace(5000)
	require.Less(t, time.Since(start), 5*time.Millisecond, "first pace call must not sleep")
	require.True(t, sess.paceSet)
	require.Equal(t, uint64(5000), sess.paceMedia)
}

// pace blocks for approximately (dts2-dts1) when the wallclock has not yet
// advanced that far past the anchor.
func TestPaceSleepsToWallclockTarget(t *testing.T) {
	sess := &rtspSession{ctx: context.Background()}
	sess.pace(0)

	target := 60 * time.Millisecond
	start := time.Now()
	sess.pace(uint64(target.Milliseconds()))
	elapsed := time.Since(start)

	require.GreaterOrEqual(t, elapsed, target-10*time.Millisecond,
		"pace must sleep close to the wallclock target")
	require.Less(t, elapsed, target+50*time.Millisecond,
		"pace must not over-sleep significantly past target")
}

// pace returns immediately when the wallclock is already past the target —
// catch-up scenario after a long upstream stall.
func TestPaceNoSleepWhenAlreadyLate(t *testing.T) {
	sess := &rtspSession{ctx: context.Background()}
	sess.paceBase = time.Now().Add(-200 * time.Millisecond)
	sess.paceMedia = 0
	sess.paceSet = true

	start := time.Now()
	sess.pace(50)
	require.Less(t, time.Since(start), 5*time.Millisecond,
		"already-late pace call must not sleep")
}

// PTS jumps far past wallclock (looped source, fresh keyframe burst) trip
// the max-sleep cap and re-anchor instead of sleeping for hours.
func TestPaceReanchorsOnHugeForwardJump(t *testing.T) {
	sess := &rtspSession{ctx: context.Background()}
	sess.pace(0)
	prevBase := sess.paceBase

	start := time.Now()
	sess.pace(uint64((10 * time.Minute).Milliseconds())) // way past max sleep
	require.Less(t, time.Since(start), 5*time.Millisecond,
		"huge forward jump must re-anchor instead of sleeping")
	require.NotEqual(t, prevBase, sess.paceBase, "anchor must move forward")
	require.Equal(t, uint64((10 * time.Minute).Milliseconds()), sess.paceMedia)
}

// Sustained underrun (we are >2s behind realtime) re-anchors instead of
// chasing the stale baseline forever.
func TestPaceReanchorsWhenFarBehind(t *testing.T) {
	sess := &rtspSession{ctx: context.Background()}
	sess.paceBase = time.Now().Add(-10 * time.Second)
	sess.paceMedia = 0
	sess.paceSet = true

	start := time.Now()
	sess.pace(100) // wallclock target = base + 100ms = -9.9s ago → way behind
	require.Less(t, time.Since(start), 5*time.Millisecond,
		"far-behind pace call must not sleep")
	require.WithinDuration(t, time.Now(), sess.paceBase, 10*time.Millisecond,
		"anchor must reset to ~now")
	require.Equal(t, uint64(100), sess.paceMedia)
}

// Backwards source DTS without the Discontinuity flag (jitter, reorder)
// must re-anchor instead of underflowing the unsigned subtraction.
func TestPaceReanchorsOnBackwardsDTS(t *testing.T) {
	sess := &rtspSession{ctx: context.Background()}
	sess.pace(5000)

	start := time.Now()
	sess.pace(4900)
	require.Less(t, time.Since(start), 5*time.Millisecond,
		"backwards dts must re-anchor without sleeping")
	require.Equal(t, uint64(4900), sess.paceMedia)
}

// Context cancellation aborts the in-flight sleep so stream teardown is
// not held up for up to rtspPaceMaxSleep.
func TestPaceAbortsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sess := &rtspSession{ctx: ctx}
	sess.pace(0)

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	sess.pace(5000) // would sleep ~5s without cancellation
	elapsed := time.Since(start)
	require.Less(t, elapsed, 200*time.Millisecond,
		"context cancellation must abort the pace sleep")
}

// ─── rtspClient.pollBytes ────────────────────────────────────────────────────
//
// pollBytes incrementalises gortsplib's cumulative `Stats().OutboundBytes`
// counter into per-poll deltas that flow through playSession.add(). Booting
// an RTSP server inside a unit test is heavyweight, so pollBytes accepts
// the rtspStatsSource interface — *gortsplib.ServerSession satisfies it
// in production, fakeRTSPStats satisfies it here.

// fakeRTSPStats is the bare-minimum rtspStatsSource. Tests mutate
// `outbound` between pollBytes calls to script the cumulative counter the
// way gortsplib would in production. statsNil lets one case verify the
// nil-stats branch (gortsplib has been seen to return nil during shutdown
// races).
type fakeRTSPStats struct {
	outbound atomic.Uint64
	statsNil atomic.Bool
}

func (f *fakeRTSPStats) SetOutbound(v uint64) { f.outbound.Store(v) }

func (f *fakeRTSPStats) Stats() *gortsplib.SessionStats {
	if f.statsNil.Load() {
		return nil
	}
	return &gortsplib.SessionStats{OutboundBytes: f.outbound.Load()}
}

// First poll establishes the baseline and credits the full counter as a
// delta — there is no prior reading to subtract from. This is the path
// hit on the very first touch tick after OnPlay.
func TestRTSPClientPollBytesFirstReadCreditsFullCounter(t *testing.T) {
	t.Parallel()
	fc := &fakeCloser{}
	c := &rtspClient{ps: &playSession{closer: fc}}
	src := &fakeRTSPStats{}
	src.SetOutbound(2048)

	c.pollBytes(src)

	require.Equal(t, int64(2048), fc.touchedSum,
		"first poll must credit the entire current OutboundBytes as a delta")
}

// Subsequent polls credit only the increment since the previous reading.
// Steady-state path: every 5 s the touch tick polls and only the bytes
// sent during that window flow into the tracker. The 5 s touch throttle
// in playSession means rapid back-to-back polls in this test all land in
// the local atomic — close() flushes whatever wasn't pushed via Touch,
// so totalCredited (Touch + Close) is the invariant we assert.
func TestRTSPClientPollBytesIncrementalDelta(t *testing.T) {
	t.Parallel()
	fc := &fakeCloser{}
	c := &rtspClient{ps: &playSession{closer: fc}}
	src := &fakeRTSPStats{}

	src.SetOutbound(1000)
	c.pollBytes(src)
	src.SetOutbound(1500)
	c.pollBytes(src)
	src.SetOutbound(1700)
	c.pollBytes(src)
	c.ps.close()

	require.Equal(t, int64(1700), fc.totalCredited(),
		"three polls of 1000/1500/1700 must credit deltas summing to 1700 across Touch + Close")
}

// A poll that finds no growth (idle frame, paused client) must not credit
// any bytes. add(0) is a no-op; this is a regression guard against future
// changes that could leak a zero-delta into the tracker.
func TestRTSPClientPollBytesNoGrowthSkipsAdd(t *testing.T) {
	t.Parallel()
	fc := &fakeCloser{}
	c := &rtspClient{ps: &playSession{closer: fc}}
	src := &fakeRTSPStats{}

	src.SetOutbound(500)
	c.pollBytes(src) // first poll publishes 500 via Touch
	require.Equal(t, int64(500), fc.touchedSum)

	c.pollBytes(src) // no growth — same 500
	c.ps.close()

	require.Equal(t, int64(500), fc.totalCredited(),
		"a no-growth second poll must not credit any extra bytes")
}

// Nil stats (gortsplib returning nil during shutdown) must not panic and
// must not advance the cursor — next poll should still see the genuine
// counter as a fresh delta.
func TestRTSPClientPollBytesNilStatsIsNoop(t *testing.T) {
	t.Parallel()
	fc := &fakeCloser{}
	c := &rtspClient{ps: &playSession{closer: fc}}
	src := &fakeRTSPStats{}
	src.SetOutbound(999)
	src.statsNil.Store(true)

	require.NotPanics(t, func() { c.pollBytes(src) })
	require.Equal(t, int64(0), fc.touchedSum,
		"nil-stats poll must not credit any bytes")

	src.statsNil.Store(false)
	c.pollBytes(src)
	c.ps.close()
	require.Equal(t, int64(999), fc.totalCredited(),
		"first non-nil read after nil polls must still credit the full counter")
}

// Nil ss falls back to c.gortspSess — used by the touch loop where the
// caller has the session pointer cached on the rtspClient struct rather
// than on the function argument.
func TestRTSPClientPollBytesFallsBackToCachedSession(t *testing.T) {
	t.Parallel()
	fc := &fakeCloser{}
	src := &fakeRTSPStats{}
	src.SetOutbound(777)
	c := &rtspClient{ps: &playSession{closer: fc}, gortspSess: src}

	c.pollBytes(nil)

	require.Equal(t, int64(777), fc.touchedSum,
		"nil ss argument must fall back to the cached gortspSess")
}

// Both ss and cached gortspSess nil — must not panic, no credit. Edge case
// for sessions that never reached OnPlay (rtspClient never registered).
func TestRTSPClientPollBytesBothNilIsNoop(t *testing.T) {
	t.Parallel()
	fc := &fakeCloser{}
	c := &rtspClient{ps: &playSession{closer: fc}}

	require.NotPanics(t, func() { c.pollBytes(nil) })
	require.Equal(t, int64(0), fc.touchedSum)
}
