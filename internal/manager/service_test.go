package manager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// ---- selectBest ----------------------------------------------------------------

func TestSelectBest_SingleIdle(t *testing.T) {
	t.Parallel()
	state := &streamState{
		inputs: map[int]*InputHealth{
			1: {Input: domain.Input{Priority: 1}, Status: domain.StatusIdle},
		},
	}
	best := selectBest(state)
	require.NotNil(t, best)
	assert.Equal(t, 1, best.Input.Priority)
}

func TestSelectBest_SkipsDegraded(t *testing.T) {
	t.Parallel()
	state := &streamState{
		inputs: map[int]*InputHealth{
			1: {Input: domain.Input{Priority: 1}, Status: domain.StatusDegraded},
			2: {Input: domain.Input{Priority: 2}, Status: domain.StatusIdle},
		},
	}
	best := selectBest(state)
	require.NotNil(t, best)
	assert.Equal(t, 2, best.Input.Priority)
}

func TestSelectBest_SkipsStopped(t *testing.T) {
	t.Parallel()
	state := &streamState{
		inputs: map[int]*InputHealth{
			1: {Input: domain.Input{Priority: 1}, Status: domain.StatusStopped},
		},
	}
	assert.Nil(t, selectBest(state))
}

func TestSelectBest_PrefersLowerPriority(t *testing.T) {
	t.Parallel()
	state := &streamState{
		inputs: map[int]*InputHealth{
			3: {Input: domain.Input{Priority: 3}, Status: domain.StatusIdle},
			1: {Input: domain.Input{Priority: 1}, Status: domain.StatusIdle},
			2: {Input: domain.Input{Priority: 2}, Status: domain.StatusIdle},
		},
	}
	best := selectBest(state)
	require.NotNil(t, best)
	assert.Equal(t, 1, best.Input.Priority)
}

func TestSelectBest_AllDegradedReturnsNil(t *testing.T) {
	t.Parallel()
	state := &streamState{
		inputs: map[int]*InputHealth{
			1: {Input: domain.Input{Priority: 1}, Status: domain.StatusDegraded},
			2: {Input: domain.Input{Priority: 2}, Status: domain.StatusDegraded},
		},
	}
	assert.Nil(t, selectBest(state))
}

func TestSelectBest_ActiveInputIsEligible(t *testing.T) {
	t.Parallel()
	state := &streamState{
		active: 1,
		inputs: map[int]*InputHealth{
			1: {Input: domain.Input{Priority: 1}, Status: domain.StatusActive},
			2: {Input: domain.Input{Priority: 2}, Status: domain.StatusIdle},
		},
	}
	best := selectBest(state)
	require.NotNil(t, best)
	assert.Equal(t, 1, best.Input.Priority)
}

func TestSelectBestOverrideWinsOverHigherPriority(t *testing.T) {
	t.Parallel()
	// Override = 3 (lowest config priority), but it should win over 1 and 2.
	p := 3
	state := &streamState{
		overridePriority: &p,
		inputs: map[int]*InputHealth{
			1: {Input: domain.Input{Priority: 1}, Status: domain.StatusIdle},
			2: {Input: domain.Input{Priority: 2}, Status: domain.StatusIdle},
			3: {Input: domain.Input{Priority: 3}, Status: domain.StatusIdle},
		},
	}
	best := selectBest(state)
	require.NotNil(t, best)
	assert.Equal(t, 3, best.Input.Priority)
}

func TestSelectBestOverrideFallsBackWhenDegraded(t *testing.T) {
	t.Parallel()
	// Override input is degraded → falls back to normal priority ordering.
	p := 3
	state := &streamState{
		overridePriority: &p,
		inputs: map[int]*InputHealth{
			1: {Input: domain.Input{Priority: 1}, Status: domain.StatusIdle},
			3: {Input: domain.Input{Priority: 3}, Status: domain.StatusDegraded},
		},
	}
	best := selectBest(state)
	require.NotNil(t, best)
	assert.Equal(t, 1, best.Input.Priority)
}

// ---- clearOverrideIfNeeded ---------------------------------------------------

func TestClearOverrideIfNeededKeepsOverrideWhenBestMatches(t *testing.T) {
	t.Parallel()
	p := 3
	state := &streamState{overridePriority: &p}
	best := &InputHealth{Input: domain.Input{Priority: 3}}
	clearOverrideIfNeeded("stream1", state, best)
	require.NotNil(t, state.overridePriority)
	assert.Equal(t, 3, *state.overridePriority)
}

func TestClearOverrideIfNeededClearsWhenBestDiffers(t *testing.T) {
	t.Parallel()
	p := 3
	state := &streamState{overridePriority: &p}
	best := &InputHealth{Input: domain.Input{Priority: 1}} // override lost, fell back to 1
	clearOverrideIfNeeded("stream1", state, best)
	assert.Nil(t, state.overridePriority)
}

func TestClearOverrideIfNeededClearsWhenBestNil(t *testing.T) {
	t.Parallel()
	p := 3
	state := &streamState{overridePriority: &p}
	clearOverrideIfNeeded("stream1", state, nil) // all inputs degraded
	assert.Nil(t, state.overridePriority)
}

func TestClearOverrideIfNeededNoopWhenNoOverride(t *testing.T) {
	t.Parallel()
	state := &streamState{overridePriority: nil}
	best := &InputHealth{Input: domain.Input{Priority: 1}}
	clearOverrideIfNeeded("stream1", state, best)
	assert.Nil(t, state.overridePriority)
}

// ---- collectTimeoutIfNeeded ---------------------------------------------------

func TestCollectTimeoutIfNeeded_ActiveTimedOut(t *testing.T) {
	t.Parallel()
	state := &streamState{
		active:     1,
		degradedAt: make(map[int]time.Time),
	}
	h := &InputHealth{Status: domain.StatusActive, LastPacketAt: time.Now().Add(-60 * time.Second)}
	timedOut := -1
	collectTimeoutIfNeeded(state, h, 1, time.Now(), 30*time.Second, &timedOut)
	assert.Equal(t, 1, timedOut)
	assert.Equal(t, domain.StatusDegraded, h.Status)
}

func TestCollectTimeoutIfNeeded_NotActive(t *testing.T) {
	t.Parallel()
	// priority=2 is NOT the active input (active=1) — should be ignored.
	state := &streamState{active: 1, degradedAt: make(map[int]time.Time)}
	h := &InputHealth{Status: domain.StatusActive, LastPacketAt: time.Now().Add(-60 * time.Second)}
	timedOut := -1
	collectTimeoutIfNeeded(state, h, 2, time.Now(), 30*time.Second, &timedOut)
	assert.Equal(t, -1, timedOut)
}

func TestCollectTimeoutIfNeeded_WithinTimeout(t *testing.T) {
	t.Parallel()
	// Last packet 5s ago, timeout = 10s → still within window.
	state := &streamState{active: 1, degradedAt: make(map[int]time.Time)}
	h := &InputHealth{Status: domain.StatusActive, LastPacketAt: time.Now().Add(-5 * time.Second)}
	timedOut := -1
	collectTimeoutIfNeeded(state, h, 1, time.Now(), 10*time.Second, &timedOut)
	assert.Equal(t, -1, timedOut)
	assert.Equal(t, domain.StatusActive, h.Status)
}

func TestCollectTimeoutIfNeeded_AlreadyDegraded(t *testing.T) {
	t.Parallel()
	state := &streamState{active: 1, degradedAt: make(map[int]time.Time)}
	h := &InputHealth{Status: domain.StatusDegraded, LastPacketAt: time.Now().Add(-60 * time.Second)}
	timedOut := -1
	collectTimeoutIfNeeded(state, h, 1, time.Now(), 30*time.Second, &timedOut)
	assert.Equal(t, -1, timedOut) // already degraded → nothing to do
}

// ---- collectProbeIfNeeded ----------------------------------------------------

func TestCollectProbeIfNeeded_QueuesDegradedNonActive(t *testing.T) {
	t.Parallel()
	state := &streamState{
		active:     1,
		degradedAt: map[int]time.Time{2: time.Now().Add(-failbackProbeCooldown - time.Second)},
		probing:    make(map[int]bool),
	}
	h := &InputHealth{Input: domain.Input{Priority: 2}, Status: domain.StatusDegraded}
	var tasks []probeTask
	collectProbeIfNeeded(state, h, 2, time.Now(), &tasks)
	require.Len(t, tasks, 1)
	assert.Equal(t, 2, tasks[0].priority)
	assert.True(t, state.probing[2])
}

func TestCollectProbeIfNeeded_SkipsActiveInputWhenNotExhausted(t *testing.T) {
	t.Parallel()
	// Live ingestor worker is reconnecting on its own — explicit probe would race.
	state := &streamState{
		active:     1,
		exhausted:  false,
		degradedAt: map[int]time.Time{1: time.Now().Add(-failbackProbeCooldown - time.Second)},
		probing:    make(map[int]bool),
	}
	h := &InputHealth{Input: domain.Input{Priority: 1}, Status: domain.StatusDegraded}
	var tasks []probeTask
	collectProbeIfNeeded(state, h, 1, time.Now(), &tasks)
	assert.Empty(t, tasks)
}

// Single-input streams: after the active worker dies (EOF / non-retriable),
// nothing reconnects and state stays exhausted forever unless the probe loop
// re-attempts the active priority. Regression for the test1/test2 case where
// pulling RTMP from a restarting upstream wedged test2 in degraded forever.
func TestCollectProbeIfNeeded_ProbesActiveWhenExhausted(t *testing.T) {
	t.Parallel()
	state := &streamState{
		active:     0,
		exhausted:  true,
		degradedAt: map[int]time.Time{0: time.Now().Add(-failbackProbeCooldown - time.Second)},
		probing:    make(map[int]bool),
	}
	h := &InputHealth{Input: domain.Input{Priority: 0}, Status: domain.StatusDegraded}
	var tasks []probeTask
	collectProbeIfNeeded(state, h, 0, time.Now(), &tasks)
	assert.Len(t, tasks, 1, "exhausted single-input must still probe to recover")
	assert.Equal(t, 0, tasks[0].priority)
}

func TestCollectProbeIfNeeded_SkipsIfCooldownNotElapsed(t *testing.T) {
	t.Parallel()
	state := &streamState{
		active:     1,
		degradedAt: map[int]time.Time{2: time.Now()}, // just degraded
		probing:    make(map[int]bool),
	}
	h := &InputHealth{Input: domain.Input{Priority: 2}, Status: domain.StatusDegraded}
	var tasks []probeTask
	collectProbeIfNeeded(state, h, 2, time.Now(), &tasks)
	assert.Empty(t, tasks)
}

func TestCollectProbeIfNeeded_SkipsIfAlreadyProbing(t *testing.T) {
	t.Parallel()
	state := &streamState{
		active:     1,
		degradedAt: map[int]time.Time{2: time.Now().Add(-failbackProbeCooldown - time.Second)},
		probing:    map[int]bool{2: true},
	}
	h := &InputHealth{Input: domain.Input{Priority: 2}, Status: domain.StatusDegraded}
	var tasks []probeTask
	collectProbeIfNeeded(state, h, 2, time.Now(), &tasks)
	assert.Empty(t, tasks)
}

// helper: exported wrapper so tests in the same package can call unexported functions.
func collectTimeoutIfNeeded(state *streamState, h *InputHealth, priority int, now time.Time, timeout time.Duration, timedOut *int) {
	var detail string
	(&Service{}).collectTimeoutIfNeeded(state, h, priority, now, timeout, timedOut, &detail)
}

func collectProbeIfNeeded(state *streamState, h *InputHealth, priority int, now time.Time, tasks *[]probeTask) {
	(&Service{}).collectProbeIfNeeded(state, h, priority, now, tasks)
}
