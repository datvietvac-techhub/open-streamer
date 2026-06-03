package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// The sweeper added in checkHealth re-evaluates every monitor tick (2s),
// so the missed-failback window is bounded by one tick instead of being
// permanent.
func TestCheckHealth_FailbackSweeper_RecoversFromProbeRace(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("racey", "rtmp://primary", "rtmp://backup"), ""))

	// Simulate the prod sequence:
	//   1. Initial Start picked P0.
	//   2. P0 timed out → tryFailover → switched to P1 (backup VOD).
	//   3. Probe of P0 succeeded WITHIN failbackSwitchCooldown → P0 went
	//      to StatusIdle but failback never fired.
	svc.ReportInputError("racey", 0, errors.New("timeout"))

	// Confirm the precondition: switch to P1 happened.
	rt, _ := svc.RuntimeStatus("racey")
	require.Equal(t, 1, rt.ActiveInputPriority,
		"precondition: stream must be on backup after timeout")

	// Manually inject the post-probe state: P0 promoted to Idle, but the
	// switch happened so recently that the runProbe failback gate
	// rejected it. Mirrors what the codebase does at runProbe line ~948
	// (task.h.Status = StatusIdle) without firing a failback.
	state := svc.streams["racey"]
	state.mu.Lock()
	state.inputs[0].Status = domain.StatusIdle
	delete(state.degradedAt, 0)
	// Pretend the switch happened well past failbackSwitchCooldown so the
	// sweeper's cooldown check passes — same outcome as waiting in real
	// time, just deterministic.
	state.lastSwitchAt = time.Now().Add(-failbackSwitchCooldown - time.Second)
	state.mu.Unlock()

	// Take a "before" snapshot of Start calls so the assertion isolates
	// the sweeper's failback from the initial Register start + the
	// timeout failover start.
	startsBefore := len(ing.startCallsCopy())

	// Drive one monitor tick. checkHealth's new sweeper step should
	// notice the lower-priority Idle input + cooldown elapsed → fire
	// SwitchReasonFailback → ingestor.Start(P0).
	svc.checkHealth("racey")

	// Allow the failover goroutine spawned by tryFailover to land its
	// Start call. tryFailover is synchronous, so this completes before
	// checkHealth returns; the wait is for ingestor.Start's onStart
	// callback chain (none here, but defensive against future changes).
	require.Eventually(t, func() bool {
		return len(ing.startCallsCopy()) > startsBefore
	}, time.Second, 5*time.Millisecond, "failback Start must fire")

	rt, _ = svc.RuntimeStatus("racey")
	assert.Equal(t, 0, rt.ActiveInputPriority,
		"sweeper must promote stream back to primary input after probe race")

	// One Start call beyond the snapshot — the failback. No double-fire.
	assert.Equal(t, startsBefore+1, len(ing.startCallsCopy()),
		"sweeper must trigger exactly one failback Start, not loop")
}

// TestCheckHealth_FailbackSweeper_RespectsCooldown verifies the sweeper
// holds off when failbackSwitchCooldown hasn't elapsed since the last
// switch. Prevents tight-loop flapping if the primary keeps recovering
// then degrading within the cooldown window.
func TestCheckHealth_FailbackSweeper_RespectsCooldown(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("cool", "rtmp://primary", "rtmp://backup"), ""))

	svc.ReportInputError("cool", 0, errors.New("timeout"))
	startsBefore := len(ing.startCallsCopy())

	state := svc.streams["cool"]
	state.mu.Lock()
	state.inputs[0].Status = domain.StatusIdle
	delete(state.degradedAt, 0)
	// Switch happened 1 second ago — well within cooldown (12s).
	state.lastSwitchAt = time.Now().Add(-1 * time.Second)
	state.mu.Unlock()

	svc.checkHealth("cool")

	// Give any spurious failover goroutine a chance to land before
	// asserting "no extra Start". Without this, a buggy implementation
	// could pass the assertion only because of timing.
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, startsBefore, len(ing.startCallsCopy()),
		"sweeper must NOT failback before failbackSwitchCooldown elapses")

	rt, _ := svc.RuntimeStatus("cool")
	assert.Equal(t, 1, rt.ActiveInputPriority,
		"stream must stay on backup until cooldown allows failback")
}

// TestCheckHealth_FailbackSweeper_HonorsManualOverride verifies the
// sweeper does NOT undo an operator's manual SwitchInput pin to a
// lower-priority input. selectBest already short-circuits on
// overridePriority — this test guards against future refactors that
// might bypass it.
func TestCheckHealth_FailbackSweeper_HonorsManualOverride(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("pin", "rtmp://primary", "rtmp://backup"), ""))

	// Operator pins to backup even though primary is healthy.
	require.NoError(t, svc.SwitchInput("pin", 1))
	startsBefore := len(ing.startCallsCopy())

	state := svc.streams["pin"]
	state.mu.Lock()
	state.lastSwitchAt = time.Now().Add(-failbackSwitchCooldown - time.Second)
	state.mu.Unlock()

	svc.checkHealth("pin")
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, startsBefore, len(ing.startCallsCopy()),
		"sweeper must respect manual override and NOT failback to primary")

	rt, _ := svc.RuntimeStatus("pin")
	assert.Equal(t, 1, rt.ActiveInputPriority,
		"stream must stay on operator-pinned backup")
}

// TestCheckHealth_FailbackSweeper_SkipsExhausted verifies that exhausted
// streams (no healthy candidates) are left alone. Exhaustion recovery
// goes through RecordPacket / runProbe with SwitchReasonRecovery; the
// sweeper handles only true failbacks (a healthy higher-priority input
// preempting the running fallback).
func TestCheckHealth_FailbackSweeper_SkipsExhausted(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("exh", "rtmp://only"), ""))

	// Single-input stream → degrade the only input → exhausted.
	svc.ReportInputError("exh", 0, errors.New("blip"))
	startsBefore := len(ing.startCallsCopy())

	rt, _ := svc.RuntimeStatus("exh")
	require.True(t, rt.Exhausted, "precondition: must be exhausted")

	state := svc.streams["exh"]
	state.mu.Lock()
	state.lastSwitchAt = time.Now().Add(-failbackSwitchCooldown - time.Second)
	state.mu.Unlock()

	svc.checkHealth("exh")
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, startsBefore, len(ing.startCallsCopy()),
		"sweeper must NOT trigger any Start while exhausted (RecordPacket path owns recovery)")
}

// TestShouldFailbackNow_TableCases exercises the predicate directly with
// pre-built streamState fixtures, isolating it from goroutines, locks,
// and the broader checkHealth orchestration.
func TestShouldFailbackNow_TableCases(t *testing.T) {
	t.Parallel()

	now := time.Now()
	pastCooldown := now.Add(-failbackSwitchCooldown - time.Second)
	withinCooldown := now.Add(-1 * time.Second)

	tests := []struct {
		name  string
		state *streamState
		want  bool
	}{
		{
			name: "lower-priority idle and cooldown elapsed → failback",
			state: &streamState{
				active:       1,
				lastSwitchAt: pastCooldown,
				inputs: map[int]*InputHealth{
					0: {Input: domain.Input{Priority: 0}, Status: domain.StatusIdle},
					1: {Input: domain.Input{Priority: 1}, Status: domain.StatusActive},
				},
			},
			want: true,
		},
		{
			name: "lower-priority idle but within cooldown → hold",
			state: &streamState{
				active:       1,
				lastSwitchAt: withinCooldown,
				inputs: map[int]*InputHealth{
					0: {Input: domain.Input{Priority: 0}, Status: domain.StatusIdle},
					1: {Input: domain.Input{Priority: 1}, Status: domain.StatusActive},
				},
			},
			want: false,
		},
		{
			name: "best is the active input (no preempt candidate) → no failback",
			state: &streamState{
				active:       0,
				lastSwitchAt: pastCooldown,
				inputs: map[int]*InputHealth{
					0: {Input: domain.Input{Priority: 0}, Status: domain.StatusActive},
					1: {Input: domain.Input{Priority: 1}, Status: domain.StatusIdle},
				},
			},
			want: false,
		},
		{
			name: "lower-priority still degraded → no failback",
			state: &streamState{
				active:       1,
				lastSwitchAt: pastCooldown,
				inputs: map[int]*InputHealth{
					0: {Input: domain.Input{Priority: 0}, Status: domain.StatusDegraded},
					1: {Input: domain.Input{Priority: 1}, Status: domain.StatusActive},
				},
			},
			want: false,
		},
		{
			name: "exhausted → sweeper defers to RecordPacket recovery path",
			state: &streamState{
				active:       0,
				exhausted:    true,
				lastSwitchAt: pastCooldown,
				inputs: map[int]*InputHealth{
					0: {Input: domain.Input{Priority: 0}, Status: domain.StatusDegraded},
				},
			},
			want: false,
		},
		{
			name: "operator override pinned to higher priority → no failback to lower",
			state: &streamState{
				active:           1,
				lastSwitchAt:     pastCooldown,
				overridePriority: intPtr(1),
				inputs: map[int]*InputHealth{
					0: {Input: domain.Input{Priority: 0}, Status: domain.StatusIdle},
					1: {Input: domain.Input{Priority: 1}, Status: domain.StatusActive},
				},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldFailbackNow(tc.state, now)
			assert.Equal(t, tc.want, got)
		})
	}
}

func intPtr(i int) *int { return &i }
