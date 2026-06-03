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

// recordSwitch keeps newest at index 0 and caps at maxSwitchHistory. This
// is the contract RuntimeStatus.Switches exposes — frontend reads
// Switches[0] for the most recent switch.
func TestRecordSwitch_OrderingAndCap(t *testing.T) {
	t.Parallel()
	state := &streamState{}
	base := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)

	// Push more than the cap so the oldest entries roll off.
	const overflow = 5
	total := maxSwitchHistory + overflow
	for i := 0; i < total; i++ {
		recordSwitch(state, SwitchEvent{
			At:     base.Add(time.Duration(i) * time.Second),
			From:   i,
			To:     i + 1,
			Reason: SwitchReasonError,
		})
	}

	require.Len(t, state.switchHistory, maxSwitchHistory,
		"ring buffer must cap at %d", maxSwitchHistory)
	assert.Equal(t, total-1, state.switchHistory[0].From, "newest entry at index 0")
	assert.Equal(t, base.Add(time.Duration(total-1)*time.Second), state.switchHistory[0].At)
	// Oldest survivor: index (total-1 - (cap-1)) = total - cap.
	assert.Equal(t, total-maxSwitchHistory, state.switchHistory[maxSwitchHistory-1].From,
		"oldest survivor at end")
}

// Initial activation in Register must produce a SwitchReasonInitial entry
// with From=-1, so the UI history shows a baseline event for fresh streams
// before any failover happens.
func TestSwitchHistory_RecordsInitialActivation(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s_init", "rtmp://primary"), ""))

	rt, _ := svc.RuntimeStatus("s_init")
	require.Len(t, rt.Switches, 1, "Register must record the initial activation")
	assert.Equal(t, SwitchReasonInitial, rt.Switches[0].Reason)
	assert.Equal(t, -1, rt.Switches[0].From, "no previous active for initial activation")
	assert.Equal(t, 0, rt.Switches[0].To, "best-priority input must be the active one")
	assert.Empty(t, rt.Switches[0].Detail, "initial activation has no extra context")
}

// ──────────────────────────────────────────────────────────────────────────
// End-to-end coverage — every callsite of tryFailover must produce a switch
// entry with the right reason. Switches[0] is the most recent (the failover
// under test); Switches[1] is the initial activation from Register.
// ──────────────────────────────────────────────────────────────────────────

// Error path: ingestor reports a failure on the active input → degrade →
// failover with reason=error and detail=err.Error().
func TestSwitchHistory_RecordsErrorReason(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s_err", "rtmp://primary", "rtmp://backup"), ""))

	svc.ReportInputError("s_err", 0, errors.New("rtmp connection reset"))

	require.Eventually(t, func() bool {
		return len(ing.startCallsCopy()) >= 2
	}, 2*time.Second, 20*time.Millisecond)

	rt, _ := svc.RuntimeStatus("s_err")
	require.Len(t, rt.Switches, 2, "initial + error failover")
	assert.Equal(t, 0, rt.Switches[0].From)
	assert.Equal(t, 1, rt.Switches[0].To)
	assert.Equal(t, SwitchReasonError, rt.Switches[0].Reason)
	assert.Equal(t, "rtmp connection reset", rt.Switches[0].Detail)
	assert.Equal(t, SwitchReasonInitial, rt.Switches[1].Reason)
}

// Manual path: SwitchInput API call → failover with reason=manual and no
// detail (operator action has no implicit context).
func TestSwitchHistory_RecordsManualReason(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s_man", "rtmp://primary", "rtmp://backup"), ""))

	require.NoError(t, svc.SwitchInput("s_man", 1))
	require.Eventually(t, func() bool {
		return len(ing.startCallsCopy()) >= 2
	}, 2*time.Second, 20*time.Millisecond)

	rt, _ := svc.RuntimeStatus("s_man")
	require.Len(t, rt.Switches, 2, "initial + manual switch")
	assert.Equal(t, SwitchReasonManual, rt.Switches[0].Reason)
	assert.Empty(t, rt.Switches[0].Detail)
	assert.Equal(t, 1, rt.Switches[0].To)
	assert.Equal(t, SwitchReasonInitial, rt.Switches[1].Reason)
}

// input_removed path: UpdateInputs deletes the active input → failover
// with reason=input_removed (takes precedence over input_added if both
// happen in the same call).
func TestSwitchHistory_RecordsInputRemovedReason(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s_rm", "rtmp://primary", "rtmp://backup"), ""))

	// Remove the active priority 0 → backup at priority 1 must take over.
	svc.UpdateInputs("s_rm", nil, []domain.Input{{Priority: 0, URL: "rtmp://primary"}}, nil)

	require.Eventually(t, func() bool {
		return len(ing.startCallsCopy()) >= 2
	}, 2*time.Second, 20*time.Millisecond)

	rt, _ := svc.RuntimeStatus("s_rm")
	require.Len(t, rt.Switches, 2)
	assert.Equal(t, SwitchReasonInputRemoved, rt.Switches[0].Reason)
	assert.Equal(t, 1, rt.Switches[0].To)
	assert.Equal(t, SwitchReasonInitial, rt.Switches[1].Reason)
}

// input_added path: UpdateInputs adds a higher-priority input while a
// lower-priority is active → failover with reason=input_added.
func TestSwitchHistory_RecordsInputAddedReason(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	// Start with a single backup input at priority 5.
	stream := &domain.Stream{
		Code:   "s_add",
		Inputs: []domain.Input{{Priority: 5, URL: "rtmp://backup"}},
	}
	require.NoError(t, svc.Register(context.Background(), stream, ""))

	// Now add a higher-priority (lower number) input.
	svc.UpdateInputs("s_add", []domain.Input{{Priority: 1, URL: "rtmp://primary"}}, nil, nil)

	require.Eventually(t, func() bool {
		return len(ing.startCallsCopy()) >= 2
	}, 2*time.Second, 20*time.Millisecond)

	rt, _ := svc.RuntimeStatus("s_add")
	require.Len(t, rt.Switches, 2)
	assert.Equal(t, SwitchReasonInputAdded, rt.Switches[0].Reason)
	assert.Equal(t, 5, rt.Switches[0].From)
	assert.Equal(t, 1, rt.Switches[0].To)
	// Initial: From=-1, To=5 (only one input was registered initially).
	assert.Equal(t, SwitchReasonInitial, rt.Switches[1].Reason)
	assert.Equal(t, -1, rt.Switches[1].From)
	assert.Equal(t, 5, rt.Switches[1].To)
}

// Switches snapshot must be a defensive copy — caller mutating the slice
// must not affect future RuntimeStatus reads.
func TestSwitchHistory_DefensiveCopy(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s_def", "rtmp://primary", "rtmp://backup"), ""))
	svc.ReportInputError("s_def", 0, errors.New("first"))

	require.Eventually(t, func() bool {
		rt, _ := svc.RuntimeStatus("s_def")
		return len(rt.Switches) >= 2
	}, 2*time.Second, 20*time.Millisecond)

	first, _ := svc.RuntimeStatus("s_def")
	require.Len(t, first.Switches, 2)
	first.Switches[0].Reason = "tampered"

	second, _ := svc.RuntimeStatus("s_def")
	require.Len(t, second.Switches, 2)
	assert.Equal(t, SwitchReasonError, second.Switches[0].Reason,
		"caller mutation of the returned slice must not leak back into state")
}
