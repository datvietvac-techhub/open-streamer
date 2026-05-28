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

// Regression: HLS / RTMP pull readers auto-reconnect on transient blips,
// causing packets to resume on the (sole) active input BEFORE the
// manager's probe cycle (~8s cooldown) has a chance to run. The active
// input flips back to StatusActive via RecordPacket, but earlier the
// `exhausted` flag set by tryFailover (no failover candidate found) was
// never cleared. UI then showed "All inputs exhausted" alert despite
// packets flowing. This test pins the fix.
func TestRecordPacket_ClearsExhaustedAfterBypassProbeRecovery(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s_byp", "rtmp://only"), ""))

	// Drive the sole input to degraded → exhausted (no failover candidate).
	svc.ReportInputError("s_byp", 0, errors.New("transient blip"))

	rt, _ := svc.RuntimeStatus("s_byp")
	require.True(t, rt.Exhausted, "single-input degradation must mark stream exhausted")
	require.Equal(t, 0, rt.ActiveInputPriority)

	// Simulate ingestor auto-reconnect: a packet arrives on the active
	// input. Without the fix, exhausted stays true forever.
	svc.RecordPacket("s_byp", 0)

	rt, _ = svc.RuntimeStatus("s_byp")
	assert.False(t, rt.Exhausted,
		"exhausted must be cleared when the active input recovers via packet flow")

	// A switch event with reason=recovery must appear so the UI history
	// shows the moment the stream came back, not just a stale "exhausted"
	// banner with no audit trail.
	require.GreaterOrEqual(t, len(rt.Switches), 2,
		"initial + bypass-recovery switch expected; got %d", len(rt.Switches))
	assert.Equal(t, SwitchReasonRecovery, rt.Switches[0].Reason)
	assert.Equal(t, 0, rt.Switches[0].From)
	assert.Equal(t, 0, rt.Switches[0].To)
	assert.NotEmpty(t, rt.Switches[0].Detail,
		"recovery entry should carry context (e.g. 'ingestor auto-reconnect')")

	// Input itself must reflect Active status with no stale errors.
	for _, in := range rt.Inputs {
		if in.InputPriority == 0 {
			assert.Equal(t, domain.StatusActive, in.Status)
			assert.Empty(t, in.Errors, "errors must be wiped on recovery")
		}
	}
}

// onRestored callback must fire so the coordinator can transition the
// stream out of StatusDegraded back to StatusActive. Without firing it,
// the coordinator's status remained "degraded" even after recovery.
func TestRecordPacket_FiresRestoredCallbackOnBypassRecovery(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s_cb", "rtmp://only"), ""))

	restoredCh := make(chan domain.StreamCode, 1)
	svc.SetRestoredCallback(func(c domain.StreamCode) {
		select {
		case restoredCh <- c:
		default:
		}
	})

	svc.ReportInputError("s_cb", 0, errors.New("blip"))
	svc.RecordPacket("s_cb", 0)

	select {
	case got := <-restoredCh:
		assert.Equal(t, domain.StreamCode("s_cb"), got)
	case <-time.After(2 * time.Second):
		t.Fatal("restored callback did not fire after bypass-probe recovery")
	}
}

// Recovery via RecordPacket must only fire when the recovering input is
// the ACTIVE one — packets on a non-active input shouldn't clear
// exhausted because the pipeline isn't actually streaming from it.
//
// Setup is fiddly: we want both inputs Degraded with state.active still
// pointing at the original priority 0. ReportInputError on the backup
// FIRST (no failover candidate change since active is still healthy),
// THEN on the active input (now both degraded → exhausted, but
// state.active was never reassigned).
func TestRecordPacket_DoesNotClearExhaustedForNonActiveInput(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s_na", "rtmp://primary", "rtmp://backup"), ""))

	// Degrade backup (priority 1) first — active stays at 0, no switch.
	svc.ReportInputError("s_na", 1, errors.New("b"))
	// Now degrade active (priority 0) → no candidate left → exhausted.
	svc.ReportInputError("s_na", 0, errors.New("a"))

	rt, _ := svc.RuntimeStatus("s_na")
	require.True(t, rt.Exhausted)
	require.Equal(t, 0, rt.ActiveInputPriority,
		"active must still be priority 0 — no failover happened in setup")

	// Packet on the non-active input (priority 1).
	svc.RecordPacket("s_na", 1)

	rt, _ = svc.RuntimeStatus("s_na")
	assert.True(t, rt.Exhausted,
		"packets on non-active input must NOT clear exhausted — pipeline still depends on a switch")
	for _, sw := range rt.Switches {
		assert.NotEqual(t, SwitchReasonRecovery, sw.Reason,
			"recovery switch should not be recorded when non-active input flips")
	}
}

// Idempotency: repeated packets on an already-recovered active input
// must NOT re-record recovery events. Otherwise the switch history would
// fill with duplicates on every packet.
func TestRecordPacket_RecoveryIsRecordedOnce(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s_once", "rtmp://only"), ""))

	svc.ReportInputError("s_once", 0, errors.New("blip"))
	svc.RecordPacket("s_once", 0)
	svc.RecordPacket("s_once", 0)
	svc.RecordPacket("s_once", 0)

	rt, _ := svc.RuntimeStatus("s_once")
	recoveryCount := 0
	for _, sw := range rt.Switches {
		if sw.Reason == SwitchReasonRecovery {
			recoveryCount++
		}
	}
	assert.Equal(t, 1, recoveryCount,
		"packets after recovery must NOT re-record recovery events")
}
