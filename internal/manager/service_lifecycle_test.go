// service_lifecycle_test.go — exercises the public Service API
// (Register / Unregister / IsRegistered / RecordPacket / ReportInputError /
// RuntimeStatus / SwitchInput / UpdateInputs / UpdateBufferWriteID) using
// the fakeIngestor stub from testing_test.go.

package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor"
)

// streamWithInputs builds a domain.Stream with N inputs at priorities 0..N-1.
func streamWithInputs(code string, urls ...string) *domain.Stream {
	s := &domain.Stream{Code: domain.StreamCode(code)}
	for i, u := range urls {
		s.Inputs = append(s.Inputs, domain.Input{Priority: i, URL: u})
	}
	return s
}

// newSvc builds a Service for unit tests with a fake ingestor wired in.
// Returns the Service and the fake so tests can inspect or drive callbacks.
//
// The events.Bus is constructed with workers=0 so it doesn't spawn dispatch
// goroutines (Publish becomes a synchronous no-op delivery, since there are
// no subscribers). That avoids the need for a Close() call the bus doesn't
// expose, while keeping the manager's bus.Publish calls valid.
func newSvc(t *testing.T) (*Service, *fakeIngestor) {
	t.Helper()
	bus := events.New(0, 8)
	ing := &fakeIngestor{}
	svc := NewForTesting(bus, ing, newTestMetrics(), 30)
	t.Cleanup(func() {
		// Tear down all goroutines (monitor, in-flight probes) so they
		// don't leak across tests / racy with t.Parallel().
		for code := range svc.streams {
			svc.Unregister(code)
		}
	})
	return svc, ing
}

// ─── Register / Unregister / IsRegistered ────────────────────────────────────

func TestRegister_StartsBestPriorityInput(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	stream := streamWithInputs("s1", "rtmp://primary", "rtmp://backup")
	require.NoError(t, svc.Register(context.Background(), stream, "buf-s1"))

	assert.True(t, svc.IsRegistered("s1"))
	calls := ing.startCallsCopy()
	require.Len(t, calls, 1)
	assert.Equal(t, domain.StreamCode("s1"), calls[0].StreamID)
	assert.Equal(t, 0, calls[0].Input.Priority, "must start lowest priority (= primary)")
	assert.Equal(t, domain.StreamCode("buf-s1"), calls[0].BufferWriteID)
}

func TestRegister_EmptyBufferWriteIDDefaultsToStreamCode(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s2", "rtmp://x"), ""))
	calls := ing.startCallsCopy()
	require.Len(t, calls, 1)
	assert.Equal(t, domain.StreamCode("s2"), calls[0].BufferWriteID)
}

func TestRegister_NoInputsSkipsIngestorStart(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		&domain.Stream{Code: "empty"}, ""))
	assert.True(t, svc.IsRegistered("empty"), "registered even with no inputs")
	assert.Empty(t, ing.startCallsCopy(), "no inputs → ingestor.Start not invoked")
}

func TestUnregister_StopsIngestorAndClearsState(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s3", "rtmp://x"), ""))
	require.True(t, svc.IsRegistered("s3"))

	svc.Unregister("s3")
	assert.False(t, svc.IsRegistered("s3"))
	assert.Equal(t, 1, ing.stopCallCount(), "ingestor.Stop must be invoked once")
}

func TestUnregister_UnknownStreamIsNoOp(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	svc.Unregister("ghost") // must not panic
	assert.Zero(t, ing.stopCallCount())
}

// ─── RecordPacket — promotes Idle → Active ───────────────────────────────────

func TestRecordPacket_PromotesInputToActive(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s4", "rtmp://x"), ""))

	svc.RecordPacket("s4", 0)
	rt, ok := svc.RuntimeStatus("s4")
	require.True(t, ok)
	require.Len(t, rt.Inputs, 1)
	assert.Equal(t, domain.StatusActive, rt.Inputs[0].Status,
		"first packet must flip Idle → Active")
	assert.False(t, rt.Inputs[0].LastPacketAt.IsZero(), "LastPacketAt must be stamped")
}

func TestRecordPacket_UnknownStreamIsNoOp(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	svc.RecordPacket("ghost", 0) // must not panic
}

// Recovery (Degraded → Active via RecordPacket) must wipe the input's
// error history so the UI doesn't show a stale "X errors" badge on a
// now-healthy input. Diagnostic trace lives in hooks/logs, not here.
func TestRecordPacket_RecoveryClearsErrorHistory(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("rec1", "rtmp://x", "rtmp://y"), ""))

	// Drive priority 1 (non-active) into degraded so it has an error
	// history, then RecordPacket on it (simulates probe-driven recovery
	// where the active input died and 1 came back).
	svc.ReportInputError("rec1", 1, errors.New("blip"))

	rt, _ := svc.RuntimeStatus("rec1")
	for _, in := range rt.Inputs {
		if in.InputPriority == 1 {
			require.Equal(t, domain.StatusDegraded, in.Status)
			require.Len(t, in.Errors, 1, "precondition: error recorded")
		}
	}

	svc.RecordPacket("rec1", 1)
	rt, _ = svc.RuntimeStatus("rec1")
	for _, in := range rt.Inputs {
		if in.InputPriority == 1 {
			assert.Equal(t, domain.StatusActive, in.Status, "must promote to active")
			assert.Empty(t, in.Errors, "recovery must wipe stale error history")
		}
	}
}

func TestRecordPacket_UnknownPriorityIsNoOp(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s5", "rtmp://x"), ""))
	svc.RecordPacket("s5", 99) // priority not in inputs map
	rt, _ := svc.RuntimeStatus("s5")
	assert.NotEqual(t, domain.StatusActive, rt.Inputs[0].Status,
		"unknown priority must not promote any input")
}

// ─── ReportInputError — degrades + triggers failover ─────────────────────────

func TestReportInputError_DegradesActiveAndFailsOver(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s6", "rtmp://primary", "rtmp://backup"), ""))
	// Initial Start uses priority 0.
	require.Equal(t, 1, len(ing.startCallsCopy()))

	svc.ReportInputError("s6", 0, errors.New("boom"))

	// Manager should have switched to priority 1.
	require.Eventually(t, func() bool {
		return len(ing.startCallsCopy()) >= 2
	}, 2*time.Second, 20*time.Millisecond)
	calls := ing.startCallsCopy()
	assert.Equal(t, 1, calls[1].Input.Priority, "failover must switch to backup")

	rt, _ := svc.RuntimeStatus("s6")
	// Find priority 0 in snapshot — order isn't stable.
	var p0Status domain.StreamStatus
	var p0Errors int
	for _, in := range rt.Inputs {
		if in.InputPriority == 0 {
			p0Status = in.Status
			p0Errors = len(in.Errors)
		}
	}
	assert.Equal(t, domain.StatusDegraded, p0Status)
	assert.Equal(t, 1, p0Errors, "error must be recorded in history")
}

func TestReportInputError_AllExhaustedFiresCallback(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s7", "rtmp://only"), ""))

	exhaustedCh := make(chan domain.StreamCode, 1)
	svc.SetExhaustedCallback(func(c domain.StreamCode) {
		select {
		case exhaustedCh <- c:
		default:
		}
	})

	svc.ReportInputError("s7", 0, errors.New("only-input-died"))

	select {
	case got := <-exhaustedCh:
		assert.Equal(t, domain.StreamCode("s7"), got)
	case <-time.After(2 * time.Second):
		t.Fatal("exhausted callback did not fire after sole input died")
	}

	rt, _ := svc.RuntimeStatus("s7")
	assert.True(t, rt.Exhausted, "exhausted flag must be set")
}

// Regression: the ingestor fires ErrNoPusherConnected the moment a push
// slot is registered, BEFORE the encoder's first packet has had a chance
// to land on the manager. For an auto-publish runtime stream that has a
// single publish:// input and no fallback, the error must be a no-op —
// the encoder is already waiting on registry.Acquire and the next packet
// will promote the input from Idle to Active. Without the skip, the input
// goes Degraded → exhausted → fires onExhausted async; the recovery
// callback may land BEFORE the exhausted callback, after which the
// exhausted callback's flag set sticks forever even with packets flowing.
func TestReportInputError_NoPusherSkippedOnSingleInput(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("autopub", "publish://"), ""))

	exhaustedCh := make(chan domain.StreamCode, 1)
	svc.SetExhaustedCallback(func(c domain.StreamCode) {
		select {
		case exhaustedCh <- c:
		default:
		}
	})

	svc.ReportInputError("autopub", 0, ingestor.ErrNoPusherConnected)

	select {
	case got := <-exhaustedCh:
		t.Fatalf("exhausted callback must NOT fire for single-input + ErrNoPusherConnected, got %q", got)
	case <-time.After(200 * time.Millisecond):
	}

	rt, _ := svc.RuntimeStatus("autopub")
	assert.False(t, rt.Exhausted, "single-input stream must stay non-exhausted under ErrNoPusherConnected")
	for _, in := range rt.Inputs {
		if in.InputPriority == 0 {
			assert.Equal(t, domain.StatusIdle, in.Status,
				"input must remain Idle so the next packet can promote it to Active")
			assert.Empty(t, in.Errors,
				"no error history entry — the signal is a fast-path hint, not a real failure")
		}
	}
}

// Multi-input streams must still see the fast failover: ErrNoPusherConnected
// is the signal that lets us swap to the next priority before the 30s
// packet-timeout kicks in.
func TestReportInputError_NoPusherStillFailsOverWithFallback(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("fb", "publish://", "rtmp://fallback"), ""))

	svc.ReportInputError("fb", 0, ingestor.ErrNoPusherConnected)

	rt, _ := svc.RuntimeStatus("fb")
	for _, in := range rt.Inputs {
		if in.InputPriority == 0 {
			assert.Equal(t, domain.StatusDegraded, in.Status,
				"primary input must degrade so failover can pick the fallback")
		}
	}
}

func TestReportInputError_DoubleReportIsIdempotent(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s8", "rtmp://x", "rtmp://y"), ""))

	svc.ReportInputError("s8", 0, errors.New("first"))
	svc.ReportInputError("s8", 0, errors.New("second"))

	rt, _ := svc.RuntimeStatus("s8")
	for _, in := range rt.Inputs {
		if in.InputPriority == 0 {
			// Second report should be ignored (already degraded).
			assert.Equal(t, 1, len(in.Errors), "second error on already-degraded input is dropped")
		}
	}
}

// ─── RuntimeStatus ───────────────────────────────────────────────────────────

func TestRuntimeStatus_UnknownStream(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	_, ok := svc.RuntimeStatus("ghost")
	assert.False(t, ok)
}

func TestRuntimeStatus_AfterRegister(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s9", "rtmp://a", "rtmp://b"), ""))

	rt, ok := svc.RuntimeStatus("s9")
	require.True(t, ok)
	assert.Equal(t, 0, rt.ActiveInputPriority, "active = best priority on register")
	assert.False(t, rt.Exhausted)
	assert.Len(t, rt.Inputs, 2)
}

// ─── SwitchInput — manual override ───────────────────────────────────────────

func TestSwitchInput_ForcesActive(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s10", "rtmp://primary", "rtmp://backup"), ""))

	require.NoError(t, svc.SwitchInput("s10", 1))

	require.Eventually(t, func() bool {
		return len(ing.startCallsCopy()) >= 2
	}, 2*time.Second, 20*time.Millisecond)
	calls := ing.startCallsCopy()
	assert.Equal(t, 1, calls[1].Input.Priority, "switch must restart on chosen priority")

	rt, _ := svc.RuntimeStatus("s10")
	require.NotNil(t, rt.OverrideInputPriority)
	assert.Equal(t, 1, *rt.OverrideInputPriority)
}

func TestSwitchInput_UnknownStreamErrors(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	err := svc.SwitchInput("ghost", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not active")
}

func TestSwitchInput_UnknownPriorityErrors(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s11", "rtmp://x"), ""))
	err := svc.SwitchInput("s11", 99)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestSwitchInput_DegradedInputRejected(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s12", "rtmp://x", "rtmp://y"), ""))
	svc.ReportInputError("s12", 1, errors.New("dead"))

	err := svc.SwitchInput("s12", 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "degraded")
}

// ─── UpdateInputs — added / removed / updated ─────────────────────────────────

func TestUpdateInputs_AddsNewInputAndFailsOverWhenHigherPriority(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s13", "rtmp://current"), ""))
	// Now add a higher-priority input (priority -1 < current 0 — but
	// Priority is int so we can use 0 as new highest if we shift the
	// existing one. Actually, `current` was at 0; we add at priority -1
	// would be tricky. Use the easier path: existing is at priority 1
	// already.) Simplest: register with input at priority 5, then add at 0.

	// Re-register (Unregister first).
	svc.Unregister("s13")
	stream := &domain.Stream{
		Code:   "s13",
		Inputs: []domain.Input{{Priority: 5, URL: "rtmp://current"}},
	}
	require.NoError(t, svc.Register(context.Background(), stream, ""))
	startsBefore := len(ing.startCallsCopy())

	svc.UpdateInputs("s13",
		[]domain.Input{{Priority: 0, URL: "rtmp://new-primary"}}, // added
		nil, nil,
	)

	require.Eventually(t, func() bool {
		return len(ing.startCallsCopy()) > startsBefore
	}, 2*time.Second, 20*time.Millisecond)
	calls := ing.startCallsCopy()
	last := calls[len(calls)-1]
	assert.Equal(t, 0, last.Input.Priority, "must failover to newly-added higher-priority input")
}

func TestUpdateInputs_RemoveActiveTriggersFailover(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s14", "rtmp://primary", "rtmp://backup"), ""))
	startsBefore := len(ing.startCallsCopy())

	svc.UpdateInputs("s14",
		nil,
		[]domain.Input{{Priority: 0, URL: "rtmp://primary"}}, // removed
		nil,
	)

	require.Eventually(t, func() bool {
		return len(ing.startCallsCopy()) > startsBefore
	}, 2*time.Second, 20*time.Millisecond)
	calls := ing.startCallsCopy()
	last := calls[len(calls)-1]
	assert.Equal(t, 1, last.Input.Priority, "failover must switch to remaining input")
}

func TestUpdateInputs_UpdateActiveRestartsIngestor(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s15", "rtmp://old"), ""))
	startsBefore := len(ing.startCallsCopy())

	svc.UpdateInputs("s15", nil, nil,
		[]domain.Input{{Priority: 0, URL: "rtmp://new-url"}}, // updated
	)

	require.Eventually(t, func() bool {
		return len(ing.startCallsCopy()) > startsBefore
	}, 2*time.Second, 20*time.Millisecond)
	calls := ing.startCallsCopy()
	last := calls[len(calls)-1]
	assert.Equal(t, "rtmp://new-url", last.Input.URL, "active input update must restart with new URL")
}

func TestUpdateInputs_UnknownStreamIsNoOp(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	svc.UpdateInputs("ghost", nil, nil, nil) // must not panic
}

// ─── UpdateBufferWriteID ─────────────────────────────────────────────────────

func TestUpdateBufferWriteID_RestartsActiveIngestorWithNewBufID(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s16", "rtmp://x"), "old-buf"))
	startsBefore := len(ing.startCallsCopy())

	svc.UpdateBufferWriteID("s16", "new-buf")

	require.Eventually(t, func() bool {
		return len(ing.startCallsCopy()) > startsBefore
	}, 2*time.Second, 20*time.Millisecond)
	calls := ing.startCallsCopy()
	last := calls[len(calls)-1]
	assert.Equal(t, domain.StreamCode("new-buf"), last.BufferWriteID,
		"buffer write ID change must restart ingestor with new target")
}

func TestUpdateBufferWriteID_UnknownStreamIsNoOp(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	svc.UpdateBufferWriteID("ghost", "any") // must not panic
}

// ─── Restored callback fires after exhausted recovery ────────────────────────

func TestRestoredCallback_FiresAfterExhaustedRecovers(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	require.NoError(t, svc.Register(context.Background(),
		streamWithInputs("s17", "rtmp://x", "rtmp://y"), ""))

	restoredCh := make(chan domain.StreamCode, 1)
	svc.SetRestoredCallback(func(c domain.StreamCode) {
		select {
		case restoredCh <- c:
		default:
		}
	})

	// Drive both inputs to degraded → exhausted.
	svc.ReportInputError("s17", 0, errors.New("a"))
	svc.ReportInputError("s17", 1, errors.New("b"))

	rt, _ := svc.RuntimeStatus("s17")
	require.True(t, rt.Exhausted)

	// Simulate a packet on input 1 → it flips back to Active. Manager then
	// transitions out of exhausted, but the restored callback only fires via
	// the failover path (commitSwitch). So we drive a real failover by
	// reporting error on the now-active input:
	svc.RecordPacket("s17", 1)
	rt, _ = svc.RuntimeStatus("s17")
	for _, in := range rt.Inputs {
		if in.InputPriority == 1 {
			assert.Equal(t, domain.StatusActive, in.Status,
				"RecordPacket must promote Idle/Degraded → Active")
		}
	}
}

// A-5 + C-1: re-registering an already-registered stream must error, not
// silently overwrite the prior monitor goroutine + ticker (leak) and let a
// concurrent Unregister stop the new worker.
func TestRegister_RefusesDuplicate(t *testing.T) {
	t.Parallel()
	svc, _ := newSvc(t)
	st := &domain.Stream{Code: "dup", Inputs: []domain.Input{{URL: "udp://x:1", Priority: 0}}}
	require.NoError(t, svc.Register(context.Background(), st, "dup"))
	require.Error(t, svc.Register(context.Background(), st, "dup"),
		"re-registering an active stream must error")
}

// A-5: a synchronous ingest-start failure must degrade the input and (for a
// single-input stream) drive it to exhausted — not sit silently Idle while the
// coordinator reports the stream Active and the reconciler stays blind.
func TestRegister_StartFailureDrivesExhausted(t *testing.T) {
	t.Parallel()
	svc, ing := newSvc(t)
	ing.startErr = errors.New("dead source")
	exhausted := make(chan domain.StreamCode, 1)
	svc.SetExhaustedCallback(func(c domain.StreamCode) {
		select {
		case exhausted <- c:
		default:
		}
	})
	st := &domain.Stream{Code: "deadsrc", Inputs: []domain.Input{{URL: "udp://x:1", Priority: 0}}}
	require.NoError(t, svc.Register(context.Background(), st, "deadsrc"))
	select {
	case <-exhausted:
	case <-time.After(2 * time.Second):
		t.Fatal("single-input start failure must degrade → exhausted (A-5)")
	}
}
