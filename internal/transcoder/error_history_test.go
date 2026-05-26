package transcoder

import (
	"testing"
	"time"

	"github.com/ntt0601zcoder/open-streamer/internal/buffer"
	"github.com/ntt0601zcoder/open-streamer/internal/domain"
	"github.com/stretchr/testify/require"
)

// newErrService builds a Service + one running streamWorker (N renditions) for
// the error/status tests, without spinning up buffer/bus/metrics. The stream is
// always registered under "live" — test bodies reference that code directly.
func newErrService(renditions int) (*Service, *streamWorker) {
	rends := make([]int, renditions)
	for i := range rends {
		rends[i] = i
	}
	sw := &streamWorker{renditions: rends}
	s := &Service{
		workers:   map[domain.StreamCode]*streamWorker{"live": sw},
		unhealthy: map[domain.StreamCode]struct{}{},
	}
	return s, sw
}

// recordError bumps the subprocess restart count AND appends an error entry
// (newest at index 0). The two are 1:1 — every recorded crash counts as one
// respawn.
func TestRecordError_IncrementsAndRecords(t *testing.T) {
	t.Parallel()
	s, sw := newErrService(1)

	s.recordError("live", "native transcoder: exit status 234")
	s.recordError("live", "native transcoder: exit status 1")

	require.Equal(t, 2, sw.restartCount)
	require.Len(t, sw.errors, 2)
	require.Equal(t, "native transcoder: exit status 1", sw.errors[0].Message, "newest first")
	require.Equal(t, "native transcoder: exit status 234", sw.errors[1].Message)
}

// Error history is capped at maxTranscoderErrorHistory, newest first.
func TestRecordError_OrderingAndCap(t *testing.T) {
	t.Parallel()
	s, sw := newErrService(1)

	for i := 0; i < 7; i++ {
		s.recordError("live", profileErrMsg(i))
	}

	require.Len(t, sw.errors, maxTranscoderErrorHistory)
	require.Equal(t, "crash-6", sw.errors[0].Message)
	require.Equal(t, "crash-2", sw.errors[maxTranscoderErrorHistory-1].Message)
}

// recordError is a no-op when the stream has been torn down — it runs from the
// respawn loop, which can fire after Stop().
func TestRecordError_NoOpOnMissing(t *testing.T) {
	t.Parallel()
	s := &Service{workers: map[domain.StreamCode]*streamWorker{}}
	require.NotPanics(t, func() {
		s.recordError("nope", "boom")
	})
}

// RuntimeStatus reports subprocess-level health (status, restart_count,
// defensively-copied errors) plus the rendition list in index order.
func TestRuntimeStatus_Shape(t *testing.T) {
	t.Parallel()
	s, sw := newErrService(3)
	sw.restartCount = 3
	sw.errors = []domain.ErrorEntry{{Message: "z", At: time.Now()}}

	rt, ok := s.RuntimeStatus("live")
	require.True(t, ok)
	require.Equal(t, ProcessStatusHealthy, rt.Status)
	require.Equal(t, 3, rt.RestartCount)
	require.Equal(t, "z", rt.Errors[0].Message)
	require.Len(t, rt.Renditions, 3)
	require.Equal(t, 0, rt.Renditions[0].Index)
	require.Equal(t, 2, rt.Renditions[2].Index)
	require.Equal(t, buffer.VideoTrackSlug(0), rt.Renditions[0].Track)

	// Mutate state after snapshot — snapshot must be unaffected (defensive copy).
	s.recordError("live", "after-snapshot")
	require.Equal(t, "z", rt.Errors[0].Message, "snapshot is a defensive copy")
	require.Equal(t, 3, rt.RestartCount, "snapshot restart_count unaffected")
}

func TestRuntimeStatus_NotRunning(t *testing.T) {
	t.Parallel()
	s := &Service{workers: map[domain.StreamCode]*streamWorker{}}
	_, ok := s.RuntimeStatus("nope")
	require.False(t, ok)
}

// Status reflects CURRENT health, not history: a subprocess with
// restart_count > 0 but not currently in the unhealthy set is "healthy".
func TestRuntimeStatus_StatusReflectsCurrentHealth(t *testing.T) {
	t.Parallel()
	s, sw := newErrService(1)
	sw.restartCount = 5 // crashed in the past, currently fine
	require.Equal(t, ProcessStatusHealthy, mustStatus(t, s, "live"))

	s.markUnhealthy("live")
	require.Equal(t, ProcessStatusUnhealthy, mustStatus(t, s, "live"))

	s.markHealthy("live")
	require.Equal(t, ProcessStatusHealthy, mustStatus(t, s, "live"))
}

// The status is snapshotted: marking unhealthy AFTER the snapshot must not
// retroactively change the returned value.
func TestRuntimeStatus_StatusIsSnapshotted(t *testing.T) {
	t.Parallel()
	s, _ := newErrService(1)

	rt, ok := s.RuntimeStatus("live")
	require.True(t, ok)
	require.Equal(t, ProcessStatusHealthy, rt.Status)

	s.markUnhealthy("live")
	require.Equal(t, ProcessStatusHealthy, rt.Status, "stale snapshot stays consistent")
}

func mustStatus(t *testing.T, s *Service, code domain.StreamCode) ProcessStatus {
	t.Helper()
	rt, ok := s.RuntimeStatus(code)
	require.True(t, ok)
	return rt.Status
}

func profileErrMsg(i int) string {
	return "crash-" + string(rune('0'+i))
}
