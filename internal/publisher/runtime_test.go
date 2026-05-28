package publisher

import (
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/stretchr/testify/require"
)

// recordPushErrorEntry contract — newest at index 0, capped at maxPushErrorHistory.
// Same shape as recordInputError / recordProfileErrorEntry so frontend can
// uniformly read errors[0] for the most recent failure across all subsystems.
func TestRecordPushErrorEntry_OrderingAndCap(t *testing.T) {
	t.Parallel()
	st := &pushState{}
	base := time.Date(2026, 4, 23, 17, 0, 0, 0, time.UTC)

	for i := 0; i < 7; i++ {
		recordPushErrorEntry(st, pushErrMsg(i), base.Add(time.Duration(i)*time.Second))
	}

	require.Len(t, st.errors, maxPushErrorHistory)
	require.Equal(t, "fail-6", st.errors[0].Message, "newest entry at index 0")
	require.Equal(t, "fail-2", st.errors[maxPushErrorHistory-1].Message, "oldest survivor at end")
}

// Lifecycle helpers must be safe to call before any state exists (lazy create
// in getOrCreatePushState) — serveRTMPPush calls them at any point in the
// retry loop without an explicit init step.
func TestPushState_LazyInit(t *testing.T) {
	t.Parallel()
	s := newPushTestService()

	s.setPushAttempt("test1", "rtmp://a/key", 5)
	rt, ok := s.RuntimeStatus("test1")
	require.True(t, ok)
	require.Len(t, rt.Pushes, 1)
	require.Equal(t, 5, rt.Pushes[0].Attempt)
	require.Equal(t, PushStatusStarting, rt.Pushes[0].Status, "default status before any setPushStatus call")
}

// setPushStatus(Active) stamps connectedAt; other transitions don't.
func TestSetPushStatus_StampsConnectedAtOnActive(t *testing.T) {
	t.Parallel()
	s := newPushTestService()

	s.setPushStatus("test1", "rtmp://a/key", PushStatusReconnecting)
	rt, _ := s.RuntimeStatus("test1")
	require.Nil(t, rt.Pushes[0].ConnectedAt, "non-active transition must not stamp connected_at")

	before := time.Now()
	s.setPushStatus("test1", "rtmp://a/key", PushStatusActive)
	after := time.Now()

	rt, _ = s.RuntimeStatus("test1")
	require.NotNil(t, rt.Pushes[0].ConnectedAt)
	require.True(t, !rt.Pushes[0].ConnectedAt.Before(before) && !rt.Pushes[0].ConnectedAt.After(after))
}

// Successful reconnect (transition to Active) clears the rolling error
// history so the UI doesn't simultaneously show "ACTIVE" with a stale
// "5 errors" badge. Errors only persist while the destination is unhealthy.
func TestSetPushStatus_ActiveClearsErrors(t *testing.T) {
	t.Parallel()
	s := newPushTestService()

	s.recordPushError("test1", "rtmp://a/key", "handshake fail")
	s.recordPushError("test1", "rtmp://a/key", "tls timeout")
	s.setPushStatus("test1", "rtmp://a/key", PushStatusReconnecting)

	rt, _ := s.RuntimeStatus("test1")
	require.Len(t, rt.Pushes[0].Errors, 2, "errors visible while reconnecting")

	// Reconnect succeeds → enter Active.
	s.setPushStatus("test1", "rtmp://a/key", PushStatusActive)
	rt, _ = s.RuntimeStatus("test1")
	require.Empty(t, rt.Pushes[0].Errors, "Active transition must wipe stale error history")
	require.NotNil(t, rt.Pushes[0].ConnectedAt, "connected_at still stamped")
}

// Leaving Active clears connectedAt — otherwise UI would keep computing
// "uptime" against a stale timestamp from a session that already ended.
func TestSetPushStatus_LeavingActiveClearsConnectedAt(t *testing.T) {
	t.Parallel()
	s := newPushTestService()

	s.setPushStatus("test1", "rtmp://a/key", PushStatusActive)
	rt, _ := s.RuntimeStatus("test1")
	require.NotNil(t, rt.Pushes[0].ConnectedAt, "Active stamps connected_at")

	s.setPushStatus("test1", "rtmp://a/key", PushStatusReconnecting)
	rt, _ = s.RuntimeStatus("test1")
	require.Nil(t, rt.Pushes[0].ConnectedAt, "Reconnecting must clear connected_at")

	s.setPushStatus("test1", "rtmp://a/key", PushStatusActive)
	rt, _ = s.RuntimeStatus("test1")
	require.NotNil(t, rt.Pushes[0].ConnectedAt, "re-Active re-stamps")

	s.setPushStatus("test1", "rtmp://a/key", PushStatusFailed)
	rt, _ = s.RuntimeStatus("test1")
	require.Nil(t, rt.Pushes[0].ConnectedAt, "Failed must also clear")
}

// Non-Active transitions must NOT clear the error history.
func TestSetPushStatus_NonActiveKeepsErrors(t *testing.T) {
	t.Parallel()
	s := newPushTestService()
	s.recordPushError("test1", "rtmp://a/key", "fail-1")

	s.setPushStatus("test1", "rtmp://a/key", PushStatusReconnecting)
	rt, _ := s.RuntimeStatus("test1")
	require.Len(t, rt.Pushes[0].Errors, 1, "Reconnecting must keep history")

	s.setPushStatus("test1", "rtmp://a/key", PushStatusFailed)
	rt, _ = s.RuntimeStatus("test1")
	require.Len(t, rt.Pushes[0].Errors, 1, "Failed must keep history (final state — debug context)")
}

// removePushState clears the entry so a stopped destination disappears from
// the API. Cleans up parent map too when last URL for stream goes away.
func TestRemovePushState_CleansUp(t *testing.T) {
	t.Parallel()
	s := newPushTestService()

	s.setPushStatus("test1", "rtmp://a/key", PushStatusActive)
	s.setPushStatus("test1", "rtmp://b/key", PushStatusActive)

	s.removePushState("test1", "rtmp://a/key")
	rt, ok := s.RuntimeStatus("test1")
	require.True(t, ok, "stream still has one push registered")
	require.Len(t, rt.Pushes, 1)
	require.Equal(t, "rtmp://b/key", rt.Pushes[0].URL)

	s.removePushState("test1", "rtmp://b/key")
	_, ok = s.RuntimeStatus("test1")
	require.False(t, ok, "stream entry removed when no pushes left")
}

// RuntimeStatus output is sorted by URL for stable JSON across calls.
func TestRuntimeStatus_SortedByURL(t *testing.T) {
	t.Parallel()
	s := newPushTestService()
	s.setPushStatus("test1", "rtmp://c/key", PushStatusActive)
	s.setPushStatus("test1", "rtmp://a/key", PushStatusActive)
	s.setPushStatus("test1", "rtmp://b/key", PushStatusReconnecting)

	rt, ok := s.RuntimeStatus("test1")
	require.True(t, ok)
	require.Len(t, rt.Pushes, 3)
	require.Equal(t, "rtmp://a/key", rt.Pushes[0].URL)
	require.Equal(t, "rtmp://b/key", rt.Pushes[1].URL)
	require.Equal(t, "rtmp://c/key", rt.Pushes[2].URL)
}

// Snapshot must be a defensive copy — caller must not see future mutations.
func TestRuntimeStatus_DefensiveErrorCopy(t *testing.T) {
	t.Parallel()
	s := newPushTestService()
	s.recordPushError("test1", "rtmp://a/key", "first")

	rt, _ := s.RuntimeStatus("test1")
	require.Equal(t, "first", rt.Pushes[0].Errors[0].Message)

	s.recordPushError("test1", "rtmp://a/key", "second")
	require.Equal(t, "first", rt.Pushes[0].Errors[0].Message,
		"snapshot must not see post-snapshot mutations")
}

func TestRuntimeStatus_NoStream(t *testing.T) {
	t.Parallel()
	s := newPushTestService()
	_, ok := s.RuntimeStatus("never-existed")
	require.False(t, ok)
}

// newPushTestService builds a Service with only the fields the runtime
// helpers touch — no DI / event bus / buffer plumbing required.
func newPushTestService() *Service {
	return &Service{
		pushStates: make(map[domain.StreamCode]map[string]*pushState),
	}
}

func pushErrMsg(i int) string {
	return "fail-" + string(rune('0'+i))
}
