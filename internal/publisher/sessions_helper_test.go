package publisher

// sessions_helper_test.go — guards the playSession bytes-credit semantics
// for connection-bound protocols (RTMP / SRT / MPEGTS / RTSP). All paths
// flow through add() → touch() (live, throttled flush) → close() (final
// flush of unswapped remainder). The fake closer below records every
// Touch / Close so the tests can verify both the cumulative totals and
// the per-call splits.

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// fakeCloser captures every Touch / Close invocation so the tests can
// assert (a) Close ran exactly once, (b) the cumulative bytes credited
// across Touch + Close match what add() received. Implements
// sessions.Closer.
type fakeCloser struct {
	mu         sync.Mutex
	calls      int
	touchCalls int
	touchedSum int64 // sum of addBytes across every Touch call
	lastBytes  int64
	lastReas   domain.SessionCloseReason
}

func (c *fakeCloser) Close(reason domain.SessionCloseReason, bytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.lastBytes = bytes
	c.lastReas = reason
}

func (c *fakeCloser) Touch(addBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.touchCalls++
	c.touchedSum += addBytes
}

// totalCredited is the bytes-credited sum across both paths — what the
// tracker record will end up with after the session lifecycle completes.
// Tests assert against this rather than `lastBytes` because bytes can
// flow through Touch (live flush) or Close (final flush) depending on
// throttle timing.
func (c *fakeCloser) totalCredited() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.touchedSum + c.lastBytes
}

// add() / close() must credit the full byte sum to the tracker, regardless
// of whether the bytes flowed through live Touch flushes or the final Close
// flush. Throttle timing makes the per-call split non-deterministic in
// production, but the total is invariant.
func TestPlaySessionCloseUsesAccumulatedBytes(t *testing.T) {
	t.Parallel()
	fc := &fakeCloser{}
	p := &playSession{closer: fc}

	p.add(1024)
	p.add(2048)
	p.close()

	require.Equal(t, 1, fc.calls)
	require.Equal(t, int64(3072), fc.totalCredited(),
		"sum of Touch + Close must equal the total bytes added")
	require.Equal(t, domain.SessionCloseClient, fc.lastReas)
}

// Disabled session (tracker not configured) must not call the closer —
// the no-op contract covers close().
func TestPlaySessionCloseNoopWhenDisabled(t *testing.T) {
	t.Parallel()
	fc := &fakeCloser{}
	p := &playSession{closer: fc, disable: true}

	p.add(1024)
	p.close()

	require.Equal(t, 0, fc.calls,
		"disabled session must not invoke closer.Close")
}

// Nil closer must be a safe no-op — happens for short-lived sessions that
// never fully open against the tracker (e.g. RTSP DESCRIBE without PLAY).
func TestPlaySessionCloseNoopWhenCloserNil(t *testing.T) {
	t.Parallel()
	p := &playSession{closer: nil}
	require.NotPanics(t, func() { p.close() })
}

// add() must call Touch with the pending byte delta so the API surfaces a
// growing counter mid-stream and the idle reaper does not mistake a
// healthy long-running session for an abandoned one. Without the delta
// the live record would stay at 0 bytes until close — which is exactly
// what RTMP / SRT / MPEGTS used to display in the dashboard before this
// path was wired up.
func TestPlaySessionAddTouchesCloserAndFlushesBytes(t *testing.T) {
	t.Parallel()
	fc := &fakeCloser{}
	p := &playSession{closer: fc}

	p.add(1024)

	require.Equal(t, 1, fc.touchCalls, "first add() must touch")
	require.Equal(t, int64(1024), fc.touchedSum,
		"touch must publish the pending byte delta to the tracker")
	require.Equal(t, int64(0), p.bytes.Load(),
		"flushed bytes must be swapped out so close() doesn't double-count")
}

// Touch is throttled — calling add() repeatedly within the throttle window
// must not hammer the tracker mutex, but bytes added between flushes must
// accumulate so the next touch (or close) credits the full total.
func TestPlaySessionTouchIsThrottled(t *testing.T) {
	t.Parallel()
	fc := &fakeCloser{}
	p := &playSession{closer: fc}

	// 100 add() calls in tight succession — only one Touch should fire
	// (throttle window is 5 s, calls happen in microseconds). The first
	// add publishes 64 bytes to the tracker; the remaining 99 calls pile
	// 99*64 bytes into the local atomic, which close() will then flush.
	for i := 0; i < 100; i++ {
		p.add(64)
	}

	require.Equal(t, 1, fc.touchCalls,
		"throttle must collapse 100 rapid touches into a single tracker call")
	require.Equal(t, int64(64), fc.touchedSum,
		"first touch flushes only the bytes accumulated up to that moment")
	require.Equal(t, int64(99*64), p.bytes.Load(),
		"unflushed bytes must remain in the local counter for close() to flush")

	p.close()
	require.Equal(t, int64(64*100), fc.totalCredited(),
		"every byte from add() must be credited via Touch + Close")
}

// Disabled / nil-closer session must not panic when add() is called from
// the hot path — the noop guard covers the byte counter AND the touch.
func TestPlaySessionAddNoopWhenDisabled(t *testing.T) {
	t.Parallel()
	fc := &fakeCloser{}
	p := &playSession{closer: fc, disable: true}

	p.add(1024)

	require.Equal(t, int64(0), p.bytes.Load())
	require.Equal(t, 0, fc.touchCalls)
}
