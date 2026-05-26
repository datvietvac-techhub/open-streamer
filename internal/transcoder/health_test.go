package transcoder

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ntt0601zcoder/open-streamer/internal/domain"
)

// markUnhealthy returns true exactly once per stream — the edge from healthy to
// unhealthy. A repeat on the same stream returns false so the coordinator only
// sees the transition (no duplicate StatusDegraded fire per crash).
func TestMarkUnhealthy_OnlyFiresOnEdge(t *testing.T) {
	t.Parallel()
	s := newHealthService()

	assert.True(t, s.markUnhealthy("s1"), "first unhealthy must report the transition")
	assert.False(t, s.markUnhealthy("s1"), "already-unhealthy stream must not re-fire")

	// Different stream → its own edge.
	assert.True(t, s.markUnhealthy("s2"), "different stream fires its own first transition")
}

// markHealthy returns true exactly once per stream — the edge from unhealthy
// back to healthy. A mark on an already-healthy stream is a no-op.
func TestMarkHealthy_OnlyFiresOnEdge(t *testing.T) {
	t.Parallel()
	s := newHealthService()

	s.markUnhealthy("s1")
	assert.True(t, s.markHealthy("s1"), "recovery must fire the all-clear edge")
	assert.False(t, s.markHealthy("s1"), "healthy mark on already-healthy stream must not fire")
}

// fire helpers invoke the callback EXACTLY on the transition edge — not on
// every crash. Many crashes between degrade and recover collapse to one call.
func TestFireUnhealthy_CoalesceMultipleCrashes(t *testing.T) {
	t.Parallel()
	s := newHealthService()

	var (
		mu             sync.Mutex
		unhealthyCalls int
		healthyCalls   int
		lastReason     string
	)
	s.SetUnhealthyCallback(func(_ domain.StreamCode, reason string) {
		mu.Lock()
		unhealthyCalls++
		lastReason = reason
		mu.Unlock()
	})
	s.SetHealthyCallback(func(_ domain.StreamCode) {
		mu.Lock()
		healthyCalls++
		mu.Unlock()
	})

	// 5 consecutive crashes → callback fires ONCE.
	for i := 0; i < 5; i++ {
		s.fireUnhealthyIfTransitioned("s1", "crash msg")
	}
	mu.Lock()
	assert.Equal(t, 1, unhealthyCalls)
	assert.Equal(t, "crash msg", lastReason)
	mu.Unlock()

	// Recovery fires healthy ONCE.
	s.fireHealthyIfTransitioned("s1")
	s.fireHealthyIfTransitioned("s1")
	mu.Lock()
	assert.Equal(t, 1, healthyCalls)
	mu.Unlock()
}

// dropHealthState fires onHealthy when the stream was unhealthy at drop time.
// Hot restart (Update → Stop → Start to swap transcoder config) relies on this:
// without the callback the coordinator's mirrored transcoderUnhealthy flag
// would stay true forever, since the fresh subprocess starts clean.
func TestDropHealthState_FiresHealthyWhenEntriesExist(t *testing.T) {
	t.Parallel()
	s := newHealthService()
	healthyCalls := 0
	s.SetHealthyCallback(func(_ domain.StreamCode) { healthyCalls++ })

	s.markUnhealthy("s1")
	s.dropHealthState("s1")

	assert.Equal(t, 1, healthyCalls,
		"dropHealthState must fire onHealthy so coordinator clears its mirrored flag")
	assert.True(t, s.markUnhealthy("s1"),
		"after dropHealthState the stream is back to baseline; new failure is a fresh edge")
}

// dropHealthState on a healthy stream must NOT fire onHealthy — graceful Stop
// on a healthy stream is common; a synthetic recovery event would be noise.
func TestDropHealthState_NoFireOnHealthyStream(t *testing.T) {
	t.Parallel()
	s := newHealthService()
	healthyCalls := 0
	s.SetHealthyCallback(func(_ domain.StreamCode) { healthyCalls++ })

	s.dropHealthState("s1")

	assert.Equal(t, 0, healthyCalls,
		"dropHealthState on a stream that was never unhealthy must not fire")
}

// nil callbacks must be safe — Service operates without coordinator wiring.
func TestFireWithoutCallbacks_NoPanic(t *testing.T) {
	t.Parallel()
	s := newHealthService()
	require.NotPanics(t, func() {
		s.fireUnhealthyIfTransitioned("s1", "x")
		s.fireHealthyIfTransitioned("s1")
	})
}

// newHealthService builds a Service with only the fields the health helpers
// touch — no buffer/bus/metrics needed for state tests.
func newHealthService() *Service {
	return &Service{
		unhealthy: make(map[domain.StreamCode]struct{}),
	}
}
