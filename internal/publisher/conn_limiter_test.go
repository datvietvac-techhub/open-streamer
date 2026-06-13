package publisher

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

func TestConnLimiter_PerStreamCap(t *testing.T) {
	t.Parallel()
	l := newConnLimiter(2, 0) // per-stream 2, total unlimited
	c := domain.StreamCode("s")
	assert.True(t, l.acquire(c))
	assert.True(t, l.acquire(c))
	assert.False(t, l.acquire(c), "3rd connection exceeds the per-stream cap")
	l.release(c)
	assert.True(t, l.acquire(c), "a slot frees after release")
}

func TestConnLimiter_GlobalCap(t *testing.T) {
	t.Parallel()
	l := newConnLimiter(0, 2) // per-stream unlimited, total 2
	assert.True(t, l.acquire("a"))
	assert.True(t, l.acquire("b"))
	assert.False(t, l.acquire("c"), "3rd connection exceeds the global cap")
	l.release("a")
	assert.True(t, l.acquire("c"), "a slot frees globally after release")
}

func TestConnLimiter_PerStreamIsolated(t *testing.T) {
	t.Parallel()
	l := newConnLimiter(1, 0)
	assert.True(t, l.acquire("a"))
	assert.False(t, l.acquire("a"), "stream a is at its cap")
	assert.True(t, l.acquire("b"), "the per-stream cap is per code, not shared")
}

func TestConnLimiter_Unlimited(t *testing.T) {
	t.Parallel()
	l := newConnLimiter(0, 0) // both unlimited
	for i := 0; i < 1000; i++ {
		assert.True(t, l.acquire("s"))
	}
}

func TestConnLimiter_SpuriousReleaseDoesNotUnderflow(t *testing.T) {
	t.Parallel()
	l := newConnLimiter(1, 1)
	l.release("s") // release with nothing acquired must not drive the count negative
	assert.True(t, l.acquire("s"), "still exactly one slot after a spurious release")
	assert.False(t, l.acquire("s"))
}

// TestConnLimiter_Concurrent stresses acquire/release under -race and asserts
// the net count balances back to zero (no leak / no underflow).
func TestConnLimiter_Concurrent(t *testing.T) {
	t.Parallel()
	l := newConnLimiter(0, 0)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if l.acquire("s") {
					l.release("s")
				}
			}
		}()
	}
	wg.Wait()
	l.mu.Lock()
	defer l.mu.Unlock()
	assert.Equal(t, 0, l.total, "total must balance to zero")
	assert.Empty(t, l.perStream, "per-stream map must be empty after all releases")
}

func TestResolvePlaybackCap(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 50, resolvePlaybackCap(50, 256), "positive configured value used as-is")
	assert.Equal(t, 256, resolvePlaybackCap(0, 256), "unset (0) falls back to the default")
	assert.Equal(t, 0, resolvePlaybackCap(-1, 256), "negative means explicitly unlimited (0)")
}
