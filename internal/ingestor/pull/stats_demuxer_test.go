package pull

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// nil onAV → constructor returns nil so no goroutine is spawned. Avoids
// idle wakeups for callers that haven't wired the stats observer.
func TestNewStatsDemuxer_NilCallback(t *testing.T) {
	t.Parallel()
	sd := NewStatsDemuxer(nil)
	assert.Nil(t, sd, "constructor must reject nil callback")
}

// Empty Feed and Close on nil receiver are no-ops — the worker calls
// Feed/Close unconditionally so these guards keep call sites simple.
func TestStatsDemuxer_NilReceiverIsNoop(t *testing.T) {
	t.Parallel()
	var sd *StatsDemuxer
	assert.NotPanics(t, func() {
		sd.Feed([]byte{0x47, 0x00})
		sd.Close()
	})
}

// Close without Feed must not block, even though no chunk was ever queued.
func TestStatsDemuxer_CloseImmediately(t *testing.T) {
	t.Parallel()
	sd := NewStatsDemuxer(func(_ *domain.AVPacket) {})
	require.NotNil(t, sd)
	done := make(chan struct{})
	go func() {
		sd.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close blocked without Feed")
	}
}

// Multiple Close calls are safe (sync.Once guard).
func TestStatsDemuxer_DoubleClose(t *testing.T) {
	t.Parallel()
	sd := NewStatsDemuxer(func(_ *domain.AVPacket) {})
	require.NotNil(t, sd)
	sd.Close()
	assert.NotPanics(t, sd.Close, "second Close must not panic on closed channel")
}

// Empty chunks dropped by Feed without going through the channel — covered
// to make sure the guard short-circuits before the buffered channel send.
func TestStatsDemuxer_FeedEmptyChunk(t *testing.T) {
	t.Parallel()
	calls := atomic.Int64{}
	sd := NewStatsDemuxer(func(_ *domain.AVPacket) {
		calls.Add(1)
	})
	require.NotNil(t, sd)
	defer sd.Close()

	sd.Feed(nil)
	sd.Feed([]byte{})
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int64(0), calls.Load(), "empty Feeds must not emit AV packets")
}

// Feed after Close drops silently via the done channel rather than panicking
// on a send to a closed channel.
func TestStatsDemuxer_FeedAfterClose(t *testing.T) {
	t.Parallel()
	sd := NewStatsDemuxer(func(_ *domain.AVPacket) {})
	require.NotNil(t, sd)
	sd.Close()
	assert.NotPanics(t, func() { sd.Feed([]byte{0x47, 0x00, 0x00}) },
		"Feed after Close must not panic")
}
