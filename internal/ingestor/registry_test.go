package ingestor_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor/push"
)

func TestRegistry_RegisterAndLookup(t *testing.T) {
	t.Parallel()

	buf := buffer.NewServiceForTesting(64)
	reg := ingestor.NewRegistry()

	reg.Register("mykey", "stream-1", buf, "")

	writeID, streamID, gotBuf, err := reg.Lookup("mykey")
	require.NoError(t, err)
	assert.Equal(t, domain.StreamCode("stream-1"), writeID)
	assert.Equal(t, domain.StreamCode("stream-1"), streamID)
	assert.Same(t, buf, gotBuf)
}

func TestRegistry_RegisterSeparateBufferWriteID(t *testing.T) {
	t.Parallel()

	buf := buffer.NewServiceForTesting(64)
	reg := ingestor.NewRegistry()
	logical := domain.StreamCode("cam1")
	raw := buffer.RawIngestBufferID(logical)

	reg.Register("pushkey", logical, buf, raw)

	writeID, streamID, gotBuf, err := reg.Lookup("pushkey")
	require.NoError(t, err)
	assert.Equal(t, raw, writeID)
	assert.Equal(t, logical, streamID)
	assert.Same(t, buf, gotBuf)
}

func TestRegistry_Lookup_NotFound(t *testing.T) {
	t.Parallel()

	reg := ingestor.NewRegistry()
	_, _, _, err := reg.Lookup("nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestRegistry_Overwrite(t *testing.T) {
	t.Parallel()

	buf1 := buffer.NewServiceForTesting(64)
	buf2 := buffer.NewServiceForTesting(64)
	reg := ingestor.NewRegistry()

	reg.Register("key", "stream-1", buf1, "")
	reg.Register("key", "stream-2", buf2, "")

	writeID, streamID, gotBuf, err := reg.Lookup("key")
	require.NoError(t, err)
	assert.Equal(t, domain.StreamCode("stream-2"), writeID)
	assert.Equal(t, domain.StreamCode("stream-2"), streamID)
	assert.Same(t, buf2, gotBuf)
}

func TestRegistry_Unregister(t *testing.T) {
	t.Parallel()

	buf := buffer.NewServiceForTesting(64)
	reg := ingestor.NewRegistry()

	reg.Register("key", "stream-1", buf, "")
	reg.Unregister("key")

	_, _, _, err := reg.Lookup("key")
	require.Error(t, err)
}

func TestRegistry_Unregister_NonExistent(t *testing.T) {
	t.Parallel()

	reg := ingestor.NewRegistry()
	// Must not panic.
	assert.NotPanics(t, func() { reg.Unregister("ghost") })
}

func TestRegistry_MultipleKeys(t *testing.T) {
	t.Parallel()

	reg := ingestor.NewRegistry()
	buf := buffer.NewServiceForTesting(64)

	keys := []string{"stream-a", "stream-b", "stream-c"}
	for _, k := range keys {
		reg.Register(k, domain.StreamCode(k), buf, "")
	}

	for _, k := range keys {
		writeID, streamID, _, err := reg.Lookup(k)
		require.NoError(t, err)
		assert.Equal(t, domain.StreamCode(k), writeID)
		assert.Equal(t, domain.StreamCode(k), streamID)
	}
}

// ---- Acquire / Release -------------------------------------------------------

func TestRegistry_Acquire_Success(t *testing.T) {
	t.Parallel()

	buf := buffer.NewServiceForTesting(64)
	reg := ingestor.NewRegistry()
	reg.Register("key", "stream-1", buf, "raw-slot")

	writeID, streamID, gotBuf, err := reg.Acquire("key")
	require.NoError(t, err)
	assert.Equal(t, domain.StreamCode("raw-slot"), writeID)
	assert.Equal(t, domain.StreamCode("stream-1"), streamID)
	assert.Same(t, buf, gotBuf)
}

func TestRegistry_Acquire_NotFound(t *testing.T) {
	t.Parallel()

	reg := ingestor.NewRegistry()
	_, _, _, err := reg.Acquire("no-such-key")
	require.Error(t, err)
}

func TestRegistry_Acquire_AlreadyActive(t *testing.T) {
	t.Parallel()

	buf := buffer.NewServiceForTesting(64)
	reg := ingestor.NewRegistry()
	reg.Register("key", "stream-1", buf, "")

	_, _, _, err := reg.Acquire("key")
	require.NoError(t, err)

	// Second Acquire must fail with ErrStreamAlreadyActive.
	_, _, _, err2 := reg.Acquire("key")
	require.ErrorIs(t, err2, push.ErrStreamAlreadyActive)
}

func TestRegistry_Release_AllowsReacquire(t *testing.T) {
	t.Parallel()

	buf := buffer.NewServiceForTesting(64)
	reg := ingestor.NewRegistry()
	reg.Register("key", "stream-1", buf, "")

	_, _, _, err := reg.Acquire("key")
	require.NoError(t, err)

	reg.Release("key")

	// After Release, Acquire must succeed again.
	_, _, _, err2 := reg.Acquire("key")
	require.NoError(t, err2)
}

func TestRegistry_Release_NonExistent(t *testing.T) {
	t.Parallel()

	reg := ingestor.NewRegistry()
	// Must not panic on a missing key.
	assert.NotPanics(t, func() { reg.Release("ghost") })
}

func TestRegistry_Release_Idempotent(t *testing.T) {
	t.Parallel()

	buf := buffer.NewServiceForTesting(64)
	reg := ingestor.NewRegistry()
	reg.Register("key", "s", buf, "")

	_, _, _, err := reg.Acquire("key")
	require.NoError(t, err)

	reg.Release("key")
	reg.Release("key") // second Release must be a no-op, not panic

	// Still acquirable.
	_, _, _, err2 := reg.Acquire("key")
	require.NoError(t, err2)
}

func TestRegistry_Acquire_DefaultWriteID(t *testing.T) {
	t.Parallel()

	buf := buffer.NewServiceForTesting(64)
	reg := ingestor.NewRegistry()
	reg.Register("key", "stream-x", buf, "") // empty bufferWriteID → defaults to streamID

	writeID, streamID, _, err := reg.Acquire("key")
	require.NoError(t, err)
	assert.Equal(t, domain.StreamCode("stream-x"), writeID)
	assert.Equal(t, domain.StreamCode("stream-x"), streamID)
}

func TestRegistry_ConcurrentAcquireRelease(t *testing.T) {
	t.Parallel()

	buf := buffer.NewServiceForTesting(64)
	reg := ingestor.NewRegistry()
	reg.Register("slot", "stream-1", buf, "")

	const workers = 20
	// Each worker races to Acquire; exactly one per cycle must succeed.
	// After the winner releases, the next cycle begins.
	for range 5 {
		var (
			wg      sync.WaitGroup
			winners int64
		)
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _, _, err := reg.Acquire("slot")
				if err == nil {
					atomic.AddInt64(&winners, 1)
					reg.Release("slot")
				}
			}()
		}
		wg.Wait()
		// At least one goroutine must have acquired the slot per cycle.
		assert.GreaterOrEqual(t, atomic.LoadInt64(&winners), int64(1))
	}
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	reg := ingestor.NewRegistry()
	buf := buffer.NewServiceForTesting(64)
	const goroutines = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			key := "key"
			reg.Register(key, domain.StreamCode("s"), buf, "")
			_, _, _, _ = reg.Lookup(key)
			if i%5 == 0 {
				reg.Unregister(key)
			}
		}(i)
	}
	wg.Wait()
}
