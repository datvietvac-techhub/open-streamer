package ingestor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// testStallThreshold and testStallCheckInterval are the shortened
// timing knobs every stall_watchdog test passes into runStallWatchdog
// — small enough that the watchdog fires within a few hundred ms
// (vs. the 15 s production threshold). Kept as test-local constants
// rather than mutating package vars so the goroutine sees stable
// values across the test's lifetime and the race detector stays
// happy.
const (
	testStallThreshold     = 200 * time.Millisecond
	testStallCheckInterval = 50 * time.Millisecond
)

// TestStallWatchdog_EmitsBoundaryAfterStallThreshold verifies the
// watchdog fires SessionStartStallRecovery on the buffer hub when no
// write has happened for more than stallThreshold and that the
// emission latches (one boundary per stall, not one per tick).
func TestStallWatchdog_EmitsBoundaryAfterStallThreshold(t *testing.T) {
	const streamID = domain.StreamCode("test-stall")
	buf := buffer.NewServiceForTesting(8)
	buf.Create(streamID)
	defer buf.Delete(streamID)

	// Subscribe so we can observe the SessionStart marker the watchdog
	// puts on the buffer (auto-stamped onto the next packet write).
	sub, err := buf.Subscribe(streamID)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer buf.Unsubscribe(streamID, sub)

	// Seed lastWriteAt comfortably in the past — gap when the watchdog
	// ticks will exceed (the test-shortened) stallThreshold and emit
	// immediately without waiting real wallclock seconds.
	var lastWriteAt atomic.Int64
	lastWriteAt.Store(time.Now().Add(-10 * time.Second).UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runStallWatchdog(ctx, streamID, streamID, buf, &lastWriteAt, testStallThreshold, testStallCheckInterval)

	// Wait for the watchdog's first tick to fire, then send a packet
	// so the auto-stamped SessionStart surfaces on sub.Recv().
	time.Sleep(testStallCheckInterval + 100*time.Millisecond)
	if err := buf.Write(streamID, buffer.TSPacket([]byte{0x47, 0x00, 0x00, 0x00})); err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case pkt := <-sub.Recv():
		if !pkt.SessionStart {
			t.Error("expected SessionStart=true on the packet that follows a stall, got false")
		}
		sess := buf.Session(streamID)
		if sess == nil || sess.Reason != domain.SessionStartStallRecovery {
			gotReason := domain.SessionStartReason(0)
			if sess != nil {
				gotReason = sess.Reason
			}
			t.Errorf("expected SessionStartStallRecovery on the active session, got %v", gotReason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no packet emitted within 2 s — watchdog did not signal session boundary")
	}
}

// TestStallWatchdog_DoesNotFireWhenWritesAreFresh confirms the
// negative path: a fresh lastWriteAt keeps the watchdog quiet.
func TestStallWatchdog_DoesNotFireWhenWritesAreFresh(t *testing.T) {
	const streamID = domain.StreamCode("test-no-stall")
	buf := buffer.NewServiceForTesting(8)
	buf.Create(streamID)
	defer buf.Delete(streamID)

	sub, err := buf.Subscribe(streamID)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer buf.Unsubscribe(streamID, sub)

	var lastWriteAt atomic.Int64
	lastWriteAt.Store(time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runStallWatchdog(ctx, streamID, streamID, buf, &lastWriteAt, testStallThreshold, testStallCheckInterval)

	// Let two watchdog ticks pass while we keep lastWriteAt fresh.
	for i := 0; i < 3; i++ {
		time.Sleep(testStallCheckInterval / 2)
		lastWriteAt.Store(time.Now().UnixNano())
	}

	// Push one normal packet — should NOT carry SessionStart.
	if err := buf.Write(streamID, buffer.TSPacket([]byte{0x47, 0x00})); err != nil {
		t.Fatalf("Write: %v", err)
	}
	select {
	case pkt := <-sub.Recv():
		if pkt.SessionStart {
			t.Errorf("watchdog fired on a healthy stream (SessionStart=true unexpectedly)")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("packet not received")
	}
}

// TestStallWatchdog_LatchClearsAfterRecovery confirms a second stall
// after the first one was signalled emits a second boundary (latch is
// per-event, not global).
func TestStallWatchdog_LatchClearsAfterRecovery(t *testing.T) {
	const streamID = domain.StreamCode("test-latch")
	buf := buffer.NewServiceForTesting(8)
	buf.Create(streamID)
	defer buf.Delete(streamID)

	sub, err := buf.Subscribe(streamID)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer buf.Unsubscribe(streamID, sub)

	var lastWriteAt atomic.Int64

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runStallWatchdog(ctx, streamID, streamID, buf, &lastWriteAt, testStallThreshold, testStallCheckInterval)

	// First stall.
	lastWriteAt.Store(time.Now().Add(-10 * time.Second).UnixNano())
	time.Sleep(testStallCheckInterval + 100*time.Millisecond)
	if err := buf.Write(streamID, buffer.TSPacket([]byte{0x47, 0x01})); err != nil {
		t.Fatalf("Write: %v", err)
	}
	pkt := <-sub.Recv()
	if !pkt.SessionStart {
		t.Fatal("first stall did not signal SessionStart")
	}

	// Recovery: a fresh write resets the lastWriteAt clock. Latch should clear.
	lastWriteAt.Store(time.Now().UnixNano())
	time.Sleep(testStallCheckInterval + 100*time.Millisecond)

	// Second stall: same as first.
	lastWriteAt.Store(time.Now().Add(-10 * time.Second).UnixNano())
	time.Sleep(testStallCheckInterval + 100*time.Millisecond)
	if err := buf.Write(streamID, buffer.TSPacket([]byte{0x47, 0x02})); err != nil {
		t.Fatalf("Write: %v", err)
	}
	pkt = <-sub.Recv()
	if !pkt.SessionStart {
		t.Error("second stall after recovery did not signal SessionStart — latch did not clear")
	}
}
