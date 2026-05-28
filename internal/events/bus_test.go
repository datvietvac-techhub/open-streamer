package events

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("waitFor: condition not met within timeout")
}

func TestBusPublishDeliversToSubscriber(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(2, 16)
	Start(ctx, b)

	var got atomic.Int32
	b.Subscribe(domain.EventStreamCreated, func(_ context.Context, _ domain.Event) error {
		got.Add(1)
		return nil
	})

	b.Publish(ctx, domain.Event{Type: domain.EventStreamCreated, StreamCode: "live"})
	waitFor(t, func() bool { return got.Load() == 1 }, time.Second)
}

func TestBusOnlyDeliversToMatchingType(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(1, 16)
	Start(ctx, b)

	var created, stopped atomic.Int32
	b.Subscribe(domain.EventStreamCreated, func(_ context.Context, _ domain.Event) error {
		created.Add(1)
		return nil
	})
	b.Subscribe(domain.EventStreamStopped, func(_ context.Context, _ domain.Event) error {
		stopped.Add(1)
		return nil
	})

	b.Publish(ctx, domain.Event{Type: domain.EventStreamCreated})
	b.Publish(ctx, domain.Event{Type: domain.EventStreamCreated})
	b.Publish(ctx, domain.Event{Type: domain.EventStreamStopped})

	waitFor(t, func() bool { return created.Load() == 2 && stopped.Load() == 1 }, time.Second)
}

func TestBusUnsubscribeStopsDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(1, 16)
	Start(ctx, b)

	var n atomic.Int32
	unsubscribe := b.Subscribe(domain.EventStreamStarted, func(_ context.Context, _ domain.Event) error {
		n.Add(1)
		return nil
	})

	b.Publish(ctx, domain.Event{Type: domain.EventStreamStarted})
	waitFor(t, func() bool { return n.Load() == 1 }, time.Second)

	unsubscribe()
	b.Publish(ctx, domain.Event{Type: domain.EventStreamStarted})

	time.Sleep(50 * time.Millisecond)
	if n.Load() != 1 {
		t.Fatalf("handler called after unsubscribe: %d", n.Load())
	}
}

func TestBusMultipleSubscribersBothFire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(2, 16)
	Start(ctx, b)

	var a, c atomic.Int32
	b.Subscribe(domain.EventInputConnected, func(_ context.Context, _ domain.Event) error {
		a.Add(1)
		return nil
	})
	b.Subscribe(domain.EventInputConnected, func(_ context.Context, _ domain.Event) error {
		c.Add(1)
		return nil
	})

	b.Publish(ctx, domain.Event{Type: domain.EventInputConnected})
	waitFor(t, func() bool { return a.Load() == 1 && c.Load() == 1 }, time.Second)
}

func TestBusHandlerErrorDoesNotStopBus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(1, 16)
	Start(ctx, b)

	var n atomic.Int32
	b.Subscribe(domain.EventStreamDeleted, func(_ context.Context, _ domain.Event) error {
		n.Add(1)
		return errors.New("boom")
	})

	for range 3 {
		b.Publish(ctx, domain.Event{Type: domain.EventStreamDeleted})
	}
	waitFor(t, func() bool { return n.Load() == 3 }, time.Second)
}

func TestBusPublishNeverBlocksWhenQueueFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(1, 1)
	// Don't Start workers — queue will fill up.

	done := make(chan struct{})
	go func() {
		for range 50 {
			b.Publish(ctx, domain.Event{Type: domain.EventStreamCreated})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked when queue full")
	}
}

func TestBusUnsubscribeUnknownSubIsNoOp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(1, 4)
	Start(ctx, b)

	unsub := b.Subscribe(domain.EventStreamStarted, func(_ context.Context, _ domain.Event) error { return nil })
	unsub()
	unsub() // double-call must not panic
}

func TestBusContextCancelStopsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	b := New(2, 4)
	Start(ctx, b)

	var n atomic.Int32
	b.Subscribe(domain.EventStreamStopped, func(_ context.Context, _ domain.Event) error {
		n.Add(1)
		return nil
	})
	b.Publish(ctx, domain.Event{Type: domain.EventStreamStopped})
	waitFor(t, func() bool { return n.Load() == 1 }, time.Second)

	cancel()
	time.Sleep(50 * time.Millisecond)

	// Publish after cancel should not deliver because workers exited;
	// queue still has capacity so Publish itself succeeds (non-blocking).
	b.Publish(context.Background(), domain.Event{Type: domain.EventStreamStopped})
	time.Sleep(50 * time.Millisecond)
	if n.Load() != 1 {
		t.Fatalf("worker still running after ctx cancel: n=%d", n.Load())
	}
}

func TestBusConcurrentPublishSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(4, 256)
	Start(ctx, b)

	var got atomic.Int32
	for range 4 {
		b.Subscribe(domain.EventStreamCreated, func(_ context.Context, _ domain.Event) error {
			got.Add(1)
			return nil
		})
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				b.Publish(ctx, domain.Event{Type: domain.EventStreamCreated})
			}
		}()
	}
	wg.Wait()
	waitFor(t, func() bool { return got.Load() >= int32(8*10*4-200) }, 2*time.Second)
}

func TestStartDoesNotPanicWithCustomBus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type customBus struct{ Bus }
	cb := &customBus{Bus: New(1, 1)}
	Start(ctx, cb) // unrecognized type, should be a no-op
}
