// Package events implements the in-process Event Bus.
// Modules publish events after completing a state change.
// The bus fans out to all subscribers asynchronously via a bounded worker pool.
// Publish never blocks the caller — if the channel is full, the event is dropped and logged.
package events

import (
	"context"
	"log/slog"
	"sync"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// HandlerFunc is a function that processes a single event.
type HandlerFunc func(ctx context.Context, event domain.Event) error

// Bus is the in-process publish/subscribe event bus.
type Bus interface {
	// Publish emits an event to all registered subscribers.
	// It is non-blocking: the event is queued and processed asynchronously.
	Publish(ctx context.Context, event domain.Event)

	// Subscribe registers a handler for a specific event type.
	// Returns an unsubscribe function that removes the handler.
	Subscribe(eventType domain.EventType, handler HandlerFunc) (unsubscribe func())
}

type subscription struct {
	id      uint64
	handler HandlerFunc
}

type inProcessBus struct {
	mu      sync.RWMutex
	subs    map[domain.EventType][]subscription
	nextID  uint64
	queue   chan domain.Event
	workers int
}

// New creates an in-process Bus with the given worker count and queue capacity.
func New(workers, queueSize int) Bus {
	b := &inProcessBus{
		subs:    make(map[domain.EventType][]subscription),
		queue:   make(chan domain.Event, queueSize),
		workers: workers,
	}
	return b
}

// Start launches the worker pool. Must be called before Publish.
// Stops when ctx is cancelled.
func Start(ctx context.Context, b Bus) {
	bus, ok := b.(*inProcessBus)
	if !ok {
		return
	}
	for range bus.workers {
		go bus.runWorker(ctx)
	}
}

// Publish queues an event for async delivery (implements Bus).
func (b *inProcessBus) Publish(_ context.Context, event domain.Event) {
	select {
	case b.queue <- event:
	default:
		slog.Warn("event bus: queue full, dropping event",
			"event_type", event.Type,
			"stream_code", event.StreamCode,
		)
	}
}

// Subscribe registers a handler and returns unsubscribe (implements Bus).
func (b *inProcessBus) Subscribe(eventType domain.EventType, handler HandlerFunc) func() {
	b.mu.Lock()
	b.nextID++
	id := b.nextID
	b.subs[eventType] = append(b.subs[eventType], subscription{id: id, handler: handler})
	b.mu.Unlock()

	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		subs := b.subs[eventType]
		for i, s := range subs {
			if s.id == id {
				b.subs[eventType] = append(subs[:i], subs[i+1:]...)
				return
			}
		}
	}
}

func (b *inProcessBus) runWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-b.queue:
			b.dispatch(ctx, event)
		}
	}
}

func (b *inProcessBus) dispatch(ctx context.Context, event domain.Event) {
	b.mu.RLock()
	subs := b.subs[event.Type]
	handlers := make([]HandlerFunc, 0, len(subs))
	for _, s := range subs {
		handlers = append(handlers, s.handler)
	}
	b.mu.RUnlock()

	for _, h := range handlers {
		if err := h(ctx, event); err != nil {
			slog.Error("event bus: handler error",
				"event_type", event.Type,
				"stream_code", event.StreamCode,
				"err", err,
			)
		}
	}
}
