package ingestor

import (
	"fmt"
	"sync"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor/push"
)

// Registry maps ingest credentials (stream code used as key) to the
// destination buffer for a specific stream. Push servers (RTMP, SRT) use this
// to route incoming connections to the correct Buffer Hub slot.
//
// Only streams configured with a publish:// input URL are registered here.
// The registry key is always the stream code — encoders do not need to know
// an app name; the stream code alone is sufficient to locate the slot.
type Registry struct {
	mu      sync.Mutex
	entries map[string]registryEntry
}

type registryEntry struct {
	streamID      domain.StreamCode // logical stream identifier
	bufferWriteID domain.StreamCode // Buffer Hub slot for buf.Write
	buf           *buffer.Service
	active        bool // true while a pusher holds this slot
}

// NewRegistry constructs an empty Registry.
func NewRegistry() *Registry {
	return &Registry{entries: make(map[string]registryEntry)}
}

// Register maps key to a stream's buffer.
// bufferWriteID is the slot used for buf.Write; if empty, streamID is used.
// Registering the same key twice overwrites the previous entry and clears
// any stale active-pusher mark (e.g. after a server restart).
func (r *Registry) Register(key string, streamID domain.StreamCode, buf *buffer.Service, bufferWriteID domain.StreamCode) {
	if bufferWriteID == "" {
		bufferWriteID = streamID
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[key] = registryEntry{
		streamID:      streamID,
		bufferWriteID: bufferWriteID,
		buf:           buf,
	}
}

// Unregister removes a key from the registry.
func (r *Registry) Unregister(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, key)
}

// Lookup returns the buffer write target, logical stream id, and buffer service
// for key without changing the active-pusher state.
// Used for pre-flight checks (e.g. SRT HandleConnect).
func (r *Registry) Lookup(key string) (bufferWriteID domain.StreamCode, streamID domain.StreamCode, buf *buffer.Service, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	if !ok {
		return "", "", nil, fmt.Errorf("ingestor: no stream registered for key %q", key)
	}
	return e.bufferWriteID, e.streamID, e.buf, nil
}

// Acquire atomically marks the slot for key as occupied and returns its targets.
// Returns push.ErrStreamAlreadyActive if another pusher is currently connected.
// Returns an error if key is not registered.
func (r *Registry) Acquire(key string) (bufferWriteID domain.StreamCode, streamID domain.StreamCode, buf *buffer.Service, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	if !ok {
		return "", "", nil, fmt.Errorf("ingestor: no stream registered for key %q", key)
	}
	if e.active {
		return "", "", nil, push.ErrStreamAlreadyActive
	}
	e.active = true
	r.entries[key] = e
	return e.bufferWriteID, e.streamID, e.buf, nil
}

// Release clears the active-pusher mark for key so the next encoder can Acquire.
// It is a no-op if key is not registered or was never acquired.
func (r *Registry) Release(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[key]
	if !ok {
		return
	}
	e.active = false
	r.entries[key] = e
}
