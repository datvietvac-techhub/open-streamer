package push

import (
	"errors"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// ErrStreamAlreadyActive is returned by Registry.Acquire when another pusher
// is already connected to the requested stream slot.
var ErrStreamAlreadyActive = errors.New("ingestor: stream already has an active pusher")

// Registry maps a stream key (the stream code) to the stream's buffer hub slot.
// Push servers (RTMP, SRT) use this to route incoming encoder connections.
//
// A stream is eligible to receive a push connection only if it has been
// registered via ingestor.Service.Start with a publish:// input URL.
// Only one active pusher per stream is allowed at a time; subsequent
// connection attempts are rejected until the current pusher disconnects.
type Registry interface {
	// Lookup checks whether key is registered and returns its targets.
	// It does NOT change the active-pusher state; use it for pre-flight
	// checks (e.g. SRT HandleConnect) where you can't yet commit.
	Lookup(key string) (bufferWriteID domain.StreamCode, streamID domain.StreamCode, buf *buffer.Service, err error)

	// Acquire atomically checks that key is registered and that no other
	// pusher is currently active, then marks the slot as occupied.
	// Returns ErrStreamAlreadyActive if another pusher holds the slot.
	Acquire(key string) (bufferWriteID domain.StreamCode, streamID domain.StreamCode, buf *buffer.Service, err error)

	// Release clears the active-pusher mark so the next encoder can Acquire.
	// It is safe to call Release even if Acquire was never called for key.
	Release(key string)
}
