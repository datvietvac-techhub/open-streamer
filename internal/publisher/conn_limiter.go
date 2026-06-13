package publisher

// conn_limiter.go — concurrent playback connection caps (A-1).
//
// The long-lived play/push protocols (RTSP, RTMP, SRT, HTTP-MPEGTS) allocate
// heavyweight per-connection state — a demux pipeline, a multi-MB write / ts
// buffer, 2-3 goroutines and an FD — with no natural bound. None of the
// handlers authenticate (the `token` param is attribution-only), so an
// unauthenticated client that knows one active stream code can open many
// concurrent connections and exhaust goroutines / FDs / memory, OOM-killing the
// shared process and taking every stream on the host down. connLimiter rejects
// a connection BEFORE that state is allocated, capping both per-stream and
// global concurrency.

import (
	"sync"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// Protective defaults applied when the operator leaves the cap unset (0). These
// are generous for the play/push protocols (HLS/DASH viewers go over stateless
// HTTP and are not counted here) while still bounding a flood. Tune via
// PublisherConfig; a negative configured value means "explicitly unlimited".
const (
	defaultMaxPlaybackConnPerStream = 256
	defaultMaxPlaybackConnTotal     = 4096
)

// connLimiter bounds concurrent playback connections per stream and globally.
// A max of 0 means unlimited for that dimension. Safe for concurrent use.
type connLimiter struct {
	mu           sync.Mutex
	perStream    map[domain.StreamCode]int
	total        int
	maxPerStream int
	maxTotal     int
}

func newConnLimiter(maxPerStream, maxTotal int) *connLimiter {
	return &connLimiter{
		perStream:    make(map[domain.StreamCode]int),
		maxPerStream: maxPerStream,
		maxTotal:     maxTotal,
	}
}

// acquire reserves one connection slot for code. It returns false WITHOUT
// reserving when either the per-stream or the global cap is already at its
// limit; the caller must then reject the connection. On true the caller MUST
// pair it with exactly one release(code) when the connection closes — wire it
// with defer at the acquire site so no exit path leaks a slot (a leaked slot
// permanently inflates the count and eventually rejects every client).
func (l *connLimiter) acquire(code domain.StreamCode) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.maxTotal > 0 && l.total >= l.maxTotal {
		return false
	}
	if l.maxPerStream > 0 && l.perStream[code] >= l.maxPerStream {
		return false
	}
	l.total++
	l.perStream[code]++
	return true
}

// release returns one slot reserved by a prior successful acquire(code). Never
// call it for a connection whose acquire returned false.
func (l *connLimiter) release(code domain.StreamCode) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.perStream[code] <= 1 {
		delete(l.perStream, code)
	} else {
		l.perStream[code]--
	}
	if l.total > 0 {
		l.total--
	}
}

// resolvePlaybackCap maps a configured cap to its effective value: a positive
// value is used as-is, a negative value is "explicitly unlimited" (0), and an
// unset value (0) falls back to the protective default.
func resolvePlaybackCap(configured, def int) int {
	switch {
	case configured > 0:
		return configured
	case configured < 0:
		return 0
	default:
		return def
	}
}
