package publisher

import (
	"io"
	"log/slog"
	"sync"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// tsbuffer.go — shared TS-bytes pipe used by serve_rtmp, serve_rtsp,
// and push_rtmp to bridge their run-loop goroutine to a gomedia
// TSDemuxer goroutine.
//
// The DASH packager has its own internal tsBuffer (in
// internal/publisher/dash/demux.go) with the same semantics but
// package-private — duplication is intentional because the two
// packages each own their own demuxer lifecycle and don't otherwise
// share state.

// tsBufferMaxBytes caps the demuxer-side TS backlog. Overflow drops
// the stale backlog rather than blocking the writer; the demuxer
// resyncs from the next PAT/PMT after the gap.
//
// Sized to hold one bursty HLS-pull chunk plus a safety margin: a
// 10 Mbps live source at 8 s chunk duration delivers ~10 MB at once,
// and a 2 MiB cap forced an overflow drop on every chunk — observed
// in production as continuous warnings on customer streams at >5 Mbps
// (single 5 Mbps × 4 s chunk = 2.5 MB > 2 MiB cap) with the resulting
// truncated NALU bytes triggering player decode failures
// (CHUNK_DEMUXER_ERROR_APPEND_FAILED). 16 MiB accommodates up to
// ~16 Mbps × 8 s without losing any chunk content.
const tsBufferMaxBytes = 16 << 20 // 16 MiB

// tsBuffer is a thread-safe bytes pipe between a producer goroutine
// (writing aligned 188-byte TS packets) and a consumer goroutine
// running mpeg2.TSDemuxer.Input.
type tsBuffer struct {
	mu          sync.Mutex
	cond        *sync.Cond
	buf         []byte
	done        bool
	streamID    domain.StreamCode
	maxBytes    int
	dropCount   int64
	droppedSize int64
}

// newTSBuffer constructs a tsBuffer tagged with streamID for log
// context on overflow drops.
func newTSBuffer(streamID domain.StreamCode) *tsBuffer {
	return newTSBufferWithCap(streamID, tsBufferMaxBytes)
}

// newTSBufferWithCap is the test-friendly constructor allowing a
// smaller cap (production code uses newTSBuffer).
func newTSBufferWithCap(streamID domain.StreamCode, maxBytes int) *tsBuffer {
	b := &tsBuffer{streamID: streamID, maxBytes: maxBytes}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Write appends p, dropping the existing backlog if appending would
// exceed maxBytes. Never blocks the caller.
func (b *tsBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	if b.done {
		b.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if b.maxBytes > 0 && len(b.buf)+len(p) > b.maxBytes && len(b.buf) > 0 {
		dropped := len(b.buf)
		b.buf = nil
		b.dropCount++
		b.droppedSize += int64(dropped)
		slog.Warn("publisher: tsBuffer overflow, dropped backlog (demuxer will resync from next PAT/PMT)",
			"stream_code", string(b.streamID),
			"dropped_bytes", dropped,
			"incoming_bytes", len(p),
			"cap_bytes", b.maxBytes,
			"drop_count", b.dropCount,
			"dropped_total_bytes", b.droppedSize,
		)
	}
	b.buf = append(b.buf, p...)
	b.cond.Signal()
	b.mu.Unlock()
	return len(p), nil
}

// Read blocks until data is available or the buffer is closed. Returns
// io.EOF on close with no remaining bytes.
//
// Once drained, the underlying array is dropped so GC reclaims peak
// capacity — important because peak-burst sizes can be 100× larger
// than steady-state and would otherwise stay pinned forever.
func (b *tsBuffer) Read(p []byte) (int, error) {
	b.mu.Lock()
	for len(b.buf) == 0 && !b.done {
		b.cond.Wait()
	}
	if len(b.buf) == 0 {
		b.mu.Unlock()
		return 0, io.EOF
	}
	n := copy(p, b.buf)
	b.buf = b.buf[n:]
	if len(b.buf) == 0 {
		b.buf = nil
	}
	b.mu.Unlock()
	return n, nil
}

// Close unblocks any pending Read and rejects further Writes.
func (b *tsBuffer) Close() {
	b.mu.Lock()
	b.done = true
	b.cond.Broadcast()
	b.mu.Unlock()
}
