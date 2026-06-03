package dash

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"

	"github.com/datvietvac-techhub/open-streamer/internal/tsdemux"
)

// demux.go — raw-TS chunk → AV frame demuxer.
//
// Sources whose buffer-hub data lives in `buffer.Packet.TS` (UDP / HLS
// pull / HTTP-MPEG-TS / SRT pull / file pull / transcoder output) feed
// this demuxer instead of taking the AV-path. The demuxer extracts
// H.264 / H.265 access units and AAC frames from the TS stream and
// hands them to the packager via onFrame, where they join the same
// per-track queue used by the direct AV-path.
//
// Architecture:
//
//   run loop                       demuxer goroutine
//   ──────────────                  ──────────────────
//   tsBuffer.Write(aligned)  ─►   tsBuffer.Read
//                                 ──► mpeg2.TSDemuxer
//                                       ──► onFrame(cid, frame, pts, dts)
//                                             ──► packager.handleH264 / handleAAC
//
// tsBuffer is the bridge: a thread-safe append-on-write / drain-on-read
// queue with a hard cap to prevent slow-consumer-induced memory growth.

// tsBufferMaxBytes caps the demuxer-side TS backlog. Overflow drops the
// stale backlog rather than blocking the writer — the demuxer will
// resync from the next PAT/PMT after the gap.
//
// Sized to hold one bursty HLS-pull chunk plus a safety margin: a
// 10 Mbps live source at 8 s chunk duration delivers ~10 MB at once,
// and a 2 MiB cap forced an overflow drop on every chunk — observed
// in production as continuous warnings on streams at >5 Mbps (single
// 5 Mbps × 4 s chunk = 2.5 MB > 2 MiB cap) with the resulting
// truncated NALU bytes triggering player decode failures
// (CHUNK_DEMUXER_ERROR_APPEND_FAILED). 16 MiB accommodates up to
// ~16 Mbps × 8 s without losing any chunk content.
const tsBufferMaxBytes = 16 << 20 // 16 MiB

// tsBuffer is a thread-safe bytes pipe between the run loop (writer)
// and the TSDemuxer goroutine (reader).
type tsBuffer struct {
	mu       sync.Mutex
	cond     *sync.Cond
	buf      []byte
	done     bool
	streamID string
	maxBytes int
}

// newTSBuffer constructs a tsBuffer tagged with streamID for log
// context on overflow drops.
func newTSBuffer(streamID string) *tsBuffer {
	b := &tsBuffer{streamID: streamID, maxBytes: tsBufferMaxBytes}
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
		slog.Warn("dash: tsBuffer overflow — dropping backlog",
			"stream_id", b.streamID,
			"dropped_bytes", dropped,
			"incoming_bytes", len(p),
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
// Once drained, the underlying array is dropped so GC can reclaim the
// peak capacity — important because peak-burst sizes are often 100×
// larger than steady-state and otherwise stay pinned forever.
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

// alignTS extracts complete 188-byte MPEG-TS packets from a stream of
// arbitrary-byte chunks. `carry` accumulates partial packets across
// calls; the caller passes the same *carry on each invocation.
//
// Returns a slice of aligned bytes (length is a multiple of 188) that
// can be safely fed to the demuxer. The remainder stays in *carry.
func alignTS(carry *[]byte, chunk []byte) []byte {
	*carry = append(*carry, chunk...)
	aligned := []byte{}
	for len(*carry) >= 188 {
		if (*carry)[0] != 0x47 {
			idx := bytes.IndexByte(*carry, 0x47)
			if idx < 0 {
				// No sync byte anywhere — keep only the last 187
				// bytes (a complete packet could start in the next
				// chunk's first 1 byte plus this tail).
				if len(*carry) > 187 {
					*carry = (*carry)[len(*carry)-187:]
				}
				return aligned
			}
			*carry = (*carry)[idx:]
			if len(*carry) < 188 {
				return aligned
			}
		}
		// Verify the NEXT packet's sync byte too (or accept if buffer
		// shorter than 376 — only one packet left, no way to double-
		// check yet).
		if len(*carry) >= 376 && (*carry)[188] != 0x47 {
			// Misalignment: skip one byte and retry the sync scan.
			*carry = (*carry)[1:]
			continue
		}
		aligned = append(aligned, (*carry)[:188]...)
		*carry = (*carry)[188:]
	}
	return aligned
}

// onTSFrameFunc is the callback signature the demuxer fires per
// extracted access unit. cid distinguishes H.264 / H.265 / AAC.
//
// Type alias of tsdemux.OnFrameFunc so caller code in this package
// stays grep-able under the historical name while the underlying
// signature is the wrapper package's.
type onTSFrameFunc = tsdemux.OnFrameFunc

// startDemuxer launches the TSDemuxer goroutine consuming from tb.
// The OnFrame callback fires synchronously from that goroutine.
// Caller must Close(tb) to unblock the demuxer on shutdown.
func startDemuxer(ctx context.Context, tb *tsBuffer, onFrame onTSFrameFunc) {
	dmx := tsdemux.New()
	dmx.OnFrame = onFrame
	go func() {
		if err := dmx.Input(ctx, tb); err != nil && err != io.EOF {
			slog.Debug("dash: demuxer exit", "err", err)
		}
	}()
}
