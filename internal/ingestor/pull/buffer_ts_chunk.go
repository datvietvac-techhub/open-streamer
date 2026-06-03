// Package pull — buffer_ts_chunk.go: adapter that exposes a Buffer Hub
// subscriber as a TSChunkReader.
//
// Used by readers that need to demux MPEG-TS bytes living in an in-process
// buffer back into AVPackets. The two current callers:
//
//   - CopyReader, when copying from an ABR upstream — the upstream's
//     rendition buffers carry TS bytes (transcoder output), which must be
//     demuxed before the downstream transcoder can consume them.
//   - MixerReader (planned), for the same reason when its video source is
//     an ABR upstream.
//
// The adapter is single-goroutine: NewTSDemuxPacketReader's pumpChunks loop
// is the only caller of Read. Subscribe-on-Open and Unsubscribe-on-Close
// keep the subscription tied to the wrapping reader's lifecycle.

package pull

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// bufferTSChunkReader implements TSChunkReader over a Buffer Hub subscriber.
// Read returns the next packet's TS payload; AV-only packets (which would
// indicate a shape mismatch) yield empty slices that the demuxer harmlessly
// ignores.
type bufferTSChunkReader struct {
	bufSvc *buffer.Service
	bufID  domain.StreamCode
	sub    *buffer.Subscriber
	closed atomic.Bool
}

func newBufferTSChunkReader(bufSvc *buffer.Service, bufID domain.StreamCode) *bufferTSChunkReader {
	return &bufferTSChunkReader{bufSvc: bufSvc, bufID: bufID}
}

// NewBufferTSDemuxReader composes newBufferTSChunkReader with the TS demuxer
// to produce a *TSDemuxPacketReader that reads AVPackets straight from a
// Buffer Hub buffer carrying MPEG-TS bytes. Convenience for callers outside
// this package (coordinator's ABR-mixer pipeline) that need this combination
// without exposing the internal chunk-reader type.
func NewBufferTSDemuxReader(bufSvc *buffer.Service, bufID domain.StreamCode) *TSDemuxPacketReader {
	return NewTSDemuxPacketReader(newBufferTSChunkReader(bufSvc, bufID))
}

// Open subscribes to the underlying buffer. Must be called before Read.
func (r *bufferTSChunkReader) Open(_ context.Context) error {
	if r.closed.Load() {
		return errors.New("buffer ts reader: open after close")
	}
	sub, err := r.bufSvc.Subscribe(r.bufID)
	if err != nil {
		return fmt.Errorf("buffer ts reader: subscribe %q: %w", r.bufID, err)
	}
	r.sub = sub
	return nil
}

// Read returns the next TS chunk; AV-only packets are skipped. Returns
// io.EOF when the upstream subscriber channel closes (buffer torn down or
// UnsubscribeAll called).
func (r *bufferTSChunkReader) Read(ctx context.Context) ([]byte, error) {
	if r.sub == nil {
		return nil, errors.New("buffer ts reader: read before open")
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case pkt, ok := <-r.sub.Recv():
			if !ok {
				return nil, io.EOF
			}
			if len(pkt.TS) == 0 {
				// AV-only packet — not what we expect from a rendition
				// buffer. Skip and wait for the next, mirroring the
				// upstream-shape-mismatch handling in CopyReader.
				continue
			}
			return pkt.TS, nil
		}
	}
}

// Close unsubscribes from the underlying buffer. Idempotent.
func (r *bufferTSChunkReader) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	if r.sub != nil {
		r.bufSvc.Unsubscribe(r.bufID, r.sub)
	}
	return nil
}
