// Package pull — copy.go: in-process copy:// PacketReader.
//
// CopyReader subscribes to another in-process stream's published buffer
// and forwards each AVPacket as if it had been read from a real network
// source. From the manager / coordinator's point of view a copy:// input
// behaves like any other PacketReader: Open / ReadPackets / Close. From
// the system's point of view there is no network IO at all — the data
// flows through the in-memory Buffer Hub.
//
// Two upstream shapes are supported:
//
//  1. Single-stream upstream (no transcoder, or video.copy=true): subscribe
//     to the upstream's main buffer (= upstream code). Packets are AVPackets
//     written directly by the upstream's ingestor.
//
//  2. ABR upstream + downstream HAS its own transcoder: subscribe to the
//     upstream's BEST rendition buffer (highest resolution/bitrate rung).
//     That buffer carries TS bytes, so we wrap it in an internal TS demuxer
//     to produce AVPackets the downstream transcoder can consume.
//
// ABR upstream + downstream WITHOUT transcoder is routed through a separate
// coordinator path (N tap goroutines mirroring the ladder), never reaching
// this reader — Coordinator.detectABRCopy diverts that case before
// NewPacketReader is called.

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

// StreamLookup resolves an upstream stream by code. Plumbed from the API
// layer (or any layer holding the repo) so this package stays free of the
// store dependency.
type StreamLookup func(domain.StreamCode) (*domain.Stream, bool)

// CopyReader implements ingestor.PacketReader for `copy://<upstream>` URLs.
//
// One of two modes is selected at construction based on the upstream shape:
//   - sub != nil: single-stream mode — subscribe to upstream's main buffer
//     (AVPackets); ReadPackets reads sub.Recv() directly.
//   - abrInner != nil: ABR mode — best rendition buffer is wrapped through
//     a TS demuxer; ReadPackets delegates to abrInner.ReadPackets.
type CopyReader struct {
	target   domain.StreamCode
	bufID    domain.StreamCode // resolved at construction (= upstream's main buffer or best rendition)
	bufSvc   *buffer.Service
	sub      *buffer.Subscriber   // single-stream mode
	abrInner *TSDemuxPacketReader // ABR mode (nil otherwise)
	abrChunk *bufferTSChunkReader // ABR mode underlying source (kept for Close ordering)
	closed   atomic.Bool
	closeErr error
}

// NewCopyReader constructs a CopyReader from a copy:// input. Errors:
//   - URL malformed (`copy://` grammar violation) — propagates protocol error
//   - upstream missing in lookup — runtime error; coordinator surfaces it
//     as input degraded so the manager can switch to a fallback
//
// Mode selection:
//   - upstream is single-stream → subscribe upstream's main buffer (AVPackets)
//   - upstream is ABR → subscribe upstream's BEST rendition buffer (TS bytes)
//     and demux to AVPackets via TSDemuxPacketReader. Coordinator only routes
//     this case here when downstream has its own transcoder; the no-transcoder
//     ABR-copy mirror path is handled by Coordinator.startABRCopy.
func NewCopyReader(input domain.Input, bufSvc *buffer.Service, lookup StreamLookup) (*CopyReader, error) {
	if bufSvc == nil {
		return nil, errors.New("copy reader: nil buffer service")
	}
	if lookup == nil {
		return nil, errors.New("copy reader: nil stream lookup")
	}

	target, err := domain.CopyInputTarget(input)
	if err != nil {
		return nil, fmt.Errorf("copy reader: %w", err)
	}

	upstream, ok := lookup(target)
	if !ok {
		return nil, fmt.Errorf("copy reader: upstream stream %q not found", target)
	}

	// PlaybackBufferID returns the main buffer for single-stream upstreams
	// (AVPackets) and the best rendition buffer for ABR (TS bytes).
	bufID := buffer.PlaybackBufferID(target, upstream.Transcoder)
	r := &CopyReader{target: target, bufID: bufID, bufSvc: bufSvc}

	// Use the TS-demuxer-over-buffer path whenever the upstream's playback
	// buffer carries raw TS bytes — true for ABR ladders (rendition buffers
	// hold TS) AND for raw-TS sources (UDP/HLS/SRT/File) whose ingestor uses
	// TSPassthroughPacketReader. Misclassifying as direct-AV silently drops
	// the entire stream (pkt.AV is nil for raw TS, the AV-only branch
	// rejects every packet).
	if streamHasRenditions(upstream) || domain.StreamMainBufferIsTS(upstream) {
		r.abrChunk = newBufferTSChunkReader(bufSvc, bufID)
		r.abrInner = NewTSDemuxPacketReader(r.abrChunk)
	}
	return r, nil
}

// Open subscribes to the upstream buffer. Returns an error when the
// buffer doesn't exist yet (upstream not started, or torn down).
func (r *CopyReader) Open(ctx context.Context) error {
	if r.closed.Load() {
		return errors.New("copy reader: open after close")
	}
	if r.abrInner != nil {
		// ABR mode: delegate to the inner demuxer (which Opens its
		// underlying bufferTSChunkReader and starts the demux loop).
		if err := r.abrInner.Open(ctx); err != nil {
			return fmt.Errorf("copy reader: abr open %q: %w", r.bufID, err)
		}
		return nil
	}
	// Single-stream mode: subscription is sync; ctx is honoured by
	// ReadPackets only.
	sub, err := r.bufSvc.Subscribe(r.bufID)
	if err != nil {
		return fmt.Errorf("copy reader: subscribe %q: %w", r.bufID, err)
	}
	r.sub = sub
	return nil
}

// ReadPackets blocks until one upstream packet arrives or ctx is done.
// Returns io.EOF when upstream tears down (subscriber channel closes).
//
// Single-stream mode: packets without an AV payload (TS-only) are skipped
// silently — they shouldn't normally appear in a main buffer. ABR mode
// delegates to the inner TS demuxer.
func (r *CopyReader) ReadPackets(ctx context.Context) ([]domain.AVPacket, error) {
	if r.abrInner != nil {
		return r.abrInner.ReadPackets(ctx)
	}
	if r.sub == nil {
		return nil, errors.New("copy reader: ReadPackets called before Open")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case pkt, ok := <-r.sub.Recv():
		if !ok {
			return nil, io.EOF
		}
		if pkt.AV == nil {
			// Upstream wrote a TS-only packet — single-stream copy doesn't
			// support that shape (ABR path would). Drop and wait for next.
			return nil, nil
		}
		return []domain.AVPacket{*pkt.AV}, nil
	}
}

// Close unsubscribes. Idempotent — safe to call from multiple goroutines.
func (r *CopyReader) Close() error {
	if r.closed.Swap(true) {
		return r.closeErr
	}
	if r.abrInner != nil {
		// Closing the inner demuxer cascades to bufferTSChunkReader.Close
		// which unsubscribes from the upstream rendition buffer.
		r.closeErr = r.abrInner.Close()
		return r.closeErr
	}
	if r.sub != nil {
		r.bufSvc.Unsubscribe(r.bufID, r.sub)
	}
	return nil
}

// streamHasRenditions mirrors domain.streamHasRenditions but kept inline
// here so the pull package doesn't have to expose a domain accessor for
// this single use. Returns true when the stream has a re-encoding
// transcoder ladder (= rendition buffers exist, distinct from main).
func streamHasRenditions(s *domain.Stream) bool {
	if s == nil || s.Transcoder == nil {
		return false
	}
	if s.Transcoder.Video.Copy {
		return false
	}
	return len(s.Transcoder.Video.Profiles) > 0
}
