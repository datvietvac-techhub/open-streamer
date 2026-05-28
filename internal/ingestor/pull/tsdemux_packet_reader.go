package pull

// tsdemux_packet_reader.go — TS chunk source → domain.AVPacket stream.
//
// Architecture:
//
//	TSChunkReader.Read ──► pumpChunks ──► chunks (chan []byte)
//	                                           │
//	                                      chanReader (io.Reader)
//	                                           │
//	                                    tsdemux.Demuxer.Input
//	                                           │ OnFrame callback
//	                                           ▼
//	                                     q (chan AVPacket)
//	                                           │
//	                                      ReadPackets ──► caller
//
// Using a buffered channel (chunks) instead of io.Pipe decouples the read goroutine
// from the demux goroutine: up to chunkQueueSize pre-fetched chunks can accumulate
// while the demuxer processes current data, eliminating per-write pipe mutex contention.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	gocodec "github.com/yapingcat/gomedia/go-codec"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/tsdemux"
)

const (
	// chunkQueueSize sets how many raw TS chunks can be buffered between pumpChunks and runDemux.
	// At 20 Mbps with 10 KB chunks this is ~256 ms of lookahead.
	chunkQueueSize = 64

	// avQueueSize is the maximum number of decoded AVPackets buffered before the caller drains them.
	avQueueSize = 16384

	// maxDemuxRestarts caps the number of times runDemux will recreate the TSDemuxer after a
	// parse error without any successful frame in between.  The gompeg2 demuxer stops on the
	// first bad TS packet; restarting it allows resync after transient corruption (e.g. the
	// first UDP burst before the encoder settles, or adaptation-field oddities).
	// A counter reset to 0 on every successful OnFrame call, so only truly unrecoverable
	// streams hit this ceiling.
	maxDemuxRestarts = 8
)

// TSChunkReader is an MPEG-TS byte source (UDP, HLS, file, SRT, …).
type TSChunkReader interface {
	Open(ctx context.Context) error
	Read(ctx context.Context) ([]byte, error)
	Close() error
}

// chanReader implements io.Reader over a channel of byte slices.
// It feeds the gompeg2.TSDemuxer without copying chunks into a pipe.
// rem holds the unconsumed portion of the current chunk.
type chanReader struct {
	ch  <-chan []byte
	rem []byte
}

// Read satisfies io.Reader. Blocks until a chunk arrives or the channel is closed (→ io.EOF).
func (r *chanReader) Read(p []byte) (int, error) {
	for len(r.rem) == 0 {
		chunk, ok := <-r.ch
		if !ok {
			return 0, io.EOF
		}
		r.rem = chunk
	}
	n := copy(p, r.rem)
	r.rem = r.rem[n:]
	return n, nil
}

// TSDemuxPacketReader wraps a TSChunkReader and emits domain.AVPacket values.
type TSDemuxPacketReader struct {
	r TSChunkReader

	mu      sync.Mutex
	started bool

	chunks chan []byte          // raw TS data from pumpChunks to chanReader/runDemux
	q      chan domain.AVPacket // decoded packets ready for ReadPackets
	done   chan struct{}        // closed by Close to unblock pumpChunks and OnFrame

	// readErr holds the first non-EOF, non-cancellation error returned by the
	// underlying TSChunkReader.  It is written once by pumpChunks (before
	// close(chunks)) and read by ReadPackets (after q closes).  The happens-before
	// chain through channel close operations makes this access race-free.
	readErr error

	readLoopDone chan struct{}
	demuxDone    chan struct{}

	// pacing throttles AVPacket emission to wall-clock based on DTS.  Used for
	// chunk-based sources that deliver media faster than real-time (HLS pulls
	// each segment as one HTTP GET → hundreds of packets in microseconds).
	// Without this, downstream RTMP push to common live platforms sends timestamps that
	// jump 13–26× wall-clock and the ingest server closes the connection.
	pacing   bool
	paceOnce bool
	paceRef  int64 // first observed DTS (ms)
	paceAt   time.Time
}

// TSDemuxOption configures a TSDemuxPacketReader at construction time.
type TSDemuxOption func(*TSDemuxPacketReader)

// WithRealtimePacing enables DTS-based real-time throttling on AVPacket emission.
// Use it for chunk-based sources (HLS) that deliver bursts faster than playback
// rate; do not use it for live transports (UDP, SRT, RTMP) where the source
// already paces itself.
func WithRealtimePacing() TSDemuxOption {
	return func(d *TSDemuxPacketReader) { d.pacing = true }
}

// NewTSDemuxPacketReader wraps r and emits domain.AVPacket values.
func NewTSDemuxPacketReader(r TSChunkReader, opts ...TSDemuxOption) *TSDemuxPacketReader {
	d := &TSDemuxPacketReader{r: r}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Open starts the pump and demux goroutines.
func (d *TSDemuxPacketReader) Open(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.started {
		return nil
	}
	if err := d.r.Open(ctx); err != nil {
		return err
	}

	d.chunks = make(chan []byte, chunkQueueSize)
	d.q = make(chan domain.AVPacket, avQueueSize)
	d.done = make(chan struct{})
	d.readLoopDone = make(chan struct{})
	d.demuxDone = make(chan struct{})
	d.started = true

	// Capture channel references in the calling goroutine (under mutex) before
	// launching workers.  Close() sets d.q = nil; if the goroutine scheduler
	// delays runDemux until after Close runs, reading d.q inside runDemux would
	// yield nil → panic on close(nil channel).
	q := d.q
	done := d.done
	chunks := d.chunks
	demuxDone := d.demuxDone

	go d.pumpChunks(ctx)
	go d.runDemux(ctx, q, done, chunks, demuxDone)
	return nil
}

// pumpChunks reads raw TS chunks from the source and forwards them to chanReader via chunks.
// It exits when the source is exhausted, the context is cancelled, or Close is called.
// Real (non-EOF, non-cancellation) errors are stored in d.readErr before the channel
// closes, so ReadPackets can propagate them to the caller.
func (d *TSDemuxPacketReader) pumpChunks(ctx context.Context) {
	defer close(d.readLoopDone)
	defer close(d.chunks) // EOF signal for chanReader → demux.Input returns

	for {
		if ctx.Err() != nil {
			return
		}
		chunk, rerr := d.r.Read(ctx)
		if len(chunk) > 0 {
			select {
			case d.chunks <- chunk:
			case <-d.done:
				return
			}
		}
		if rerr != nil {
			d.captureReadErr(rerr, ctx)
			return
		}
	}
}

// captureReadErr stores err in d.readErr when it represents a genuine source failure:
// not a clean EOF, not a context cancellation, and not triggered by Close().
func (d *TSDemuxPacketReader) captureReadErr(err error, ctx context.Context) {
	if errors.Is(err, io.EOF) || ctx.Err() != nil {
		return
	}
	select {
	case <-d.done:
		// Close() was called concurrently; suppress to avoid confusing callers
		// with a spurious "connection closed" from d.r.Close().
	default:
		d.readErr = err
	}
}

// runDemux feeds chanReader into the MPEG-TS demuxer and emits AVPackets onto q.
//
// astits's Demuxer.NextData returns parse errors via the err channel
// rather than crashing the goroutine. runDemux restarts the demuxer
// after a parse error so the pipeline can tolerate transient
// corruption (a startup burst before the encoder stabilises, an
// occasional bad adaptation field). The chanReader picks up exactly
// where it left off (it buffers the channel in rem), and a fresh
// demuxer's first NextData() will scan forward for the next 0x47 sync
// byte. consecutiveErrors caps the restart count so a truly
// unrecoverable stream eventually returns and the caller reconnects.
func (d *TSDemuxPacketReader) runDemux(
	ctx context.Context,
	q chan domain.AVPacket,
	done chan struct{},
	chunks chan []byte,
	demuxDone chan struct{},
) {
	defer close(demuxDone)
	defer close(q)

	cr := &chanReader{ch: chunks}

	consecutiveErrors := 0
	for {
		dmx := tsdemux.New()
		dmx.OnFrame = func(cid tsdemux.StreamType, frame []byte, pts, dts uint64) {
			if len(frame) == 0 {
				return
			}
			p, ok := buildAVPacket(cid, frame, pts, dts)
			if !ok {
				return
			}
			// A successful frame means the demuxer re-synced; reset the error counter
			// so the pipeline can tolerate isolated bad patches without hitting the cap.
			consecutiveErrors = 0
			if d.pacing {
				d.pace(int64(dts), done) //nolint:gosec
			}
			select {
			case q <- p:
			case <-done:
			}
		}

		err := demuxInputSafe(ctx, dmx, cr)
		if err == nil {
			// Normal exit: chanReader returned io.EOF because d.chunks was closed.
			return
		}

		// Parse error (bad adaptation field, unexpected PES, missing sync).
		// Restart with a fresh demuxer so it scans forward for the next 0x47.
		consecutiveErrors++
		slog.Debug("ts demux: parse error, restarting demuxer",
			"err", err,
			"consecutive_errors", consecutiveErrors,
			"max", maxDemuxRestarts,
		)
		if consecutiveErrors >= maxDemuxRestarts {
			// Persistent parse failures — the stream is likely unrecoverable.
			// Return so the caller (ReadPackets) sees io.EOF and triggers
			// a reconnect via the normal error path.
			slog.Warn("ts demux: too many consecutive parse errors, giving up",
				"consecutive_errors", consecutiveErrors,
			)
			return
		}
	}
}

// pace blocks until wall-clock catches up to the packet's DTS, mirroring the
// real-time emission rate of the source.  The first call captures the reference
// (paceRef = first DTS, paceAt = now); subsequent calls sleep until
// paceAt + (dts - paceRef).  Signed arithmetic tolerates out-of-order DTS
// between audio and video streams without underflowing — a packet whose DTS
// is behind the reference simply skips the wait.  Cancellable via done.
func (d *TSDemuxPacketReader) pace(dtsMS int64, done <-chan struct{}) {
	if !d.paceOnce {
		d.paceOnce = true
		d.paceRef = dtsMS
		d.paceAt = time.Now()
		return
	}
	target := d.paceAt.Add(time.Duration(dtsMS-d.paceRef) * time.Millisecond)
	wait := time.Until(target)
	if wait <= 0 {
		return
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}

// demuxInputSafe calls dmx.Input and converts any panic into a
// returned error. astits returns parse errors via the error path
// rather than panics, so this is now a defensive wrapper rather than
// a workaround for a known library bug — it costs ~nothing and
// guards against future regressions or malformed-input edge cases.
func demuxInputSafe(ctx context.Context, dmx *tsdemux.Demuxer, cr *chanReader) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("ts demux panic: %v", r)
		}
	}()
	return dmx.Input(ctx, cr)
}

// streamTypeMPEG1Audio is astits's stream_type for MPEG-1 audio
// (ISO/IEC 11172-3); we accept either MPEG-1 or MPEG-2 audio and
// inspect the frame header to tell Layer III (MP3) from Layer I/II.
const streamTypeMPEG1Audio tsdemux.StreamType = 0x03

// buildAVPacket constructs a domain.AVPacket from a TS demuxer
// frame callback. Returns ok=false for unsupported stream types.
func buildAVPacket(cid tsdemux.StreamType, frame []byte, pts, dts uint64) (domain.AVPacket, bool) {
	var cdc domain.AVCodec
	var keyFrame bool

	switch cid { //nolint:exhaustive // intentionally unsupported codecs return ok=false
	case tsdemux.StreamTypeH264:
		cdc = domain.AVCodecH264
		keyFrame = gocodec.IsH264IDRFrame(frame)
	case tsdemux.StreamTypeH265:
		cdc = domain.AVCodecH265
		keyFrame = gocodec.IsH265IDRFrame(frame)
	case tsdemux.StreamTypeAAC:
		cdc = domain.AVCodecAAC
	case streamTypeMPEG1Audio, 0x04: // 0x04 = MPEG-2 audio
		// TS container doesn't encode the Layer (I/II/III) — peek at the
		// frame header to split MP3 (Layer III) from MP1/MP2. Done here so
		// the UI label and codec-aware consumers see "mp3" instead of the
		// generic "mp2a" for Layer III sources.
		cdc = mpegAudioCodecFromFrame(frame)
	default:
		return domain.AVPacket{}, false
	}

	return domain.AVPacket{
		Codec:    cdc,
		Data:     bytes.Clone(frame),
		PTSms:    pts,
		DTSms:    dts,
		KeyFrame: keyFrame,
	}, true
}

// mpegAudioCodecFromFrame inspects the first MPEG audio frame header in the
// PES payload to split MP3 (Layer III) from MP1/MP2 (Layer I/II). The TS
// container's stream_type (0x03/0x04) doesn't encode the Layer, so the frame
// header is the only place to tell them apart for UI labelling and codec-
// aware routing (mixer, transcoder copy decision).
//
// MPEG audio frame header layout (first 2 bytes, the rest is bitrate /
// sample-rate / channel info we don't need here):
//
//	byte 0: 0xFF                              (sync byte 1, all bits set)
//	byte 1: 1 1 1 V V L L B
//	            ^^^ sync continuation = 111
//	                ^^^ MPEG version (00=v2.5, 01=reserved, 10=v2, 11=v1)
//	                  ^^^ Layer (00=reserved, 01=Layer III, 10=Layer II, 11=Layer I)
//	                      ^ protection bit
//
// Falls back to AVCodecMP2 when the header is absent or ambiguous so the
// pipeline degrades into the prior "everything is mp2a" mode rather than
// silently dropping audio.
func mpegAudioCodecFromFrame(frame []byte) domain.AVCodec {
	if len(frame) < 2 {
		return domain.AVCodecMP2
	}
	// Sync: 0xFF followed by a byte whose top 3 bits are 111.
	if frame[0] != 0xFF || frame[1]&0xE0 != 0xE0 {
		return domain.AVCodecMP2
	}
	const layerIII = 0b01
	if (frame[1]>>1)&0x03 == layerIII {
		return domain.AVCodecMP3
	}
	return domain.AVCodecMP2
}

// ReadPackets blocks until at least one AVPacket is available, then drains as many
// as are ready (up to 256 at once) to amortise call overhead.
// Returns (nil, io.EOF) when the source is exhausted.
func (d *TSDemuxPacketReader) ReadPackets(ctx context.Context) ([]domain.AVPacket, error) {
	d.mu.Lock()
	q := d.q
	d.mu.Unlock()

	if q == nil {
		return nil, io.EOF
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case p, ok := <-q:
		if !ok {
			// Prefer the real source error over a generic EOF so the caller
			// (runPullWorker) can distinguish "clean end" from "source failed".
			if d.readErr != nil {
				return nil, d.readErr
			}
			return nil, io.EOF
		}
		batch := []domain.AVPacket{p}
		for len(batch) < 256 {
			select {
			case p2, ok2 := <-q:
				if !ok2 {
					return batch, nil
				}
				batch = append(batch, p2)
			default:
				return batch, nil
			}
		}
		return batch, nil
	}
}

// Close tears down both goroutines and the underlying reader.
// It is safe to call Close concurrently or multiple times.
func (d *TSDemuxPacketReader) Close() error {
	d.mu.Lock()
	if !d.started {
		d.mu.Unlock()
		return d.r.Close()
	}
	d.started = false
	done := d.done
	readLoopDone := d.readLoopDone
	demuxDone := d.demuxDone
	d.q = nil // prevents ReadPackets from using a stale reference
	d.mu.Unlock()

	// Unblock pumpChunks if it is blocked on: (a) chunks send or (b) d.r.Read.
	// done must be closed before d.r.Close so the select in pumpChunks fires first.
	close(done)
	_ = d.r.Close()

	<-readLoopDone // wait for pumpChunks (it closes chunks on exit)
	<-demuxDone    // wait for runDemux   (it closes q on exit)
	return nil
}
