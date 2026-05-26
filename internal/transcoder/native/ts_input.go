package native

import (
	"context"
	"io"

	gocodec "github.com/yapingcat/gomedia/go-codec"

	"github.com/ntt0601zcoder/open-streamer/internal/tsdemux"
)

// tsInput adapts raw MPEG-TS chunks (the format the buffer hub stores
// for UDP / HLS-pull / HTTP-TS sources via tsnorm) into elementary-
// stream H.264 / H.265 NALUs that an AVCodecContext can decode, plus
// AAC frames the audio passthrough path emits as-is.
//
// Two output queues keep video and audio independent: the video queue
// drives the encoder pipeline (decoder → scaler → encoder, slow),
// while the audio queue passes bytes straight to the supervisor's
// AV-path write (fast, no codec work). One blocking queue would
// couple their pacing — a stalled video encoder would back up audio.
//
// Why this lives in the subprocess: the supervisor only knows
// buffer.Packet shapes, not codec semantics. libavformat-style demux
// belongs next to the codec it feeds — keeping main process cgo-free.
type tsInput struct {
	chunks      chan []byte
	frames      chan esFrame // H.264 / H.265 video access units
	audio       chan esFrame // AAC frames (passthrough)
	done        chan struct{}
	demuxDoneCh chan struct{}
}

// esFrame is one demuxed elementary-stream access unit ready for the
// pipeline. PTS/DTS are in milliseconds (tsdemux already converts from
// the 90 kHz wire clock at the boundary). keyframe is true when the
// frame is an IDR access unit — the pipeline uses this to skip
// pre-IDR frames that lack a referenced SPS/PPS (subscribing
// mid-GOP). Audio frames always have keyframe == false; AAC has no
// IDR concept.
//
// codec lets downstream callers route H.264 vs H.265 vs AAC without
// re-inspecting bytes.
type esFrame struct {
	data     []byte
	pts      int64
	dts      int64
	keyframe bool
	codec    esFrameCodec
}

// esFrameCodec is the local-to-native identifier for an ES frame's
// codec. Mapped to the proto Codec enum at the gRPC boundary.
type esFrameCodec uint8

const (
	esCodecUnknown esFrameCodec = iota
	esCodecH264
	esCodecH265
	esCodecAAC
)

const (
	// tsChunksBufferSize bounds the chunks queue between Feed callers
	// and the demuxer goroutine. 64 chunks * ~64 KiB typical = ~4 MiB,
	// roughly 500 ms of 60 Mbps headroom — enough to absorb the bursts
	// HLS-pull produces when a segment lands without blocking gRPC.
	tsChunksBufferSize = 64

	// tsFramesBufferSize bounds the ES queue between demuxer and
	// pipeline. Sized for ~10 s of 25 fps video so a brief encoder
	// stall doesn't backpressure the demuxer's PSI assembly state.
	tsFramesBufferSize = 256
)

// newTSInput wires the demux pipeline and starts the demuxer
// goroutine. Caller MUST eventually call Close to stop the goroutine
// and release the chunk queue.
func newTSInput(ctx context.Context) *tsInput {
	t := &tsInput{
		chunks:      make(chan []byte, tsChunksBufferSize),
		frames:      make(chan esFrame, tsFramesBufferSize),
		audio:       make(chan esFrame, tsFramesBufferSize),
		done:        make(chan struct{}),
		demuxDoneCh: make(chan struct{}),
	}
	go t.runDemux(ctx)
	return t
}

// Feed enqueues one TS chunk for demuxing. Blocks when the chunk queue
// is full — that backpressure is intentional: it pushes ingest
// flow-control all the way back to the supervisor's gRPC pump, so the
// system slows the producer rather than dropping data.
//
// Returns io.ErrClosedPipe after Close so the caller (the pipeline)
// surfaces a hard error to its own gRPC layer and the supervisor can
// respawn cleanly instead of silently dropping packets.
func (t *tsInput) Feed(chunk []byte) error {
	// Two-stage select: Go's select picks uniformly among ready
	// cases, so a single select with `<-done` + `chunks <- chunk`
	// would still send to the chunks queue half the time after Close
	// closed done — the chunk would then sit unread forever because
	// runDemux has exited. The non-blocking pre-check forces the
	// post-Close path to always return ErrClosedPipe.
	select {
	case <-t.done:
		return io.ErrClosedPipe
	default:
	}
	// demuxDoneCh guards against a permanent block: if the demuxer
	// goroutine exited on its own (astits returned EOF / unrecoverable
	// parse error) the chunks queue has no consumer, so a plain
	// `chunks <- chunk` would fill the 64-slot buffer and then block
	// the caller — and the pipeline's dispatch loop — forever. Failing
	// fast here lets the pipeline tear this tsInput down and rebuild a
	// fresh one on the next packet. This was the root cause of the
	// hard stall under rapid input switching.
	select {
	case <-t.done:
		return io.ErrClosedPipe
	case <-t.demuxDoneCh:
		return io.ErrClosedPipe
	case t.chunks <- chunk:
		return nil
	}
}

// DrainReady pops every video ES frame currently buffered without
// blocking. Called by the pipeline after each Feed; missed frames
// from the just-fed chunk land in the queue and get drained on the
// next call, which is fine because live streams have a continuous
// packet flow.
func (t *tsInput) DrainReady() []esFrame {
	var out []esFrame
	for {
		select {
		case f := <-t.frames:
			out = append(out, f)
		default:
			return out
		}
	}
}

// DrainReadyAudio is the audio counterpart of DrainReady, draining the
// AAC passthrough queue. Kept separate so a stalled video encoder
// can't backpressure audio (and vice versa).
func (t *tsInput) DrainReadyAudio() []esFrame {
	var out []esFrame
	for {
		select {
		case f := <-t.audio:
			out = append(out, f)
		default:
			return out
		}
	}
}

// Close stops the demuxer goroutine and releases queues. Idempotent.
//
// We do NOT close the chunks channel: a concurrent Feed selecting on
// both `chunks <- chunk` (would panic on a closed channel) and
// `<-done` could pick either case, so the safe shutdown path is to
// signal via done only and let runDemux's chanReader observe it. The
// demuxer goroutine drops residual chunks on exit; the GC frees them.
func (t *tsInput) Close() {
	select {
	case <-t.done:
		return
	default:
	}
	close(t.done)
	<-t.demuxDoneCh
}

// runDemux reads TS bytes from the chunks channel via chanReader,
// dispatching every assembled PES through tsdemux.OnFrame into the
// frames / audio queues.
//
// The demuxer is RESTARTED on any astits error or early return rather
// than letting the goroutine exit. astits stops on the first bad TS
// packet — common when a fresh source's first bytes land mid-PES or
// before the PAT/PMT (exactly what happens on every input switch).
// If the goroutine exited there, the chunks channel would lose its
// consumer, Feed would fill the buffer and block the whole pipeline
// (the hard stall under rapid switching). A fresh astits demuxer
// resumes from wherever chanReader left off, scanning forward for the
// next 0x47 sync byte. Only a closed `done` (Close) or a drained
// reader (io.EOF with done observed) ends the loop.
//
// Audio frames take the passthrough queue; H.264 / H.265 take the
// video queue. Other stream types are dropped.
func (t *tsInput) runDemux(ctx context.Context) {
	defer close(t.demuxDoneCh)

	cr := &tsChanReader{ch: t.chunks, done: t.done}
	onFrame := func(cid tsdemux.StreamType, frame []byte, pts, dts uint64) {
		if len(frame) == 0 {
			return
		}
		var queue chan esFrame
		f := esFrame{
			data: append([]byte(nil), frame...),
			pts:  int64(pts), //nolint:gosec
			dts:  int64(dts), //nolint:gosec
		}
		switch cid {
		case tsdemux.StreamTypeH264:
			f.codec = esCodecH264
			f.keyframe = gocodec.IsH264IDRFrame(frame)
			queue = t.frames
		case tsdemux.StreamTypeH265:
			f.codec = esCodecH265
			f.keyframe = gocodec.IsH265IDRFrame(frame)
			queue = t.frames
		case tsdemux.StreamTypeAAC:
			f.codec = esCodecAAC
			queue = t.audio
		default:
			return
		}
		select {
		case queue <- f:
		case <-t.done:
		}
	}

	for {
		select {
		case <-t.done:
			return
		default:
		}
		dmx := tsdemux.New()
		dmx.OnFrame = onFrame
		// Input returns on: done-driven io.EOF (clean stop), or an
		// astits parse error (transient — restart). The done check at
		// the top of the loop is the only exit; everything else loops
		// to a fresh demuxer so the chunks consumer never dies while
		// the input is live.
		_ = dmx.Input(ctx, cr)
	}
}

// tsChanReader is an io.Reader that drains a channel of byte slices,
// mirroring the chanReader pattern in tsdemux_packet_reader.go so the
// astits Demuxer can pull chunks at its own pace without per-chunk
// copies into an io.Pipe.
//
// done lets Close signal the demuxer to exit even when the chunks
// channel still has buffered data — without it, dmx.Input would block
// in Read until the chunks channel closed, and we deliberately don't
// close that channel (concurrent Feed senders would panic).
type tsChanReader struct {
	ch   <-chan []byte
	done <-chan struct{}
	rem  []byte
}

func (r *tsChanReader) Read(p []byte) (int, error) {
	for len(r.rem) == 0 {
		select {
		case <-r.done:
			return 0, io.EOF
		case chunk, ok := <-r.ch:
			if !ok {
				return 0, io.EOF
			}
			r.rem = chunk
		}
	}
	n := copy(p, r.rem)
	r.rem = r.rem[n:]
	return n, nil
}

// looksLikeTS reports whether data is the start of a raw MPEG-TS
// chunk. Used for one-shot format detection on the first packet a
// StreamPipeline sees — Annex-B Elementary Stream starts with a
// 0x00 0x00 0x00 0x01 (or 0x00 0x00 0x01) start code, never with the
// 0x47 TS sync byte, so a single-byte probe is sufficient.
//
// We also peek at offset 188 when the chunk is large enough: TS sync
// bytes must repeat every 188 bytes. A coincidental 0x47 at offset 0
// followed by anything else at 188 is almost certainly NOT TS.
func looksLikeTS(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	if data[0] != 0x47 {
		return false
	}
	if len(data) > 188 && data[188] != 0x47 {
		return false
	}
	return true
}
