// Package tsnorm wallclock-anchors the PTS / DTS of an upstream MPEG-TS
// chunk by running every PES through timeline.Normaliser and remuxing.
//
// # Why this exists
//
// The AV-path (`internal/ingestor/worker.writeOnePacket`) already routes
// each domain.AVPacket through a per-stream timeline.Normaliser before
// writing to the buffer hub, so consumers (DASH packager, HLS segmenter,
// RTSP / RTMP re-stream) see one consistent wallclock-anchored timeline.
//
// The raw-TS path (`writeRawTSChunk`) historically passed bytes through
// untouched. Sources delivered via the raw-TS path (UDP, HLS-pull, SRT,
// file, copy://, mixer://) carry whatever PTS the upstream encoder
// chose: clock drift, sudden CDN-resync jumps, V/A skew on mixer-style
// composition, occasional 0-second stalls. All of that propagated
// directly into downstream timeline math — root cause of MPD overlap,
// audio under-emit, large segment durs, "DASH live edge ahead of
// publishTime", and player freezes documented in
// docs/DASH_OUTSTANDING_BUGS.md.
//
// # Pipeline
//
//	Process(chunk) ──► chunks chan ──► chunkReader (io.Reader)
//	                                          │
//	                            astits.Demuxer.NextData (goroutine loop)
//	                                          │ PMT → pidStream
//	                                          │ PES → handlePES
//	                                          │       │
//	                            timeline.Normaliser.Apply (drop on Apply=false)
//	                                          │
//	                                astits.Muxer.WriteData
//	                          (RandomAccessIndicator on H.264/H.265 IDR
//	                           forces PAT+PMT to be emitted immediately
//	                           before the IDR PES — fixes the HLS
//	                           "non-existing PPS 0 referenced" decoder
//	                           error window that the original
//	                           gomedia-based path's 400 ms PSI cadence
//	                           left open)
//	                                          │
//	                                ─io.Writer→ outBuf → return
//
// # Synchronization
//
// The demuxer runs on a per-Normaliser goroutine. Process feeds chunks
// via the chunks channel and waits on the ack channel — the ack fires
// when chunkReader has exhausted the chunk's bytes AND the demuxer
// has come back to ask for more, which is the precise moment when
// any output the chunk could produce has landed in outBuf. Process
// then drains outBuf and returns. This pattern lets cross-chunk PES
// assembly state survive in astits.Demuxer's internal buffers (a fresh
// Demuxer per Process call would lose partial PES at chunk boundaries
// — common on file:// 188-byte feeds and UDP 1500-byte MTUs).
//
// # Why astits over gomedia for the demuxer
//
// gomedia/go-mpeg2.TSDemuxer.splitH264Frame / splitH265Frame fires
// OnFrame for every internal access-unit boundary it thinks it sees in
// the PES payload (AUD/SPS/PPS/SEI followed by VCL, or P-slice with
// first_mb_in_slice=0). For multi-slice H.264 sources — typical of
// file:// MP4 encodes — that means one source PES becomes many
// OnFrame callbacks all carrying the same pts/dts. Re-muxing each as
// its own downstream PES produced 9–14× duplicated-DTS PES streams
// that strict HLS players (hls.js) refused to play. astits.Demuxer
// has no such behaviour: it emits one PES per source PES, exactly
// once. Migration to astits closes that bug class at source.
//
// # Session boundary
//
// OnSession tears down the demuxer goroutine, resets the per-track
// timeline anchor, rebuilds the muxer (fresh continuity counters +
// PSI version), and clears the PID → StreamType map. The goroutine
// is lazily restarted on the next Process call.
package tsnorm

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/asticode/go-astits"
	gocodec "github.com/yapingcat/gomedia/go-codec"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/timeline"
)

// Output PID convention. Matches tsmux.FromAV so diagnostic tools see
// the same PIDs across all our TS-emitting paths. H.265 sits on a
// dedicated PID so it can be lazy-added when (and only when) an H.265
// frame actually arrives — avoids a phantom HEVC entry in the PMT for
// H.264-only sources (which strict demuxers like ffprobe surface as a
// "second video stream" and broke the DASH packager's init-segment
// build state in early s3-s4 testing).
const (
	videoPID = 0x100 // H.264
	audioPID = 0x101 // AAC
	h265PID  = 0x102 // H.265 (lazy-added on first H.265 frame)
)

// muxerTablesRetransmitPeriod is the PAT/PMT retransmit cadence in PES
// packets. The keyframe-triggered force-emit (RandomAccessIndicator in
// the adaptation field) is the primary mechanism; this catches the gap
// between IDRs for sources with very long GOPs.
const muxerTablesRetransmitPeriod = 40

// tsPacketSize is the standard ISO/IEC 13818-1 packet size. Pinned via
// DemuxerOptPacketSize so astits skips autoDetectPacketSize (which
// consumes the first 193 bytes from a non-bufio reader and would
// otherwise force us to wrap chunkReader in bufio.Reader — bringing
// its prefetch behaviour into the ack-timing logic).
const tsPacketSize = 188

// Normaliser wallclock-anchors the per-PES PTS / DTS values inside one
// stream's MPEG-TS byte feed. NOT safe for concurrent Process calls —
// the internal mutex serialises them; callers running in a single
// readLoop goroutine pay no contention cost.
type Normaliser struct {
	mu sync.Mutex

	// ctx is the long-lived context threaded into astits.NewMuxer /
	// astits.NewDemuxer so cancellation propagates from the worker's
	// readLoop down into the demux / mux pipeline. Stored on the
	// struct because buildMuxer is called from New AND from OnSession
	// (which rebuilds the muxer mid-stream).
	ctx context.Context

	norm *timeline.Normaliser

	// Mux pipeline state. The astits.Muxer writes to outBuf via the
	// bufWriter; Process drains outBuf after each chunk's ACK.
	muxer  *astits.Muxer
	outBuf bytes.Buffer

	// hasH265 tracks whether the muxer has had the H.265 stream
	// added. Pre-registering H.265 at construction would put a
	// phantom HEVC PID in the PMT for H.264-only sources; lazy-add
	// avoids that.
	hasH265 bool

	// Demuxer pipeline state. The goroutine is lazily started on the
	// first Process call and rebuilt on OnSession.
	started   bool
	chunks    chan []byte
	chunkAck  chan struct{}
	quit      chan struct{}
	demuxDone chan struct{}

	// pidStream maps elementary PID to its StreamType, populated by
	// every PMT astits.Demuxer hands us. PES on un-mapped PIDs are
	// dropped (matches the spec: PES on PIDs not announced in PMT
	// must not be processed).
	pidStream map[uint16]astits.StreamType

	// pendingDisc tracks which output PIDs need
	// adaptation_field.discontinuity_indicator=1 on their NEXT emitted
	// packet. Set on every OnSession (because buildMuxer resets each
	// PID's continuity_counter to 0) and cleared per-PID after the flag
	// has been written once. Per ISO/IEC 13818-1 §2.4.3.4 a strict
	// demuxer (TSDuck tsanalyze, ATSC / DVB compliance suites) flags a
	// CC discontinuity without the indicator as a stream-corruption
	// event; modern players tolerate it but compliance reports fail.
	pendingDisc map[uint16]bool
}

// New constructs a Normaliser with the given timeline config. The
// caller usually passes timeline.DefaultConfig().
//
// ctx is threaded into astits.NewMuxer + astits.NewDemuxer so
// cancellation propagates into the demux pump goroutine; pass the
// worker's readLoop ctx.
//
// astits's RandomAccessIndicator on PCR-PID PES auto-emits PAT + PMT
// immediately before that PES. We set RAI on every H.264 / H.265
// keyframe so each IDR is preceded by fresh PSI — the fix for the
// HLS "non-existing PPS 0 referenced" decode failures the original
// gomedia-based path produced when its fixed 400 ms PAT cadence
// happened to land mid-GOP.
func New(ctx context.Context, cfg timeline.Config) *Normaliser {
	n := &Normaliser{
		ctx:         ctx,
		norm:        timeline.New(cfg),
		pidStream:   make(map[uint16]astits.StreamType),
		pendingDisc: make(map[uint16]bool),
	}
	n.buildMuxer()
	return n
}

// buildMuxer (re)builds the astits.Muxer and pre-registers the
// canonical H.264 + AAC elementary streams. Called from New and after
// OnSession so post-boundary output starts with fresh continuity
// counters + PSI version.
//
// H.264 + AAC are pre-registered (PMT lists them from byte 0). H.265
// is registered lazily on first H.265 frame to avoid a phantom HEVC
// PID confusing strict downstream demuxers on H.264-only sources.
//
// Discontinuity flag: the fresh muxer resets every PID's
// continuity_counter to 0. Per ISO/IEC 13818-1 §2.4.3.4 the next
// packet on each PID with a payload must carry
// adaptation_field.discontinuity_indicator=1 so strict analyzers
// (TSDuck, DVB compliance) don't flag the CC reset as packet loss.
// pendingDisc is seeded for the canonical PIDs here; lazy-registered
// PIDs (H.265) are added when their stream is registered.
func (n *Normaliser) buildMuxer() {
	n.outBuf.Reset()
	n.muxer = astits.NewMuxer(
		n.ctx,
		bufWriter{buf: &n.outBuf},
		astits.MuxerOptTablesRetransmitPeriod(muxerTablesRetransmitPeriod),
	)
	_ = n.muxer.AddElementaryStream(astits.PMTElementaryStream{
		ElementaryPID: videoPID,
		StreamType:    astits.StreamTypeH264Video,
	})
	_ = n.muxer.AddElementaryStream(astits.PMTElementaryStream{
		ElementaryPID: audioPID,
		StreamType:    astits.StreamTypeAACAudio,
	})
	n.muxer.SetPCRPID(videoPID)
	n.hasH265 = false
	if n.pendingDisc == nil {
		n.pendingDisc = make(map[uint16]bool)
	} else {
		for k := range n.pendingDisc {
			delete(n.pendingDisc, k)
		}
	}
	n.pendingDisc[videoPID] = true
	n.pendingDisc[audioPID] = true
}

// bufWriter is the io.Writer astits emits TS bytes into. Process()
// drains it on every call so it never grows unbounded.
type bufWriter struct{ buf *bytes.Buffer }

func (w bufWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

// Process feeds one upstream TS chunk through the demux → normalise →
// mux pipeline and returns the remuxed bytes for THIS chunk.
//
// Output may lag input by less than one PES — astits.Demuxer holds
// PES bytes until the next PUSI=true on the same PID confirms the
// previous PES is complete, and that next-PUSI may arrive in a later
// Process call. For continuous streams the lag stays under one frame
// (≤ 33 ms at 30 fps) and downstream consumers never notice; for
// abrupt shutdowns the worker's readLoop defers Flush() to drain
// whatever PES astits is still holding so the last frame isn't lost.
//
// Empty chunks (len == 0) short-circuit and never touch the demuxer
// goroutine — saves a needless round-trip through the channel pair.
func (n *Normaliser) Process(chunk []byte) ([]byte, error) {
	if len(chunk) == 0 {
		return nil, nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.started {
		n.startDemux()
	}

	n.outBuf.Reset()

	// Hand the chunk to the demuxer goroutine and wait for the ack
	// (= chunkReader has consumed every byte of `chunk` AND the
	// demuxer has come back to ask for more). At that moment any
	// output this chunk could produce is in outBuf.
	select {
	case n.chunks <- chunk:
	case <-n.demuxDone:
		return nil, io.EOF
	}
	select {
	case <-n.chunkAck:
	case <-n.demuxDone:
		// Drain whatever the goroutine did manage to emit before
		// dying — gives the worker a chance to surface a partial
		// segment instead of swallowing it on shutdown.
		if n.outBuf.Len() == 0 {
			return nil, io.EOF
		}
		out := make([]byte, n.outBuf.Len())
		copy(out, n.outBuf.Bytes())
		return out, io.EOF
	}

	if n.outBuf.Len() == 0 {
		return nil, nil
	}
	out := make([]byte, n.outBuf.Len())
	copy(out, n.outBuf.Bytes())
	return out, nil
}

// Flush forces the demuxer to emit whatever PES bytes it's holding —
// astits buffers the in-flight PES until the next PUSI confirms it's
// complete, so the last frame of a stream stays in the buffer until
// either the next frame arrives or someone tells the demuxer EOF.
//
// Implementation: close the chunks channel from the caller side, wait
// for the goroutine to drain via demuxDone, drain outBuf, then
// transparently restart the goroutine so subsequent Process calls
// keep working. Production callers invoke Flush at session boundary
// and at worker exit; tests call it to drain single-frame fixtures.
func (n *Normaliser) Flush() []byte {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.started {
		return nil
	}
	// Closing chunks makes chunkReader return io.EOF, astits then
	// flushes its in-flight PES to OnData and exits. We capture the
	// final output via outBuf.
	n.outBuf.Reset()
	close(n.chunks)
	<-n.demuxDone
	n.started = false

	// Rebuild for the next Process call — pidStream + muxer state
	// must restart cleanly. Note we keep the pidStream MAP between
	// calls (just clear it inside startDemux) so subsequent post-
	// Flush Process calls don't lose the PMT until the source re-
	// emits one.
	out := make([]byte, n.outBuf.Len())
	copy(out, n.outBuf.Bytes())
	if out != nil && len(out) == 0 {
		out = nil
	}
	return out
}

// OnSession resets per-track timeline anchors so the first post-
// boundary packet seeds against a fresh wallclock origin, and rebuilds
// the muxer so output continuity counters + PSI restart cleanly.
//
// nil session is a no-op (matches timeline.Normaliser.OnSession
// semantics).
func (n *Normaliser) OnSession(sess *domain.StreamSession) {
	if sess == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started {
		close(n.chunks)
		<-n.demuxDone
		n.started = false
	}
	n.norm.OnSession(sess)
	n.pidStream = make(map[uint16]astits.StreamType)
	n.buildMuxer()
}

// startDemux launches the demuxer goroutine. Caller holds n.mu.
func (n *Normaliser) startDemux() {
	// Capacity 1 on chunks — Process is the single producer and the
	// ack handshake is one-in-flight, so no larger queue is needed.
	// Capacity 1 on chunkAck — chunkReader sends ACK exactly once
	// per chunk consumed.
	n.chunks = make(chan []byte, 1)
	n.chunkAck = make(chan struct{}, 1)
	n.quit = make(chan struct{})
	n.demuxDone = make(chan struct{})
	n.started = true
	go n.runDemux()
}

// runDemux is the per-Normaliser demuxer goroutine. Reads from the
// chunks channel via chunkReader, feeds astits.Demuxer, dispatches
// PMT / PES to the handlers. Exits cleanly when chunks is closed
// (Flush / OnSession) or quit is closed (hard shutdown).
func (n *Normaliser) runDemux() {
	defer close(n.demuxDone)

	rdr := &chunkReader{
		chunks: n.chunks,
		ack:    n.chunkAck,
		quit:   n.quit,
	}
	dmx := astits.NewDemuxer(
		n.ctx,
		rdr,
		astits.DemuxerOptPacketSize(tsPacketSize),
	)

	for {
		data, err := dmx.NextData()
		if err != nil {
			if errors.Is(err, astits.ErrNoMorePackets) || errors.Is(err, io.EOF) {
				return
			}
			// Parse errors are recoverable in principle (the next PSI
			// re-syncs the demuxer) but astits returns them as fatal.
			// We log and bail — the caller's session-boundary path
			// rebuilds us on the next OnSession.
			slog.Debug("tsnorm: demuxer exit on error", "err", err)
			return
		}
		switch {
		case data.PMT != nil:
			for _, es := range data.PMT.ElementaryStreams {
				n.pidStream[es.ElementaryPID] = es.StreamType
			}
		case data.PES != nil:
			st, ok := n.pidStream[data.PID]
			if !ok {
				continue
			}
			n.handlePES(st, data.PES)
		}
	}
}

// handlePES processes one PES from the demuxer: build AVPacket, run
// through timeline.Normaliser, re-mux via astits.Muxer. Runs on the
// demuxer goroutine; Process is blocked on the ack chan so n.outBuf /
// n.muxer / n.norm have no contention.
//
// The Normaliser may emit 0 packets (dropped or held during joint-seed),
// 1 packet (steady state, returned slice is just [av]), or N packets
// (joint-seed completion — buffered partner-track packets are drained
// alongside the current one). The per-packet `isVideo` recomputes from
// each emitted packet's codec because the slice may contain packets of
// both kinds when joint-seed completes.
func (n *Normaliser) handlePES(st astits.StreamType, pes *astits.PESData) {
	av, _, ok := buildAVPacketFromPES(st, pes)
	if !ok {
		return
	}
	for _, q := range n.norm.Apply(av, time.Now()) {
		n.writeMuxed(q, q.Codec.IsVideo())
	}
}

// writeMuxed routes a normalised AVPacket to the astits muxer's PID
// for its codec and sets RandomAccessIndicator on H.26x keyframes
// (which triggers astits to emit fresh PAT/PMT before this PES — the
// HLS fix this package exists for).
//
// H.265 PID is added lazily here so streams that never carry H.265
// don't get a phantom HEVC entry in their PMT.
func (n *Normaliser) writeMuxed(av *domain.AVPacket, isVideo bool) {
	var pid uint16
	switch av.Codec { //nolint:exhaustive // buildAVPacketFromPES constrains the codec set
	case domain.AVCodecH264:
		pid = videoPID
	case domain.AVCodecH265:
		if !n.hasH265 {
			_ = n.muxer.AddElementaryStream(astits.PMTElementaryStream{
				ElementaryPID: h265PID,
				StreamType:    astits.StreamTypeH265Video,
			})
			// Force PMT to refresh now so downstream demuxers learn the
			// HEVC PID before the first H.265 PES bytes hit the wire.
			_, _ = n.muxer.WriteTables()
			n.hasH265 = true
			// Lazy-added PID also starts at CC=0 → first packet on this
			// PID needs the discontinuity_indicator for strict
			// analyzers (§2.4.3.4).
			n.pendingDisc[h265PID] = true
		}
		pid = h265PID
	case domain.AVCodecAAC:
		pid = audioPID
	default:
		return
	}

	// Per-frame indicator: BothPresent only when DTS truly differs
	// from PTS. The "always BothPresent" approach we briefly tried
	// regressed test_puhser / test5 / test_mixer (sources that emit
	// near-duplicate DTS get tolerated when DTS is implicit but
	// flagged as non-monotonic when DTS is explicit).
	dts := av.DTSms
	if dts == 0 {
		dts = av.PTSms
	}
	indicator := uint8(astits.PTSDTSIndicatorOnlyPTS)
	if dts != av.PTSms {
		indicator = astits.PTSDTSIndicatorBothPresent
	}

	// Pull the discontinuity_indicator flag for THIS PID's next emit.
	// pendingDisc is seeded on buildMuxer / lazy-add and cleared on
	// first emit so the flag fires exactly once per PID per session.
	needDisc := n.pendingDisc[pid]
	if needDisc {
		delete(n.pendingDisc, pid)
	}

	var af *astits.PacketAdaptationField
	if isVideo {
		af = &astits.PacketAdaptationField{
			HasPCR:                 true,
			PCR:                    &astits.ClockReference{Base: int64(dts) * 90}, //nolint:gosec
			RandomAccessIndicator:  av.KeyFrame,
			DiscontinuityIndicator: needDisc,
		}
	} else if needDisc {
		// Audio has no PCR / RAI but still needs the discontinuity
		// flag on its first post-rebuild packet.
		af = &astits.PacketAdaptationField{
			DiscontinuityIndicator: true,
		}
	}

	_, _ = n.muxer.WriteData(&astits.MuxerData{
		PID:             pid,
		AdaptationField: af,
		PES: &astits.PESData{
			Data: av.Data,
			Header: &astits.PESHeader{
				OptionalHeader: &astits.PESOptionalHeader{
					PTS:             &astits.ClockReference{Base: int64(av.PTSms) * 90}, //nolint:gosec
					DTS:             &astits.ClockReference{Base: int64(dts) * 90},      //nolint:gosec
					PTSDTSIndicator: indicator,
				},
			},
		},
	})
}

// chunkReader is the io.Reader astits.Demuxer pulls from. Each
// chunks <- chunk in Process is matched by exactly one ack <-
// struct{}{} from chunkReader once the chunk's bytes are fully
// consumed AND the demuxer has come back to ask for more (which is
// our signal that any output the chunk could produce is in outBuf).
type chunkReader struct {
	chunks  <-chan []byte
	ack     chan<- struct{}
	quit    <-chan struct{}
	rem     []byte
	needAck bool
}

// Read returns up to len(p) bytes from the current chunk; blocks
// until the next chunk arrives if the current one is exhausted.
// Sends the ack JUST BEFORE blocking, so Process unblocks the moment
// the demuxer signals it needs more bytes (= it's done emitting for
// the current chunk).
func (r *chunkReader) Read(p []byte) (int, error) {
	// If we've exhausted a chunk and the demuxer is asking for more,
	// that's the moment to ack the chunk — every byte of payload has
	// been processed and any frames that completed are in outBuf.
	if r.needAck && len(r.rem) == 0 {
		select {
		case r.ack <- struct{}{}:
		case <-r.quit:
			return 0, io.EOF
		}
		r.needAck = false
	}
	for len(r.rem) == 0 {
		select {
		case chunk, ok := <-r.chunks:
			if !ok {
				// Channel closed (Flush / OnSession). Send the pending
				// ack first so a Process caller waiting on the last
				// chunk doesn't deadlock.
				if r.needAck {
					select {
					case r.ack <- struct{}{}:
					case <-r.quit:
					}
					r.needAck = false
				}
				return 0, io.EOF
			}
			r.rem = chunk
			r.needAck = true
		case <-r.quit:
			return 0, io.EOF
		}
	}
	n := copy(p, r.rem)
	r.rem = r.rem[n:]
	return n, nil
}

// buildAVPacketFromPES converts an astits PES record + its PID's
// StreamType into a domain.AVPacket. Returns (_, _, false) for
// codecs Open-Streamer's TS pipelines don't carry.
func buildAVPacketFromPES(st astits.StreamType, pes *astits.PESData) (av *domain.AVPacket, isVideo, ok bool) {
	if pes == nil || len(pes.Data) == 0 {
		return nil, false, false
	}
	var codec domain.AVCodec
	var keyFrame bool
	switch st { //nolint:exhaustive // unsupported codecs intentionally drop — Open-Streamer's TS pipelines only carry H.264 / H.265 / AAC today; add cases when we wire AV1 / VP9 / Opus
	case astits.StreamTypeH264Video:
		codec = domain.AVCodecH264
		keyFrame = gocodec.IsH264IDRFrame(pes.Data)
		isVideo = true
	case astits.StreamTypeH265Video:
		codec = domain.AVCodecH265
		keyFrame = gocodec.IsH265IDRFrame(pes.Data)
		isVideo = true
	case astits.StreamTypeAACAudio:
		codec = domain.AVCodecAAC
	default:
		return nil, false, false
	}

	pts, dts := pesTimestampsMs(pes)
	return &domain.AVPacket{
		Codec:    codec,
		Data:     bytes.Clone(pes.Data),
		PTSms:    pts,
		DTSms:    dts,
		KeyFrame: keyFrame,
	}, isVideo, true
}

// pesTimestampsMs extracts PTS / DTS in milliseconds from astits's
// 90 kHz ClockReference fields. When the PES uses PTS-only encoding
// (PTSDTSIndicator=OnlyPTS — typical for I/P-frame-only streams or
// any frame where decode order equals presentation order) the
// returned dtsMs is set to ptsMs, NOT zero. Reason: downstream
// timeline.Normaliser.Apply computes `cto = inPts - inDts`, and a
// stray dts=0 with pts=100000 makes Apply treat the frame as a
// 100-second composition-time-offset B-frame — which then maps the
// output PTS to anchor+100000 instead of the intended anchor+0.
// Returning (0, 0) only when the PES has no optional header
// (genuinely no timestamp at all).
func pesTimestampsMs(pes *astits.PESData) (ptsMs, dtsMs uint64) {
	if pes == nil || pes.Header == nil || pes.Header.OptionalHeader == nil {
		return 0, 0
	}
	oh := pes.Header.OptionalHeader
	if oh.PTS != nil {
		ptsMs = uint64(oh.PTS.Base) / 90 //nolint:gosec
	}
	if oh.DTS != nil {
		dtsMs = uint64(oh.DTS.Base) / 90 //nolint:gosec
	} else {
		// PTS-only PES: DTS is implicitly equal to PTS by spec.
		dtsMs = ptsMs
	}
	return ptsMs, dtsMs
}
