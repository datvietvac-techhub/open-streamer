package pull

// file.go — pull reader for local media files.
//
// Three container formats are supported without FFmpeg:
//
//	MPEG-TS (.ts .mts .m2ts)  — raw passthrough; bytes emitted as-is at
//	                             read speed (no pacing, no re-mux).
//	MP4     (.mp4 .m4v .mov)  — demux via gomedia/go-mp4 → re-mux to TS;
//	                             real-time paced using packet DTS.
//	FLV     (.flv)             — demux via gomedia/go-flv → re-mux to TS;
//	                             real-time paced using frame DTS.
//
// FileReader operates on a resolved absolute filesystem path; URL parsing and
// VOD-mount resolution belong to internal/vod (called from internal/ingestor
// before constructing this reader).
//
// When loop=true the file is rewound and replayed after EOF, simulating
// a continuous live source.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	gocodec "github.com/yapingcat/gomedia/go-codec"
	goflv "github.com/yapingcat/gomedia/go-flv"
	gomp4 "github.com/yapingcat/gomedia/go-mp4"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/tsmux"
)

const (
	fileTSChunk = 188 * 56  // ~10 KB; aligned to TS packet boundary
	flvReadBuf  = 32 * 1024 // 32 KB read buffer for FLV chunk feeding
)

// fileHandler is the internal strategy selected based on the file extension.
// Each implementation is single-goroutine — the caller must not call read
// and close concurrently.
type fileHandler interface {
	read(ctx context.Context) ([]byte, error)
	close() error
}

// FileReader is a pull source for local media files.
// It implements TSChunkReader and is intended to wrap with NewTSDemuxPacketReader.
type FileReader struct {
	path    string
	loop    bool
	handler fileHandler
}

// NewFileReader constructs a FileReader without opening the file.
// path must be a resolved absolute filesystem path. The caller (typically
// internal/ingestor.NewPacketReader) is responsible for resolving any
// file:// URL through the VOD registry first.
//
// Logs the loop flag at construction so operators can verify the deployed
// binary's vod.Registry resolver hands the expected value (loop=true is the
// default for file:// URLs without an explicit ?loop=false override).
func NewFileReader(path string, loop bool) *FileReader {
	slog.Info("file reader: construct", "path", path, "loop", loop)
	return &FileReader{path: path, loop: loop}
}

// Open opens the file and selects the appropriate handler based on extension.
// Returns an error if the path does not exist or is a directory.
func (r *FileReader) Open(ctx context.Context) error {
	if r.handler != nil {
		return nil // idempotent
	}

	st, err := os.Stat(r.path)
	if err != nil {
		return fmt.Errorf("file reader: stat %q: %w", r.path, err)
	}
	if st.IsDir() {
		return fmt.Errorf("file reader: %q is a directory", r.path)
	}

	switch strings.ToLower(filepath.Ext(r.path)) {
	case ".mp4", ".m4v", ".mov":
		h, err := newMP4Handler(ctx, r.path, r.loop)
		if err != nil {
			return fmt.Errorf("file reader: mp4 init %q: %w", r.path, err)
		}
		r.handler = h

	case ".flv":
		h, err := newFLVHandler(ctx, r.path, r.loop)
		if err != nil {
			return fmt.Errorf("file reader: flv init %q: %w", r.path, err)
		}
		r.handler = h

	default:
		// .ts, .mts, .m2ts or unknown → raw passthrough
		h, err := newTSHandler(r.path, r.loop)
		if err != nil {
			return fmt.Errorf("file reader: open %q: %w", r.path, err)
		}
		r.handler = h
	}

	return nil
}

// Read returns the next raw MPEG-TS chunk.
// Returns io.EOF when the file is exhausted (and loop is not set).
func (r *FileReader) Read(ctx context.Context) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if r.handler == nil {
		return nil, fmt.Errorf("file reader: not opened")
	}
	return r.handler.read(ctx)
}

// Close releases the open file handle.
// Calling Close before Open, or calling it twice, is safe.
func (r *FileReader) Close() error {
	if r.handler == nil {
		return nil
	}
	err := r.handler.close()
	r.handler = nil
	return err
}

// ─── MPEG-TS passthrough ─────────────────────────────────────────────────────

type tsHandler struct {
	path string
	loop bool
	f    *os.File
	buf  []byte
}

func newTSHandler(path string, loop bool) (*tsHandler, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &tsHandler{
		path: path,
		loop: loop,
		f:    f,
		buf:  make([]byte, fileTSChunk),
	}, nil
}

func (h *tsHandler) read(ctx context.Context) ([]byte, error) {
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		n, err := h.f.Read(h.buf)
		if n > 0 {
			out := make([]byte, n)
			copy(out, h.buf[:n])
			return out, nil
		}

		if !errors.Is(err, io.EOF) {
			return nil, err
		}

		if !h.loop {
			return nil, io.EOF
		}
		slog.Info("file reader: ts loop reset", "path", h.path)
		if _, seekErr := h.f.Seek(0, io.SeekStart); seekErr != nil {
			return nil, fmt.Errorf("file reader: ts loop seek: %w", seekErr)
		}
	}
}

func (h *tsHandler) close() error {
	if h.f == nil {
		return nil
	}
	err := h.f.Close()
	h.f = nil
	return err
}

// ─── MP4 → MPEG-TS ──────────────────────────────────────────────────────────

type mp4Handler struct {
	path  string
	loop  bool
	f     *os.File
	demux *gomp4.MovDemuxer
	mux   *tsmux.FromAV
	queue [][]byte

	// Continuous-PTS state. On loop reset, ptsOffset jumps forward by the
	// duration of the just-finished iteration so the new iteration's
	// timestamps continue monotonically from where the previous one ended —
	// downstream (transcoder, HLS segmenter, RTMP play, RTSP) sees one
	// seamless stream instead of a PTS-rewind discontinuity that would
	// stall output for several seconds while consumers re-sync.
	ptsOffset    uint64 // added to every (pts, dts) before muxing (ms)
	maxDtsSeen   uint64 // largest DTS observed in current iteration (raw, pre-offset)
	firstDtsSeen uint64 // first DTS observed in current iteration (raw)
	hasFirstDts  bool

	// real-time pacing: emit at original media rate
	paceOnce bool
	paceRef  uint64    // adjusted DTS of first packet (ms)
	paceAt   time.Time // wall-clock of first packet
}

func newMP4Handler(ctx context.Context, path string, loop bool) (*mp4Handler, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	h := &mp4Handler{path: path, loop: loop, f: f}
	h.mux = tsmux.NewFromAV(ctx)

	if err := h.reset(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return h, nil
}

func (h *mp4Handler) reset() error {
	start := time.Now()
	if _, err := h.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("mp4 seek: %w", err)
	}
	dmx := gomp4.CreateMp4Demuxer(h.f)
	if _, err := dmx.ReadHead(); err != nil {
		return fmt.Errorf("mp4 read head: %w", err)
	}
	h.demux = dmx
	h.queue = h.queue[:0]
	// Advance ptsOffset to keep the output timeline monotonically increasing
	// across the loop boundary. Each new iteration's packet (pts+offset)
	// continues right after the previous iteration's last packet — no
	// discontinuity, no downstream re-sync gap.
	if h.hasFirstDts {
		h.ptsOffset += (h.maxDtsSeen - h.firstDtsSeen) + loopFrameAdvanceMs
	}
	h.maxDtsSeen = 0
	h.firstDtsSeen = 0
	h.hasFirstDts = false
	// Keep the shared *tsmux.FromAV across loops: same PIDs and same
	// continuity counters → downstream consumers don't see a fake
	// stream-shape change at loop boundaries. astits's PSI emission
	// resets automatically the next time a keyframe with
	// RandomAccessIndicator lands, so a player joining mid-loop still
	// gets a fresh PAT/PMT quickly.
	h.paceOnce = false
	slog.Info("file reader: mp4 loop reset",
		"path", h.path,
		"took_ms", time.Since(start).Milliseconds(),
		"new_pts_offset_ms", h.ptsOffset,
	)
	return nil
}

// loopFrameAdvanceMs is the synthetic gap inserted between the last frame
// of one loop iteration and the first frame of the next. Picked to be
// larger than a typical inter-frame interval (~16-40ms at 25-60fps) so
// PTS strictly increases without overlap, but small enough to be invisible
// in playback.
const loopFrameAdvanceMs = 40

func (h *mp4Handler) read(ctx context.Context) ([]byte, error) {
	for {
		if len(h.queue) > 0 {
			out := h.queue[0]
			h.queue = h.queue[1:]
			return out, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err := h.feedNextPacket(ctx); err != nil {
			return nil, err
		}
	}
}

func (h *mp4Handler) feedNextPacket(ctx context.Context) error {
	pkt, err := h.demux.ReadPacket()
	if errors.Is(err, io.EOF) {
		if !h.loop {
			return io.EOF
		}
		return h.reset()
	}
	if err != nil {
		return fmt.Errorf("mp4 read packet: %w", err)
	}

	// Track raw min/max DTS in the current iteration so reset() can advance
	// ptsOffset by the iteration's true duration.
	if !h.hasFirstDts {
		h.firstDtsSeen = pkt.Dts
		h.hasFirstDts = true
	}
	if pkt.Dts > h.maxDtsSeen {
		h.maxDtsSeen = pkt.Dts
	}

	adjPts := pkt.Pts + h.ptsOffset
	adjDts := pkt.Dts + h.ptsOffset

	h.pace(ctx, adjDts)
	h.muxPacket(pkt, adjPts, adjDts)
	return nil
}

func (h *mp4Handler) muxPacket(pkt *gomp4.AVPacket, adjPts, adjDts uint64) {
	av := mp4PacketToAV(pkt, adjPts, adjDts)
	if av == nil {
		return
	}
	h.mux.Write(av, h.enqueueTS)
}

// enqueueTS is the OnPacket callback for tsmux.FromAV — each emitted
// 188-byte TS packet is appended to the handler's queue (read() drains
// it one packet at a time).
func (h *mp4Handler) enqueueTS(pkt []byte) {
	out := make([]byte, len(pkt))
	copy(out, pkt)
	h.queue = append(h.queue, out)
}

// mp4PacketToAV converts a gomp4 demuxer packet to an AVPacket the
// tsmux layer accepts. Returns nil for codecs we don't re-mux to TS
// (G.711, MP2/MP3, Opus — gomp4 surfaces them but Open-Streamer's
// downstream path doesn't carry them on the AV side).
func mp4PacketToAV(pkt *gomp4.AVPacket, adjPts, adjDts uint64) *domain.AVPacket {
	switch pkt.Cid { //nolint:exhaustive // unsupported codecs intentionally drop — gomp4 emits G.711 / MP2 / MP3 / Opus but we don't carry them in TS pipelines
	case gomp4.MP4_CODEC_H264:
		return &domain.AVPacket{
			Codec:    domain.AVCodecH264,
			Data:     pkt.Data,
			PTSms:    adjPts,
			DTSms:    adjDts,
			KeyFrame: tsmux.KeyFrameH264(pkt.Data),
		}
	case gomp4.MP4_CODEC_H265:
		return &domain.AVPacket{
			Codec:    domain.AVCodecH265,
			Data:     pkt.Data,
			PTSms:    adjPts,
			DTSms:    adjDts,
			KeyFrame: tsmux.KeyFrameH265(pkt.Data),
		}
	case gomp4.MP4_CODEC_AAC:
		return &domain.AVPacket{
			Codec: domain.AVCodecAAC,
			Data:  pkt.Data,
			PTSms: adjPts,
			DTSms: adjDts,
		}
	}
	return nil
}

func (h *mp4Handler) pace(ctx context.Context, dtsMS uint64) {
	if !h.paceOnce {
		h.paceOnce = true
		h.paceRef = dtsMS
		h.paceAt = time.Now()
		return
	}
	target := h.paceAt.Add(time.Duration(dtsMS-h.paceRef) * time.Millisecond)
	if wait := time.Until(target); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	}
}

func (h *mp4Handler) close() error {
	if h.f == nil {
		return nil
	}
	err := h.f.Close()
	h.f = nil
	return err
}

// ─── FLV → MPEG-TS ──────────────────────────────────────────────────────────

type flvHandler struct {
	path   string
	loop   bool
	f      *os.File
	reader *goflv.FlvReader
	mux    *tsmux.FromAV
	queue  [][]byte
	buf    []byte

	// Continuous-PTS state across loops — same rationale as mp4Handler.
	// FLV PTS/DTS are 32-bit milliseconds, so the offset is uint32 too.
	ptsOffset    uint32
	maxDtsSeen   uint32
	firstDtsSeen uint32
	hasFirstDts  bool

	// real-time pacing — ctx stored during read so the OnFrame callback can use it.
	// Safe because read is always called from a single goroutine and OnFrame fires
	// synchronously inside reader.Input (no concurrency).
	paceCtx  context.Context
	paceOnce bool
	paceRef  uint32
	paceAt   time.Time
}

func newFLVHandler(ctx context.Context, path string, loop bool) (*flvHandler, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	h := &flvHandler{
		path: path,
		loop: loop,
		f:    f,
		buf:  make([]byte, flvReadBuf),
	}
	h.mux = tsmux.NewFromAV(ctx)
	h.buildReader() //nolint:contextcheck // per-read ctx is stored in h.paceCtx and consulted inside the OnFrame closure
	return h, nil
}

// enqueueTS is the OnPacket callback for tsmux.FromAV — each emitted
// 188-byte TS packet is appended to the handler's queue (read() drains
// it one packet at a time).
func (h *flvHandler) enqueueTS(pkt []byte) {
	out := make([]byte, len(pkt))
	copy(out, pkt)
	h.queue = append(h.queue, out)
}

// buildReader creates (or recreates) the stateful FlvReader with all callbacks wired.
func (h *flvHandler) buildReader() {
	r := goflv.CreateFlvReader()
	r.OnFrame = func(cid gocodec.CodecID, frame []byte, pts uint32, dts uint32) {
		ctx := h.paceCtx
		if ctx == nil {
			ctx = context.Background()
		}
		// Track raw min/max DTS so reset() can advance ptsOffset by the
		// iteration's true duration (continuous-PTS — see mp4Handler comment).
		if !h.hasFirstDts {
			h.firstDtsSeen = dts
			h.hasFirstDts = true
		}
		if dts > h.maxDtsSeen {
			h.maxDtsSeen = dts
		}
		adjPts := pts + h.ptsOffset
		adjDts := dts + h.ptsOffset
		h.pace(ctx, adjDts)
		h.muxFrame(cid, frame, adjPts, adjDts)
	}
	h.reader = r
}

// muxFrame routes a decoded FLV frame to the MPEG-TS muxer.
// tsmux.FromAV handles per-codec elementary-stream registration and
// PSI emit cadence (force PAT/PMT on H.264/H.265 keyframes).
func (h *flvHandler) muxFrame(cid gocodec.CodecID, frame []byte, pts uint32, dts uint32) {
	av := flvFrameToAV(cid, frame, pts, dts)
	if av == nil {
		return
	}
	h.mux.Write(av, h.enqueueTS)
}

// flvFrameToAV maps an FLV-demuxer codec ID + payload to an AVPacket.
// Returns nil for codecs Open-Streamer's TS-path doesn't carry (VP8,
// G.711, Opus, MP3 — gomedia's FlvReader surfaces them but downstream
// pipelines aren't wired for them today).
func flvFrameToAV(cid gocodec.CodecID, frame []byte, pts, dts uint32) *domain.AVPacket {
	switch cid { //nolint:exhaustive // unsupported codecs intentionally drop — gomedia's FlvReader surfaces VP8 / G.711 / Opus / MP3 but Open-Streamer's TS-path doesn't carry them today
	case gocodec.CODECID_VIDEO_H264:
		return &domain.AVPacket{
			Codec:    domain.AVCodecH264,
			Data:     frame,
			PTSms:    uint64(pts),
			DTSms:    uint64(dts),
			KeyFrame: tsmux.KeyFrameH264(frame),
		}
	case gocodec.CODECID_VIDEO_H265:
		return &domain.AVPacket{
			Codec:    domain.AVCodecH265,
			Data:     frame,
			PTSms:    uint64(pts),
			DTSms:    uint64(dts),
			KeyFrame: tsmux.KeyFrameH265(frame),
		}
	case gocodec.CODECID_AUDIO_AAC:
		return &domain.AVPacket{
			Codec: domain.AVCodecAAC,
			Data:  frame,
			PTSms: uint64(pts),
			DTSms: uint64(dts),
		}
	}
	return nil
}

func (h *flvHandler) reset() error {
	start := time.Now()
	if _, err := h.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("flv loop seek: %w", err)
	}
	h.queue = h.queue[:0]
	if h.hasFirstDts {
		h.ptsOffset += (h.maxDtsSeen - h.firstDtsSeen) + loopFrameAdvanceMs
	}
	h.maxDtsSeen = 0
	h.firstDtsSeen = 0
	h.hasFirstDts = false
	// Keep vpid/apid + vset/aset across loops (same TSMuxer, same PIDs).
	h.paceOnce = false
	h.buildReader()
	slog.Info("file reader: flv loop reset",
		"path", h.path,
		"took_ms", time.Since(start).Milliseconds(),
		"new_pts_offset_ms", h.ptsOffset,
	)
	return nil
}

func (h *flvHandler) read(ctx context.Context) ([]byte, error) {
	h.paceCtx = ctx
	defer func() { h.paceCtx = nil }()

	for {
		if len(h.queue) > 0 {
			out := h.queue[0]
			h.queue = h.queue[1:]
			return out, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if err := h.feedChunk(); err != nil { //nolint:contextcheck // ctx passed via h.paceCtx
			return nil, err
		}
	}
}

// feedChunk reads one buffer of raw FLV bytes and feeds it to the parser.
// Returns io.EOF when the file is exhausted (after looping if enabled).
func (h *flvHandler) feedChunk() error {
	n, err := h.f.Read(h.buf)
	if n > 0 {
		if ferr := h.reader.Input(h.buf[:n]); ferr != nil {
			return fmt.Errorf("flv parse: %w", ferr)
		}
	}
	if err == nil {
		return nil
	}
	if !errors.Is(err, io.EOF) {
		return fmt.Errorf("flv read: %w", err)
	}
	if !h.loop {
		return io.EOF
	}
	return h.reset()
}

func (h *flvHandler) pace(ctx context.Context, dtsMS uint32) {
	if !h.paceOnce {
		h.paceOnce = true
		h.paceRef = dtsMS
		h.paceAt = time.Now()
		return
	}

	elapsed := dtsMS - h.paceRef
	if elapsed == 0 {
		return
	}
	target := h.paceAt.Add(time.Duration(elapsed) * time.Millisecond)
	if wait := time.Until(target); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	}
}

func (h *flvHandler) close() error {
	if h.f == nil {
		return nil
	}
	err := h.f.Close()
	h.f = nil
	return err
}
