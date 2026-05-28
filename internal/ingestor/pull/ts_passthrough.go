package pull

// ts_passthrough.go — adapter that surfaces a TSChunkReader as a
// PacketReader without going through the gompeg2 demuxer.
//
// Purpose: for ingest sources that already deliver MPEG-TS bytes (UDP, HLS,
// SRT, File), the previous design forced every chunk through
// `gompeg2.TSDemuxer` → `domain.AVPacket` → buffer hub → `tsmux.FromAV`
// → segment file. That demux/remux cycle:
//
//   - drops PCR continuity (only PES PTS survives),
//   - re-assigns PIDs (output PMT differs from source),
//   - splits one PES across multiple AVPackets (timing reassembled by muxer
//     guesswork), and
//   - on long-running streams pushes 33-bit PTS values through arithmetic
//     that the downstream muxer mis-handles, producing 25-second segments
//     whose ffprobe-reported duration is *hours*.
//
// Net effect: the player either rejects the segment outright (no playback)
// or stutters badly. production media servers avoid
// this entirely by treating raw TS as opaque bytes — only re-demuxing when
// a transcode is actually required.
//
// The adapter packs each chunk into an AVPacket with `Codec ==
// AVCodecRawTSChunk` and `Data == raw bytes`. The ingestor worker
// recognises that marker codec and writes `buffer.Packet{TS: data}` instead
// of `buffer.Packet{AV: ...}`. Consumers that prefer raw TS (HLS / DASH
// segmenters, transcoder ffmpeg-stdin pump) forward bytes verbatim.
//
// Stats observability: per-stream codec / bitrate / resolution stats are
// not surfaced for raw-TS sources by this adapter — `inputTrackStats` only
// updates on AVCodec{H264,H265,AAC} packets. A side-channel demuxer can be
// added later if those panels need to show real values; for now correctness
// of the data path takes priority over the UI metric.

import (
	"context"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// TSPassthroughPacketReader adapts a TSChunkReader to the PacketReader
// interface by emitting one `AVPacket{Codec: AVCodecRawTSChunk}` per chunk.
type TSPassthroughPacketReader struct {
	r TSChunkReader
}

// NewTSPassthroughPacketReader wraps r so each TS chunk surfaces as a
// raw-TS-marker AVPacket — see file header for why this beats demux/remux.
func NewTSPassthroughPacketReader(r TSChunkReader) *TSPassthroughPacketReader {
	return &TSPassthroughPacketReader{r: r}
}

// Open delegates to the underlying chunk reader.
func (p *TSPassthroughPacketReader) Open(ctx context.Context) error {
	return p.r.Open(ctx)
}

// ReadPackets reads ONE chunk per call and surfaces it as a single
// raw-TS-marker AVPacket. Returning a one-element batch matches the existing
// PacketReader contract; the buffer-hub fan-out is the same regardless of
// batch size.
func (p *TSPassthroughPacketReader) ReadPackets(ctx context.Context) ([]domain.AVPacket, error) {
	chunk, err := p.r.Read(ctx)
	if err != nil {
		return nil, err
	}
	if len(chunk) == 0 {
		return nil, nil
	}
	return []domain.AVPacket{{
		Codec: domain.AVCodecRawTSChunk,
		Data:  chunk,
	}}, nil
}

// Close delegates to the underlying chunk reader.
func (p *TSPassthroughPacketReader) Close() error {
	return p.r.Close()
}
