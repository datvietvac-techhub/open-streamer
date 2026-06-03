package pull

// stats_demuxer.go — side-channel codec/bitrate scanner for raw-TS sources.
//
// Why a side channel: the data path for raw-TS sources (UDP / HLS / SRT /
// File) intentionally bypasses demux/remux to preserve PCR / PID / PTS
// continuity end-to-end. That preserves correctness but leaves the stats
// channel — which is fed by `cb.onMedia(streamID, priority, *AVPacket)` —
// without any AVPackets to observe. As a result the UI shows "No tracks
// reported" for every UDP / HTTP-MPEG-TS / SRT input even when the data
// path is healthy.
//
// StatsDemuxer fixes this without disturbing the data path: it owns its
// own goroutine and a PMT-based scanner, fed via a buffered channel of
// chunk copies. If the scanner can't keep up (constrained CPU, GC pause)
// chunks are dropped silently — stats are best-effort.
//
// Implementation note: this file is now a thin wrapper around
// statsPMTScanner (stats_pmt_scanner.go). The earlier gompeg2-based
// implementation only recognised H.264 / H.265 / AAC / MP2 / MP3 — DVB
// AC-3, E-AC-3, MPEG-2 Video and AV1 silently disappeared into the panel-
// is-empty bucket. The PMT scanner sees PID-and-codec mappings directly
// from PSI tables so any codec the broadcaster signals via PMT (or DVB
// descriptors) is now recognised, regardless of whether gompeg2 has a
// frame extractor for it.
//
// Lifecycle:
//
//	NewStatsDemuxer(onAV) → goroutine running the PMT scanner
//	Feed(chunk)           → non-blocking enqueue (drops on full)
//	Close()               → stops the goroutine, drains remaining chunks

import (
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// statsChunkBuffer is the chunks-channel depth between Feed() and the
// scanner goroutine. 32 chunks × ~1316 bytes ≈ 42 KiB ≈ ~28 ms at 12 Mbps.
// A backlog larger than that means the scanner is genuinely behind; further
// chunks should be dropped rather than queued indefinitely. Shared with the
// underlying PMT scanner — see stats_pmt_scanner.go.
const statsChunkBuffer = 32

// StatsDemuxer surfaces per-codec stats (codec, bitrate) for raw-TS sources
// by parsing PSI tables off the data path. Construct via NewStatsDemuxer;
// feed via Feed; tear down via Close. Safe to call Feed and Close
// concurrently from different goroutines.
type StatsDemuxer struct {
	scanner *statsPMTScanner
}

// NewStatsDemuxer starts a goroutine that scans chunks fed via Feed and
// invokes onAV for every recognised TS packet. Returns nil for nil callback
// so callers don't need to guard the construction site.
func NewStatsDemuxer(onAV func(p *domain.AVPacket)) *StatsDemuxer {
	scanner := newStatsPMTScanner(onAV)
	if scanner == nil {
		return nil
	}
	return &StatsDemuxer{scanner: scanner}
}

// Feed enqueues chunk for asynchronous PMT scanning. Non-blocking: if the
// channel is full the chunk is dropped (stats are best-effort). Empty
// chunks and post-Close calls are silently ignored. The slice is copied
// internally so the caller is free to reuse the underlying buffer.
func (sd *StatsDemuxer) Feed(chunk []byte) {
	if sd == nil {
		return
	}
	sd.scanner.Feed(chunk)
}

// Close stops the scanner goroutine and waits for it to exit. Safe to
// call multiple times and from any goroutine.
func (sd *StatsDemuxer) Close() {
	if sd == nil {
		return
	}
	sd.scanner.Close()
}
