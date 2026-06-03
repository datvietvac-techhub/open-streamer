package publisher

import (
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// TestHLSSegmenter_MediaPTSCutCadence locks in the P8.1 fix: the cut
// decision uses the source-stream PTS, so an IDR landing exactly at
// the segDur boundary triggers a flush. Without the media-PTS check
// the encoder's "IDR every 100 frames at 25 fps = 4 s" output was
// silently producing 8 s segments because wallclock fell just short.
func TestHLSSegmenter_MediaPTSCutCadence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := &hlsSegmenter{
		streamID:     "test",
		streamDir:    dir,
		manifestPath: dir + "/index.m3u8",
		segSec:       4,
		window:       5,
	}
	segDur := 4 * time.Second

	// Seed: audio frame at t=10000ms anchors the segment.
	audio := &domain.AVPacket{Codec: domain.AVCodecAAC, PTSms: 10000}
	p.recordMediaTime(audio)

	// First IDR at t=10040ms — only 40 ms into segment, no cut.
	idr1 := &domain.AVPacket{Codec: domain.AVCodecH264, PTSms: 10040, KeyFrame: true}
	p.recordMediaTime(idr1)
	p.segBuf = []byte{0x47, 0x00}
	p.maybeFlushAtKeyframe(idr1, segDur)
	if p.segN != 0 {
		t.Fatalf("first IDR should not cut; segN = %d, want 0", p.segN)
	}

	// Next IDR at exactly t=14040ms — age 4000 ms, must cut.
	idr2 := &domain.AVPacket{Codec: domain.AVCodecH264, PTSms: 14040, KeyFrame: true}
	p.recordMediaTime(idr2)
	p.maybeFlushAtKeyframe(idr2, segDur)
	if p.segN != 1 {
		t.Fatalf("second IDR at exactly segDur should cut; segN = %d, want 1", p.segN)
	}

	// After cut, segment is re-anchored to idr2's PTS. Audio at 14100,
	// then IDR at 18040 (4000 ms after idr2) must cut again.
	p.segBuf = []byte{0x47, 0x00}
	audio2 := &domain.AVPacket{Codec: domain.AVCodecAAC, PTSms: 14100}
	p.recordMediaTime(audio2)
	idr3 := &domain.AVPacket{Codec: domain.AVCodecH264, PTSms: 18040, KeyFrame: true}
	p.recordMediaTime(idr3)
	p.maybeFlushAtKeyframe(idr3, segDur)
	if p.segN != 2 {
		t.Fatalf("third IDR 4s after second must cut; segN = %d, want 2", p.segN)
	}
}

// TestHLSSegmenter_RecordMediaTime_FirstWinsForStart ensures the
// segment start anchor latches to the FIRST AV packet, even when
// that packet is audio. Setting it from any later packet would skew
// EXTINF durations and confuse downstream playlists.
func TestHLSSegmenter_RecordMediaTime_FirstWinsForStart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := &hlsSegmenter{
		streamID:     "test",
		streamDir:    dir,
		manifestPath: dir + "/index.m3u8",
	}

	p.recordMediaTime(&domain.AVPacket{Codec: domain.AVCodecAAC, PTSms: 5000})
	p.recordMediaTime(&domain.AVPacket{Codec: domain.AVCodecH264, PTSms: 5040})
	p.recordMediaTime(&domain.AVPacket{Codec: domain.AVCodecAAC, PTSms: 5080})

	if p.segStartMediaPTS != 5000 {
		t.Fatalf("segStartMediaPTS = %d, want 5000 (first packet wins)", p.segStartMediaPTS)
	}
	if p.lastMediaPTS != 5080 {
		t.Fatalf("lastMediaPTS = %d, want 5080 (latest)", p.lastMediaPTS)
	}
}

// TestHLSSegmenter_MaybeFlush_BeforeFirstAVPacket — defensive: a stray
// keyframe arriving before any AV anchor must NOT cut. Without the
// hasMediaStart guard, segStartMediaPTS=0 would make any PTS look like
// a multi-hour-old segment and trigger an unwanted flush.
func TestHLSSegmenter_MaybeFlush_BeforeFirstAVPacket(t *testing.T) {
	t.Parallel()
	p := &hlsSegmenter{streamID: "test", segBuf: []byte{0x47, 0x00}}
	p.maybeFlushAtKeyframe(
		&domain.AVPacket{Codec: domain.AVCodecH264, PTSms: 999999999, KeyFrame: true},
		4*time.Second,
	)
	if p.segN != 0 {
		t.Fatalf("flush fired without an anchored segment start; segN = %d", p.segN)
	}
}
