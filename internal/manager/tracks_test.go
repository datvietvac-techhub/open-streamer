package manager

import (
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// TestInputTrackStats_BitrateEWMA verifies that bytes accumulated over the
// trackBitrateWindow flush into a bitrate value, and that subsequent windows
// blend (EWMA) rather than fully replace.
func TestInputTrackStats_BitrateEWMA(t *testing.T) {
	st := newInputTrackStats()
	now := time.Unix(0, 0)

	// 100 KB over 4s ≈ 200 kbps. One observation per ms wouldn't trip the
	// flush — a single observation just past the window does.
	st.observe(&domain.AVPacket{Codec: domain.AVCodecH264, Data: make([]byte, 100_000)}, now)
	st.observe(&domain.AVPacket{Codec: domain.AVCodecH264, Data: make([]byte, 0)}, now.Add(4*time.Second))

	got := st.totalBitrateKbps()
	if got < 150 || got > 250 {
		t.Fatalf("expected ~200 kbps, got %d", got)
	}

	// Second window same volume — EWMA stays in-range.
	base := now.Add(4 * time.Second)
	st.observe(&domain.AVPacket{Codec: domain.AVCodecH264, Data: make([]byte, 100_000)}, base)
	st.observe(&domain.AVPacket{Codec: domain.AVCodecH264, Data: make([]byte, 0)}, base.Add(4*time.Second))
	got = st.totalBitrateKbps()
	if got < 150 || got > 250 {
		t.Fatalf("expected ~200 kbps after second window, got %d", got)
	}
}

// TestInputTrackStats_PerCodecBuckets ensures distinct codecs accumulate into
// their own counters and snapshot returns video-then-audio order.
func TestInputTrackStats_PerCodecBuckets(t *testing.T) {
	st := newInputTrackStats()
	now := time.Unix(0, 0)

	st.observe(&domain.AVPacket{Codec: domain.AVCodecH264, Data: make([]byte, 50_000)}, now)
	st.observe(&domain.AVPacket{Codec: domain.AVCodecAAC, Data: make([]byte, 20_000)}, now)
	// Flush window
	st.observe(&domain.AVPacket{Codec: domain.AVCodecH264, Data: nil}, now.Add(4*time.Second))
	st.observe(&domain.AVPacket{Codec: domain.AVCodecAAC, Data: nil}, now.Add(4*time.Second))

	tracks := st.snapshot()
	if len(tracks) != 2 {
		t.Fatalf("expected 2 tracks, got %d (%+v)", len(tracks), tracks)
	}
	if tracks[0].Kind != domain.MediaTrackVideo || tracks[0].Codec != "h264" {
		t.Fatalf("expected video first, got %+v", tracks[0])
	}
	if tracks[1].Kind != domain.MediaTrackAudio || tracks[1].Codec != "aac" {
		t.Fatalf("expected audio second, got %+v", tracks[1])
	}
}

// TestInputTrackStats_MP2AppearsInSnapshot ensures MP2 audio (DVB radio /
// SD-TV audio) is surfaced in the runtime panel — without the explicit
// AVCodecMP2 entry in snapshot's whitelist, MP2 sources would silently show
// "No tracks reported" even with the data path fully working.
func TestInputTrackStats_MP2AppearsInSnapshot(t *testing.T) {
	st := newInputTrackStats()
	now := time.Unix(0, 0)
	st.observe(&domain.AVPacket{Codec: domain.AVCodecMP2, Data: make([]byte, 30_000)}, now)
	st.observe(&domain.AVPacket{Codec: domain.AVCodecMP2, Data: nil}, now.Add(4*time.Second))

	tracks := st.snapshot()
	if len(tracks) != 1 {
		t.Fatalf("expected 1 track (audio MP2), got %d (%+v)", len(tracks), tracks)
	}
	if tracks[0].Kind != domain.MediaTrackAudio {
		t.Errorf("MP2 must classify as audio, got %s", tracks[0].Kind)
	}
	if tracks[0].Codec != "mp2a" {
		t.Errorf("expected codec label 'mp2a' (operator-dashboard-compatible), got %q", tracks[0].Codec)
	}
	if tracks[0].BitrateKbps == 0 {
		t.Error("MP2 bitrate must be reported, got 0")
	}
}

// TestInputTrackStats_MP3AppearsAsDistinctTrack ensures MP3 (Layer III)
// surfaces in the panel under its own "mp3" label rather than collapsing
// into the generic "mp2a" bucket. Critical for operators running production media servers
// alongside Open-Streamer and expect identical codec naming.
func TestInputTrackStats_MP3AppearsAsDistinctTrack(t *testing.T) {
	st := newInputTrackStats()
	now := time.Unix(0, 0)
	st.observe(&domain.AVPacket{Codec: domain.AVCodecMP3, Data: make([]byte, 30_000)}, now)
	st.observe(&domain.AVPacket{Codec: domain.AVCodecMP3, Data: nil}, now.Add(4*time.Second))

	tracks := st.snapshot()
	if len(tracks) != 1 {
		t.Fatalf("expected 1 track, got %d (%+v)", len(tracks), tracks)
	}
	if tracks[0].Codec != "mp3" {
		t.Errorf("expected codec label 'mp3', got %q", tracks[0].Codec)
	}
	if tracks[0].Kind != domain.MediaTrackAudio {
		t.Errorf("MP3 must classify as audio, got %s", tracks[0].Kind)
	}
}

// TestInputTrackStats_NilAndUnknown verifies hot-path safety on garbage input.
func TestInputTrackStats_NilAndUnknown(t *testing.T) {
	st := newInputTrackStats()
	st.observe(nil, time.Now())
	st.observe(&domain.AVPacket{Codec: domain.AVCodecUnknown}, time.Now())
	if got := st.snapshot(); got != nil {
		t.Fatalf("expected no tracks, got %+v", got)
	}
}

// TestInputTrackStats_Reset verifies switch-time housekeeping clears stats.
func TestInputTrackStats_Reset(t *testing.T) {
	st := newInputTrackStats()
	now := time.Unix(0, 0)
	st.observe(&domain.AVPacket{Codec: domain.AVCodecH264, Data: make([]byte, 50_000)}, now)
	st.observe(&domain.AVPacket{Codec: domain.AVCodecH264, Data: nil}, now.Add(4*time.Second))
	if st.totalBitrateKbps() == 0 {
		t.Fatal("expected non-zero bitrate before reset")
	}
	st.reset()
	if got := st.totalBitrateKbps(); got != 0 {
		t.Fatalf("expected 0 kbps after reset, got %d", got)
	}
	if got := st.snapshot(); got != nil {
		t.Fatalf("expected no tracks after reset, got %+v", got)
	}
}
