package buffer

import (
	"testing"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

func TestVideoTrackSlug(t *testing.T) {
	cases := []struct {
		idx  int
		want string
	}{
		{0, "track_1"},
		{1, "track_2"},
		{4, "track_5"},
	}
	for _, c := range cases {
		if got := VideoTrackSlug(c.idx); got != c.want {
			t.Errorf("VideoTrackSlug(%d)=%s want %s", c.idx, got, c.want)
		}
	}
}

func TestRenditionBufferID(t *testing.T) {
	got := RenditionBufferID("live", "track_2")
	if got != "$r$live$track_2" {
		t.Fatalf("unexpected rendition id: %s", got)
	}
}

func TestRenditionsForTranscoderNil(t *testing.T) {
	if got := RenditionsForTranscoder("c", nil); got != nil {
		t.Fatalf("expected nil for nil tc, got %v", got)
	}
}

func TestRenditionsForTranscoderFullCopy(t *testing.T) {
	tc := &domain.TranscoderConfig{
		Video: domain.VideoTranscodeConfig{Copy: true},
		Audio: domain.AudioTranscodeConfig{Copy: true},
	}
	if got := RenditionsForTranscoder("c", tc); got != nil {
		t.Fatalf("expected nil for full-copy, got %v", got)
	}
}

func TestRenditionsForTranscoderVideoCopyOnly(t *testing.T) {
	tc := &domain.TranscoderConfig{
		Video: domain.VideoTranscodeConfig{Copy: true},
		Audio: domain.AudioTranscodeConfig{Copy: false},
	}
	got := RenditionsForTranscoder("c", tc)
	if len(got) != 1 || got[0].Slug != "track_1" || got[0].BitrateKbps != 0 {
		t.Fatalf("unexpected single passthrough: %+v", got)
	}
	if got[0].BufferID != RenditionBufferID("c", "track_1") {
		t.Fatalf("unexpected buffer id: %s", got[0].BufferID)
	}
}

func TestRenditionsForTranscoderLadderProfiles(t *testing.T) {
	tc := &domain.TranscoderConfig{
		Video: domain.VideoTranscodeConfig{
			Profiles: []domain.VideoProfile{
				{Width: 1280, Height: 720, Bitrate: 2500},
				{Width: 854, Height: 480, Bitrate: 0},
			},
		},
	}
	got := RenditionsForTranscoder("live", tc)
	if len(got) != 2 {
		t.Fatalf("expected 2 renditions, got %d", len(got))
	}
	if got[0].Slug != "track_1" || got[0].Width != 1280 || got[0].BitrateKbps != 2500 {
		t.Fatalf("unexpected first rendition: %+v", got[0])
	}
	if got[1].BitrateKbps != 2500 {
		t.Fatalf("default bitrate not applied for zero, got %d", got[1].BitrateKbps)
	}
}

func TestBestRenditionIndex(t *testing.T) {
	if got := BestRenditionIndex(nil); got != 0 {
		t.Fatalf("empty: want 0, got %d", got)
	}
	rends := []RenditionPlayout{
		{Width: 854, Height: 480, BitrateKbps: 1500},
		{Width: 1920, Height: 1080, BitrateKbps: 5000},
		{Width: 1280, Height: 720, BitrateKbps: 3000},
	}
	if got := BestRenditionIndex(rends); got != 1 {
		t.Fatalf("want index 1 (1080p), got %d", got)
	}
}

func TestBestRenditionIndexBitrateTiebreak(t *testing.T) {
	rends := []RenditionPlayout{
		{Width: 1280, Height: 720, BitrateKbps: 2500},
		{Width: 1280, Height: 720, BitrateKbps: 4000},
	}
	if got := BestRenditionIndex(rends); got != 1 {
		t.Fatalf("want higher bitrate at index 1, got %d", got)
	}
}

func TestPlaybackBufferIDNoTranscoder(t *testing.T) {
	if got := PlaybackBufferID("live", nil); got != "live" {
		t.Fatalf("nil tc should return raw code, got %s", got)
	}
}

func TestPlaybackBufferIDPicksBestRendition(t *testing.T) {
	tc := &domain.TranscoderConfig{
		Video: domain.VideoTranscodeConfig{
			Profiles: []domain.VideoProfile{
				{Width: 854, Height: 480, Bitrate: 1500},
				{Width: 1920, Height: 1080, Bitrate: 5000},
			},
		},
	}
	got := PlaybackBufferID("live", tc)
	want := RenditionBufferID("live", "track_2")
	if got != want {
		t.Fatalf("want %s, got %s", want, got)
	}
}

func TestRenditionPlayoutBandwidthBps(t *testing.T) {
	cases := []struct {
		kbps int
		want int
	}{
		{2500, 2_500_000},
		{0, 2_500_000},
		{-10, 2_500_000},
		{500, 500_000},
	}
	for _, c := range cases {
		r := RenditionPlayout{BitrateKbps: c.kbps}
		if got := r.BandwidthBps(); got != c.want {
			t.Errorf("kbps=%d want %d, got %d", c.kbps, c.want, got)
		}
	}
}
