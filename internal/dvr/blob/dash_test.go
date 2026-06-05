package blob

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeSpanningArchive drives the writer across a UTC hour boundary so the
// archive holds >= 2 hours for both tracks (exercises the audio hour-finding
// neighbour scan).
func writeSpanningArchive(t *testing.T, dir string) int64 {
	t.Helper()
	var hours []HourRecord
	sink := func(_ string, hr HourRecord) { hours = append(hours, hr) }
	w := newProfileWriter(dir, "p0", true, time.Second, sink)
	start := time.Date(2026, 6, 5, 12, 59, 55, 0, time.UTC)
	feed(t, w, 600, 25, 40, start) // 24 s, crosses 13:00 with content emitted in both hours
	require.NoError(t, w.Close())

	_, originWallMs, set := w.Origin()
	require.True(t, set)
	require.GreaterOrEqual(t, len(hours), 2, "archive must span >= 2 hours")

	cat := &Catalog{
		StreamCode: "s", Format: CatalogFormat, VideoTimescale: 90000,
		RecordingMediaOriginUnixMs: originWallMs, RecordingMediaOriginTicks: 0,
		AudioProfile: "p0", BestProfile: "p0",
		Profiles: []ProfileDesc{{ID: "p0", Width: 1920, Height: 1080, Bandwidth: 6_000_000, Codec: "avc1.4d4028"}},
	}
	for _, hr := range hours {
		upsertHour(&cat.Profiles[0], hr)
		extendAvailable(&cat.Profiles[0], hr)
	}
	require.NoError(t, cat.Save(dir))
	return originWallMs
}

// TestRenderMPD_TimeRoundTrip — the static MPD is well-formed and every <S t>
// it advertises resolves, by exact-tick match, back to the same bytes the
// per-ref reader returns.
func TestRenderMPD_TimeRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	originWallMs := writeArchive(t, dir)
	br, err := NewReader(dir)
	require.NoError(t, err)

	// Query from a mid-recording instant so the window starts past the origin
	// (T0 > 0) and the per-rep presentationTimeOffset is exercised end-to-end.
	win, err := br.Query("p0", time.UnixMilli(originWallMs+1200).UTC(), 0)
	require.NoError(t, err)
	require.NotEmpty(t, win.Video)
	require.NotEmpty(t, win.Audio)
	require.Positive(t, win.Video[0].MediaTicks, "window must start past the origin")

	mpd, err := RenderMPD(br.Catalog(), map[string]*Window{"p0": win}, DefaultSegDur)
	require.NoError(t, err)
	s := string(mpd)

	assert.Contains(t, s, `type="static"`)
	assert.Contains(t, s, `profiles="urn:mpeg:dash:profile:isoff-main:2011"`)
	assert.Contains(t, s, `mediaPresentationDuration="PT`)
	assert.Contains(t, s, `media="dvrt-p0-0-$Time$.m4s"`)
	assert.Contains(t, s, `media="dvrt-p0-1-$Time$.m4s"`)
	assert.Contains(t, s, `initialization="dvri-p0-0.mp4"`)
	assert.Contains(t, s, fmt.Sprintf(`presentationTimeOffset="%d"`, win.Video[0].MediaTicks))

	for _, ref := range win.Video {
		want, err := br.ReadFragment(ref)
		require.NoError(t, err)
		got, err := br.FragmentByTime("p0", TrackVideo, ref.MediaTicks)
		require.NoError(t, err, "video tick %d", ref.MediaTicks)
		assert.Equal(t, want, got)
		assert.Contains(t, s, fmt.Sprintf(`t="%d"`, ref.MediaTicks))
	}
	for _, ref := range win.Audio {
		want, err := br.ReadFragment(ref)
		require.NoError(t, err)
		got, err := br.FragmentByTime("p0", TrackAudio, ref.MediaTicks)
		require.NoError(t, err, "audio tick %d", ref.MediaTicks)
		assert.Equal(t, want, got)
	}
}

// TestFragmentByTime_AcrossHourBoundary — every fragment in a window that
// crosses a UTC-hour boundary resolves by media-time tick, including audio
// fragments whose media-time floor lands in a neighbouring hour.
func TestFragmentByTime_AcrossHourBoundary(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	originWallMs := writeSpanningArchive(t, dir)
	br, err := NewReader(dir)
	require.NoError(t, err)

	win, err := br.Query("p0", time.UnixMilli(originWallMs).UTC(), 0)
	require.NoError(t, err)
	require.NotEmpty(t, win.Audio)

	seen := map[string]bool{}
	for _, f := range win.Video {
		seen[f.HourCompact] = true
	}
	require.GreaterOrEqual(t, len(seen), 2, "window must cross an hour boundary")

	for _, ref := range win.Video {
		got, err := br.FragmentByTime("p0", TrackVideo, ref.MediaTicks)
		require.NoError(t, err, "video tick %d hour %s", ref.MediaTicks, ref.HourCompact)
		want, _ := br.ReadFragment(ref)
		assert.Equal(t, want, got)
	}
	for _, ref := range win.Audio {
		got, err := br.FragmentByTime("p0", TrackAudio, ref.MediaTicks)
		require.NoError(t, err, "audio tick %d hour %s", ref.MediaTicks, ref.HourCompact)
		want, _ := br.ReadFragment(ref)
		assert.Equal(t, want, got)
	}

	// A tick that exists in no record is a clean not-found.
	_, err = br.FragmentByTime("p0", TrackVideo, 1<<62)
	assert.Error(t, err)
}

// TestRenderMPD_MultiProfileSharedTimeline — multiple video profiles land in one
// AdaptationSet (best first) sharing a single presentation origin; the audio
// AdaptationSet points at the audio-source profile.
func TestRenderMPD_MultiProfileSharedTimeline(t *testing.T) {
	t.Parallel()
	cat := &Catalog{
		StreamCode: "s", VideoTimescale: 90000,
		AudioProfile: "p1", BestProfile: "p1",
		Profiles: []ProfileDesc{
			{ID: "p0", Width: 1280, Height: 720, Bandwidth: 3_000_000, Codec: "avc1.4d401f"},
			{ID: "p1", Width: 1920, Height: 1080, Bandwidth: 6_000_000, Codec: "avc1.4d4028"},
		},
	}
	vrefs := []FragmentRef{
		{Track: TrackVideo, MediaTicks: 540_000, DurTicks: 540_000, Keyframe: true},
		{Track: TrackVideo, MediaTicks: 1_080_000, DurTicks: 540_000, Keyframe: true},
	}
	arefs := []FragmentRef{
		{Track: TrackAudio, MediaTicks: 288_000, DurTicks: 288_000},
		{Track: TrackAudio, MediaTicks: 576_000, DurTicks: 288_000},
	}
	windows := map[string]*Window{
		"p0": {Profile: "p0", Video: vrefs, VideoTimescale: 90000},
		"p1": {Profile: "p1", Video: vrefs, Audio: arefs, VideoTimescale: 90000, AudioTimescale: 48000},
	}
	mpd, err := RenderMPD(cat, windows, DefaultSegDur)
	require.NoError(t, err)
	s := string(mpd)

	assert.Equal(t, 1, strings.Count(s, `contentType="video"`), "one video AdaptationSet")
	assert.Equal(t, 1, strings.Count(s, `contentType="audio"`), "one audio AdaptationSet")
	assert.Contains(t, s, `media="dvrt-p0-0-$Time$.m4s"`)
	assert.Contains(t, s, `media="dvrt-p1-0-$Time$.m4s"`)
	assert.Contains(t, s, `media="dvrt-p1-1-$Time$.m4s"`) // audio on the audio-source profile
	// Shared origin: both video reps carry PTO = T0 = 540000.
	assert.Equal(t, 2, strings.Count(s, `presentationTimeOffset="540000"`))
	// Audio PTO = 540000 * 48000 / 90000 = 288000 (same instant, audio ticks).
	assert.Contains(t, s, `presentationTimeOffset="288000"`)
	// Best profile (p1) is emitted before p0.
	assert.Less(t, strings.Index(s, `id="p1"`), strings.Index(s, `id="p0"`))
}

// TestRenderMPD_ProfileCoverageFilter — a profile with no fragments in the
// window is dropped; an entirely-uncovered window is an error.
func TestRenderMPD_ProfileCoverageFilter(t *testing.T) {
	t.Parallel()
	cat := &Catalog{
		VideoTimescale: 90000, AudioProfile: "p0", BestProfile: "p0",
		Profiles: []ProfileDesc{{ID: "p0"}, {ID: "p1"}},
	}
	vrefs := []FragmentRef{{Track: TrackVideo, MediaTicks: 0, DurTicks: 540_000, Keyframe: true}}
	windows := map[string]*Window{
		"p0": {Profile: "p0", Video: vrefs, VideoTimescale: 90000},
		"p1": {Profile: "p1"}, // no coverage in the window
	}
	mpd, err := RenderMPD(cat, windows, DefaultSegDur)
	require.NoError(t, err)
	s := string(mpd)
	assert.Contains(t, s, `media="dvrt-p0-0-$Time$.m4s"`)
	assert.NotContains(t, s, "dvrt-p1-")

	_, err = RenderMPD(cat, map[string]*Window{"p0": {Profile: "p0"}, "p1": {Profile: "p1"}}, DefaultSegDur)
	assert.Error(t, err, "no covering profile must be an error")
}

func TestParseTimeFragmentName(t *testing.T) {
	t.Parallel()
	p, tr, ticks, ok := ParseTimeFragmentName("dvrt-p2-1-1234567.m4s")
	require.True(t, ok)
	assert.Equal(t, "p2", p)
	assert.EqualValues(t, TrackAudio, tr)
	assert.EqualValues(t, 1234567, ticks)

	for _, bad := range []string{
		"dvrt-p0-0.m4s", "dvrt-x-0-1.m4s", "dvrt-p0-9-1.m4s",
		"dvrt-p0-0-abc.m4s", "dvrf-p0-2026060514-0-1.m4s", "dvrt-p0-0-1.mp4",
	} {
		_, _, _, ok := ParseTimeFragmentName(bad)
		assert.False(t, ok, "must reject %q", bad)
	}
	assert.True(t, IsBlobMediaFile("dvrt-p0-0-1.m4s"))
}
