package blob

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Eyevinn/mp4ff/mp4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeArchive drives the writer for one profile and persists a catalog, the
// way the Service does, so NewReader has a real archive to read.
func writeArchive(t *testing.T, dir string) (originWallMs int64) {
	t.Helper()
	var hours []HourRecord
	sink := func(_ string, hr HourRecord) { hours = append(hours, hr) }
	w := newProfileWriter(dir, "p0", true, time.Second, sink)
	start := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	feed(t, w, 150, 25, 40, start) // 6 s
	require.NoError(t, w.Close())

	_, originWallMs, set := w.Origin()
	require.True(t, set)
	require.NotEmpty(t, hours)

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

func TestBlobReader_QueryRenderAndSlice(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	originWallMs := writeArchive(t, dir)

	br, err := NewReader(dir)
	require.NoError(t, err)

	win, err := br.Query(t.Context(), "p0", time.UnixMilli(originWallMs).UTC(), 0)
	require.NoError(t, err)
	require.NotEmpty(t, win.Video, "window must hold video fragments")
	require.NotEmpty(t, win.Audio, "window must pair audio fragments")
	assert.True(t, win.Video[0].Keyframe, "window must open on a keyframe")
	assert.EqualValues(t, 90000, win.VideoTimescale)
	assert.False(t, win.PDT0.IsZero())

	// Served fragment bytes decode as a CMAF fragment (styp first).
	fb, err := br.ReadFragment(win.Video[0])
	require.NoError(t, err)
	box, err := mp4.DecodeHeader(bytes.NewReader(fb))
	require.NoError(t, err)
	assert.Equal(t, "styp", box.Name)

	// URL-addressed fetch resolves the same bytes.
	fb2, err := br.FragmentByHour("p0", win.Video[0].HourCompact, TrackVideo, win.Video[0].SeqInHour)
	require.NoError(t, err)
	assert.Equal(t, fb, fb2)

	// Init decodes as ftyp+moov.
	init, err := br.ReadInit("p0", TrackVideo)
	require.NoError(t, err)
	ib, err := mp4.DecodeHeader(bytes.NewReader(init))
	require.NoError(t, err)
	assert.Equal(t, "ftyp", ib.Name)

	// Media playlist is a well-formed v7 fMP4 VOD playlist.
	pl := RenderMediaPlaylist(win, TrackVideo)
	assert.Contains(t, pl, "#EXT-X-VERSION:7")
	assert.Contains(t, pl, `#EXT-X-MAP:URI="dvri-p0-0.mp4"`)
	assert.Contains(t, pl, "#EXT-X-PROGRAM-DATE-TIME:")
	assert.Contains(t, pl, "#EXTINF:")
	assert.Contains(t, pl, "dvrf-p0-")
	assert.Contains(t, pl, "#EXT-X-ENDLIST")

	// The fragment URI round-trips through the name parser back to a fetch.
	prof, hourC, trk, seq, ok := ParseFragmentName(firstFragName(pl))
	require.True(t, ok)
	assert.Equal(t, "p0", prof)
	fb3, err := br.FragmentByHour(prof, hourC, trk, seq)
	require.NoError(t, err)
	assert.Equal(t, fb, fb3)

	// Master lists the variant + the audio group.
	master := RenderMaster(br.Catalog(), "from=1749000000&dur=30")
	assert.Contains(t, master, "#EXT-X-STREAM-INF")
	assert.Contains(t, master, "&profile=p0&track=v")
	assert.Contains(t, master, "TYPE=AUDIO")
}

func TestBlobReader_NoArchive(t *testing.T) {
	t.Parallel()
	_, err := NewReader(t.TempDir())
	assert.Error(t, err)
}

// TestResolveWindowBounds locks the A-2 clamp: an absent/oversized dur is
// capped to maxMs (so Query can't admit the whole archive), and a start below
// the earliest hour is pulled up to it (so from=0 / ago=<huge> can't anchor at
// the epoch).
func TestResolveWindowBounds(t *testing.T) {
	t.Parallel()
	const earliest, maxMs = 1_000_000, 60_000
	tcs := []struct {
		name           string
		hasEarliest    bool
		fromMs, durMs  int64
		wantLo, wantHi int64
	}{
		{"open_ended_dur_clamped_to_max", true, 2_000_000, 0, 2_000_000, 2_000_000 + maxMs},
		{"negative_dur_clamped_to_max", true, 2_000_000, -5, 2_000_000, 2_000_000 + maxMs},
		{"oversized_dur_clamped_to_max", true, 2_000_000, maxMs * 100, 2_000_000, 2_000_000 + maxMs},
		{"in_range_dur_preserved", true, 2_000_000, 10_000, 2_000_000, 2_010_000},
		{"from_below_earliest_pulled_up", true, 0, 0, earliest, earliest + maxMs},
		{"from_above_earliest_preserved", true, 5_000_000, 0, 5_000_000, 5_000_000 + maxMs},
		{"no_earliest_leaves_from", false, 0, 0, 0, maxMs},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lo, hi := resolveWindowBounds(earliest, tc.hasEarliest, tc.fromMs, tc.durMs, maxMs)
			assert.Equal(t, tc.wantLo, lo, "lo")
			assert.Equal(t, tc.wantHi, hi, "hi")
		})
	}
}

// TestBlobReader_QueryClampsAndCancels exercises the clamp + ctx threading on a
// real archive: a from=epoch (effectively from=0) query still returns the
// archive's fragments because the start is pulled up to the earliest hour
// (pre-fix it would scan from the epoch), and a cancelled context aborts the
// scan instead of reading every hour.
func TestBlobReader_QueryClampsAndCancels(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeArchive(t, dir)
	br, err := NewReader(dir)
	require.NoError(t, err)

	// from=epoch, open-ended dur: start clamps up to the earliest hour, so the
	// 6 s archive is returned rather than an epoch-anchored empty/giant scan.
	win, err := br.Query(t.Context(), "p0", time.UnixMilli(0).UTC(), 0)
	require.NoError(t, err)
	require.NotEmpty(t, win.Video, "from=0 must clamp to earliest hour and return fragments")

	// A cancelled request aborts the hour scan with the context error.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = br.Query(ctx, "p0", time.UnixMilli(0).UTC(), 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestParseNames(t *testing.T) {
	t.Parallel()
	p, h, tr, s, ok := ParseFragmentName("dvrf-p2-2026060514-1-37.m4s")
	require.True(t, ok)
	assert.Equal(t, "p2", p)
	assert.Equal(t, "2026060514", h)
	assert.EqualValues(t, TrackAudio, tr)
	assert.Equal(t, 37, s)

	ip, it, ok := ParseInitName("dvri-p0-0.mp4")
	require.True(t, ok)
	assert.Equal(t, "p0", ip)
	assert.EqualValues(t, TrackVideo, it)

	for _, bad := range []string{"seg_000001.ts", "dvrf-p0-2026.m4s", "dvrf-x-2026060514-0-1.m4s", "dvri-p0.mp4", "dvrf-p0-2026060514-9-1.m4s"} {
		_, _, _, _, ok := ParseFragmentName(bad)
		_, _, iok := ParseInitName(bad)
		assert.False(t, ok && iok, "must reject %q", bad)
	}
	assert.True(t, IsBlobMediaFile("dvrf-p0-2026060514-0-1.m4s"))
	assert.True(t, IsBlobMediaFile("dvri-p0-0.mp4"))
	assert.False(t, IsBlobMediaFile("index.m3u8"))
}

// firstFragName returns the first .m4s URI line in a media playlist.
func firstFragName(playlist string) string {
	for _, ln := range strings.Split(playlist, "\n") {
		if strings.HasSuffix(ln, ".m4s") && !strings.HasPrefix(ln, "#") {
			return ln
		}
	}
	return ""
}
