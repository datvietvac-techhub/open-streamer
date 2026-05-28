package dvr

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// resolveSegDir must accept valid stream codes (including multi-segment
// codes that contain `/`) and reject any path that would escape root.
func TestResolveSegDir_HappyPath(t *testing.T) {
	t.Parallel()
	root := t.TempDir()

	got, err := resolveSegDir(root, "foo")
	require.NoError(t, err)
	rootAbs, _ := filepath.Abs(root)
	assert.Equal(t, filepath.Join(rootAbs, "foo"), got)
}

func TestResolveSegDir_MultiSegmentCode(t *testing.T) {
	t.Parallel()
	root := t.TempDir()

	got, err := resolveSegDir(root, "region/north/live")
	require.NoError(t, err)
	rootAbs, _ := filepath.Abs(root)
	assert.Equal(t, filepath.Join(rootAbs, "region", "north", "live"), got)
	assert.True(t, strings.HasPrefix(got, rootAbs),
		"resolved segment dir must stay under the dvr root")
}

func TestResolveSegDir_RejectsEmptyCode(t *testing.T) {
	t.Parallel()
	_, err := resolveSegDir(t.TempDir(), "")
	require.Error(t, err)
}

// Traversal attempts must never produce a path outside root, even if the
// input bypasses the stream-code validator. filepath.Clean normalises the
// `..` segments away; the explicit containment check is the second line
// of defence and the one CodeQL's path-injection model recognises.
func TestResolveSegDir_TraversalStaysInsideRoot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	rootAbs, _ := filepath.Abs(root)

	cases := []string{
		"../escape",
		"foo/../../etc",
		"/../../etc/passwd",
	}
	for _, code := range cases {
		t.Run(code, func(t *testing.T) {
			got, err := resolveSegDir(root, code)
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(got, rootAbs+string(filepath.Separator)) || got == rootAbs,
				"resolved %q must stay inside %q", got, rootAbs)
		})
	}
}

// ---- loadIndex / saveIndex round-trip ----------------------------------------

func TestLoadIndex_Missing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	idx, err := loadIndex(dir)
	require.NoError(t, err)
	assert.Nil(t, idx)
}

func TestLoadIndex_Invalid(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, indexFileName), []byte("not json"), 0o644))
	_, err := loadIndex(dir)
	require.Error(t, err)
}

func TestSaveAndLoadIndex_RoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UTC().Truncate(time.Millisecond)
	orig := &domain.DVRIndex{
		StreamCode:     "test-stream",
		StartedAt:      now,
		LastSegmentAt:  now.Add(10 * time.Second),
		SegmentCount:   5,
		TotalSizeBytes: 1024 * 1024,
		Gaps: []domain.DVRGap{
			{From: now.Add(2 * time.Second), To: now.Add(4 * time.Second), Duration: 2 * time.Second},
		},
	}
	require.NoError(t, saveIndex(dir, orig))

	got, err := loadIndex(dir)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, orig.StreamCode, got.StreamCode)
	assert.Equal(t, orig.SegmentCount, got.SegmentCount)
	assert.Equal(t, orig.TotalSizeBytes, got.TotalSizeBytes)
	require.Len(t, got.Gaps, 1)
	assert.Equal(t, orig.Gaps[0].Duration, got.Gaps[0].Duration)
}

func TestSaveIndex_Atomic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	idx := &domain.DVRIndex{StreamCode: "atomic-test", SegmentCount: 1}
	require.NoError(t, saveIndex(dir, idx))
	// Temp file must be gone.
	_, err := os.Stat(filepath.Join(dir, indexFileName+".tmp"))
	assert.True(t, os.IsNotExist(err), "tmp file should be gone after atomic write")
}

// ---- parsePlaylist -----------------------------------------------------------

func TestParsePlaylist_Missing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segs, err := parsePlaylist(dir)
	require.NoError(t, err)
	assert.Nil(t, segs)
}

func TestParsePlaylist_Empty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "playlist.m3u8"), []byte("#EXTM3U\n"), 0o644))
	segs, err := parsePlaylist(dir)
	require.NoError(t, err)
	assert.Empty(t, segs)
}

func TestParsePlaylist_SingleSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Create the segment file so Stat can return a size.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000001.ts"), make([]byte, 512), 0o644))

	playlist := "#EXTM3U\n" +
		"#EXT-X-VERSION:3\n" +
		"#EXT-X-TARGETDURATION:5\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2024-01-01T10:00:00.000Z\n" +
		"#EXTINF:4.000,\n" +
		"000001.ts\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "playlist.m3u8"), []byte(playlist), 0o644))

	segs, err := parsePlaylist(dir)
	require.NoError(t, err)
	require.Len(t, segs, 1)
	assert.Equal(t, 1, segs[0].index)
	assert.InDelta(t, 4.0, segs[0].duration.Seconds(), 0.001)
	assert.False(t, segs[0].discontinuity)
	assert.Equal(t, int64(512), segs[0].size)
}

func TestParsePlaylist_MultipleSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for i := range 3 {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%06d.ts", i+1)), make([]byte, 100), 0o644))
	}

	playlist := "#EXTM3U\n" +
		"#EXT-X-VERSION:3\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2024-01-01T10:00:00.000Z\n" +
		"#EXTINF:4.000,\n" +
		"000001.ts\n" +
		"#EXTINF:4.000,\n" +
		"000002.ts\n" +
		"#EXTINF:4.000,\n" +
		"000003.ts\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "playlist.m3u8"), []byte(playlist), 0o644))

	segs, err := parsePlaylist(dir)
	require.NoError(t, err)
	require.Len(t, segs, 3)
	for i, s := range segs {
		assert.Equal(t, i+1, s.index)
		assert.False(t, s.discontinuity)
	}
}

func TestParsePlaylist_Discontinuity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000001.ts"), []byte{}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000002.ts"), []byte{}, 0o644))

	playlist := "#EXTM3U\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2024-01-01T10:00:00.000Z\n" +
		"#EXTINF:4.000,\n" +
		"000001.ts\n" +
		"#EXT-X-DISCONTINUITY\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2024-01-01T10:01:00.000Z\n" +
		"#EXTINF:3.500,\n" +
		"000002.ts\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "playlist.m3u8"), []byte(playlist), 0o644))

	segs, err := parsePlaylist(dir)
	require.NoError(t, err)
	require.Len(t, segs, 2)
	assert.False(t, segs[0].discontinuity)
	assert.True(t, segs[1].discontinuity)
	assert.InDelta(t, 3.5, segs[1].duration.Seconds(), 0.001)
}

func TestParsePlaylist_SkipsNonNumericFilenames(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Only "000002.ts" has a numeric base.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000002.ts"), []byte{}, 0o644))

	playlist := "#EXTM3U\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2024-01-01T10:00:00.000Z\n" +
		"#EXTINF:4.000,\n" +
		"badname.ts\n" +
		"#EXTINF:4.000,\n" +
		"000002.ts\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "playlist.m3u8"), []byte(playlist), 0o644))

	segs, err := parsePlaylist(dir)
	require.NoError(t, err)
	require.Len(t, segs, 1)
	assert.Equal(t, 2, segs[0].index)
}

// Production playlists carry only ONE EXT-X-PROGRAM-DATE-TIME at the start
// (writePlaylist emits PDT once per discontinuity group, not per segment).
// parsePlaylist must therefore advance pendingWall by each segment's
// duration so subsequent segments inherit a correctly accumulating
// wall-clock — without the advance, every segment ends up with the same
// wallTime as the first one and timeshift queries past the first segment's
// end return 404 NO_SEGMENTS_IN_RANGE for any `from` inside the actual
// covered window.
//
// Regression guard for the production bug observed on 2026-05-07: a DVR
// retention=600s window from 06:50:41 to 07:00:42 returned NO_SEGMENTS_IN_RANGE
// for from=06:55:58 because all 88 parsed segments shared wallTime=06:50:41.
func TestParsePlaylist_AdvancesWallTimeAcrossSegmentsWithSinglePDT(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for i := range 4 {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%06d.ts", i+1)), []byte{}, 0o644))
	}

	// Single PDT at the top, four segments — mirrors what writePlaylist
	// emits for an uninterrupted recording.
	playlist := "#EXTM3U\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2024-01-01T10:00:00.000Z\n" +
		"#EXTINF:6.000,\n" +
		"000001.ts\n" +
		"#EXTINF:4.000,\n" +
		"000002.ts\n" +
		"#EXTINF:9.000,\n" +
		"000003.ts\n" +
		"#EXTINF:6.000,\n" +
		"000004.ts\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "playlist.m3u8"), []byte(playlist), 0o644))

	segs, err := parsePlaylist(dir)
	require.NoError(t, err)
	require.Len(t, segs, 4)

	base := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	assert.Equal(t, base, segs[0].wallTime, "first segment anchored at the PDT")
	assert.Equal(t, base.Add(6*time.Second), segs[1].wallTime,
		"second segment starts 6s after the first (PDT + first.duration)")
	assert.Equal(t, base.Add(10*time.Second), segs[2].wallTime,
		"third segment starts 10s after PDT (sum of first two durations)")
	assert.Equal(t, base.Add(19*time.Second), segs[3].wallTime,
		"fourth segment starts 19s after PDT (sum of first three durations)")
}

// After a discontinuity the next PDT resets the wall-clock anchor —
// segments BEFORE the discontinuity must still accumulate from the
// initial PDT, segments AFTER must accumulate from the second PDT,
// and the two groups must NOT bleed into each other.
func TestParsePlaylist_AdvancesWallTimeWithinDiscontinuityGroups(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for i := range 4 {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%06d.ts", i+1)), []byte{}, 0o644))
	}

	// Two segments, then a 1-minute gap (discontinuity + new PDT), then
	// two more segments. Each group has only one PDT.
	playlist := "#EXTM3U\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2024-01-01T10:00:00.000Z\n" +
		"#EXTINF:4.000,\n" +
		"000001.ts\n" +
		"#EXTINF:4.000,\n" +
		"000002.ts\n" +
		"#EXT-X-DISCONTINUITY\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2024-01-01T10:01:00.000Z\n" +
		"#EXTINF:5.000,\n" +
		"000003.ts\n" +
		"#EXTINF:5.000,\n" +
		"000004.ts\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "playlist.m3u8"), []byte(playlist), 0o644))

	segs, err := parsePlaylist(dir)
	require.NoError(t, err)
	require.Len(t, segs, 4)

	t0 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2024, 1, 1, 10, 1, 0, 0, time.UTC)

	assert.Equal(t, t0, segs[0].wallTime)
	assert.Equal(t, t0.Add(4*time.Second), segs[1].wallTime,
		"second segment in first group accumulates from t0")
	assert.Equal(t, t1, segs[2].wallTime,
		"third segment anchored on second PDT, NOT t0+8s")
	assert.True(t, segs[2].discontinuity, "discontinuity flag must propagate")
	assert.Equal(t, t1.Add(5*time.Second), segs[3].wallTime,
		"fourth segment accumulates from second PDT")
}
