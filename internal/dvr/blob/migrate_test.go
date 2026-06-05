package blob

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/tsmux"
)

// writeLegacyTS muxes a 6 s legacy .ts segment (150 frames at 40 ms, IDR every
// 25) plus one audio frame per video frame, starting at startPTSms.
func writeLegacyTS(t *testing.T, path string, startPTSms uint64) {
	t.Helper()
	const (
		nFrames = 150
		gop     = 25
		frameMs = 40
	)
	mux := tsmux.NewFromAV(context.Background())
	var buf []byte
	onTS := func(b []byte) { buf = append(buf, b...) }
	for i := 0; i < nFrames; i++ {
		pts := startPTSms + uint64(i)*frameMs
		v := &domain.AVPacket{Codec: domain.AVCodecH264, Data: annexB(wtSlice), PTSms: pts, DTSms: pts}
		if i%gop == 0 {
			v = &domain.AVPacket{Codec: domain.AVCodecH264, Data: annexB(wtSPS, wtPPS, wtIDR), PTSms: pts, DTSms: pts, KeyFrame: true}
		}
		mux.Write(v, onTS)
		mux.Write(&domain.AVPacket{Codec: domain.AVCodecAAC, Data: wtADTS, PTSms: pts}, onTS)
	}
	require.NoError(t, os.WriteFile(path, buf, 0o644))
}

func TestParseLegacyPlaylist(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	pl := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:7\n#EXT-X-PLAYLIST-TYPE:VOD\n\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2026-06-05T12:00:00.000Z\n#EXTINF:6.000,\ndvr_000000.ts\n" +
		"#EXTINF:6.000,\ndvr_000001.ts\n" +
		"#EXT-X-DISCONTINUITY\n#EXT-X-PROGRAM-DATE-TIME:2026-06-05T12:00:42.000Z\n#EXTINF:6.000,\ndvr_000002.ts\n" +
		"#EXT-X-ENDLIST\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, legacyPlaylist), []byte(pl), 0o644))

	segs, err := parseLegacyPlaylist(dir)
	require.NoError(t, err)
	require.Len(t, segs, 3)

	base := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, "dvr_000000.ts", segs[0].file)
	assert.Equal(t, base, segs[0].wallTime)
	assert.False(t, segs[0].discontinuity)
	// Second segment has no PDT line → wall = prev wall + prev dur.
	assert.Equal(t, base.Add(6*time.Second), segs[1].wallTime)
	assert.False(t, segs[1].discontinuity)
	// Third segment is after a discontinuity with its own PDT anchor (a gap).
	assert.True(t, segs[2].discontinuity)
	assert.Equal(t, base.Add(42*time.Second), segs[2].wallTime)
}

func TestMigrate_RoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	base := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	// Two contiguous 6 s segments (continuous PTS + PDT, no gap).
	writeLegacyTS(t, filepath.Join(dir, "dvr_000000.ts"), 0)
	writeLegacyTS(t, filepath.Join(dir, "dvr_000001.ts"), 6000)
	pl := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-PLAYLIST-TYPE:VOD\n\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2026-06-05T12:00:00.000Z\n#EXTINF:6.000,\ndvr_000000.ts\n" +
		"#EXTINF:6.000,\ndvr_000001.ts\n#EXT-X-ENDLIST\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, legacyPlaylist), []byte(pl), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, legacyIndex), []byte(`{}`), 0o644))

	require.True(t, IsLegacyRecording(dir))

	res, err := Migrate(context.Background(), dir, MigrateOptions{StreamCode: "s", SegDur: time.Second})
	require.NoError(t, err)
	assert.Equal(t, 2, res.Segments)
	assert.GreaterOrEqual(t, res.Hours, 1)
	assert.Positive(t, res.VideoFrags)
	assert.Positive(t, res.AudioFrags)
	assert.Equal(t, 0, res.Gaps)

	// Catalog now governs serving + idempotency.
	require.True(t, HasCatalog(dir))
	require.False(t, IsLegacyRecording(dir))
	_, err = os.Stat(filepath.Join(dir, migratedMarker))
	require.NoError(t, err)

	br, err := NewReader(dir)
	require.NoError(t, err)
	win, err := br.Query("p0", base, 0)
	require.NoError(t, err)
	require.NotEmpty(t, win.Video)
	require.NotEmpty(t, win.Audio)

	// Migrated wall times are monotonic and within the source range.
	var prev int64
	for _, f := range win.Video {
		assert.GreaterOrEqual(t, f.WallTimeMs, prev, "wall time must be monotonic")
		prev = f.WallTimeMs
		assert.GreaterOrEqual(t, f.WallTimeMs, res.SourceFromMs)
		assert.LessOrEqual(t, f.WallTimeMs, res.SourceToMs)
	}

	// Re-running is a no-op (catalog present).
	_, err = Migrate(context.Background(), dir, MigrateOptions{StreamCode: "s"})
	assert.Error(t, err, "second migrate must short-circuit")
}

func TestMigrate_Discontinuity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	base := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	writeLegacyTS(t, filepath.Join(dir, "dvr_000000.ts"), 0)
	// Second run after a gap: PTS resets, PDT jumps 30 s ahead, DISCONTINUITY.
	writeLegacyTS(t, filepath.Join(dir, "dvr_000001.ts"), 0)
	pl := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-PLAYLIST-TYPE:VOD\n\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2026-06-05T12:00:00.000Z\n#EXTINF:6.000,\ndvr_000000.ts\n" +
		"#EXT-X-DISCONTINUITY\n#EXT-X-PROGRAM-DATE-TIME:2026-06-05T12:00:36.000Z\n#EXTINF:6.000,\ndvr_000001.ts\n" +
		"#EXT-X-ENDLIST\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, legacyPlaylist), []byte(pl), 0o644))

	res, err := Migrate(context.Background(), dir, MigrateOptions{StreamCode: "s", SegDur: time.Second, Prune: true})
	require.NoError(t, err)
	assert.Equal(t, 1, res.Gaps, "exactly one gap carried across")

	br, err := NewReader(dir)
	require.NoError(t, err)
	win, err := br.Query("p0", base, 0)
	require.NoError(t, err)

	disc := 0
	for _, f := range win.Video {
		if f.Discontinuity {
			disc++
		}
	}
	assert.Equal(t, 1, disc, "exactly one video fragment is flagged discontinuous")

	// Prune removed the legacy files.
	_, err = os.Stat(filepath.Join(dir, "dvr_000000.ts"))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(dir, legacyPlaylist))
	assert.True(t, os.IsNotExist(err))
}
