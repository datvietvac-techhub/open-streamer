package blob

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// gibPerHour is the placeholder size each test hour reports (1 GiB).
const gibPerHour = 1 << 30

// mkHour writes placeholder hour files for p0 and appends a catalog entry.
func mkHour(t *testing.T, dir string, cat *Catalog, hour string, wallToMs int64, sealed bool) {
	t.Helper()
	stem := filepath.Join(profileDirPath(dir, "p0"), filepath.FromSlash(hour))
	require.NoError(t, os.MkdirAll(filepath.Dir(stem), 0o755))
	for _, ext := range []string{".cmfv", ".ranges"} {
		require.NoError(t, os.WriteFile(stem+ext, []byte("x"), 0o644))
	}
	cat.Profiles[0].Hours = append(cat.Profiles[0].Hours, HourRecord{
		Hour: hour, WallFromMs: wallToMs - 3_600_000, WallToMs: wallToMs, Sealed: sealed, SizeBytes: gibPerHour,
	})
}

func hourFileExists(dir, hour string) bool {
	stem := filepath.Join(profileDirPath(dir, "p0"), filepath.FromSlash(hour))
	_, err := os.Stat(stem + ".cmfv")
	return err == nil
}

func TestRetention_PrunesByAgeKeepsUnsealed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Date(2026, 6, 5, 15, 0, 0, 0, time.UTC)
	cat := &Catalog{StreamCode: "s", VideoTimescale: 90000, Profiles: []ProfileDesc{{ID: "p0"}}}
	mkHour(t, dir, cat, "2026/06/05/10", time.Date(2026, 6, 5, 10, 0, 0, 0, time.UTC).UnixMilli(), true)
	mkHour(t, dir, cat, "2026/06/05/11", time.Date(2026, 6, 5, 11, 0, 0, 0, time.UTC).UnixMilli(), true)
	mkHour(t, dir, cat, "2026/06/05/12", time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC).UnixMilli(), true)
	mkHour(t, dir, cat, "2026/06/05/14", now.UnixMilli(), false) // in-progress
	require.NoError(t, cat.Save(dir))
	rec := &recording{streamDir: dir, cat: cat}

	// Keep 1 hour of age → the three old sealed hours are pruned.
	rec.pruneOnce(&domain.StreamDVRConfig{RetentionSec: 3600}, now)

	for _, h := range []string{"2026/06/05/10", "2026/06/05/11", "2026/06/05/12"} {
		assert.False(t, hourFileExists(dir, h), "hour %s should be pruned", h)
	}
	assert.True(t, hourFileExists(dir, "2026/06/05/14"), "in-progress hour must survive")
	require.Len(t, cat.Profiles[0].Hours, 1)
	assert.Equal(t, "2026/06/05/14", cat.Profiles[0].Hours[0].Hour)
}

func TestRetention_PrunesBySize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Date(2026, 6, 5, 20, 0, 0, 0, time.UTC)
	cat := &Catalog{StreamCode: "s", VideoTimescale: 90000, Profiles: []ProfileDesc{{ID: "p0"}}}
	mkHour(t, dir, cat, "2026/06/05/10", time.Date(2026, 6, 5, 10, 0, 0, 0, time.UTC).UnixMilli(), true)
	mkHour(t, dir, cat, "2026/06/05/11", time.Date(2026, 6, 5, 11, 0, 0, 0, time.UTC).UnixMilli(), true)
	mkHour(t, dir, cat, "2026/06/05/12", time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC).UnixMilli(), true)
	require.NoError(t, cat.Save(dir))
	rec := &recording{streamDir: dir, cat: cat}

	// 3 GiB on disk, cap 2.5 GB → prune the oldest until under cap (one hour).
	rec.pruneOnce(&domain.StreamDVRConfig{MaxSizeGB: 2.5}, now)

	assert.False(t, hourFileExists(dir, "2026/06/05/10"), "oldest hour pruned for size")
	assert.True(t, hourFileExists(dir, "2026/06/05/11"))
	assert.True(t, hourFileExists(dir, "2026/06/05/12"))
	require.Len(t, cat.Profiles[0].Hours, 2)
}

func TestRetention_KeepForeverWhenUnset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cat := &Catalog{StreamCode: "s", VideoTimescale: 90000, Profiles: []ProfileDesc{{ID: "p0"}}}
	mkHour(t, dir, cat, "2026/06/05/10", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), true)
	rec := &recording{streamDir: dir, cat: cat}
	rec.pruneOnce(&domain.StreamDVRConfig{}, time.Now()) // no age/size limits
	assert.True(t, hourFileExists(dir, "2026/06/05/10"), "nothing pruned with no limits")
}
