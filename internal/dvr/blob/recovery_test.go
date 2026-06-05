package blob

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildHour writes a self-consistent hour (init + n video fragments) and returns
// the stem and the blob size after the last valid fragment.
func buildHour(t *testing.T, dir string, n int) (stem string, validEnd int64) {
	t.Helper()
	stem = filepath.Join(dir, "12")
	init := []byte("INIT-ftyp-moov")
	vb, err := createBlobFile(stem+".cmfv", init)
	require.NoError(t, err)
	rf, err := createRangesFile(stem+".ranges", RangesHeader{VideoInitLen: uint32(len(init)), VideoTimescale: 90000})
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		frag := []byte(fmt.Sprintf("styp-moof-mdat-fragment-%02d", i))
		off, err := vb.Append(frag)
		require.NoError(t, err)
		require.NoError(t, rf.Append(FragRecord{
			Track: TrackVideo, Keyframe: i == 0, SampleCount: 25,
			MediaTicks: uint64(i) * 90000, ByteOffset: uint64(off), ByteLen: uint32(len(frag)), DurTicks: 90000,
		}))
		validEnd = off + int64(len(frag))
	}
	require.NoError(t, rf.WriteHeader())
	require.NoError(t, rf.Sync())
	require.NoError(t, rf.Close())
	require.NoError(t, vb.Sync())
	require.NoError(t, vb.Close())
	return stem, validEnd
}

func TestRepairHour_TruncatesTornTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	stem, validEnd := buildHour(t, dir, 3)

	// Simulate a crash mid-4th-fragment: extra (torn) bytes in the blob, a
	// garbage record in the index, and the ".open" sentinel still present.
	appendAt(t, stem+".cmfv", validEnd, []byte("torn-partial-fragment-bytes"))
	garbage := make([]byte, FragRecordSize)
	for i := range garbage {
		garbage[i] = 0xAB // fails CRC
	}
	appendAt(t, stem+".ranges", int64(RangesHeadSize)+3*FragRecordSize, garbage)
	require.NoError(t, createSentinel(stem))

	require.NoError(t, repairHour(stem))

	// Sentinel removed; hour sealed; only the 3 valid records survive.
	assert.NoFileExists(t, sentinelPath(stem))
	bsize := fileSize(stem + ".cmfv")
	assert.Equal(t, validEnd, bsize, "blob truncated to last durable fragment")
	hdr, recs, err := ReadRanges(stem+".ranges", bsize)
	require.NoError(t, err)
	assert.Equal(t, HeaderFlagSealed, hdr.Flags&HeaderFlagSealed)
	assert.EqualValues(t, 3, hdr.RecordCount)
	require.Len(t, recs, 3)
	assert.True(t, recs[0].Keyframe)
	assert.EqualValues(t, 2*90000, recs[2].MediaTicks)
}

func TestRepairHour_CleanHourUnchanged(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	stem, validEnd := buildHour(t, dir, 4)
	require.NoError(t, createSentinel(stem)) // crashed but everything durable

	require.NoError(t, repairHour(stem))

	assert.NoFileExists(t, sentinelPath(stem))
	assert.Equal(t, validEnd, fileSize(stem+".cmfv"))
	hdr, recs, err := ReadRanges(stem+".ranges", fileSize(stem+".cmfv"))
	require.NoError(t, err)
	assert.EqualValues(t, 4, hdr.RecordCount)
	assert.Len(t, recs, 4)
}

func TestRepairStream_WalksProfiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// p0/2026/06/05/12.* with a dangling sentinel.
	hourDir := filepath.Join(dir, "p0", "2026", "06", "05")
	require.NoError(t, os.MkdirAll(hourDir, 0o755))
	stem, _ := buildHour(t, hourDir, 2)
	require.NoError(t, createSentinel(stem))

	require.NoError(t, RepairStream(dir))
	assert.NoFileExists(t, sentinelPath(stem))
	hdr, _, err := ReadRanges(stem+".ranges", fileSize(stem+".cmfv"))
	require.NoError(t, err)
	assert.Equal(t, HeaderFlagSealed, hdr.Flags&HeaderFlagSealed)

	// A stream dir that never recorded → no error.
	require.NoError(t, RepairStream(filepath.Join(dir, "nope")))
}

// appendAt writes b at off in path (creating/extending as needed).
func appendAt(t *testing.T, path string, off int64, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	_, err = f.WriteAt(b, off)
	require.NoError(t, err)
}
