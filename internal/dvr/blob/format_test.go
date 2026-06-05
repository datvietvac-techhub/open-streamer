package blob

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRangesHeader_RoundTrip(t *testing.T) {
	t.Parallel()
	in := RangesHeader{
		Version: rangesVersion, RecordSize: FragRecordSize,
		Flags:        HeaderFlagSealed | HeaderFlagStartDiscont,
		VideoInitLen: 1234, AudioInitLen: 567,
		VideoTimescale: 90000, AudioTimescale: 48000,
		RecordCount: 42, HourStartUnixMs: 1749132000000,
		BaseMediaTicksV: 351320, BaseMediaTicksA: 187392,
	}
	b := MarshalRangesHeader(in)
	out, err := UnmarshalRangesHeader(b[:])
	require.NoError(t, err)
	assert.Equal(t, in, out)
}

func TestRangesHeader_BadMagicAndVersion(t *testing.T) {
	t.Parallel()
	b := MarshalRangesHeader(RangesHeader{Version: rangesVersion, RecordSize: FragRecordSize})
	b[0] = 'X'
	_, err := UnmarshalRangesHeader(b[:])
	assert.Error(t, err)

	b2 := MarshalRangesHeader(RangesHeader{Version: 99, RecordSize: FragRecordSize})
	_, err = UnmarshalRangesHeader(b2[:])
	assert.Error(t, err)

	_, err = UnmarshalRangesHeader(make([]byte, 10))
	assert.Error(t, err)
}

func TestFragRecord_RoundTripAndCRC(t *testing.T) {
	t.Parallel()
	in := FragRecord{
		Track: TrackVideo, Keyframe: true, Discontinuity: true,
		SampleCount: 150, WallTimeMs: 1749132002120,
		MediaTicks: 351320, ByteOffset: 4096, ByteLen: 612345, DurTicks: 360000,
	}
	b := MarshalFragRecord(in)
	out, ok := UnmarshalFragRecord(b[:])
	require.True(t, ok)
	assert.Equal(t, in, out)

	// Flip a payload byte → CRC must reject (torn-record discriminator).
	b[20] ^= 0xFF
	_, ok = UnmarshalFragRecord(b[:])
	assert.False(t, ok, "corrupted record must fail CRC")

	// Short buffer.
	_, ok = UnmarshalFragRecord(make([]byte, 10))
	assert.False(t, ok)
}

func TestSearchByMedia(t *testing.T) {
	t.Parallel()
	recs := []FragRecord{
		{MediaTicks: 0}, {MediaTicks: 100}, {MediaTicks: 200}, {MediaTicks: 300},
	}
	assert.Equal(t, 0, SearchByMedia(recs, 0))  // exact match on first
	assert.Equal(t, 0, SearchByMedia(recs, 50)) // last <= 50
	assert.Equal(t, 1, SearchByMedia(recs, 100))
	assert.Equal(t, 3, SearchByMedia(recs, 9999))
	assert.Equal(t, -1, SearchByMedia(nil, 5))
	// strictly-before-first → -1
	recs2 := []FragRecord{{MediaTicks: 100}, {MediaTicks: 200}}
	assert.Equal(t, -1, SearchByMedia(recs2, 50))
}

func TestBlobFile_AppendReadTruncate(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "14.cmfv")
	init := []byte("INIT-ftyp+moov")
	b, err := createBlobFile(path, init)
	require.NoError(t, err)
	assert.EqualValues(t, len(init), b.Size())

	frag0 := []byte("styp-moof-mdat-0")
	off0, err := b.Append(frag0)
	require.NoError(t, err)
	assert.EqualValues(t, len(init), off0)

	frag1 := []byte("styp-moof-mdat-frag-1")
	off1, err := b.Append(frag1)
	require.NoError(t, err)
	assert.EqualValues(t, len(init)+len(frag0), off1)

	// Read init + each fragment back by (offset,len).
	gotInit, err := b.ReadSlice(0, len(init))
	require.NoError(t, err)
	assert.Equal(t, init, gotInit)
	got1, err := b.ReadSlice(off1, len(frag1))
	require.NoError(t, err)
	assert.Equal(t, frag1, got1)

	// Truncate away frag1 (simulate torn-tail recovery).
	require.NoError(t, b.Truncate(off1))
	assert.EqualValues(t, off1, b.Size())
	require.NoError(t, b.Close())

	// Reopen resumes at the truncated end.
	b2, err := openBlobFile(path)
	require.NoError(t, err)
	defer b2.Close()
	assert.EqualValues(t, off1, b2.Size())

	// createBlobFile refuses to clobber an existing hour.
	_, err = createBlobFile(path, init)
	assert.Error(t, err)
}

func TestRangesFile_AppendReadSeal(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "14.ranges")
	hdr := RangesHeader{VideoInitLen: 100, VideoTimescale: 90000, HourStartUnixMs: 1749132000000}
	rf, err := createRangesFile(path, hdr)
	require.NoError(t, err)

	recs := []FragRecord{
		{Track: TrackVideo, Keyframe: true, MediaTicks: 0, ByteOffset: 100, ByteLen: 50, DurTicks: 360000, SampleCount: 150},
		{Track: TrackVideo, MediaTicks: 360000, ByteOffset: 150, ByteLen: 40, DurTicks: 360000, SampleCount: 150},
		{Track: TrackVideo, Keyframe: true, MediaTicks: 720000, ByteOffset: 190, ByteLen: 60, DurTicks: 360000, SampleCount: 150},
	}
	for _, r := range recs {
		require.NoError(t, rf.Append(r))
	}
	require.NoError(t, rf.Seal())
	require.NoError(t, rf.Close())

	// blobSize large enough so the bounds check passes for all records.
	gotHdr, gotRecs, err := ReadRanges(path, 1_000_000)
	require.NoError(t, err)
	assert.Equal(t, HeaderFlagSealed, gotHdr.Flags&HeaderFlagSealed)
	assert.EqualValues(t, 3, gotHdr.RecordCount)
	require.Len(t, gotRecs, 3)
	assert.Equal(t, recs[2].MediaTicks, gotRecs[2].MediaTicks)
	assert.True(t, gotRecs[0].Keyframe)

	// A blobSize that cuts off the last record's bytes → that record is dropped
	// (bytes not durable).
	_, trimmed, err := ReadRanges(path, 190) // last rec needs offset 190+60
	require.NoError(t, err)
	assert.Len(t, trimmed, 2)
}

func TestRangesFile_TruncateRepair(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "14.ranges")
	rf, err := createRangesFile(path, RangesHeader{VideoTimescale: 90000})
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		require.NoError(t, rf.Append(FragRecord{Track: TrackVideo, MediaTicks: uint64(i) * 100, ByteOffset: uint64(i) * 10, ByteLen: 10}))
	}
	require.NoError(t, rf.WriteHeader())
	require.NoError(t, rf.Truncate(3)) // recovery drops the last 2
	require.NoError(t, rf.Close())

	hdr, recs, err := ReadRanges(path, 1_000_000)
	require.NoError(t, err)
	assert.EqualValues(t, 3, hdr.RecordCount)
	assert.Len(t, recs, 3)
}

func TestCatalog_SaveLoadAndClocks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	assert.False(t, HasCatalog(dir))

	c := &Catalog{
		StreamCode: "region/north/live", VideoTimescale: 90000,
		RecordingMediaOriginTicks: 351320, RecordingMediaOriginUnixMs: 1749132902120,
		AudioProfile: "p2", BestProfile: "p2",
		Profiles: []ProfileDesc{
			{
				ID: "p0", BufferSlug: "track_1", Codec: "avc1.4D401F", Width: 1280, Height: 720, Bandwidth: 3_000_000,
				Available: []MediaWindow{{FromMs: 1749132902120, ToMs: 1749140102120}},
				Hours:     []HourRecord{{Hour: "2026/06/05/14", WallFromMs: 1749132902120, WallToMs: 1749136500000, Sealed: true, FragCountV: 900, SizeBytes: 612_000_000}},
			},
			{
				ID: "p2", BufferSlug: "track_3", Codec: "avc1.4D4028", Width: 1920, Height: 1080, Bandwidth: 6_000_000,
				AudioCodec: "mp4a.40.2", AudioTimescale: 48000,
				Available: []MediaWindow{{FromMs: 1749132902120, ToMs: 1749140102120}},
			},
		},
		Retention: RetentionCfg{MaxAgeSec: 86400, MaxSizeGB: 50},
	}
	require.NoError(t, c.Save(dir))
	assert.True(t, HasCatalog(dir))

	got, ok, err := LoadCatalog(dir)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, CatalogFormat, got.Format)
	assert.Equal(t, "p2", got.AudioProfile)
	require.Len(t, got.Profiles, 2)
	p2, found := got.Profile("p2")
	require.True(t, found)
	assert.Equal(t, 1080, p2.Height)

	// wall <-> media round-trip at the origin and one hour later.
	assert.EqualValues(t, c.RecordingMediaOriginTicks, got.WallToMediaTicks(c.RecordingMediaOriginUnixMs))
	oneHourLaterMs := c.RecordingMediaOriginUnixMs + 3_600_000
	ticks := got.WallToMediaTicks(oneHourLaterMs)
	assert.EqualValues(t, c.RecordingMediaOriginTicks+3600*90000, ticks)
	assert.InDelta(t, oneHourLaterMs, got.MediaTicksToWall(ticks), 1)

	// missing dir → ok=false, no error
	_, ok, err = LoadCatalog(t.TempDir())
	require.NoError(t, err)
	assert.False(t, ok)
}
