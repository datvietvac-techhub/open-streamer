package blob

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Eyevinn/mp4ff/mp4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// Real-world H.264 1080p Main@4.0 SPS/PPS (so mp4ff.ParseSPSNALUnit succeeds in
// BuildH264Init) + a minimal IDR/P slice + one ADTS frame. Copied from the dash
// packager test fixtures (public bitstream constants, not internal API).
var (
	wtSPS   = []byte{0x67, 0x4d, 0x40, 0x28, 0xeb, 0x05, 0x07, 0x80, 0x44, 0x00, 0x00, 0x03, 0x00, 0x04, 0x00, 0x00, 0x03, 0x00, 0xf0, 0x3c, 0x60, 0xc6, 0x58}
	wtPPS   = []byte{0x68, 0xee, 0x3c, 0x80}
	wtIDR   = []byte{0x65, 0x88, 0x80, 0x40, 0x00, 0x00}
	wtSlice = []byte{0x41, 0x9a, 0x12, 0x34, 0x56}
	wtADTS  = []byte{0xFF, 0xF1, 0x4C, 0x80, 0x01, 0x1F, 0xFC, 0xAA}
)

func annexB(nalus ...[]byte) []byte {
	sc := []byte{0x00, 0x00, 0x00, 0x01}
	var out []byte
	for _, n := range nalus {
		out = append(out, sc...)
		out = append(out, n...)
	}
	return out
}

func vPacket(data []byte, ptsMs uint64, idr bool) buffer.Packet {
	return buffer.Packet{AV: &domain.AVPacket{
		Codec: domain.AVCodecH264, Data: data, PTSms: ptsMs, DTSms: ptsMs, KeyFrame: idr,
	}}
}

func aPacket(ptsMs uint64) buffer.Packet {
	return buffer.Packet{AV: &domain.AVPacket{
		Codec: domain.AVCodecAAC, Data: wtADTS, PTSms: ptsMs,
	}}
}

// feed drives n video frames (IDR every gop frames) at frameMs spacing from
// PTS 0, with one audio frame per video frame, starting at wall time start.
//
//nolint:unparam // gop is an explicit GOP-size knob kept for test readability
func feed(t *testing.T, w *profileWriter, n, gop int, frameMs uint64, start time.Time) {
	t.Helper()
	for i := 0; i < n; i++ {
		pts := uint64(i) * frameMs
		now := start.Add(time.Duration(pts) * time.Millisecond)
		var vp buffer.Packet
		if i%gop == 0 {
			vp = vPacket(annexB(wtSPS, wtPPS, wtIDR), pts, true)
		} else {
			vp = vPacket(annexB(wtSlice), pts, false)
		}
		require.NoError(t, w.Ingest(vp, now))
		require.NoError(t, w.Ingest(aPacket(pts), now))
	}
}

func TestProfileWriter_ProducesValidCMAFBlob(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w := newProfileWriter(dir, "p0", true, time.Second, nil)

	// 25 fps (40 ms), IDR each second, 4 s of media → ~4 one-second fragments.
	start := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	feed(t, w, 100, 25, 40, start)
	require.NoError(t, w.Close())

	_, _, set := w.Origin()
	assert.True(t, set, "origin must be anchored")

	// Files exist under p0/2026/06/05/12.*
	hdir := filepath.Join(dir, "p0", "2026", "06", "05")
	stem := filepath.Join(hdir, "12")
	for _, ext := range []string{".cmfv", ".cmfa", ".ranges"} {
		_, err := os.Stat(stem + ext)
		require.NoError(t, err, "missing %s", ext)
	}
	assert.NoFileExists(t, sentinelPath(stem), "sealed hour must drop its .open sentinel")

	// init at [0,initLen) decodes as ftyp+moov; ranges header carries the len.
	vblobBytes, err := os.ReadFile(stem + ".cmfv")
	require.NoError(t, err)
	hdr, recs, err := ReadRanges(stem+".ranges", int64(len(vblobBytes)))
	require.NoError(t, err)
	assert.Equal(t, HeaderFlagSealed, hdr.Flags&HeaderFlagSealed)
	require.NotZero(t, hdr.VideoInitLen)
	firstBox, err := mp4.DecodeHeader(bytes.NewReader(vblobBytes[:hdr.VideoInitLen]))
	require.NoError(t, err)
	assert.Equal(t, "ftyp", firstBox.Name, "init must start with ftyp")

	// Split records by track.
	var vrecs, arecs []FragRecord
	for _, r := range recs {
		if r.Track == TrackVideo {
			vrecs = append(vrecs, r)
		} else {
			arecs = append(arecs, r)
		}
	}
	require.GreaterOrEqual(t, len(vrecs), 2, "expected several video fragments")
	require.NotEmpty(t, arecs, "expected audio fragments")

	// First video fragment: keyframe, tfdt 0, and its bytes start with styp.
	assert.True(t, vrecs[0].Keyframe)
	assert.EqualValues(t, 0, vrecs[0].MediaTicks)
	frag := vblobBytes[vrecs[0].ByteOffset : vrecs[0].ByteOffset+uint64(vrecs[0].ByteLen)]
	box, err := mp4.DecodeHeader(bytes.NewReader(frag))
	require.NoError(t, err)
	assert.Equal(t, "styp", box.Name, "fragment must start with styp")

	// Video tfdt is contiguous: tfdt[n] == tfdt[n-1] + dur[n-1].
	for i := 1; i < len(vrecs); i++ {
		assert.EqualValues(t, vrecs[i-1].MediaTicks+uint64(vrecs[i-1].DurTicks), vrecs[i].MediaTicks,
			"video fragment %d not PTS-contiguous", i)
	}
}

func TestProfileWriter_RotatesAtHourBoundary(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w := newProfileWriter(dir, "p0", false, time.Second, nil) // video-only lane

	// Origin wall at 12:59:56Z so several seconds of media cross into hour 13.
	start := time.Date(2026, 6, 5, 12, 59, 56, 0, time.UTC)
	feed(t, w, 200, 25, 40, start) // 8 s
	require.NoError(t, w.Close())

	day := filepath.Join(dir, "p0", "2026", "06", "05")
	_, err12 := os.Stat(filepath.Join(day, "12.cmfv"))
	_, err13 := os.Stat(filepath.Join(day, "13.cmfv"))
	require.NoError(t, err12, "hour 12 blob missing")
	require.NoError(t, err13, "hour 13 blob missing")

	// Hour 12 sealed; hour 13 sealed after Close. tfdt stays absolute &
	// contiguous across the rotation (single recording origin).
	b12, _ := os.ReadFile(filepath.Join(day, "12.cmfv"))
	b13, _ := os.ReadFile(filepath.Join(day, "13.cmfv"))
	_, r12, err := ReadRanges(filepath.Join(day, "12.ranges"), int64(len(b12)))
	require.NoError(t, err)
	_, r13, err := ReadRanges(filepath.Join(day, "13.ranges"), int64(len(b13)))
	require.NoError(t, err)
	require.NotEmpty(t, r12)
	require.NotEmpty(t, r13)
	last12 := r12[len(r12)-1]
	first13 := r13[0]
	assert.EqualValues(t, last12.MediaTicks+uint64(last12.DurTicks), first13.MediaTicks,
		"tfdt must stay contiguous across the hour rotation")
	assert.True(t, first13.Keyframe, "new hour must open on a keyframe")
}
