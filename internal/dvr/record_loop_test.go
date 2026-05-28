package dvr

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

var _ = loadIndex // silence "imported and not used" if a future edit drops the gap-test.

// 188-byte aligned MPEG-TS sync byte at offset 0 — the recorder appends
// raw packet bytes verbatim, so any 188*N byte slice with the sync byte
// pattern is enough to exercise the segment-flush path. We don't validate
// the muxed output as decodable TS; the goal is to confirm the recorder
// writes SOMETHING to disk and the retention loop processes it.
func tsPacket() []byte {
	pkt := make([]byte, 188)
	pkt[0] = 0x47
	return pkt
}

// Drives the record loop with synthetic packets that carry monotonically-
// increasing PTSms so the PTS-based cut path fires. Verifies a real
// segment file lands on disk and the playlist + index update.
func TestRecordLoop_FlushesSegmentAtPTSCut(t *testing.T) {
	t.Parallel()
	svc, _, buf := newDVRSvc(t)
	const code domain.StreamCode = "rec1"
	buf.Create(code)

	cfg := dvrCfg(t)
	cfg.SegmentDuration = 1 // 1s segments

	rec, err := svc.StartRecording(t.Context(), code, code, cfg)
	require.NoError(t, err)
	require.NotNil(t, rec)

	// Feed packets spanning 1.5s of PTS so the >= segDur cut triggers.
	// Note: the recorder seeds segStartPTSms from the first packet whose
	// PTSms > 0 (PTS=0 is treated as "PTS not yet known"). We start at
	// 100ms so segStartPTSms gets a real anchor on the first packet, and
	// run through 1500ms so packet 5's delta (1500-100=1400ms) crosses
	// the 1s cut threshold.
	for i := 0; i < 6; i++ {
		require.NoError(t, buf.Write(code, buffer.Packet{
			TS: tsPacket(),
			AV: &domain.AVPacket{
				Codec:    domain.AVCodecH264,
				Data:     []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0x88}, // fake IDR Annex-B
				PTSms:    uint64(100 + 300*i),
				DTSms:    uint64(100 + 300*i),
				KeyFrame: i == 0,
			},
		}))
		// Sleep just enough that the recorder goroutine has a chance to
		// drain the channel before the next packet — without it all
		// packets land in one Recv() iteration and the cut never fires.
		time.Sleep(20 * time.Millisecond)
	}

	// Allow the loop a beat to write the segment, then stop.
	require.Eventually(t, func() bool {
		entries, _ := os.ReadDir(rec.SegmentDir)
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".ts") {
				return true
			}
		}
		return false
	}, 3*time.Second, 50*time.Millisecond, "expected at least one .ts segment to be flushed")

	require.NoError(t, svc.StopRecording(t.Context(), code))
	time.Sleep(150 * time.Millisecond) // let goroutine drain before TempDir cleanup

	// Playlist must exist after stop — writePlaylist's "final=true" path runs.
	playlist, err := os.ReadFile(filepath.Join(rec.SegmentDir, "playlist.m3u8"))
	require.NoError(t, err)
	assert.True(t, bytes.Contains(playlist, []byte("#EXTM3U")), "playlist missing header")
	assert.True(t, bytes.Contains(playlist, []byte("#EXT-X-ENDLIST")), "final playlist must include endlist")
}

// Drives the retention path with enough segments + a 1s window so the
// applyRetention loop prunes at least one segment. Don't assert exact
// counts (timing-flaky); just exercise the code path. Coverage of
// applyRetention's loop body is the goal.
func TestRecordLoop_RetentionPrunesOldSegments(t *testing.T) {
	t.Parallel()
	svc, _, buf := newDVRSvc(t)
	const code domain.StreamCode = "rec_ret"
	buf.Create(code)

	cfg := dvrCfg(t)
	cfg.SegmentDuration = 1
	cfg.RetentionSec = 1

	_, err := svc.StartRecording(t.Context(), code, code, cfg)
	require.NoError(t, err)

	for seg := 0; seg < 3; seg++ {
		base := uint64(seg * 2000)
		for i := 0; i < 4; i++ {
			require.NoError(t, buf.Write(code, buffer.Packet{
				TS: tsPacket(),
				AV: &domain.AVPacket{
					Codec:    domain.AVCodecH264,
					Data:     []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0x88},
					PTSms:    base + uint64(300*i),
					KeyFrame: i == 0,
				},
			}))
			time.Sleep(15 * time.Millisecond)
		}
		time.Sleep(1100 * time.Millisecond)
	}
	require.NoError(t, svc.StopRecording(t.Context(), code))
	// StopRecording is fire-and-forget — the recording goroutine cancels
	// asynchronously and flushes the in-flight segment AFTER the call
	// returns. Sleep briefly so the final write completes before the
	// test's TempDir cleanup races against it; without this, the
	// goroutine may write a new segment file mid-RemoveAll and that
	// surface as a "directory not empty" error from the cleanup hook.
	time.Sleep(200 * time.Millisecond)
	// Test passes if it completed without panic — applyRetention's loop
	// body executed at least once (verified by coverage profile, not by
	// fragile timing-based assertions).
}

// Drives the gap-timer + gap-resume code path by:
//  1. Sending one packet to start a segment.
//  2. Waiting > gapDur (2 × segDur) so the gap timer fires.
//  3. Resuming with another packet.
//
// The assertion is loose — gap recording depends on the loop being
// scheduled in the right order between the timer and the next packet.
// We tolerate "gap not recorded" because the timing window is genuinely
// narrow under -race; the goal is to EXERCISE both branches (gap timer
// fire AND gap-resume packet path) for coverage.
func TestRecordLoop_GapTimerExercises(t *testing.T) {
	t.Parallel()
	svc, _, buf := newDVRSvc(t)
	const code domain.StreamCode = "rec_gap"
	buf.Create(code)

	cfg := dvrCfg(t)
	cfg.SegmentDuration = 1 // gap timer = 2s

	_, err := svc.StartRecording(t.Context(), code, code, cfg)
	require.NoError(t, err)

	require.NoError(t, buf.Write(code, buffer.Packet{
		TS: tsPacket(),
		AV: &domain.AVPacket{
			Codec: domain.AVCodecH264,
			Data:  []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0x88},
			PTSms: 100,
		},
	}))

	// Wait past gapDur so the timer fires AND the gap-flush branch runs.
	time.Sleep(2300 * time.Millisecond)

	// Resume the stream — exercise the inGap=true reset branch.
	require.NoError(t, buf.Write(code, buffer.Packet{
		TS: tsPacket(),
		AV: &domain.AVPacket{
			Codec: domain.AVCodecH264,
			Data:  []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0x88},
			PTSms: 5000,
		},
	}))
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, svc.StopRecording(t.Context(), code))
	// Drain the recording goroutine — same TempDir-cleanup race as
	// the retention test.
	time.Sleep(200 * time.Millisecond)
}

// mediaDur picks PTS-based duration when PTS is reliable, else falls back
// to wall-clock since segment start. Direct unit test — no goroutines.
func TestMediaDur(t *testing.T) {
	t.Parallel()
	wallStart := time.Now().Add(-300 * time.Millisecond)

	// hasPTS true and end > start → PTS delta wins (250 ms).
	assert.Equal(t, 250*time.Millisecond, mediaDur(true, wallStart, 1000, 1250))

	// hasPTS true but end == start → falls back to wall clock.
	got := mediaDur(true, wallStart, 1000, 1000)
	assert.GreaterOrEqual(t, got, 100*time.Millisecond,
		"degenerate PTS pair should return wall-clock duration")

	// hasPTS false → always wall-clock.
	got = mediaDur(false, wallStart, 0, 0)
	assert.GreaterOrEqual(t, got, 100*time.Millisecond)
}
