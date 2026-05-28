package publisher

import (
	"context"
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

// ---- windowTailEntries -------------------------------------------------------

func TestWindowTailEntries_Empty(t *testing.T) {
	t.Parallel()
	assert.Empty(t, windowTailEntries(nil, 5))
	assert.Empty(t, windowTailEntries([]hlsSegEntry{}, 5))
}

func TestWindowTailEntries_ZeroWindow(t *testing.T) {
	t.Parallel()
	entries := []hlsSegEntry{{name: "a"}, {name: "b"}}
	// n=0 returns all entries unchanged.
	got := windowTailEntries(entries, 0)
	assert.Equal(t, entries, got)
}

func TestWindowTailEntries_LargerThanSlice(t *testing.T) {
	t.Parallel()
	entries := []hlsSegEntry{{name: "a"}, {name: "b"}}
	got := windowTailEntries(entries, 10)
	assert.Equal(t, entries, got)
}

func TestWindowTailEntries_ExactWindow(t *testing.T) {
	t.Parallel()
	entries := []hlsSegEntry{{name: "a"}, {name: "b"}, {name: "c"}}
	got := windowTailEntries(entries, 3)
	assert.Equal(t, entries, got)
}

func TestWindowTailEntries_TruncatesOldest(t *testing.T) {
	t.Parallel()
	entries := []hlsSegEntry{{name: "a"}, {name: "b"}, {name: "c"}, {name: "d"}}
	got := windowTailEntries(entries, 2)
	require.Len(t, got, 2)
	assert.Equal(t, "c", got[0].name)
	assert.Equal(t, "d", got[1].name)
}

// ---- hlsCodecString ----------------------------------------------------------

func TestHLSCodecString(t *testing.T) {
	t.Parallel()
	// Video-only — no "mp4a" suffix.
	assert.Equal(t, "avc1.640028", hlsCodecString(1920, 1080, false))
	assert.Equal(t, "avc1.640028", hlsCodecString(3840, 2160, false))
	assert.Equal(t, "avc1.4d401f", hlsCodecString(1280, 720, false))
	assert.Equal(t, "avc1.42e01e", hlsCodecString(854, 480, false))
	assert.Equal(t, "avc1.42e01e", hlsCodecString(0, 0, false))
}

// ---- hlsSegmenter.writeManifestLocked / flushLocked -------------------------

func TestHLSSegmenter_WriteManifest_EmptyOnDisk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := &hlsSegmenter{
		streamID:     "test",
		streamDir:    dir,
		manifestPath: filepath.Join(dir, "index.m3u8"),
		segSec:       2,
		window:       5,
	}
	// No segments — manifest should still write without error.
	err := p.writeManifestLocked()
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(dir, "index.m3u8"))
	require.NoError(t, err)
	assert.Contains(t, string(data), "#EXTM3U")
	assert.Contains(t, string(data), "#EXT-X-VERSION:3")
}

func TestHLSSegmenter_WriteManifest_DiscontinuityTag(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := &hlsSegmenter{
		streamID:     "test",
		streamDir:    dir,
		manifestPath: filepath.Join(dir, "index.m3u8"),
		segSec:       2,
		window:       5,
		onDisk: []hlsSegEntry{
			{name: "seg_000001.ts", dur: 2.0, disc: false},
			{name: "seg_000002.ts", dur: 2.0, disc: true},
			{name: "seg_000003.ts", dur: 2.0, disc: false},
		},
	}
	require.NoError(t, p.writeManifestLocked())
	data, err := os.ReadFile(filepath.Join(dir, "index.m3u8"))
	require.NoError(t, err)
	content := string(data)
	assert.Contains(t, content, "#EXT-X-DISCONTINUITY")
	// Only one DISCONTINUITY tag (for seg_000002).
	assert.Equal(t, 1, strings.Count(content, "#EXT-X-DISCONTINUITY"))
}

func TestHLSSegmenter_WriteManifest_TargetDuration(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := &hlsSegmenter{
		streamID:     "test",
		streamDir:    dir,
		manifestPath: filepath.Join(dir, "index.m3u8"),
		segSec:       2,
		window:       5,
		onDisk: []hlsSegEntry{
			{name: "seg_000001.ts", dur: 5.7},
		},
	}
	require.NoError(t, p.writeManifestLocked())
	data, err := os.ReadFile(filepath.Join(dir, "index.m3u8"))
	require.NoError(t, err)
	// int(5.7)+1 = 6
	assert.Contains(t, string(data), "#EXT-X-TARGETDURATION:6")
}

func TestHLSSegmenter_WriteManifest_MediaSequence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := &hlsSegmenter{
		streamID:     "test",
		streamDir:    dir,
		manifestPath: filepath.Join(dir, "index.m3u8"),
		segSec:       2,
		window:       3,
		segN:         10,
		onDisk: []hlsSegEntry{
			{name: "seg_000008.ts", dur: 2.0},
			{name: "seg_000009.ts", dur: 2.0},
			{name: "seg_000010.ts", dur: 2.0},
		},
	}
	require.NoError(t, p.writeManifestLocked())
	data, err := os.ReadFile(filepath.Join(dir, "index.m3u8"))
	require.NoError(t, err)
	// segN=10, window=3 → mediaSeq=7
	assert.Contains(t, string(data), "#EXT-X-MEDIA-SEQUENCE:7")
}

func TestHLSSegmenter_WriteManifest_NoPathNoABR(t *testing.T) {
	t.Parallel()
	// manifestPath="" and abrMaster=nil → should be a no-op.
	p := &hlsSegmenter{
		manifestPath: "",
		abrMaster:    nil,
	}
	err := p.writeManifestLocked()
	require.NoError(t, err)
}

// ---- runHLSSegmenter integration (AV packets → segment files) ---------------

func TestRunHLSSegmenter_FlushesOnContextCancel(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	buf := buffer.NewServiceForTesting(64)
	bufID := domain.StreamCode("seg-test")
	buf.Create(bufID)

	sub, err := buf.Subscribe(bufID)
	require.NoError(t, err)
	defer buf.Unsubscribe(bufID, sub)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		runHLSSegmenter(ctx, bufID, sub, dir, filepath.Join(dir, "index.m3u8"), 2, 5, 0, false, nil)
	}()

	// Write a few TS sync bytes to give the segmenter something to buffer.
	pkt := make([]byte, 188)
	pkt[0] = 0x47
	for i := range 3 {
		_ = i
		_ = buf.Write(bufID, buffer.TSPacket(pkt))
	}

	// Give segmenter time to receive packets, then cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("segmenter did not exit after context cancel")
	}
}

// ---- discardUntilIDR --------------------------------------------------------

// After a force-flush on the AV path the segmenter must DROP non-keyframe
// AVPackets until the next keyframe arrives — otherwise the next segment
// would start mid-GOP without SPS/PPS in scope and downstream players
// can't decode it. This was the root cause of test5's HLS not playing
// when the source GOP exceeded HLS maxDur (1.5×segSec).
func TestHLSSegmenter_ShouldDropAVPacket_DropsAfterForceFlush(t *testing.T) {
	t.Parallel()
	p := &hlsSegmenter{streamID: "test"}
	p.discardUntilIDR = true

	pNonKey := &domain.AVPacket{Codec: domain.AVCodecH264, KeyFrame: false}
	require.True(t, p.shouldDropAVPacket(pNonKey),
		"non-keyframe must be dropped while discardUntilIDR is set")
	require.True(t, p.discardUntilIDR,
		"flag must remain set across non-keyframe packets")
}

// shouldDropAVPacket clears the flag and admits the keyframe packet so the
// next segment starts cleanly at the IDR.
func TestHLSSegmenter_ShouldDropAVPacket_ClearsAtKeyframe(t *testing.T) {
	t.Parallel()
	p := &hlsSegmenter{streamID: "test"}
	p.discardUntilIDR = true

	pKey := &domain.AVPacket{Codec: domain.AVCodecH264, KeyFrame: true}
	require.False(t, p.shouldDropAVPacket(pKey),
		"keyframe must NOT be dropped — it starts the next segment")
	require.False(t, p.discardUntilIDR,
		"flag must be cleared on first keyframe")
}

// When the flag is unset (normal operation) the segmenter must admit every
// packet, keyframe or not.
func TestHLSSegmenter_ShouldDropAVPacket_NoOpWhenNotDiscarding(t *testing.T) {
	t.Parallel()
	p := &hlsSegmenter{streamID: "test"}

	require.False(t, p.shouldDropAVPacket(&domain.AVPacket{KeyFrame: false}))
	require.False(t, p.shouldDropAVPacket(&domain.AVPacket{KeyFrame: true}))
}

// Audio packets must NEVER be dropped during the discard window — they
// would otherwise cause audible stutter for the 3-4s gap between
// force-flush and the next video IDR. KeyFrame is unset on audio in
// this codebase so the flag-only check would mis-classify them as
// "non-keyframes" worth dropping.
func TestHLSSegmenter_ShouldDropAVPacket_AudioPassesThroughDiscard(t *testing.T) {
	t.Parallel()
	p := &hlsSegmenter{streamID: "test"}
	p.discardUntilIDR = true

	audio := &domain.AVPacket{Codec: domain.AVCodecAAC, KeyFrame: false}
	require.False(t, p.shouldDropAVPacket(audio),
		"audio packet must never be dropped, even while waiting for video IDR")
	require.True(t, p.discardUntilIDR,
		"discardUntilIDR must remain set — only a video keyframe clears it")
}

// ---- hlsCodecString ---------------------------------------------------------

// hlsCodecString must include "mp4a.40.2" when the rendition has audio.
// Without this, hls.js + MSE addSourceBuffer with a video-only codec list
// and silently drop the audio track.
func TestHLSCodecStringIncludesAudioWhenPresent(t *testing.T) {
	t.Parallel()
	got := hlsCodecString(1280, 720, true)
	require.Contains(t, got, "avc1.4d401f", "video codec must be present")
	require.Contains(t, got, "mp4a.40.2", "audio codec must be present when hasAudio=true")
}

// hlsCodecString must NOT include "mp4a.40.2" when the rendition is video-only
// — declaring audio that does not exist also breaks playback.
func TestHLSCodecStringOmitsAudioWhenAbsent(t *testing.T) {
	t.Parallel()
	got := hlsCodecString(1280, 720, false)
	require.Contains(t, got, "avc1.4d401f")
	require.NotContains(t, got, "mp4a", "audio codec must be absent when hasAudio=false")
}

// hlsCodecString picks the right H.264 profile/level by resolution.
func TestHLSCodecStringByResolution(t *testing.T) {
	t.Parallel()
	require.Contains(t, hlsCodecString(1920, 1080, false), "avc1.640028", "1080p → High@L4.0")
	require.Contains(t, hlsCodecString(1280, 720, false), "avc1.4d401f", "720p → Main@L3.1")
	require.Contains(t, hlsCodecString(640, 360, false), "avc1.42e01e", "<720p → Baseline@L3.0")
}

// writeHLSMasterPlaylist emits CODECS with audio for renditions where
// hasAudio=true — regression guard for the bug where transcoded ABR streams
// declared video only and would not play in browsers.
func TestWriteHLSMasterPlaylistEmitsAudioCodec(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "index.m3u8")

	reps := []hlsABRRep{
		{slug: "track_1", bwBps: 3500000, width: 1280, height: 720, hasAudio: true},
	}
	require.NoError(t, writeHLSMasterPlaylist(path, reps))

	body, err := os.ReadFile(path)
	require.NoError(t, err)
	got := string(body)
	require.Contains(t, got, "BANDWIDTH=3500000")
	require.Contains(t, got, "RESOLUTION=1280x720")
	require.Contains(t, got, "avc1.4d401f")
	require.Contains(t, got, "mp4a.40.2")
	require.Contains(t, got, "track_1/index.m3u8")
}

// writeHLSMasterPlaylist omits audio from CODECS when the rendition is
// video-only.
func TestWriteHLSMasterPlaylistOmitsAudioCodecWhenVideoOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "index.m3u8")

	reps := []hlsABRRep{
		{slug: "track_1", bwBps: 1000000, width: 640, height: 360, hasAudio: false},
	}
	require.NoError(t, writeHLSMasterPlaylist(path, reps))

	body, err := os.ReadFile(path)
	require.NoError(t, err)
	got := string(body)
	require.Contains(t, got, "avc1.42e01e")
	require.NotContains(t, got, "mp4a", "audio codec must not appear when video-only")
}
