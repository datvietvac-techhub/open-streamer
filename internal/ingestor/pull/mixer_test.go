package pull

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// mixerCamRadioURL is the canonical happy-path URL used across mixer tests:
// `cam` is the video upstream, `radio` is the audio upstream, default policy.
const mixerCamRadioURL = "mixer://cam,radio"

// videoPkt / audioPkt build AVPackets the mixer reader would treat as the
// matching media type. Codec drives the IsVideo/IsAudio classification.
func videoPkt(payload byte) *domain.AVPacket {
	return &domain.AVPacket{Codec: domain.AVCodecH264, Data: []byte{payload}}
}

func audioPkt(payload byte) *domain.AVPacket {
	return &domain.AVPacket{Codec: domain.AVCodecAAC, Data: []byte{payload}}
}

// mkCamRadioLookup is the standard 2-stream lookup used by happy-path tests.
func mkCamRadioLookup() StreamLookup {
	return mkLookup(&domain.Stream{Code: "cam"}, &domain.Stream{Code: "radio"})
}

// ─── construction errors ─────────────────────────────────────────────────────

func TestNewMixerReader_RejectsMalformedURL(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	_, err := NewMixerReader(domain.Input{URL: "rtmp://nope"}, bs, mkLookup())
	require.Error(t, err)
}

func TestNewMixerReader_RejectsMissingVideoUpstream(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	radio := &domain.Stream{Code: "radio"}
	_, err := NewMixerReader(domain.Input{URL: "mixer://ghost,radio"}, bs, mkLookup(radio))
	require.Error(t, err)
	require.Contains(t, err.Error(), "video upstream")
	require.Contains(t, err.Error(), "ghost")
}

func TestNewMixerReader_RejectsMissingAudioUpstream(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	cam := &domain.Stream{Code: "cam"}
	_, err := NewMixerReader(domain.Input{URL: "mixer://cam,ghost"}, bs, mkLookup(cam))
	require.Error(t, err)
	require.Contains(t, err.Error(), "audio upstream")
}

// ABR upstream is now ACCEPTED for both video and audio: the reader picks
// the best rendition buffer and demuxes TS bytes back into AVPackets.
func TestNewMixerReader_AcceptsABRUpstreams(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	abr := &domain.Stream{
		Code: "camABR",
		Transcoder: &domain.TranscoderConfig{
			Video: domain.VideoTranscodeConfig{Profiles: []domain.VideoProfile{
				{Width: 1920, Height: 1080, Bitrate: 4500},
				{Width: 1280, Height: 720, Bitrate: 2500},
			}},
		},
	}
	radio := &domain.Stream{Code: "radio"}
	r, err := NewMixerReader(domain.Input{URL: "mixer://camABR,radio"}, bs, mkLookup(abr, radio))
	require.NoError(t, err)
	require.NotNil(t, r)
	// Best rendition (track_1 = 1080p) selected for the video source.
	require.Equal(t, buffer.RenditionBufferID("camABR", buffer.VideoTrackSlug(0)), r.videoBufID)
	require.NotNil(t, r.videoInner, "ABR video must wrap a TS demuxer")
	require.Nil(t, r.audioInner, "single-stream audio must use direct subscription")
}

// ─── data flow: codec routing ────────────────────────────────────────────────

// Video from video source must pass through; audio from video source dropped.
func TestMixerReader_ForwardsVideoFromVideoSource(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("cam")
	bs.Create("radio")
	r, err := NewMixerReader(
		domain.Input{URL: mixerCamRadioURL}, bs,
		mkCamRadioLookup(),
	)
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = bs.Write("cam", buffer.Packet{AV: videoPkt(0xAA)})
	}()

	got, err := r.ReadPackets(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, byte(0xAA), got[0].Data[0])
	require.True(t, got[0].Codec.IsVideo())
}

// Audio from audio source must pass through; video from audio source dropped.
func TestMixerReader_ForwardsAudioFromAudioSource(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("cam")
	bs.Create("radio")
	r, err := NewMixerReader(
		domain.Input{URL: mixerCamRadioURL}, bs,
		mkCamRadioLookup(),
	)
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = bs.Write("radio", buffer.Packet{AV: audioPkt(0xBB)})
	}()

	got, err := r.ReadPackets(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, byte(0xBB), got[0].Data[0])
	require.True(t, got[0].Codec.IsAudio())
}

// Audio packets from the VIDEO source are dropped (returned as nil batch +
// no error so the readLoop calls again). Same for video from audio source.
func TestMixerReader_DropsCrossSourcePackets(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("cam")
	bs.Create("radio")
	r, err := NewMixerReader(
		domain.Input{URL: mixerCamRadioURL}, bs,
		mkCamRadioLookup(),
	)
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	// Audio on video channel — must be dropped.
	_ = bs.Write("cam", buffer.Packet{AV: audioPkt(0xC0)})
	got, err := r.ReadPackets(context.Background())
	require.NoError(t, err)
	require.Empty(t, got, "audio from video source must be dropped")

	// Video on audio channel — must be dropped.
	_ = bs.Write("radio", buffer.Packet{AV: videoPkt(0xC1)})
	got, err = r.ReadPackets(context.Background())
	require.NoError(t, err)
	require.Empty(t, got, "video from audio source must be dropped")
}

// ─── failure policy ──────────────────────────────────────────────────────────

// Video upstream tear-down ALWAYS aborts — no option for "continue audio-only".
// We force the mixer's video sub closed via UnsubscribeAll (the buffer-level
// equivalent of "stream is gone, all consumers should disconnect").
func TestMixerReader_VideoCloseAlwaysReturnsEOF(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("cam")
	bs.Create("radio")
	r, err := NewMixerReader(
		// Use continue mode — we still expect video close to abort.
		domain.Input{URL: "mixer://cam,radio?audio_failure=continue"}, bs,
		mkCamRadioLookup(),
	)
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	bs.UnsubscribeAll("cam") // closes mixer's video sub → io.EOF expected

	_, err = r.ReadPackets(context.Background())
	require.True(t, errors.Is(err, io.EOF), "video close must return io.EOF, got: %v", err)
}

// Audio close + default policy → io.EOF (whole stream stops).
func TestMixerReader_AudioCloseDefaultDownReturnsEOF(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("cam")
	bs.Create("radio")
	r, err := NewMixerReader(
		domain.Input{URL: mixerCamRadioURL}, bs, // no audio_failure → default down
		mkCamRadioLookup(),
	)
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	bs.UnsubscribeAll("radio") // closes mixer's audio sub

	_, err = r.ReadPackets(context.Background())
	require.True(t, errors.Is(err, io.EOF), "audio close (default) must return io.EOF, got: %v", err)
}

// Audio close + ?audio_failure=continue → reader keeps going video-only.
func TestMixerReader_AudioCloseContinueModeKeepsVideo(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("cam")
	bs.Create("radio")
	r, err := NewMixerReader(
		domain.Input{URL: "mixer://cam,radio?audio_failure=continue"}, bs,
		mkCamRadioLookup(),
	)
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	bs.UnsubscribeAll("radio") // tear down audio

	// First call: reader observes audio chan close, applies policy (continue
	// → unsubscribe + return nil batch).
	got, err := r.ReadPackets(context.Background())
	require.NoError(t, err, "continue mode must not error on audio close")
	require.Empty(t, got)

	// Subsequent video write must still flow through.
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = bs.Write("cam", buffer.Packet{AV: videoPkt(0xFF)})
	}()
	got, err = r.ReadPackets(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1, "video must continue flowing after audio close in continue mode")
	require.Equal(t, byte(0xFF), got[0].Data[0])
}

// ─── lifecycle ───────────────────────────────────────────────────────────────

func TestMixerReader_HonoursContextCancel(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("cam")
	bs.Create("radio")
	r, err := NewMixerReader(
		domain.Input{URL: mixerCamRadioURL}, bs,
		mkCamRadioLookup(),
	)
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.ReadPackets(ctx)
		done <- err
	}()

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("ReadPackets did not return after ctx cancel")
	}
}

func TestMixerReader_CloseIdempotent(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("cam")
	bs.Create("radio")
	r, err := NewMixerReader(
		domain.Input{URL: mixerCamRadioURL}, bs,
		mkCamRadioLookup(),
	)
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))
	require.NoError(t, r.Close())
	require.NoError(t, r.Close()) // safe to call again
}

// ─── PTS normalisation ───────────────────────────────────────────────────────

// videoPktTS / audioPktTS extend the helpers above with explicit PTS / DTS so
// the normalisation tests can exercise the offset arithmetic directly.
// The `disco` parameter is kept on the helpers' signature for historical
// continuity with the pre-Phase-5 tests but is now unused (Discontinuity
// field deleted; session boundaries flow via buffer.Packet.SessionStart).
func videoPktTS(payload byte, pts, dts uint64, _ bool) *domain.AVPacket {
	return &domain.AVPacket{
		Codec: domain.AVCodecH264,
		Data:  []byte{payload},
		PTSms: pts,
		DTSms: dts,
	}
}

func audioPktTS(payload byte, pts, dts uint64, _ bool) *domain.AVPacket {
	return &domain.AVPacket{
		Codec: domain.AVCodecAAC,
		Data:  []byte{payload},
		PTSms: pts,
		DTSms: dts,
	}
}

// readOne is a tiny helper that pumps one packet through the bus → mixer
// pipeline so PTS-normalisation tests don't all duplicate the goroutine boilerplate.
func readOne(t *testing.T, r *MixerReader, bs *buffer.Service, srcCode string, p *domain.AVPacket) domain.AVPacket {
	t.Helper()
	go func() {
		time.Sleep(2 * time.Millisecond)
		_ = bs.Write(domain.StreamCode(srcCode), buffer.Packet{AV: p})
	}()
	got, err := r.ReadPackets(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	return got[0]
}

// MixerReader.ReadPackets passes upstream PTS/DTS through unchanged.
// Timeline anchoring (per-track wallclock anchor, V/A snap, monotonic
// floor, drift cap) is the responsibility of the ingestor's per-worker
// Normaliser, which sees this reader's output and applies anchoring once
// at the buffer-write step. The Normaliser's contract is tested in
// internal/timeline — duplicating those cases here would couple this
// reader's tests to internal Normaliser semantics for no added coverage.
func TestMixerReader_PassesPTSThrough(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("cam")
	bs.Create("radio")
	r, err := NewMixerReader(
		domain.Input{URL: mixerCamRadioURL}, bs,
		mkCamRadioLookup(),
	)
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	got := readOne(t, r, bs, "cam", videoPktTS(0xAA, 10000, 10000, false))
	require.Equal(t, uint64(10000), got.PTSms, "ReadPackets must not rewrite PTS")
	require.Equal(t, uint64(10000), got.DTSms, "ReadPackets must not rewrite DTS")

	got = readOne(t, r, bs, "radio", audioPktTS(0xB0, 5000, 5000, false))
	require.Equal(t, uint64(5000), got.DTSms,
		"audio packet must pass through too (no in-reader normalisation)")
}
