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

func mkLookup(streams ...*domain.Stream) StreamLookup {
	m := make(map[domain.StreamCode]*domain.Stream, len(streams))
	for _, s := range streams {
		m[s.Code] = s
	}
	return func(c domain.StreamCode) (*domain.Stream, bool) {
		s, ok := m[c]
		return s, ok
	}
}

// Construction validates the copy:// URL grammar through the same code
// path as the API handler, so the runtime sees the same errors.
func TestNewCopyReader_RejectsMalformedURL(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	_, err := NewCopyReader(domain.Input{URL: "rtmp://nope"}, bs, mkLookup())
	require.Error(t, err)
}

// Missing upstream is a runtime error — the manager surfaces it as
// "input degraded" so the failover/probe machinery can retry later.
func TestNewCopyReader_RejectsMissingUpstream(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	_, err := NewCopyReader(domain.Input{URL: "copy://ghost"}, bs, mkLookup())
	require.Error(t, err)
	require.Contains(t, err.Error(), "ghost")
	require.Contains(t, err.Error(), "not found")
}

// ABR upstream is now ACCEPTED in CopyReader: subscribes to the best
// rendition buffer and demuxes TS bytes back into AVPackets internally.
// This is the path used when a downstream stream copies an ABR upstream
// AND adds its own transcoder. (Coordinator's startABRCopy handles the
// no-transcoder mirror case.)
func TestNewCopyReader_AcceptsABRUpstream(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	abr := &domain.Stream{
		Code: "up",
		Transcoder: &domain.TranscoderConfig{
			Video: domain.VideoTranscodeConfig{
				Profiles: []domain.VideoProfile{
					{Width: 1920, Height: 1080, Bitrate: 4500},
					{Width: 1280, Height: 720, Bitrate: 2500},
				},
			},
		},
	}
	r, err := NewCopyReader(domain.Input{URL: "copy://up"}, bs, mkLookup(abr))
	require.NoError(t, err)
	require.NotNil(t, r)

	// Best rendition (track_1 = 1080p) should be selected as the source.
	bestSlug := buffer.VideoTrackSlug(0)
	wantBufID := buffer.RenditionBufferID("up", bestSlug)
	require.Equal(t, wantBufID, r.bufID)
	require.NotNil(t, r.abrInner, "ABR mode must wrap a TS demuxer")
}

// Subscribing before the upstream's buffer exists is an error — the
// coordinator's "wait for upstream" loop is what handles this in practice;
// here we just verify the contract.
func TestCopyReader_OpenFailsBeforeUpstreamBufferCreated(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	upstream := &domain.Stream{Code: "up"}
	r, err := NewCopyReader(domain.Input{URL: "copy://up"}, bs, mkLookup(upstream))
	require.NoError(t, err)
	require.Error(t, r.Open(context.Background()), "subscribe must fail when upstream buffer is missing")
}

// Happy path: upstream writes AV packets, copy reader forwards them.
func TestCopyReader_ForwardsAVPacketsFromUpstreamBuffer(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("up")
	upstream := &domain.Stream{Code: "up"}

	r, err := NewCopyReader(domain.Input{URL: "copy://up"}, bs, mkLookup(upstream))
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	// Producer goroutine writes one AV packet to upstream buffer.
	want := domain.AVPacket{Data: []byte{0xDE, 0xAD, 0xBE, 0xEF}, PTSms: 33, DTSms: 33}
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = bs.Write("up", buffer.Packet{AV: &want})
	}()

	got, err := r.ReadPackets(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, want.Data, got[0].Data)
	require.Equal(t, want.PTSms, got[0].PTSms)
}

// TS-only packets (rendition buffer shape) are dropped — single-stream
// copy reader doesn't carry that shape; ABR path would.
func TestCopyReader_DropsTSOnlyPackets(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("up")
	upstream := &domain.Stream{Code: "up"}

	r, err := NewCopyReader(domain.Input{URL: "copy://up"}, bs, mkLookup(upstream))
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = bs.Write("up", buffer.TSPacket([]byte{0x47, 0, 0, 0}))
	}()

	got, err := r.ReadPackets(context.Background())
	require.NoError(t, err)
	require.Empty(t, got, "TS-only packets are dropped silently")
}

// Buffer.Delete now closes every subscriber channel before removing the
// map entry — this is the contract the mixer / copy reconnect logic
// depends on. So an upstream tear-down surfaces as EOF immediately and
// downstream readers can decide to retry against the freshly-created
// upstream buffer instead of staying blocked forever on a stale ringBuffer.
//
// The previous design left subscribers dangling and relied on the manager's
// packet-timeout to eventually fail the stream over; that worked for normal
// pipelines but broke ABR mixer/copy because their tap goroutines never see
// the timeout signal (it's reported up to the manager, not down to taps).
func TestCopyReader_ReturnsEOFAfterUpstreamDeleted(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("up")
	upstream := &domain.Stream{Code: "up"}

	r, err := NewCopyReader(domain.Input{URL: "copy://up"}, bs, mkLookup(upstream))
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	bs.Delete("up")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = r.ReadPackets(ctx)
	require.ErrorIs(t, err, io.EOF,
		"upstream Delete must surface as EOF so downstream can reconnect")
}

// Explicit subscriber unsubscribe (rare path — coordinator teardown could
// theoretically do this) closes the channel and surfaces as io.EOF.
func TestCopyReader_ReturnsEOFOnExplicitUnsubscribe(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("up")
	upstream := &domain.Stream{Code: "up"}

	r, err := NewCopyReader(domain.Input{URL: "copy://up"}, bs, mkLookup(upstream))
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))

	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = r.Close() // triggers Unsubscribe → closes channel
	}()

	_, err = r.ReadPackets(context.Background())
	require.True(t, errors.Is(err, io.EOF), "channel close after Unsubscribe must surface as EOF, got: %v", err)
}

// Context cancel returns ctx.Err — caller can distinguish "we asked it to
// stop" from "upstream went away".
func TestCopyReader_HonoursContextCancel(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("up")
	upstream := &domain.Stream{Code: "up"}

	r, err := NewCopyReader(domain.Input{URL: "copy://up"}, bs, mkLookup(upstream))
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = r.ReadPackets(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// Close is idempotent so it can be called from teardown paths that
// don't track whether they've already disposed the reader.
func TestCopyReader_CloseIdempotent(t *testing.T) {
	t.Parallel()
	bs := buffer.NewServiceForTesting(8)
	bs.Create("up")
	upstream := &domain.Stream{Code: "up"}

	r, err := NewCopyReader(domain.Input{URL: "copy://up"}, bs, mkLookup(upstream))
	require.NoError(t, err)
	require.NoError(t, r.Open(context.Background()))

	require.NoError(t, r.Close())
	require.NoError(t, r.Close(), "second Close must be a no-op")
}
