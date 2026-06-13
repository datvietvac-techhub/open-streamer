package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// A-6: when a transcoder-topology Update tears the pipeline down and the new
// transcoder fails to start, the coordinator must NOT leave the stream
// IsRunning=true with no output (the reconciler would never restart it). It
// falls back to a full stop so the reconciler self-heals on the next tick.
func TestUpdate_ReloadFailureFullStops(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	old := &domain.Stream{Code: "rs", Inputs: []domain.Input{{URL: "udp://x:1", Priority: 0}}}
	require.NoError(t, h.coord.Start(context.Background(), old))
	require.True(t, h.coord.IsRunning("rs"))

	h.tc.startErr = errors.New("transcoder binary missing")
	// nil → non-nil transcoder is a topology change → reloadTranscoderFull.
	updated := &domain.Stream{
		Code:   "rs",
		Inputs: []domain.Input{{URL: "udp://x:1", Priority: 0}},
		Transcoder: &domain.TranscoderConfig{
			Video: domain.VideoTranscodeConfig{Profiles: []domain.VideoProfile{{Width: 1280, Height: 720, Bitrate: 2000}}},
		},
	}
	err := h.coord.Update(context.Background(), old, updated)
	require.Error(t, err, "reload must surface the transcoder start failure")
	require.False(t, h.coord.IsRunning("rs"),
		"a mid-flight reload failure must full-stop so the reconciler restarts it (A-6)")
}

// C-1: concurrent Start/Stop on the same stream must be serialised by the
// per-stream lifecycle lock — without it the racing ops corrupt shared
// buffer/rendition state and leak monitor goroutines. Run under -race; the
// assertion is the absence of a data race / panic.
func TestLifecycle_ConcurrentStartStopSerialised(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	st := &domain.Stream{Code: "cc", Inputs: []domain.Input{{URL: "udp://x:1", Priority: 0}}}
	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _ = h.coord.Start(context.Background(), st) }()
		go func() { defer wg.Done(); h.coord.Stop(context.Background(), "cc") }()
	}
	wg.Wait()
	h.coord.Stop(context.Background(), "cc") // leave clean
}
