package coordinator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// mixerDownstream returns a stream coded "mix" whose only input is
// `mixer://<videoCode>,<audioCode>`. Used by ABR-mixer detection tests.
func mixerDownstream(videoCode, audioCode string) *domain.Stream {
	return &domain.Stream{
		Code: "mix",
		Inputs: []domain.Input{
			{Priority: 0, URL: "mixer://" + videoCode + "," + audioCode},
		},
	}
}

// ─── detection ────────────────────────────────────────────────────────────────

func TestDetectABRMixer_RequiresMixerInput(t *testing.T) {
	t.Parallel()
	h := newABRHarness(t, abrUpstream(2))
	s := &domain.Stream{
		Code:   "mix",
		Inputs: []domain.Input{{Priority: 0, URL: "rtmp://origin/x"}},
	}
	_, _, ok := h.coord.detectABRMixer(s)
	assert.False(t, ok)
}

func TestDetectABRMixer_RequiresABRVideoUpstream(t *testing.T) {
	t.Parallel()
	// Video upstream is single-stream → mixer ABR mirror doesn't apply,
	// fall through to normal MixerReader pipeline.
	h := newABRHarness(t,
		&domain.Stream{Code: "vidSingle"},
		&domain.Stream{Code: "audSingle"},
	)
	_, _, ok := h.coord.detectABRMixer(mixerDownstream("vidSingle", "audSingle"))
	assert.False(t, ok)
}

func TestDetectABRMixer_RejectsDownstreamTranscoder(t *testing.T) {
	t.Parallel()
	// Downstream has its own transcoder → use normal MixerReader path
	// (best-rendition tap + encoder-driven ladder), not the mirror path.
	abr := abrUpstream(2)
	h := newABRHarness(t, abr, &domain.Stream{Code: "audio"})
	s := mixerDownstream("up", "audio")
	s.Transcoder = &domain.TranscoderConfig{
		Video: domain.VideoTranscodeConfig{Profiles: []domain.VideoProfile{{Width: 1280, Height: 720}}},
	}
	_, _, ok := h.coord.detectABRMixer(s)
	assert.False(t, ok)
}

func TestDetectABRMixer_HappyPath(t *testing.T) {
	t.Parallel()
	abr := abrUpstream(3)
	audio := &domain.Stream{Code: "radio"}
	h := newABRHarness(t, abr, audio)
	v, a, ok := h.coord.detectABRMixer(mixerDownstream("up", "radio"))
	require.True(t, ok)
	assert.Equal(t, domain.StreamCode("up"), v.Code)
	assert.Equal(t, domain.StreamCode("radio"), a.Code)
}

// ─── pipeline lifecycle ───────────────────────────────────────────────────────

func TestStart_ABRMixer_BypassesIngestorAndTranscoder(t *testing.T) {
	t.Parallel()
	abr := abrUpstream(2)
	audio := &domain.Stream{Code: "radio"}
	h := newABRHarness(t, abr, audio)

	dn := mixerDownstream("up", "radio")
	require.NoError(t, h.coord.Start(context.Background(), dn))
	defer h.coord.Stop(context.Background(), dn.Code)

	assert.False(t, h.mgr.IsRegistered("mix"), "manager must not be registered for ABR mixer")
	h.tc.mu.Lock()
	assert.Empty(t, h.tc.started, "transcoder must not be started for ABR mixer")
	h.tc.mu.Unlock()

	// Publisher must see the synthesized transcoder so it serves ABR.
	h.capPub.mu.Lock()
	require.Len(t, h.capPub.started, 1)
	pubStream := h.capPub.started[0]
	h.capPub.mu.Unlock()
	require.NotNil(t, pubStream.Transcoder, "publisher must receive synthesized transcoder")
	assert.Len(t, pubStream.Transcoder.Video.Profiles, 2)

	// Original downstream pointer must NOT be mutated.
	assert.Nil(t, dn.Transcoder)

	assert.True(t, h.coord.IsRunning("mix"))
}

func TestStart_ABRMixer_CreatesDownstreamRenditionBuffers(t *testing.T) {
	t.Parallel()
	abr := abrUpstream(3)
	audio := &domain.Stream{Code: "radio"}
	h := newABRHarness(t, abr, audio)

	require.NoError(t, h.coord.Start(context.Background(), mixerDownstream("up", "radio")))
	defer h.coord.Stop(context.Background(), "mix")

	for i := 0; i < 3; i++ {
		bid := buffer.RenditionBufferID("mix", buffer.VideoTrackSlug(i))
		_, err := h.buf.Subscribe(bid)
		assert.NoError(t, err, "downstream rendition buffer track_%d must exist", i+1)
	}
}

func TestStop_ABRMixer_DeletesBuffersAndUnregisters(t *testing.T) {
	t.Parallel()
	abr := abrUpstream(2)
	audio := &domain.Stream{Code: "radio"}
	h := newABRHarness(t, abr, audio)

	require.NoError(t, h.coord.Start(context.Background(), mixerDownstream("up", "radio")))
	require.True(t, h.coord.IsRunning("mix"))

	h.coord.Stop(context.Background(), "mix")
	assert.False(t, h.coord.IsRunning("mix"))

	for i := 0; i < 2; i++ {
		bid := buffer.RenditionBufferID("mix", buffer.VideoTrackSlug(i))
		_, err := h.buf.Subscribe(bid)
		assert.Error(t, err, "downstream rendition buffer track_%d must be deleted", i+1)
	}
}

func TestABRMixer_TapStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	abr := abrUpstream(1)
	audio := &domain.Stream{Code: "radio"}
	h := newABRHarness(t, abr, audio)

	upRends := buffer.RenditionsForTranscoder(abr.Code, abr.Transcoder)
	for _, r := range upRends {
		h.buf.Create(r.BufferID)
	}
	h.buf.Create(audio.Code)

	require.NoError(t, h.coord.Start(context.Background(), mixerDownstream("up", "radio")))

	done := make(chan struct{})
	go func() {
		h.coord.Stop(context.Background(), "mix")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not return — taps did not honour ctx cancellation")
	}
}

// TestStartABRMixerConcurrentStartIsIdempotent regression-tests the race
// where two callers both pass the unlocked IsRunning check, both reach
// startABRMixer, and the second caller's pub.Start fails with
// "stream already running" while the rollback path then deletes the
// rendition buffers the first start was using — observed in production
// as paired errors:
//
//	"abr mixer publisher: publisher: stream X already running"
//	"HLS ABR subscribe failed: buffer ... not found"
//
// The fix serialises startABRMixer on c.abrMu so concurrent callers see
// a consistent state: first one wins, the rest no-op.
func TestStartABRMixerConcurrentStartIsIdempotent(t *testing.T) {
	t.Parallel()
	abr := abrUpstream(2)
	audio := &domain.Stream{Code: "radio"}
	h := newABRHarness(t, abr, audio)
	defer h.coord.Stop(context.Background(), "mix")

	const callers = 10
	dn := mixerDownstream("up", "radio")
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			errs <- h.coord.Start(context.Background(), dn)
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err, "concurrent Start must be idempotent (no errors)")
	}

	// pub.Start must have been called exactly once across all racers.
	h.capPub.mu.Lock()
	pubStarts := len(h.capPub.started)
	h.capPub.mu.Unlock()
	assert.Equal(t, 1, pubStarts, "pub.Start should run once; got %d", pubStarts)

	// Rendition buffers must still exist (no rollback misfire).
	for i := 0; i < 2; i++ {
		bid := buffer.RenditionBufferID("mix", buffer.VideoTrackSlug(i))
		_, err := h.buf.Subscribe(bid)
		assert.NoError(t, err, "rendition buffer track_%d must survive concurrent Start", i+1)
	}

	assert.True(t, h.coord.IsRunning("mix"))
}

// ─── runtime status ──────────────────────────────────────────────────────────

func TestABRMixerRuntimeStatus_NotRunningReturnsFalse(t *testing.T) {
	t.Parallel()
	h := newABRHarness(t)
	_, ok := h.coord.ABRMixerRuntimeStatus("nonexistent")
	require.False(t, ok)
}

func TestABRMixerRuntimeStatus_DegradedBeforeFirstPacket(t *testing.T) {
	t.Parallel()
	abr := abrUpstream(1)
	audio := &domain.Stream{Code: "radio"}
	h := newABRHarness(t, abr, audio)
	require.NoError(t, h.coord.Start(context.Background(), mixerDownstream("up", "radio")))
	defer h.coord.Stop(context.Background(), "mix")

	rt, ok := h.coord.ABRMixerRuntimeStatus("mix")
	require.True(t, ok)
	require.True(t, rt.PipelineActive)
	require.Len(t, rt.Inputs, 1)
	assert.Equal(t, domain.StatusDegraded, rt.Inputs[0].Status,
		"no packets seen yet → degraded")
}

// ─── abr_mixer timeline normalisation ────────────────────────────────────────
//
// The abr_mixer pipeline no longer maintains its own rebaser — both V tap
// and A fan-out anchor packets via internal/timeline.Normaliser. Wallclock
// anchoring, per-track baselines, monotonic floor, drift cap, B-frame CTO
// preservation, cross-track snap, and jump re-anchor semantics are covered
// by the Normaliser's own tests in internal/timeline/normaliser_test.go.
// Duplicating those cases here would couple this package's tests to the
// Normaliser's internal semantics with no added coverage.
