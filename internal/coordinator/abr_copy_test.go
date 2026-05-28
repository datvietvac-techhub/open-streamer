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
	"github.com/datvietvac-techhub/open-streamer/internal/events"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

// abrUpstream returns a stream coded "up" with N profile rungs in its ladder.
func abrUpstream(profiles int) *domain.Stream {
	pp := make([]domain.VideoProfile, profiles)
	for i := range pp {
		pp[i] = domain.VideoProfile{
			Width:   1920 - i*640,
			Height:  1080 - i*360,
			Bitrate: 4500 - i*1500,
		}
	}
	return &domain.Stream{
		Code: "up",
		Transcoder: &domain.TranscoderConfig{
			Video: domain.VideoTranscodeConfig{Profiles: pp},
		},
	}
}

// copyDownstream returns a stream coded "dn" whose only input is `copy://upstream`.
func copyDownstream(upstream string) *domain.Stream {
	return &domain.Stream{
		Code: "dn",
		Inputs: []domain.Input{
			{Priority: 0, URL: "copy://" + upstream},
		},
	}
}

// upstreamLookupOf builds a static lookup over a list of streams.
func upstreamLookupOf(streams ...*domain.Stream) func(domain.StreamCode) (*domain.Stream, bool) {
	idx := make(map[domain.StreamCode]*domain.Stream, len(streams))
	for _, s := range streams {
		if s != nil {
			idx[s.Code] = s
		}
	}
	return func(code domain.StreamCode) (*domain.Stream, bool) {
		s, ok := idx[code]
		return s, ok
	}
}

// captureStartPub is a pubDep spy that captures the *domain.Stream passed to
// Start so tests can assert that the synthesized transcoder was applied.
type captureStartPub struct {
	*spyPub
	mu      sync.Mutex
	started []*domain.Stream
}

func newCaptureStartPub() *captureStartPub {
	return &captureStartPub{spyPub: &spyPub{}}
}

func (p *captureStartPub) Start(ctx context.Context, s *domain.Stream) error {
	p.mu.Lock()
	p.started = append(p.started, s)
	p.mu.Unlock()
	return p.spyPub.Start(ctx, s)
}

// abrHarness wraps testHarness with a pub that captures Start arguments.
type abrHarness struct {
	*testHarness
	capPub *captureStartPub
}

func newABRHarness(t *testing.T, upstreams ...*domain.Stream) *abrHarness {
	t.Helper()
	buf := buffer.NewServiceForTesting(64)
	mgr := newSpyMgr()
	tc := &spyTC{}
	capPub := newCaptureStartPub()
	dvr := newSpyDVR()
	bus := events.New(1, 8)
	coord := newForTesting(buf, mgr, tc, capPub, dvr, bus, nil)
	coord.SetUpstreamLookupForTesting(upstreamLookupOf(upstreams...))
	return &abrHarness{
		testHarness: &testHarness{coord: coord, mgr: mgr, tc: tc, pub: capPub.spyPub, dvr: dvr, buf: buf},
		capPub:      capPub,
	}
}

// ─── detection ────────────────────────────────────────────────────────────────

func TestDetectABRCopy_NilLookup_ReturnsNil(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	// No SetUpstreamLookupForTesting call → upstreamLookup is nil.
	assert.Nil(t, h.coord.detectABRCopy(copyDownstream("up")))
}

func TestDetectABRCopy_NoCopyInput_ReturnsNil(t *testing.T) {
	t.Parallel()
	h := newABRHarness(t, abrUpstream(2))
	s := &domain.Stream{
		Code:   "dn",
		Inputs: []domain.Input{{Priority: 0, URL: "rtmp://origin/dn"}},
	}
	assert.Nil(t, h.coord.detectABRCopy(s))
}

func TestDetectABRCopy_MissingUpstream_ReturnsNil(t *testing.T) {
	t.Parallel()
	h := newABRHarness(t) // no upstreams registered
	assert.Nil(t, h.coord.detectABRCopy(copyDownstream("ghost")))
}

func TestDetectABRCopy_SingleStreamUpstream_ReturnsNil(t *testing.T) {
	t.Parallel()
	single := &domain.Stream{Code: "up"} // no transcoder
	h := newABRHarness(t, single)
	assert.Nil(t, h.coord.detectABRCopy(copyDownstream("up")))
}

func TestDetectABRCopy_VideoCopyUpstream_ReturnsNil(t *testing.T) {
	t.Parallel()
	up := &domain.Stream{
		Code: "up",
		Transcoder: &domain.TranscoderConfig{
			Video: domain.VideoTranscodeConfig{Copy: true},
		},
	}
	h := newABRHarness(t, up)
	assert.Nil(t, h.coord.detectABRCopy(copyDownstream("up")))
}

func TestDetectABRCopy_ABRUpstream_ReturnsUpstream(t *testing.T) {
	t.Parallel()
	up := abrUpstream(3)
	h := newABRHarness(t, up)
	got := h.coord.detectABRCopy(copyDownstream("up"))
	require.NotNil(t, got)
	assert.Equal(t, domain.StreamCode("up"), got.Code)
}

// ─── mirror config ────────────────────────────────────────────────────────────

func TestMirrorTranscoderForCopy_NilSrc(t *testing.T) {
	t.Parallel()
	assert.Nil(t, mirrorTranscoderForCopy(nil))
}

func TestMirrorTranscoderForCopy_ClonesProfiles(t *testing.T) {
	t.Parallel()
	src := &domain.TranscoderConfig{
		Video: domain.VideoTranscodeConfig{
			Profiles: []domain.VideoProfile{
				{Width: 1920, Height: 1080, Bitrate: 4500},
				{Width: 1280, Height: 720, Bitrate: 3000},
			},
		},
	}
	out := mirrorTranscoderForCopy(src)
	require.NotNil(t, out)
	require.Len(t, out.Video.Profiles, 2)
	assert.Equal(t, 1920, out.Video.Profiles[0].Width)

	// Mutate the clone — original must not change.
	out.Video.Profiles[0].Width = 9999
	assert.Equal(t, 1920, src.Video.Profiles[0].Width, "src must not be aliased by clone")
}

// ─── pipeline lifecycle ───────────────────────────────────────────────────────

func TestStart_ABRCopy_BypassesIngestorAndTranscoder(t *testing.T) {
	t.Parallel()
	up := abrUpstream(2)
	h := newABRHarness(t, up)

	dn := copyDownstream("up")
	require.NoError(t, h.coord.Start(context.Background(), dn))
	defer h.coord.Stop(context.Background(), dn.Code)

	// Manager and transcoder must NOT be touched on the ABR-copy path.
	assert.False(t, h.mgr.IsRegistered("dn"), "manager must not be registered for ABR copy")
	h.tc.mu.Lock()
	assert.Empty(t, h.tc.started, "transcoder must not be started for ABR copy")
	h.tc.mu.Unlock()

	// Publisher must see the synthesized transcoder so it serves ABR.
	h.capPub.mu.Lock()
	require.Len(t, h.capPub.started, 1)
	pubStream := h.capPub.started[0]
	h.capPub.mu.Unlock()
	require.NotNil(t, pubStream.Transcoder, "publisher must receive synthesized transcoder")
	assert.Len(t, pubStream.Transcoder.Video.Profiles, 2)

	// Original downstream stream pointer must NOT have been mutated.
	assert.Nil(t, dn.Transcoder, "input stream pointer must not be mutated")

	assert.True(t, h.coord.IsRunning("dn"))
}

func TestStart_ABRCopy_CreatesDownstreamRenditionBuffers(t *testing.T) {
	t.Parallel()
	up := abrUpstream(3)
	h := newABRHarness(t, up)

	require.NoError(t, h.coord.Start(context.Background(), copyDownstream("up")))
	defer h.coord.Stop(context.Background(), "dn")

	for i := 0; i < 3; i++ {
		slug := buffer.VideoTrackSlug(i)
		bid := buffer.RenditionBufferID("dn", slug)
		_, err := h.buf.Subscribe(bid)
		assert.NoError(t, err, "downstream rendition buffer %s must exist", slug)
	}
}

func TestStop_ABRCopy_DeletesBuffersAndUnregisters(t *testing.T) {
	t.Parallel()
	up := abrUpstream(2)
	h := newABRHarness(t, up)

	require.NoError(t, h.coord.Start(context.Background(), copyDownstream("up")))
	require.True(t, h.coord.IsRunning("dn"))

	h.coord.Stop(context.Background(), "dn")
	assert.False(t, h.coord.IsRunning("dn"))

	// Rendition buffers must be gone.
	for i := 0; i < 2; i++ {
		bid := buffer.RenditionBufferID("dn", buffer.VideoTrackSlug(i))
		_, err := h.buf.Subscribe(bid)
		assert.Error(t, err, "downstream rendition buffer %s must be deleted", bid)
	}

	// Publisher Stop must have been called.
	h.pub.mu.Lock()
	assert.Contains(t, h.pub.stopped, domain.StreamCode("dn"))
	h.pub.mu.Unlock()
}

func TestStart_ABRCopy_UpstreamMissing_FallsThroughToNormalPath(t *testing.T) {
	t.Parallel()
	// Upstream not registered → detectABRCopy returns nil → Start follows
	// normal pipeline path (which will register with manager).
	h := newABRHarness(t)
	dn := copyDownstream("ghost")
	require.NoError(t, h.coord.Start(context.Background(), dn))
	defer h.coord.Stop(context.Background(), dn.Code)

	assert.True(t, h.mgr.IsRegistered("dn"), "must use normal path when upstream missing")
}

// ─── data flow through taps ──────────────────────────────────────────────────

func TestABRCopy_TapForwardsPacketsAcrossRungs(t *testing.T) {
	t.Parallel()
	up := abrUpstream(2)
	h := newABRHarness(t, up)

	// Pre-create upstream rendition buffers (mimics what coordinator.Start
	// would do for the upstream — we skip starting the upstream pipeline
	// because we only need its rendition buffers wired).
	upRends := buffer.RenditionsForTranscoder(up.Code, up.Transcoder)
	for _, r := range upRends {
		h.buf.Create(r.BufferID)
	}

	require.NoError(t, h.coord.Start(context.Background(), copyDownstream("up")))
	defer h.coord.Stop(context.Background(), "dn")

	// Subscribe to each downstream rendition.
	subs := make([]*buffer.Subscriber, len(upRends))
	for i, r := range upRends {
		downBid := buffer.RenditionBufferID("dn", r.Slug)
		s, err := h.buf.Subscribe(downBid)
		require.NoError(t, err)
		subs[i] = s
	}

	// Give tap goroutines a moment to subscribe to upstream buffers.
	// The tap loop subscribes synchronously on first iteration; a brief
	// sleep avoids the race where we Write before Subscribe ran.
	time.Sleep(50 * time.Millisecond)

	// Write distinct payloads to each upstream rendition.
	for i, r := range upRends {
		require.NoError(t, h.buf.Write(r.BufferID, buffer.TSPacket([]byte{byte(0xA0 + i)})))
	}

	// Each downstream subscriber should receive its rung's payload.
	for i, sub := range subs {
		select {
		case pkt := <-sub.Recv():
			require.Len(t, pkt.TS, 1)
			assert.Equal(t, byte(0xA0+i), pkt.TS[0],
				"downstream rendition %d should receive payload from matching upstream", i)
		case <-time.After(2 * time.Second):
			t.Fatalf("downstream rendition %d: no packet received within timeout", i)
		}
	}
}

// ABRCopyRuntimeStatus returns ok=false when the stream isn't running as an
// ABR copy — caller falls back to the normal manager.RuntimeStatus path.
func TestABRCopyRuntimeStatus_NotRunningReturnsFalse(t *testing.T) {
	t.Parallel()
	h := newABRHarness(t)
	_, ok := h.coord.ABRCopyRuntimeStatus("nonexistent")
	require.False(t, ok)
}

// Before any packet flows through a tap, status is Degraded (no recent
// activity) — distinguishes "tap goroutine spawned" from "actually feeding".
func TestABRCopyRuntimeStatus_DegradedBeforeFirstPacket(t *testing.T) {
	t.Parallel()
	up := abrUpstream(1)
	h := newABRHarness(t, up)
	require.NoError(t, h.coord.Start(context.Background(), copyDownstream("up")))
	defer h.coord.Stop(context.Background(), "dn")

	rt, ok := h.coord.ABRCopyRuntimeStatus("dn")
	require.True(t, ok)
	require.True(t, rt.PipelineActive)
	require.Equal(t, 0, rt.ActiveInputPriority)
	require.Len(t, rt.Inputs, 1)
	assert.Equal(t, domain.StatusDegraded, rt.Inputs[0].Status,
		"no packets seen yet → degraded (lets UI distinguish 'pipeline up but silent' from 'pipeline up and flowing')")
	assert.True(t, rt.Inputs[0].LastPacketAt.IsZero())
}

// After a packet flows through a tap, status flips to Active and
// LastPacketAt is stamped — this is what fixes the UI "UNKNOWN" badge.
func TestABRCopyRuntimeStatus_ActiveAfterTapPacket(t *testing.T) {
	t.Parallel()
	up := abrUpstream(2)
	h := newABRHarness(t, up)
	upRends := buffer.RenditionsForTranscoder(up.Code, up.Transcoder)
	for _, r := range upRends {
		h.buf.Create(r.BufferID)
	}
	require.NoError(t, h.coord.Start(context.Background(), copyDownstream("up")))
	defer h.coord.Stop(context.Background(), "dn")

	// Wait for tap subscribe + emit one packet on rung 0.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, h.buf.Write(upRends[0].BufferID, buffer.TSPacket([]byte{0x01})))

	// Poll briefly — the stamp happens after Write returns to the tap.
	require.Eventually(t, func() bool {
		rt, ok := h.coord.ABRCopyRuntimeStatus("dn")
		if !ok || len(rt.Inputs) != 1 {
			return false
		}
		return rt.Inputs[0].Status == domain.StatusActive
	}, 2*time.Second, 20*time.Millisecond, "input must flip to active after packet flows")

	rt, _ := h.coord.ABRCopyRuntimeStatus("dn")
	assert.False(t, rt.Inputs[0].LastPacketAt.IsZero(), "LastPacketAt must be stamped")
}

func TestABRCopy_TapStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	up := abrUpstream(1)
	h := newABRHarness(t, up)
	upRends := buffer.RenditionsForTranscoder(up.Code, up.Transcoder)
	for _, r := range upRends {
		h.buf.Create(r.BufferID)
	}

	require.NoError(t, h.coord.Start(context.Background(), copyDownstream("up")))

	// Stop must complete — the wg.Wait inside stopABRCopy will hang if any
	// tap goroutine ignores ctx cancellation.
	done := make(chan struct{})
	go func() {
		h.coord.Stop(context.Background(), "dn")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not return — taps did not honour ctx cancellation")
	}
}
