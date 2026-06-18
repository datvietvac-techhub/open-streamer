package coordinator

// update_test.go — integration tests for Coordinator.Update routing.
//
// Each test uses spy implementations of the four service interfaces (mgrDep, tcDep,
// pubDep, dvrDep) so we can assert which methods were called without starting real
// ingestors, FFmpeg processes, or RTSP servers.

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/dvr/blob"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/manager"
	"github.com/datvietvac-techhub/open-streamer/internal/publisher"
	"github.com/datvietvac-techhub/open-streamer/internal/transcoder"
)

// ─── spy implementations ──────────────────────────────────────────────────────

// spyMgr records calls to the manager interface.
type spyMgr struct {
	mu            sync.Mutex
	registered    map[domain.StreamCode]bool
	updatedInputs []domain.StreamCode
	updatedBufID  []domain.StreamCode
	exhaustedCB   func(domain.StreamCode)
	restoredCB    func(domain.StreamCode)
}

func newSpyMgr() *spyMgr {
	return &spyMgr{registered: make(map[domain.StreamCode]bool)}
}

func (m *spyMgr) IsRegistered(c domain.StreamCode) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.registered[c]
}

func (m *spyMgr) RegisteredStreams() []domain.StreamCode {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]domain.StreamCode, 0, len(m.registered))
	for c, ok := range m.registered {
		if ok {
			out = append(out, c)
		}
	}
	return out
}

func (m *spyMgr) Register(_ context.Context, s *domain.Stream, _ domain.StreamCode) error {
	m.mu.Lock()
	m.registered[s.Code] = true
	m.mu.Unlock()
	return nil
}

func (m *spyMgr) Unregister(c domain.StreamCode) {
	m.mu.Lock()
	delete(m.registered, c)
	m.mu.Unlock()
}

func (m *spyMgr) UpdateInputs(c domain.StreamCode, _, _, _ []domain.Input) {
	m.mu.Lock()
	m.updatedInputs = append(m.updatedInputs, c)
	m.mu.Unlock()
}

func (m *spyMgr) UpdateBufferWriteID(c domain.StreamCode, _ domain.StreamCode) {
	m.mu.Lock()
	m.updatedBufID = append(m.updatedBufID, c)
	m.mu.Unlock()
}

func (m *spyMgr) SetExhaustedCallback(fn func(domain.StreamCode)) {
	m.mu.Lock()
	m.exhaustedCB = fn
	m.mu.Unlock()
}

func (m *spyMgr) SetRestoredCallback(fn func(domain.StreamCode)) {
	m.mu.Lock()
	m.restoredCB = fn
	m.mu.Unlock()
}

func (m *spyMgr) RuntimeStatus(_ domain.StreamCode) (manager.RuntimeStatus, bool) {
	return manager.RuntimeStatus{}, false
}

// spyTC records calls to the transcoder interface.
type spyTC struct {
	mu              sync.Mutex
	started         []domain.StreamCode
	stopped         []domain.StreamCode
	profilesStopped []int
	profilesStarted []int
	startErr        error // when set, Start returns it (drives the A-6 reload-failure path)
}

func (t *spyTC) Start(_ context.Context, c domain.StreamCode, _ domain.StreamCode, _ *domain.TranscoderConfig, _ []transcoder.RenditionTarget) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.startErr != nil {
		return t.startErr
	}
	t.started = append(t.started, c)
	return nil
}

func (t *spyTC) Stop(c domain.StreamCode) {
	t.mu.Lock()
	t.stopped = append(t.stopped, c)
	t.mu.Unlock()
}

func (t *spyTC) StopProfile(_ domain.StreamCode, idx int) {
	t.mu.Lock()
	t.profilesStopped = append(t.profilesStopped, idx)
	t.mu.Unlock()
}

func (t *spyTC) StartProfile(_ domain.StreamCode, idx int, _ transcoder.RenditionTarget) error {
	t.mu.Lock()
	t.profilesStarted = append(t.profilesStarted, idx)
	t.mu.Unlock()
	return nil
}

// SetUnhealthyCallback / SetHealthyCallback are no-ops on the spy —
// tests that need to drive transcoder health transitions invoke the
// coordinator's handleTranscoderUnhealthy / handleTranscoderHealthy
// directly rather than going through the spy. Match the interface
// surface so the spy still satisfies tcDep.
func (t *spyTC) SetUnhealthyCallback(_ func(domain.StreamCode, string)) {}
func (t *spyTC) SetHealthyCallback(_ func(domain.StreamCode))           {}

// spyPub records calls to the publisher interface.
type spyPub struct {
	mu               sync.Mutex
	started          []domain.StreamCode
	stopped          []domain.StreamCode
	protocolsUpdated []domain.StreamCode
	hlsDashRestarted []domain.StreamCode
	abrMetaUpdated   []domain.StreamCode
}

func (p *spyPub) Start(_ context.Context, s *domain.Stream) error {
	p.mu.Lock()
	p.started = append(p.started, s.Code)
	p.mu.Unlock()
	return nil
}

func (p *spyPub) Stop(c domain.StreamCode) {
	p.mu.Lock()
	p.stopped = append(p.stopped, c)
	p.mu.Unlock()
}

func (p *spyPub) UpdateProtocols(_ context.Context, _, s *domain.Stream) error {
	p.mu.Lock()
	p.protocolsUpdated = append(p.protocolsUpdated, s.Code)
	p.mu.Unlock()
	return nil
}

func (p *spyPub) RestartHLSDASH(_ context.Context, s *domain.Stream) error {
	p.mu.Lock()
	p.hlsDashRestarted = append(p.hlsDashRestarted, s.Code)
	p.mu.Unlock()
	return nil
}

func (p *spyPub) UpdateABRMasterMeta(c domain.StreamCode, _ []publisher.ABRRepMeta) {
	p.mu.Lock()
	p.abrMetaUpdated = append(p.abrMetaUpdated, c)
	p.mu.Unlock()
}

// spyDVR records calls to the dvr interface.
type spyDVR struct {
	mu        sync.Mutex
	recording map[domain.StreamCode]bool
	started   []domain.StreamCode
	stopped   []domain.StreamCode
}

func newSpyDVR() *spyDVR {
	return &spyDVR{recording: make(map[domain.StreamCode]bool)}
}

func (d *spyDVR) IsRecording(c domain.StreamCode) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.recording[c]
}

func (d *spyDVR) StartRecording(_ context.Context, c domain.StreamCode, _ []blob.ProfileSub, _ *domain.StreamDVRConfig) (*domain.Recording, error) {
	d.mu.Lock()
	d.recording[c] = true
	d.started = append(d.started, c)
	d.mu.Unlock()
	return &domain.Recording{}, nil
}

func (d *spyDVR) StopRecording(_ context.Context, c domain.StreamCode) error {
	d.mu.Lock()
	delete(d.recording, c)
	d.stopped = append(d.stopped, c)
	d.mu.Unlock()
	return nil
}

// ─── harness ──────────────────────────────────────────────────────────────────

type testHarness struct {
	coord *Coordinator
	mgr   *spyMgr
	tc    *spyTC
	pub   *spyPub
	dvr   *spyDVR
	buf   *buffer.Service
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()
	buf := buffer.NewServiceForTesting(64)
	mgr := newSpyMgr()
	tc := &spyTC{}
	pub := &spyPub{}
	dvr := newSpyDVR()
	bus := events.New(1, 8)
	coord := newForTesting(buf, mgr, tc, pub, dvr, bus, nil)
	return &testHarness{coord: coord, mgr: mgr, tc: tc, pub: pub, dvr: dvr, buf: buf}
}

// simulateRunning marks a stream as "running" in the spy manager, creates its
// buffer, and records renditions (mimics what Coordinator.Start does).
func (h *testHarness) simulateRunning(stream *domain.Stream) {
	h.buf.Create(stream.Code)
	_ = h.mgr.Register(context.Background(), stream, stream.Code)
}

// ─── tests ────────────────────────────────────────────────────────────────────

func TestUpdate_NilStreams_ReturnsError(t *testing.T) {
	h := newHarness(t)
	err := h.coord.Update(context.Background(), nil, baseStream())
	require.Error(t, err)
	err = h.coord.Update(context.Background(), baseStream(), nil)
	require.Error(t, err)
}

func TestUpdate_NowDisabled_StopsPipeline(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	h.simulateRunning(old)
	require.True(t, h.coord.IsRunning(old.Code))

	new := baseStream()
	new.Disabled = true

	err := h.coord.Update(context.Background(), old, new)
	require.NoError(t, err)
	assert.False(t, h.coord.IsRunning(old.Code), "pipeline should be stopped")
	assert.Contains(t, h.pub.stopped, old.Code)
}

func TestUpdate_InputsAdded_CallsManagerUpdateInputs(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	h.simulateRunning(old)

	new := baseStream()
	new.Inputs = append(new.Inputs, domain.Input{URL: "http://c.m3u8", Priority: 2})

	require.NoError(t, h.coord.Update(context.Background(), old, new))
	assert.Contains(t, h.mgr.updatedInputs, old.Code)
}

func TestUpdate_InputsRemoved_CallsManagerUpdateInputs(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	h.simulateRunning(old)

	new := baseStream()
	new.Inputs = new.Inputs[:1] // remove priority-1 input

	require.NoError(t, h.coord.Update(context.Background(), old, new))
	assert.Contains(t, h.mgr.updatedInputs, old.Code)
}

func TestUpdate_InputsUnchanged_DoesNotCallManagerUpdateInputs(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	h.simulateRunning(old)

	new := baseStream()
	new.Protocols.DASH = true // change something else

	require.NoError(t, h.coord.Update(context.Background(), old, new))
	assert.Empty(t, h.mgr.updatedInputs, "UpdateInputs must not be called when inputs are unchanged")
}

func TestUpdate_TranscoderTopologyChanged_FullReload(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	old.Transcoder = nil
	h.simulateRunning(old)

	new := baseStream() // adds transcoder

	require.NoError(t, h.coord.Update(context.Background(), old, new))
	// Full reload: publisher must be stopped then started.
	assert.Contains(t, h.pub.stopped, old.Code)
	assert.Contains(t, h.pub.started, old.Code)
	// Transcoder must be started.
	assert.Contains(t, h.tc.started, old.Code)
}

// The native transcoder subprocess owns all renditions (no per-profile
// lifecycle), so ANY transcoder change — a per-profile video tweak, a profile
// add/remove, or an audio/global/decoder change — restarts the whole subprocess
// and the publisher via reloadTranscoderFull.

func TestUpdate_ProfileUpdated_FullSubprocessReload(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	h.simulateRunning(old)

	new := baseStream()
	new.Transcoder.Video.Profiles[1] = domain.VideoProfile{
		Width: 320, Height: 180, Bitrate: 400, Codec: "h264",
	}

	require.NoError(t, h.coord.Update(context.Background(), old, new))

	// Full reload: subprocess + publisher restarted.
	assert.Contains(t, h.tc.stopped, old.Code)
	assert.Contains(t, h.tc.started, old.Code)
	assert.Contains(t, h.pub.stopped, old.Code)
	assert.Contains(t, h.pub.started, old.Code)
}

func TestUpdate_ProfileAdded_FullReload(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	h.simulateRunning(old)

	new := baseStream()
	new.Transcoder.Video.Profiles = append(new.Transcoder.Video.Profiles,
		domain.VideoProfile{Width: 320, Height: 180, Bitrate: 400, Codec: "h264"},
	)

	require.NoError(t, h.coord.Update(context.Background(), old, new))

	assert.Contains(t, h.tc.started, old.Code)
	assert.Contains(t, h.pub.stopped, old.Code)
	assert.Contains(t, h.pub.started, old.Code)
}

func TestUpdate_ProfileRemoved_FullReload(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	h.simulateRunning(old)

	new := baseStream()
	new.Transcoder.Video.Profiles = new.Transcoder.Video.Profiles[:1] // keep FHD only

	require.NoError(t, h.coord.Update(context.Background(), old, new))

	assert.Contains(t, h.tc.started, old.Code)
	assert.Contains(t, h.pub.stopped, old.Code)
	assert.Contains(t, h.pub.started, old.Code)
}

// TestUpdate_AudioChanged_FullReload is the regression for the live-update bug:
// an audio config change used to route to the per-profile reload (StartProfile)
// and fail with ErrNotImplemented. It must now restart the whole subprocess.
func TestUpdate_AudioChanged_FullReload(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	h.simulateRunning(old)

	new := baseStream()
	new.Transcoder.Audio.Bitrate = old.Transcoder.Audio.Bitrate + 64 // any audio change

	require.NoError(t, h.coord.Update(context.Background(), old, new))

	assert.Contains(t, h.tc.stopped, old.Code)
	assert.Contains(t, h.tc.started, old.Code)
	assert.Contains(t, h.pub.stopped, old.Code)
	assert.Contains(t, h.pub.started, old.Code)
}

func TestUpdate_ProtocolChanged_UpdateProtocolsCalled(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	h.simulateRunning(old)

	new := baseStream()
	new.Protocols.DASH = true // toggle DASH on

	require.NoError(t, h.coord.Update(context.Background(), old, new))

	assert.Contains(t, h.pub.protocolsUpdated, old.Code)
	// Full publisher stop should NOT be called.
	assert.Empty(t, h.pub.stopped)
}

func TestUpdate_PushChanged_UpdateProtocolsCalled(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	h.simulateRunning(old)

	new := baseStream()
	new.Push = []domain.PushDestination{{URL: "rtmp://yt.com/live/key", Enabled: true}}

	require.NoError(t, h.coord.Update(context.Background(), old, new))

	assert.Contains(t, h.pub.protocolsUpdated, old.Code)
	assert.Empty(t, h.pub.stopped)
}

func TestUpdate_DVREnabled_StartsRecording(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	h.simulateRunning(old)

	new := baseStream()
	new.DVR = &domain.StreamDVRConfig{Enabled: true, RetentionSec: 3600}

	require.NoError(t, h.coord.Update(context.Background(), old, new))

	assert.Contains(t, h.dvr.started, old.Code)
}

func TestUpdate_DVRDisabled_StopsRecording(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	old.DVR = &domain.StreamDVRConfig{Enabled: true, RetentionSec: 3600}
	h.simulateRunning(old)
	// Simulate DVR already recording.
	_, _ = h.dvr.StartRecording(context.Background(), old.Code, nil, old.DVR)

	new := baseStream()
	new.DVR = &domain.StreamDVRConfig{Enabled: false}

	require.NoError(t, h.coord.Update(context.Background(), old, new))

	assert.Contains(t, h.dvr.stopped, old.Code)
}

func TestUpdate_NoChanges_NothingCalled(t *testing.T) {
	h := newHarness(t)
	old := baseStream()
	h.simulateRunning(old)

	new := baseStream()

	require.NoError(t, h.coord.Update(context.Background(), old, new))

	assert.Empty(t, h.mgr.updatedInputs)
	assert.Empty(t, h.tc.profilesStopped)
	assert.Empty(t, h.tc.profilesStarted)
	assert.Empty(t, h.pub.stopped)
	assert.Empty(t, h.pub.protocolsUpdated)
	assert.Empty(t, h.pub.hlsDashRestarted)
	assert.Empty(t, h.pub.abrMetaUpdated)
	assert.Empty(t, h.dvr.started)
}
