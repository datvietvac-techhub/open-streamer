// service_test.go — exercises the public Service API (StartRecording,
// StopRecording, IsRecording, LoadIndex, ParsePlaylist) using NewForTesting
// + an in-memory RecordingRepository fake.

package dvr

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/metrics"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

// ─── fakes ───────────────────────────────────────────────────────────────────

// fakeRecordingRepo is an in-memory RecordingRepository used by tests so we
// don't have to spin up a JSON/YAML store on disk.
type fakeRecordingRepo struct {
	mu      sync.Mutex
	byID    map[domain.RecordingID]*domain.Recording
	saveErr error // when set, Save returns this; nil = success
}

func newFakeRepo() *fakeRecordingRepo {
	return &fakeRecordingRepo{byID: make(map[domain.RecordingID]*domain.Recording)}
}

func (f *fakeRecordingRepo) Save(_ context.Context, rec *domain.Recording) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	cp := *rec
	f.byID[rec.ID] = &cp
	return nil
}

func (f *fakeRecordingRepo) FindByID(_ context.Context, id domain.RecordingID) (*domain.Recording, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.byID[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *rec
	return &cp, nil
}

func (f *fakeRecordingRepo) ListByStream(_ context.Context, streamCode domain.StreamCode) ([]*domain.Recording, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*domain.Recording
	for _, rec := range f.byID {
		if rec.StreamCode == streamCode {
			cp := *rec
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (f *fakeRecordingRepo) Delete(_ context.Context, id domain.RecordingID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.byID, id)
	return nil
}

// newDVRTestMetrics returns a Metrics with only the collectors DVR touches,
// bound to fresh prometheus.Vecs (no global-registry pollution).
func newDVRTestMetrics() *metrics.Metrics {
	return &metrics.Metrics{
		DVRSegmentsWrittenTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_dvr_segments_written_total",
		}, []string{"stream_code"}),
		DVRBytesWrittenTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_dvr_bytes_written_total",
		}, []string{"stream_code"}),
		DVRRecordingActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_dvr_recording_active",
		}, []string{"stream_code"}),
	}
}

// newDVRSvc builds a Service for tests with real buffer + bus + fake repo.
func newDVRSvc(t *testing.T) (*Service, *fakeRecordingRepo, *buffer.Service) {
	t.Helper()
	buf := buffer.NewServiceForTesting(64)
	bus := events.New(0, 8)
	repo := newFakeRepo()
	svc := NewForTesting(buf, bus, newDVRTestMetrics(), repo)
	t.Cleanup(func() {
		// Stop any active recordings so their goroutines exit before the
		// test ends — avoids leaks when -race detector is on.
		svc.mu.Lock()
		codes := make([]domain.StreamCode, 0, len(svc.sessions))
		for c := range svc.sessions {
			codes = append(codes, c)
		}
		svc.mu.Unlock()
		for _, c := range codes {
			_ = svc.StopRecording(context.Background(), c)
		}
	})
	return svc, repo, buf
}

// dvrCfg is a minimal enabled DVR config rooted at a temp dir.
func dvrCfg(t *testing.T) *domain.StreamDVRConfig {
	return &domain.StreamDVRConfig{
		Enabled:         true,
		SegmentDuration: 1, // 1s segments — keep tests fast
		StoragePath:     t.TempDir(),
	}
}

// ─── StartRecording ──────────────────────────────────────────────────────────

func TestStartRecording_DisabledConfigErrors(t *testing.T) {
	t.Parallel()
	svc, _, _ := newDVRSvc(t)
	_, err := svc.StartRecording(context.Background(), "s1", "s1", &domain.StreamDVRConfig{Enabled: false})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not enabled")
}

func TestStartRecording_NilConfigErrors(t *testing.T) {
	t.Parallel()
	svc, _, _ := newDVRSvc(t)
	_, err := svc.StartRecording(context.Background(), "s1", "s1", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not enabled")
}

func TestStartRecording_HappyPath(t *testing.T) {
	t.Parallel()
	svc, repo, buf := newDVRSvc(t)
	buf.Create("s2") // recording subscribes to media buffer

	cfg := dvrCfg(t)
	rec, err := svc.StartRecording(context.Background(), "s2", "s2", cfg)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, domain.RecordingID("s2"), rec.ID)
	assert.Equal(t, domain.StreamCode("s2"), rec.StreamCode)
	assert.Equal(t, domain.RecordingStatusRecording, rec.Status)
	assert.True(t, svc.IsRecording("s2"))

	// Repo must have the recording persisted.
	saved, err := repo.FindByID(context.Background(), "s2")
	require.NoError(t, err)
	assert.Equal(t, domain.RecordingStatusRecording, saved.Status)
	// Storage path under StoragePath/{streamCode}.
	assert.True(t, filepath.IsAbs(rec.SegmentDir))
}

func TestStartRecording_DoubleStartErrors(t *testing.T) {
	t.Parallel()
	svc, _, buf := newDVRSvc(t)
	buf.Create("s3")
	cfg := dvrCfg(t)
	_, err := svc.StartRecording(context.Background(), "s3", "s3", cfg)
	require.NoError(t, err)

	_, err = svc.StartRecording(context.Background(), "s3", "s3", cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already recording")
}

// When the buffer doesn't exist, the recording starts (the recording loop
// silently exits on the missing subscription). The recording session is
// still registered — operators can fix the buffer + restart.
func TestStartRecording_MissingBufferStillStartsButLoopExits(t *testing.T) {
	t.Parallel()
	svc, _, _ := newDVRSvc(t)
	cfg := dvrCfg(t)
	rec, err := svc.StartRecording(context.Background(), "s4", "missing-buf", cfg)
	require.NoError(t, err)
	assert.Equal(t, domain.RecordingStatusRecording, rec.Status)
	assert.True(t, svc.IsRecording("s4"))
}

// Start propagates the underlying repo error so callers get a clear message.
func TestStartRecording_RepoSaveFailureSurfaces(t *testing.T) {
	t.Parallel()
	svc, repo, buf := newDVRSvc(t)
	buf.Create("s5")
	repo.saveErr = errors.New("disk full")
	_, err := svc.StartRecording(context.Background(), "s5", "s5", dvrCfg(t))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "save recording")
	assert.False(t, svc.IsRecording("s5"), "failed save must not register session")
}

// ─── StopRecording ───────────────────────────────────────────────────────────

func TestStopRecording_NoActiveRecordingErrors(t *testing.T) {
	t.Parallel()
	svc, _, _ := newDVRSvc(t)
	err := svc.StopRecording(context.Background(), "ghost")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active recording")
}

func TestStopRecording_HappyPath(t *testing.T) {
	t.Parallel()
	svc, repo, buf := newDVRSvc(t)
	buf.Create("s6")
	_, err := svc.StartRecording(context.Background(), "s6", "s6", dvrCfg(t))
	require.NoError(t, err)
	require.True(t, svc.IsRecording("s6"))

	require.NoError(t, svc.StopRecording(context.Background(), "s6"))
	assert.False(t, svc.IsRecording("s6"))

	// Repo must reflect Stopped status + StoppedAt populated.
	saved, err := repo.FindByID(context.Background(), "s6")
	require.NoError(t, err)
	assert.Equal(t, domain.RecordingStatusStopped, saved.Status)
	require.NotNil(t, saved.StoppedAt)
	assert.WithinDuration(t, time.Now(), *saved.StoppedAt, 5*time.Second)
}

// ─── IsRecording ─────────────────────────────────────────────────────────────

func TestIsRecording_FalseByDefault(t *testing.T) {
	t.Parallel()
	svc, _, _ := newDVRSvc(t)
	assert.False(t, svc.IsRecording("never-started"))
}

// ─── LoadIndex / ParsePlaylist exported wrappers ─────────────────────────────

func TestLoadIndex_DelegatesToInternalLoader(t *testing.T) {
	t.Parallel()
	svc, _, _ := newDVRSvc(t)
	dir := t.TempDir()

	// Empty dir → loadIndex returns (nil, nil) for a fresh recording.
	idx, err := svc.LoadIndex(dir)
	require.NoError(t, err)
	assert.Nil(t, idx, "empty dir → no index → returns nil")
}

func TestParsePlaylist_DelegatesAndReturnsExportedShape(t *testing.T) {
	t.Parallel()
	svc, _, _ := newDVRSvc(t)
	dir := t.TempDir()

	// No playlist.m3u8 → parsePlaylist returns (nil, nil).
	out, err := svc.ParsePlaylist(dir)
	require.NoError(t, err)
	assert.Empty(t, out)
}
