package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/dvr"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

// ─── Stubs ────────────────────────────────────────────────────────────────────

type fakeRecRepo struct {
	mu      sync.Mutex
	byID    map[domain.RecordingID]*domain.Recording
	byCode  map[domain.StreamCode][]*domain.Recording
	listErr error
	findErr error
}

func newFakeRecRepo() *fakeRecRepo {
	return &fakeRecRepo{
		byID:   make(map[domain.RecordingID]*domain.Recording),
		byCode: make(map[domain.StreamCode][]*domain.Recording),
	}
}

func (f *fakeRecRepo) seed(rec *domain.Recording) {
	f.mu.Lock()
	defer f.mu.Unlock()
	clone := *rec
	f.byID[rec.ID] = &clone
	f.byCode[rec.StreamCode] = append(f.byCode[rec.StreamCode], &clone)
}

func (f *fakeRecRepo) ListByStream(_ context.Context, code domain.StreamCode) ([]*domain.Recording, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.byCode[code], nil
}

func (f *fakeRecRepo) List(_ context.Context) ([]*domain.Recording, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]*domain.Recording, 0, len(f.byID))
	for _, r := range f.byID {
		out = append(out, r)
	}
	return out, nil
}

func (f *fakeRecRepo) FindByID(_ context.Context, id domain.RecordingID) (*domain.Recording, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.findErr != nil {
		return nil, f.findErr
	}
	if r, ok := f.byID[id]; ok {
		clone := *r
		return &clone, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeRecRepo) Save(_ context.Context, r *domain.Recording) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	clone := *r
	f.byID[r.ID] = &clone
	return nil
}

func (f *fakeRecRepo) Delete(_ context.Context, id domain.RecordingID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.byID, id)
	return nil
}

// stubDVR implements dvrIndexReader with controllable returns.
type stubDVR struct {
	indexByDir map[string]*domain.DVRIndex
	indexErr   error
	playByDir  map[string][]dvr.SegmentMeta
	playErr    error
}

func (s *stubDVR) LoadIndex(segDir string) (*domain.DVRIndex, error) {
	if s.indexErr != nil {
		return nil, s.indexErr
	}
	return s.indexByDir[segDir], nil
}

func (s *stubDVR) ParsePlaylist(segDir string) ([]dvr.SegmentMeta, error) {
	if s.playErr != nil {
		return nil, s.playErr
	}
	return s.playByDir[segDir], nil
}

func newRecHandlerForTest(t *testing.T) (*RecordingHandler, *fakeRecRepo, *stubDVR) {
	t.Helper()
	repo := newFakeRecRepo()
	d := &stubDVR{indexByDir: map[string]*domain.DVRIndex{}, playByDir: map[string][]dvr.SegmentMeta{}}
	h := &RecordingHandler{
		dvr:     d,
		recRepo: repo,
	}
	return h, repo, d
}

// recReq builds a request whose chi route context exposes "code" as the
// URL param the catch-all dispatcher would have populated in production.
// All recording routes are GET-only — the method parameter is kept so the
// helper signature reads symmetrically with chiReq, even though every
// caller passes http.MethodGet today.
func recReq(t *testing.T, method, target, code string) *http.Request { //nolint:unparam // see doc
	t.Helper()
	return chiReq(t, method, target, nil, map[string]string{"code": code})
}

// ─── RecordingStatusJSON ─────────────────────────────────────────────────────

func TestRecordingStatusJSON_NoSegmentDir_404(t *testing.T) {
	t.Parallel()
	h, repo, _ := newRecHandlerForTest(t)
	repo.seed(&domain.Recording{ID: "blank", StreamCode: "blank"}) // SegmentDir empty
	w := httptest.NewRecorder()
	h.RecordingStatusJSON(w, recReq(t, http.MethodGet, "/blank/recording_status.json", "blank"))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestRecordingStatusJSON_HappyPath(t *testing.T) {
	t.Parallel()
	h, repo, d := newRecHandlerForTest(t)
	dir := t.TempDir()
	repo.seed(&domain.Recording{ID: "live", StreamCode: "live", SegmentDir: dir})
	d.indexByDir[dir] = &domain.DVRIndex{
		StartedAt:      time.Now().Add(-time.Hour),
		LastSegmentAt:  time.Now().Add(-5 * time.Minute),
		SegmentCount:   42,
		TotalSizeBytes: 12345,
	}

	w := httptest.NewRecorder()
	h.RecordingStatusJSON(w, recReq(t, http.MethodGet, "/live/recording_status.json", "live"))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"segment_count":42`)
}

func TestRecordingStatusJSON_NotFound(t *testing.T) {
	t.Parallel()
	h, _, _ := newRecHandlerForTest(t)
	w := httptest.NewRecorder()
	h.RecordingStatusJSON(w, recReq(t, http.MethodGet, "/missing/recording_status.json", "missing"))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestRecordingStatusJSON_IndexErrorReturns500(t *testing.T) {
	t.Parallel()
	h, repo, d := newRecHandlerForTest(t)
	dir := t.TempDir()
	repo.seed(&domain.Recording{ID: "live", StreamCode: "live", SegmentDir: dir})
	d.indexErr = errors.New("disk gone")
	w := httptest.NewRecorder()
	h.RecordingStatusJSON(w, recReq(t, http.MethodGet, "/live/recording_status.json", "live"))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestRecordingStatusJSON_NilIndexReturns404(t *testing.T) {
	t.Parallel()
	h, repo, d := newRecHandlerForTest(t)
	dir := t.TempDir()
	repo.seed(&domain.Recording{ID: "live", StreamCode: "live", SegmentDir: dir})
	d.indexByDir[dir] = nil
	w := httptest.NewRecorder()
	h.RecordingStatusJSON(w, recReq(t, http.MethodGet, "/live/recording_status.json", "live"))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestRecordingStatusJSON_InvalidStreamCode_400(t *testing.T) {
	t.Parallel()
	h, _, _ := newRecHandlerForTest(t)
	w := httptest.NewRecorder()
	h.RecordingStatusJSON(w, recReq(t, http.MethodGet, "/$bad/recording_status.json", "$bad"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// ─── ServeTimeshift (from/delay/dur/ago params) ─────────────────────────────

func TestServeTimeshift_NoPlaylistReturns404(t *testing.T) {
	t.Parallel()
	h, repo, d := newRecHandlerForTest(t)
	dir := t.TempDir()
	repo.seed(&domain.Recording{ID: "live", StreamCode: "live", SegmentDir: dir})
	d.playByDir[dir] = nil
	w := httptest.NewRecorder()
	h.ServeTimeshift(w, recReq(t, http.MethodGet, "/live/index.m3u8?delay=10", "live"))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestServeTimeshift_FromUnixSeconds(t *testing.T) {
	t.Parallel()
	h, repo, d := newRecHandlerForTest(t)
	dir := t.TempDir()
	repo.seed(&domain.Recording{ID: "live", StreamCode: "live", SegmentDir: dir})

	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	d.playByDir[dir] = []dvr.SegmentMeta{
		{Index: 1, WallTime: base, Duration: 4 * time.Second},
		{Index: 2, WallTime: base.Add(4 * time.Second), Duration: 4 * time.Second},
	}

	url := fmt.Sprintf("/live/index.m3u8?from=%d", base.Unix())
	w := httptest.NewRecorder()
	h.ServeTimeshift(w, recReq(t, http.MethodGet, url, "live"))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "#EXT-X-PLAYLIST-TYPE:VOD")
	assert.Contains(t, w.Body.String(), "000001.ts")
	assert.Contains(t, w.Body.String(), "#EXT-X-ENDLIST")
}

func TestServeTimeshift_DelayFromNow(t *testing.T) {
	t.Parallel()
	h, repo, d := newRecHandlerForTest(t)
	dir := t.TempDir()
	repo.seed(&domain.Recording{ID: "live", StreamCode: "live", SegmentDir: dir})

	// One segment ~30s in the past — a delay=60 query should include it
	// (window starts 60s before now), delay=5 should not.
	d.playByDir[dir] = []dvr.SegmentMeta{
		{Index: 1, WallTime: time.Now().Add(-30 * time.Second), Duration: 4 * time.Second},
	}

	w := httptest.NewRecorder()
	h.ServeTimeshift(w, recReq(t, http.MethodGet, "/live/index.m3u8?delay=60", "live"))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "000001.ts")

	w = httptest.NewRecorder()
	h.ServeTimeshift(w, recReq(t, http.MethodGet, "/live/index.m3u8?delay=5", "live"))
	assert.Equal(t, http.StatusNotFound, w.Code, "delay=5 starts after the segment ended")
}

// `ago` is documented as an alias of `delay`; behaviour must match.
func TestServeTimeshift_AgoIsAliasOfDelay(t *testing.T) {
	t.Parallel()
	h, repo, d := newRecHandlerForTest(t)
	dir := t.TempDir()
	repo.seed(&domain.Recording{ID: "live", StreamCode: "live", SegmentDir: dir})
	d.playByDir[dir] = []dvr.SegmentMeta{
		{Index: 1, WallTime: time.Now().Add(-30 * time.Second), Duration: 4 * time.Second},
	}

	w := httptest.NewRecorder()
	h.ServeTimeshift(w, recReq(t, http.MethodGet, "/live/index.m3u8?ago=60", "live"))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "000001.ts")
}

func TestServeTimeshift_DurWindowsClipLength(t *testing.T) {
	t.Parallel()
	h, repo, d := newRecHandlerForTest(t)
	dir := t.TempDir()
	repo.seed(&domain.Recording{ID: "live", StreamCode: "live", SegmentDir: dir})

	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	d.playByDir[dir] = []dvr.SegmentMeta{
		{Index: 1, WallTime: base, Duration: 4 * time.Second},
		{Index: 2, WallTime: base.Add(4 * time.Second), Duration: 4 * time.Second},
		{Index: 3, WallTime: base.Add(8 * time.Second), Duration: 4 * time.Second},
	}

	// from=base, dur=5 → segment 1 (wall_time base) included, 2 (wall_time
	// base+4) included since base+4 <= base+5, but 3 (base+8) excluded.
	url := fmt.Sprintf("/live/index.m3u8?from=%d&dur=5", base.Unix())
	w := httptest.NewRecorder()
	h.ServeTimeshift(w, recReq(t, http.MethodGet, url, "live"))
	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	assert.Contains(t, body, "000001.ts")
	assert.Contains(t, body, "000002.ts")
	assert.NotContains(t, body, "000003.ts")
}

func TestServeTimeshift_InvalidFromReturns400(t *testing.T) {
	t.Parallel()
	h, repo, d := newRecHandlerForTest(t)
	dir := t.TempDir()
	repo.seed(&domain.Recording{ID: "live", StreamCode: "live", SegmentDir: dir})
	d.playByDir[dir] = []dvr.SegmentMeta{{Index: 1, WallTime: time.Now(), Duration: time.Second}}

	w := httptest.NewRecorder()
	h.ServeTimeshift(w, recReq(t, http.MethodGet, "/live/index.m3u8?from=garbage", "live"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestServeTimeshift_InvalidDurReturns400(t *testing.T) {
	t.Parallel()
	h, repo, d := newRecHandlerForTest(t)
	dir := t.TempDir()
	repo.seed(&domain.Recording{ID: "live", StreamCode: "live", SegmentDir: dir})
	d.playByDir[dir] = []dvr.SegmentMeta{{Index: 1, WallTime: time.Now(), Duration: time.Second}}

	w := httptest.NewRecorder()
	h.ServeTimeshift(w, recReq(t, http.MethodGet, "/live/index.m3u8?from=1700000000&dur=-1", "live"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestServeTimeshift_NoSegmentDirReturns404(t *testing.T) {
	t.Parallel()
	h, repo, _ := newRecHandlerForTest(t)
	repo.seed(&domain.Recording{ID: "live", StreamCode: "live"})
	w := httptest.NewRecorder()
	h.ServeTimeshift(w, recReq(t, http.MethodGet, "/live/index.m3u8?delay=10", "live"))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestServeTimeshift_RecordingNotFound(t *testing.T) {
	t.Parallel()
	h, _, _ := newRecHandlerForTest(t)
	w := httptest.NewRecorder()
	h.ServeTimeshift(w, recReq(t, http.MethodGet, "/missing/index.m3u8?from=1700000000", "missing"))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestServeTimeshift_InvalidStreamCode_400(t *testing.T) {
	t.Parallel()
	h, _, _ := newRecHandlerForTest(t)
	w := httptest.NewRecorder()
	h.ServeTimeshift(w, recReq(t, http.MethodGet, "/$bad/index.m3u8?delay=10", "$bad"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}
