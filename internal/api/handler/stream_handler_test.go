package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/autopublish"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/manager"
	"github.com/datvietvac-techhub/open-streamer/internal/publisher"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
	"github.com/datvietvac-techhub/open-streamer/internal/transcoder"
)

// ─── Stubs ────────────────────────────────────────────────────────────────────

type fakeStreamRepoFull struct {
	mu      sync.Mutex
	items   map[domain.StreamCode]*domain.Stream
	listErr error
	saveErr error
	delErr  error
}

func newFakeStreamRepoFull() *fakeStreamRepoFull {
	return &fakeStreamRepoFull{items: make(map[domain.StreamCode]*domain.Stream)}
}

func (f *fakeStreamRepoFull) seed(streams ...*domain.Stream) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range streams {
		clone := *s
		f.items[s.Code] = &clone
	}
}

func (f *fakeStreamRepoFull) List(_ context.Context, _ store.StreamFilter) ([]*domain.Stream, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]*domain.Stream, 0, len(f.items))
	for _, s := range f.items {
		clone := *s
		out = append(out, &clone)
	}
	return out, nil
}

func (f *fakeStreamRepoFull) FindByCode(_ context.Context, code domain.StreamCode) (*domain.Stream, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.items[code]; ok {
		clone := *s
		return &clone, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeStreamRepoFull) Save(_ context.Context, s *domain.Stream) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	clone := *s
	f.items[s.Code] = &clone
	return nil
}

func (f *fakeStreamRepoFull) Delete(_ context.Context, code domain.StreamCode) error {
	if f.delErr != nil {
		return f.delErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.items, code)
	return nil
}

// stubCoord satisfies streamCoordinator with controllable per-method
// behaviour. Each method records the codes it received so tests can
// assert on dispatch. Default returns are zero-value friendly so tests
// only set the fields they care about.
type stubCoord struct {
	mu              sync.Mutex
	startErr        error
	updateErr       error
	statusByCode    map[domain.StreamCode]domain.StreamStatus
	runningByCode   map[domain.StreamCode]bool
	startedAtByCode map[domain.StreamCode]time.Time
	startedCodes    []domain.StreamCode
	stoppedCodes    []domain.StreamCode
	updatedCodes    []domain.StreamCode
	abrCopyByCode   map[domain.StreamCode]manager.RuntimeStatus
	abrMixerByCode  map[domain.StreamCode]manager.RuntimeStatus
}

func newStubCoord() *stubCoord {
	return &stubCoord{
		statusByCode:    make(map[domain.StreamCode]domain.StreamStatus),
		runningByCode:   make(map[domain.StreamCode]bool),
		startedAtByCode: make(map[domain.StreamCode]time.Time),
		abrCopyByCode:   make(map[domain.StreamCode]manager.RuntimeStatus),
		abrMixerByCode:  make(map[domain.StreamCode]manager.RuntimeStatus),
	}
}

func (s *stubCoord) StreamStatus(code domain.StreamCode) domain.StreamStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.statusByCode[code]; ok {
		return v
	}
	return domain.StatusStopped
}

func (s *stubCoord) StreamStartedAt(code domain.StreamCode) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.startedAtByCode[code]
	return t, ok
}

func (s *stubCoord) IsRunning(code domain.StreamCode) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runningByCode[code]
}

func (s *stubCoord) Start(_ context.Context, stream *domain.Stream) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startedCodes = append(s.startedCodes, stream.Code)
	if s.startErr == nil {
		s.runningByCode[stream.Code] = true
	}
	return s.startErr
}

func (s *stubCoord) Stop(_ context.Context, code domain.StreamCode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stoppedCodes = append(s.stoppedCodes, code)
	delete(s.runningByCode, code)
}

func (s *stubCoord) Update(_ context.Context, _, new *domain.Stream) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updatedCodes = append(s.updatedCodes, new.Code)
	return s.updateErr
}

func (s *stubCoord) ABRCopyRuntimeStatus(code domain.StreamCode) (manager.RuntimeStatus, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.abrCopyByCode[code]
	return v, ok
}

func (s *stubCoord) ABRMixerRuntimeStatus(code domain.StreamCode) (manager.RuntimeStatus, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.abrMixerByCode[code]
	return v, ok
}

type stubMgr struct {
	mu              sync.Mutex
	statusByCode    map[domain.StreamCode]manager.RuntimeStatus
	registeredCodes map[domain.StreamCode]bool
	switchErr       error
	switchedTo      map[domain.StreamCode]int
}

func newStubMgr() *stubMgr {
	return &stubMgr{
		statusByCode:    make(map[domain.StreamCode]manager.RuntimeStatus),
		registeredCodes: make(map[domain.StreamCode]bool),
		switchedTo:      make(map[domain.StreamCode]int),
	}
}

func (s *stubMgr) RuntimeStatus(code domain.StreamCode) (manager.RuntimeStatus, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.statusByCode[code]
	return v, s.registeredCodes[code]
}

func (s *stubMgr) SwitchInput(code domain.StreamCode, priority int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.switchErr != nil {
		return s.switchErr
	}
	s.switchedTo[code] = priority
	return nil
}

type stubTranscoder struct {
	statusByCode map[domain.StreamCode]transcoder.RuntimeStatus
}

func (s *stubTranscoder) RuntimeStatus(code domain.StreamCode) (transcoder.RuntimeStatus, bool) {
	if s == nil || s.statusByCode == nil {
		return transcoder.RuntimeStatus{}, false
	}
	v, ok := s.statusByCode[code]
	return v, ok
}

type stubPublisher struct {
	statusByCode map[domain.StreamCode]publisher.RuntimeStatus
}

func (s *stubPublisher) RuntimeStatus(code domain.StreamCode) (publisher.RuntimeStatus, bool) {
	if s == nil || s.statusByCode == nil {
		return publisher.RuntimeStatus{}, false
	}
	v, ok := s.statusByCode[code]
	return v, ok
}

type fakeBus struct {
	mu     sync.Mutex
	events []domain.Event
}

func (b *fakeBus) Publish(_ context.Context, ev domain.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, ev)
}

func (b *fakeBus) Subscribe(_ domain.EventType, _ events.HandlerFunc) func() {
	return func() {}
}

// newStreamHandlerForTest wires the StreamHandler with stubs across every
// dependency so each test starts from a controllable baseline.
func newStreamHandlerForTest(t *testing.T) (*StreamHandler, *fakeStreamRepoFull, *stubCoord, *stubMgr, *fakeBus) {
	t.Helper()
	repo := newFakeStreamRepoFull()
	co := newStubCoord()
	mg := newStubMgr()
	tc := &stubTranscoder{}
	pb := &stubPublisher{}
	bus := &fakeBus{}
	h := &StreamHandler{
		streamRepo:  repo,
		coordinator: co,
		manager:     mg,
		transcoder:  tc,
		publisher:   pb,
		bus:         bus,
	}
	return h, repo, co, mg, bus
}

// chiReq builds an http.Request with a chi route param pre-populated so
// chi.URLParam("code") works inside the handler under test.
func chiReq(t *testing.T, method, path string, body []byte, params map[string]string) *http.Request {
	t.Helper()
	bodyReader := bytes.NewReader(body)
	r := httptest.NewRequestWithContext(t.Context(), method, path, bodyReader)
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
	return r
}

// ─── List ────────────────────────────────────────────────────────────────────

func TestStreamHandler_List_AllStreams(t *testing.T) {
	t.Parallel()
	h, repo, _, _, _ := newStreamHandlerForTest(t)
	repo.seed(
		&domain.Stream{Code: "a"},
		&domain.Stream{Code: "b"},
	)

	w := httptest.NewRecorder()
	h.List(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/streams", nil))

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Total int              `json:"total"`
		Data  []streamResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, 2, resp.Total)
}

func TestStreamHandler_List_StatusFilter(t *testing.T) {
	t.Parallel()
	h, repo, co, _, _ := newStreamHandlerForTest(t)
	repo.seed(&domain.Stream{Code: "active"}, &domain.Stream{Code: "stopped"})
	co.runningByCode["active"] = true
	co.statusByCode["active"] = domain.StatusActive
	co.statusByCode["stopped"] = domain.StatusStopped

	w := httptest.NewRecorder()
	h.List(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/streams?status=active", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Total int `json:"total"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, 1, resp.Total, "only active stream should match filter")
}

func TestStreamHandler_List_InvalidStatusReturns400(t *testing.T) {
	t.Parallel()
	h, _, _, _, _ := newStreamHandlerForTest(t)
	w := httptest.NewRecorder()
	h.List(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/streams?status=garbage", nil))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestStreamHandler_List_RepoErrorReturns500(t *testing.T) {
	t.Parallel()
	h, repo, _, _, _ := newStreamHandlerForTest(t)
	repo.listErr = errors.New("db down")
	w := httptest.NewRecorder()
	h.List(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/streams", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// ─── Get ─────────────────────────────────────────────────────────────────────

func TestStreamHandler_Get_Found(t *testing.T) {
	t.Parallel()
	h, repo, _, _, _ := newStreamHandlerForTest(t)
	repo.seed(&domain.Stream{Code: "live", Name: "Live"})

	w := httptest.NewRecorder()
	h.Get(w, chiReq(t, http.MethodGet, "/streams/live", nil, map[string]string{"code": "live"}))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"code":"live"`)
}

func TestStreamHandler_Get_NotFound(t *testing.T) {
	t.Parallel()
	h, _, _, _, _ := newStreamHandlerForTest(t)
	w := httptest.NewRecorder()
	h.Get(w, chiReq(t, http.MethodGet, "/streams/missing", nil, map[string]string{"code": "missing"}))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// TestStreamHandler_Get_StartedAtAndUptime asserts the runtime envelope
// surfaces the wallclock the pipeline went live (started_at) and the
// elapsed seconds at response time (uptime_sec) — these come from
// coordinator.StreamStartedAt and let frontend render uptime without
// reading wallclock + doing the subtraction itself.
func TestStreamHandler_Get_StartedAtAndUptime(t *testing.T) {
	t.Parallel()
	h, repo, coord, _, _ := newStreamHandlerForTest(t)
	repo.seed(&domain.Stream{Code: "live", Name: "Live"})
	startedAt := time.Now().Add(-90 * time.Second)
	coord.mu.Lock()
	coord.startedAtByCode["live"] = startedAt
	coord.runningByCode["live"] = true
	coord.mu.Unlock()

	w := httptest.NewRecorder()
	h.Get(w, chiReq(t, http.MethodGet, "/streams/live", nil, map[string]string{"code": "live"}))
	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.String()
	assert.Contains(t, body, `"started_at":`, "started_at must be present")
	// uptime_sec should be ~90, but allow a small window for test-machine
	// jitter between the seed and the Get call.
	var got struct {
		Data struct {
			Runtime struct {
				StartedAt time.Time `json:"started_at"`
				UptimeSec int64     `json:"uptime_sec"`
			} `json:"runtime"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.WithinDuration(t, startedAt, got.Data.Runtime.StartedAt, time.Second)
	assert.GreaterOrEqual(t, got.Data.Runtime.UptimeSec, int64(89))
	assert.LessOrEqual(t, got.Data.Runtime.UptimeSec, int64(91))
}

// Stopped stream → started_at omitted and uptime_sec stays zero so the UI
// can show "—" instead of an epoch-zero timestamp.
func TestStreamHandler_Get_StoppedStreamOmitsUptime(t *testing.T) {
	t.Parallel()
	h, repo, _, _, _ := newStreamHandlerForTest(t)
	repo.seed(&domain.Stream{Code: "off", Name: "Off"})

	w := httptest.NewRecorder()
	h.Get(w, chiReq(t, http.MethodGet, "/streams/off", nil, map[string]string{"code": "off"}))
	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	assert.NotContains(t, body, `"started_at"`, "stopped stream must omit started_at")
	assert.NotContains(t, body, `"uptime_sec"`, "stopped stream must omit uptime_sec")
}

// Runtime streams aren't in the on-disk repo but Get must still resolve
// them via autopublish.Lookup so the UI can render the detail panel.
func TestStreamHandler_Get_FallsBackToRuntimeStream(t *testing.T) {
	t.Parallel()
	h, _, _, _, _ := newStreamHandlerForTest(t)
	h.autopublish = newFakeRuntimeLister(autopublish.RuntimeEntry{
		Code:         "receiver/test_tpl_push",
		TemplateCode: "profile_a",
	})

	w := httptest.NewRecorder()
	h.Get(w, chiReq(t, http.MethodGet,
		"/streams/receiver/test_tpl_push", nil,
		map[string]string{"code": "receiver/test_tpl_push"}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"code":"receiver/test_tpl_push"`)
	assert.Contains(t, w.Body.String(), `"source":"runtime"`)
	assert.Contains(t, w.Body.String(), `"template":"profile_a"`)
}

// fakeRuntimeLister is the minimal runtimeStreamLister used across
// runtime-related handler tests below. Each test seeds the entries it
// needs; StopRuntime mutates the same map so the handler's call site
// observes the teardown immediately.
type fakeRuntimeLister struct {
	mu      sync.Mutex
	entries map[domain.StreamCode]autopublish.RuntimeEntry
	stopped []domain.StreamCode
}

func newFakeRuntimeLister(seed ...autopublish.RuntimeEntry) *fakeRuntimeLister {
	f := &fakeRuntimeLister{entries: make(map[domain.StreamCode]autopublish.RuntimeEntry, len(seed))}
	for _, e := range seed {
		f.entries[e.Code] = e
	}
	return f
}

func (f *fakeRuntimeLister) ListRuntime() []autopublish.RuntimeEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]autopublish.RuntimeEntry, 0, len(f.entries))
	for _, e := range f.entries {
		out = append(out, e)
	}
	return out
}

func (f *fakeRuntimeLister) IsRuntime(code domain.StreamCode) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.entries[code]
	return ok
}

func (f *fakeRuntimeLister) Lookup(code domain.StreamCode) (autopublish.RuntimeEntry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.entries[code]
	return e, ok
}

func (f *fakeRuntimeLister) StopRuntime(_ context.Context, code domain.StreamCode) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.entries[code]; !ok {
		return false
	}
	delete(f.entries, code)
	f.stopped = append(f.stopped, code)
	return true
}

// ─── Put: create + update + edge cases ───────────────────────────────────────

func TestStreamHandler_Put_CreateNew(t *testing.T) {
	t.Parallel()
	h, repo, co, _, bus := newStreamHandlerForTest(t)

	body := domain.Stream{
		Inputs: []domain.Input{{URL: "rtmp://src", Priority: 0}},
	}
	raw, _ := json.Marshal(body)
	req := chiReq(t, http.MethodPost, "/streams/news", raw, map[string]string{"code": "news"})
	w := httptest.NewRecorder()
	h.Put(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	saved, err := repo.FindByCode(t.Context(), "news")
	require.NoError(t, err)
	assert.Equal(t, domain.StreamCode("news"), saved.Code)
	assert.Contains(t, co.startedCodes, domain.StreamCode("news"), "fresh non-disabled stream must be Started")
	assert.Len(t, bus.events, 1, "stream.created event must fire")
	assert.Equal(t, domain.EventStreamCreated, bus.events[0].Type)
}

func TestStreamHandler_Put_UpdateExisting(t *testing.T) {
	t.Parallel()
	h, repo, co, _, bus := newStreamHandlerForTest(t)
	repo.seed(&domain.Stream{
		Code:   "live",
		Inputs: []domain.Input{{URL: "rtmp://orig"}},
	})
	co.runningByCode["live"] = true

	updated := domain.Stream{Inputs: []domain.Input{{URL: "rtmp://updated"}}}
	raw, _ := json.Marshal(updated)
	req := chiReq(t, http.MethodPost, "/streams/live", raw, map[string]string{"code": "live"})
	w := httptest.NewRecorder()
	h.Put(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, co.updatedCodes, domain.StreamCode("live"), "update path must call coordinator.Update")
	updatedEvts := 0
	for _, e := range bus.events {
		if e.Type == domain.EventStreamUpdated {
			updatedEvts++
		}
	}
	assert.Equal(t, 1, updatedEvts, "stream.updated event must fire on update")
}

func TestStreamHandler_Put_InvalidJSONReturns400(t *testing.T) {
	t.Parallel()
	h, _, _, _, _ := newStreamHandlerForTest(t)
	req := chiReq(t, http.MethodPost, "/streams/x", []byte(`{not-json`), map[string]string{"code": "x"})
	w := httptest.NewRecorder()
	h.Put(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestStreamHandler_Put_RepoSaveFailure500(t *testing.T) {
	t.Parallel()
	h, repo, _, _, _ := newStreamHandlerForTest(t)
	repo.saveErr = errors.New("disk full")
	body := domain.Stream{Inputs: []domain.Input{{URL: "rtmp://x"}}}
	raw, _ := json.Marshal(body)
	req := chiReq(t, http.MethodPost, "/streams/y", raw, map[string]string{"code": "y"})
	w := httptest.NewRecorder()
	h.Put(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestStreamHandler_Put_DisabledStreamSkipsStart(t *testing.T) {
	t.Parallel()
	h, _, co, _, _ := newStreamHandlerForTest(t)
	body := domain.Stream{
		Disabled: true,
		Inputs:   []domain.Input{{URL: "rtmp://x"}},
	}
	raw, _ := json.Marshal(body)
	req := chiReq(t, http.MethodPost, "/streams/z", raw, map[string]string{"code": "z"})
	w := httptest.NewRecorder()
	h.Put(w, req)
	require.Equal(t, http.StatusCreated, w.Code)
	assert.Empty(t, co.startedCodes, "disabled stream must NOT be Started")
}

// ─── Delete ─────────────────────────────────────────────────────────────────

func TestStreamHandler_Delete_Existing(t *testing.T) {
	t.Parallel()
	h, repo, co, _, bus := newStreamHandlerForTest(t)
	repo.seed(&domain.Stream{Code: "doomed"})
	co.runningByCode["doomed"] = true

	req := chiReq(t, http.MethodDelete, "/streams/doomed", nil, map[string]string{"code": "doomed"})
	w := httptest.NewRecorder()
	h.Delete(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	_, err := repo.FindByCode(t.Context(), "doomed")
	assert.ErrorIs(t, err, store.ErrNotFound)
	assert.Contains(t, co.stoppedCodes, domain.StreamCode("doomed"))
	deletedEvts := 0
	for _, e := range bus.events {
		if e.Type == domain.EventStreamDeleted {
			deletedEvts++
		}
	}
	assert.Equal(t, 1, deletedEvts)
}

// Delete is idempotent — repeating the call after a successful delete (or
// against a code that was never created) returns 204 because both Stop()
// and the underlying store.Delete() tolerate "no such stream". The
// front-end relies on this for retry semantics.
func TestStreamHandler_Delete_MissingIsIdempotent(t *testing.T) {
	t.Parallel()
	h, _, _, _, _ := newStreamHandlerForTest(t)
	req := chiReq(t, http.MethodDelete, "/streams/missing", nil, map[string]string{"code": "missing"})
	w := httptest.NewRecorder()
	h.Delete(w, req)
	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestStreamHandler_Delete_RepoErrorReturns500(t *testing.T) {
	t.Parallel()
	h, repo, _, _, _ := newStreamHandlerForTest(t)
	repo.delErr = errors.New("disk down")
	req := chiReq(t, http.MethodDelete, "/streams/x", nil, map[string]string{"code": "x"})
	w := httptest.NewRecorder()
	h.Delete(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestStreamHandler_Put_StartCoordinatorErrorReturns500(t *testing.T) {
	t.Parallel()
	h, _, co, _, _ := newStreamHandlerForTest(t)
	co.startErr = errors.New("buffer full")
	body := domain.Stream{Inputs: []domain.Input{{URL: "rtmp://x"}}}
	raw, _ := json.Marshal(body)
	req := chiReq(t, http.MethodPost, "/streams/y", raw, map[string]string{"code": "y"})
	w := httptest.NewRecorder()
	h.Put(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestStreamHandler_Put_UpdateExistingRunningCallsUpdate(t *testing.T) {
	t.Parallel()
	h, repo, co, _, _ := newStreamHandlerForTest(t)
	repo.seed(&domain.Stream{Code: "live", Inputs: []domain.Input{{URL: "rtmp://a"}}})
	co.runningByCode["live"] = true

	body := domain.Stream{Code: "live", Inputs: []domain.Input{{URL: "rtmp://b"}}}
	raw, _ := json.Marshal(body)
	req := chiReq(t, http.MethodPut, "/streams/live", raw, map[string]string{"code": "live"})
	w := httptest.NewRecorder()
	h.Put(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, co.updatedCodes, domain.StreamCode("live"))
}

func TestStreamHandler_Put_UpdateRunningCoordinatorErrorReturns500(t *testing.T) {
	t.Parallel()
	h, repo, co, _, _ := newStreamHandlerForTest(t)
	repo.seed(&domain.Stream{Code: "live", Inputs: []domain.Input{{URL: "rtmp://a"}}})
	co.runningByCode["live"] = true
	co.updateErr = errors.New("transcoder reload failed")

	body := domain.Stream{Code: "live", Inputs: []domain.Input{{URL: "rtmp://b"}}}
	raw, _ := json.Marshal(body)
	req := chiReq(t, http.MethodPut, "/streams/live", raw, map[string]string{"code": "live"})
	w := httptest.NewRecorder()
	h.Put(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// ─── Restart ────────────────────────────────────────────────────────────────

func TestStreamHandler_Restart_HappyPath(t *testing.T) {
	t.Parallel()
	h, repo, co, _, _ := newStreamHandlerForTest(t)
	repo.seed(&domain.Stream{Code: "restart-me"})

	req := chiReq(t, http.MethodPost, "/streams/restart-me/restart", nil, map[string]string{"code": "restart-me"})
	w := httptest.NewRecorder()
	h.Restart(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, co.stoppedCodes, domain.StreamCode("restart-me"), "restart must Stop first")
	assert.Contains(t, co.startedCodes, domain.StreamCode("restart-me"), "restart must Start after Stop")
}

func TestStreamHandler_Restart_DisabledRejected(t *testing.T) {
	t.Parallel()
	h, repo, _, _, _ := newStreamHandlerForTest(t)
	repo.seed(&domain.Stream{Code: "off", Disabled: true})

	req := chiReq(t, http.MethodPost, "/streams/off/restart", nil, map[string]string{"code": "off"})
	w := httptest.NewRecorder()
	h.Restart(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestStreamHandler_Restart_StartFailure500(t *testing.T) {
	t.Parallel()
	h, repo, co, _, _ := newStreamHandlerForTest(t)
	repo.seed(&domain.Stream{Code: "fail"})
	co.startErr = errors.New("ffmpeg not found")

	req := chiReq(t, http.MethodPost, "/streams/fail/restart", nil, map[string]string{"code": "fail"})
	w := httptest.NewRecorder()
	h.Restart(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// Restart on a runtime stream (auto-publish — not in the repo) must
// route through autopublish.StopRuntime and return 200 with a hint
// instead of 404ing through the store. The handler does NOT re-Start
// the pipeline: runtime streams have no persistent record to fetch a
// fresh config from, so the next push handshake re-materialises the
// runtime stream via the prefix matcher.
func TestStreamHandler_Restart_RuntimeStreamStopsViaAutopublish(t *testing.T) {
	t.Parallel()
	h, _, co, _, _ := newStreamHandlerForTest(t)
	const code domain.StreamCode = "live/foo"
	rt := newFakeRuntimeLister(autopublish.RuntimeEntry{Code: code, TemplateCode: "profile_a"})
	h.autopublish = rt

	req := chiReq(t, http.MethodPost, "/streams/live/foo/restart", nil, map[string]string{"code": string(code)})
	w := httptest.NewRecorder()
	h.Restart(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"status":"stopped"`)
	assert.Contains(t, w.Body.String(), `"source":"runtime"`)
	assert.Equal(t, []domain.StreamCode{code}, rt.stopped,
		"autopublish.StopRuntime must be the teardown path for runtime streams")

	co.mu.Lock()
	defer co.mu.Unlock()
	assert.Empty(t, co.startedCodes,
		"runtime restart must NOT call coordinator.Start; encoder reconnect drives re-materialisation")
}

// When neither the repo nor the autopublish registry knows the code,
// Restart still 404s — the legacy behaviour for unknown streams.
func TestStreamHandler_Restart_UnknownReturns404(t *testing.T) {
	t.Parallel()
	h, _, _, _, _ := newStreamHandlerForTest(t)
	h.autopublish = newFakeRuntimeLister()
	req := chiReq(t, http.MethodPost, "/streams/missing/restart", nil, map[string]string{"code": "missing"})
	w := httptest.NewRecorder()
	h.Restart(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// Put on a code that is currently auto-published must NOT silently
// create a parallel config record. Without the guard the runtime
// pipeline keeps running with template-resolved config, the body
// is persisted but never applied, and GET /streams shows two entries
// for the same code (one source=config, one source=runtime). The
// guard returns 409 with the referenced template so the operator
// knows where to make the change.
func TestStreamHandler_Put_RejectsRuntimeStreamCode(t *testing.T) {
	t.Parallel()
	h, repo, co, _, _ := newStreamHandlerForTest(t)
	const code domain.StreamCode = "live/foo"
	h.autopublish = newFakeRuntimeLister(autopublish.RuntimeEntry{
		Code:         code,
		TemplateCode: "profile_a",
	})

	body, _ := json.Marshal(domain.Stream{Inputs: []domain.Input{{URL: "rtmp://x"}}})
	req := chiReq(t, http.MethodPost, "/streams/live/foo", body, map[string]string{"code": string(code)})
	w := httptest.NewRecorder()
	h.Put(w, req)

	require.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), `"error":"RUNTIME_STREAM_NOT_EDITABLE"`)
	assert.Contains(t, w.Body.String(), `"template_code":"profile_a"`)

	// Side effects we must NOT have triggered.
	_, findErr := repo.FindByCode(context.Background(), code)
	assert.ErrorIs(t, findErr, store.ErrNotFound,
		"must not persist a config record under a runtime-only code")
	co.mu.Lock()
	defer co.mu.Unlock()
	assert.Empty(t, co.startedCodes, "must not call coordinator.Start while the runtime pipeline owns this code")
}

// Operator-initiated Delete on a runtime stream must tear it down via
// autopublish.StopRuntime — the entry + observer + coordinator-level
// pipeline all come down atomically, closing the race window where a
// fresh push could observe a stale registry entry.
func TestStreamHandler_Delete_RuntimeStreamRoutesThroughAutopublish(t *testing.T) {
	t.Parallel()
	h, _, co, _, bus := newStreamHandlerForTest(t)
	const code domain.StreamCode = "live/foo"
	rt := newFakeRuntimeLister(autopublish.RuntimeEntry{Code: code, TemplateCode: "profile_a"})
	h.autopublish = rt

	req := chiReq(t, http.MethodDelete, "/streams/live/foo", nil, map[string]string{"code": string(code)})
	w := httptest.NewRecorder()
	h.Delete(w, req)

	require.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, []domain.StreamCode{code}, rt.stopped)
	co.mu.Lock()
	defer co.mu.Unlock()
	assert.Empty(t, co.stoppedCodes,
		"runtime delete must NOT call coordinator.Stop directly — autopublish owns that orchestration")

	bus.mu.Lock()
	defer bus.mu.Unlock()
	var sawDeleted bool
	for _, e := range bus.events {
		if e.Type == domain.EventStreamDeleted && e.StreamCode == code {
			sawDeleted = true
		}
	}
	assert.True(t, sawDeleted, "stream.deleted event must still fire for runtime stream deletes")
}

// ─── SwitchInput ────────────────────────────────────────────────────────────

func TestStreamHandler_SwitchInput_HappyPath(t *testing.T) {
	t.Parallel()
	h, _, _, mg, _ := newStreamHandlerForTest(t)

	body := bytes.NewBufferString(`{"priority": 2}`)
	req := chiReq(t, http.MethodPost, "/streams/live/switch", body.Bytes(), map[string]string{"code": "live"})
	w := httptest.NewRecorder()
	h.SwitchInput(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 2, mg.switchedTo["live"])
}

func TestStreamHandler_SwitchInput_BadJSON(t *testing.T) {
	t.Parallel()
	h, _, _, _, _ := newStreamHandlerForTest(t)
	req := chiReq(t, http.MethodPost, "/streams/x/switch", []byte(`{nope`), map[string]string{"code": "x"})
	w := httptest.NewRecorder()
	h.SwitchInput(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestStreamHandler_SwitchInput_ManagerError(t *testing.T) {
	t.Parallel()
	h, _, _, mg, _ := newStreamHandlerForTest(t)
	mg.switchErr = fmt.Errorf("priority not registered")
	req := chiReq(t, http.MethodPost, "/streams/x/switch", []byte(`{"priority":1}`), map[string]string{"code": "x"})
	w := httptest.NewRecorder()
	h.SwitchInput(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// ─── withStatus + buildMediaSummary ────────────────────────────────────────

// withStatus assembles the {Stream, Runtime} response envelope. Verify it
// substitutes the ABR-copy / ABR-mixer runtime when the stream is not
// registered with the manager (concrete behaviour the front-end depends on
// to surface "unknown" state correctly).
func TestStreamHandler_WithStatus_ABRCopyFallback(t *testing.T) {
	t.Parallel()
	h, _, co, _, _ := newStreamHandlerForTest(t)
	co.statusByCode["abr"] = domain.StatusActive
	abrRT := manager.RuntimeStatus{ActiveInputPriority: 0}
	co.abrCopyByCode["abr"] = abrRT

	resp := h.withStatus(&domain.Stream{Code: "abr"})
	assert.Equal(t, domain.StatusActive, resp.Runtime.Status)
}

func TestBuildMediaSummary_NoTranscoderMirrorsInputs(t *testing.T) {
	t.Parallel()
	s := &domain.Stream{Code: "x"}
	inputs := []manager.InputHealthSnapshot{
		{
			InputPriority: 0,
			BitrateKbps:   1500,
			Tracks: []domain.MediaTrackInfo{
				{Kind: domain.MediaTrackVideo, Codec: domain.CodecLabel(domain.AVCodecH264)},
			},
		},
	}
	got := buildMediaSummary(s, inputs, 0)
	require.NotNil(t, got)
	assert.Equal(t, inputs[0].BitrateKbps, got.InputBitrateKbps)
	assert.Equal(t, inputs[0].BitrateKbps, got.OutputBitrateKbps,
		"no transcoder → output mirrors input")
}
