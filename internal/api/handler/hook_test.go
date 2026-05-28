package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/hooks"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

// stubHookTester is a controllable hookTester for the Test() handler tests.
type stubHookTester struct {
	err error
}

func (s *stubHookTester) DeliverTestEvent(_ context.Context, _ domain.HookID) error {
	return s.err
}

const (
	hooksPath  = "/hooks"
	hookXPath  = "/hooks/x"
	statusFmt  = "status=%d"
	hookIDX    = "x"
	hidParam   = "hid"
	missingHid = "missing"
)

// fakeHookRepo is an in-memory hook repository for handler tests.
type fakeHookRepo struct {
	mu      sync.Mutex
	hooks   map[domain.HookID]*domain.Hook
	listErr error
	saveErr error
	delErr  error
}

func newFakeHookRepo() *fakeHookRepo {
	return &fakeHookRepo{hooks: make(map[domain.HookID]*domain.Hook)}
}

func (r *fakeHookRepo) Save(_ context.Context, h *domain.Hook) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hooks[h.ID] = h
	return nil
}

func (r *fakeHookRepo) FindByID(_ context.Context, id domain.HookID) (*domain.Hook, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.hooks[id]; ok {
		return h, nil
	}
	return nil, store.ErrNotFound
}

func (r *fakeHookRepo) List(_ context.Context) ([]*domain.Hook, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*domain.Hook, 0, len(r.hooks))
	for _, h := range r.hooks {
		out = append(out, h)
	}
	return out, nil
}

func (r *fakeHookRepo) Delete(_ context.Context, id domain.HookID) error {
	if r.delErr != nil {
		return r.delErr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.hooks, id)
	return nil
}

// withHookID wraps a request so chi.URLParam(r, "hid") returns val.
func withHookID(req *http.Request, val string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(hidParam, val)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func newReq(t *testing.T, method, path string, body []byte) *http.Request {
	t.Helper()
	var br *bytes.Reader
	if body != nil {
		br = bytes.NewReader(body)
	}
	if br == nil {
		return httptest.NewRequestWithContext(t.Context(), method, path, nil)
	}
	return httptest.NewRequestWithContext(t.Context(), method, path, br)
}

func newHookHandler(repo *fakeHookRepo) *HookHandler {
	return &HookHandler{hookRepo: repo}
}

func TestHookListReturnsTotal(t *testing.T) {
	repo := newFakeHookRepo()
	repo.hooks["a"] = &domain.Hook{ID: "a"}
	repo.hooks["b"] = &domain.Hook{ID: "b"}
	h := newHookHandler(repo)

	w := httptest.NewRecorder()
	h.List(w, newReq(t, http.MethodGet, hooksPath, nil))
	if w.Code != http.StatusOK {
		t.Fatalf(statusFmt, w.Code)
	}
	var got struct {
		Data  []*domain.Hook `json:"data"`
		Total int            `json:"total"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got.Total != 2 || len(got.Data) != 2 {
		t.Errorf("body=%+v", got)
	}
}

func TestHookListErrorReturns500(t *testing.T) {
	repo := newFakeHookRepo()
	repo.listErr = errors.New("db error")
	h := newHookHandler(repo)

	w := httptest.NewRecorder()
	h.List(w, newReq(t, http.MethodGet, hooksPath, nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf(statusFmt, w.Code)
	}
}

func TestHookCreatePersistsHook(t *testing.T) {
	repo := newFakeHookRepo()
	h := newHookHandler(repo)

	body, _ := json.Marshal(domain.Hook{ID: "new", Type: domain.HookTypeHTTP, Target: "https://x"})
	req := newReq(t, http.MethodPost, hooksPath, body)
	w := httptest.NewRecorder()
	h.Create(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if _, ok := repo.hooks["new"]; !ok {
		t.Error("hook not persisted")
	}
}

func TestHookCreateInvalidJSON(t *testing.T) {
	h := newHookHandler(newFakeHookRepo())

	req := newReq(t, http.MethodPost, hooksPath, []byte("{bad"))
	w := httptest.NewRecorder()
	h.Create(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf(statusFmt, w.Code)
	}
}

func TestHookCreateSaveError(t *testing.T) {
	repo := newFakeHookRepo()
	repo.saveErr = errors.New("disk full")
	h := newHookHandler(repo)

	body, _ := json.Marshal(domain.Hook{ID: hookIDX})
	req := newReq(t, http.MethodPost, hooksPath, body)
	w := httptest.NewRecorder()
	h.Create(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf(statusFmt, w.Code)
	}
}

func TestHookGetReturnsHook(t *testing.T) {
	repo := newFakeHookRepo()
	repo.hooks[hookIDX] = &domain.Hook{ID: hookIDX, Name: "hello"}
	h := newHookHandler(repo)

	req := withHookID(newReq(t, http.MethodGet, hookXPath, nil), hookIDX)
	w := httptest.NewRecorder()
	h.Get(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf(statusFmt, w.Code)
	}
	var got struct {
		Data *domain.Hook `json:"data"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got.Data == nil || got.Data.Name != "hello" {
		t.Errorf("body=%+v", got)
	}
}

func TestHookGetNotFound(t *testing.T) {
	h := newHookHandler(newFakeHookRepo())
	req := withHookID(newReq(t, http.MethodGet, "/hooks/missing", nil), missingHid)
	w := httptest.NewRecorder()
	h.Get(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf(statusFmt, w.Code)
	}
}

func TestHookUpdatePreservesID(t *testing.T) {
	repo := newFakeHookRepo()
	repo.hooks[hookIDX] = &domain.Hook{ID: hookIDX, Name: "old"}
	h := newHookHandler(repo)

	body, _ := json.Marshal(domain.Hook{ID: "wrong", Name: "new"})
	req := withHookID(newReq(t, http.MethodPut, hookXPath, body), hookIDX)
	w := httptest.NewRecorder()
	h.Update(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if got := repo.hooks[hookIDX]; got == nil || got.ID != hookIDX || got.Name != "new" {
		t.Errorf("hook not updated correctly: %+v", got)
	}
}

func TestHookUpdateNotFound(t *testing.T) {
	h := newHookHandler(newFakeHookRepo())
	body, _ := json.Marshal(domain.Hook{Name: hookIDX})
	req := withHookID(newReq(t, http.MethodPut, "/hooks/missing", body), missingHid)
	w := httptest.NewRecorder()
	h.Update(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf(statusFmt, w.Code)
	}
}

func TestHookUpdateInvalidJSON(t *testing.T) {
	repo := newFakeHookRepo()
	repo.hooks[hookIDX] = &domain.Hook{ID: hookIDX}
	h := newHookHandler(repo)

	req := withHookID(newReq(t, http.MethodPut, hookXPath, []byte("{bad")), hookIDX)
	w := httptest.NewRecorder()
	h.Update(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf(statusFmt, w.Code)
	}
}

func TestHookDelete(t *testing.T) {
	repo := newFakeHookRepo()
	repo.hooks[hookIDX] = &domain.Hook{ID: hookIDX}
	h := newHookHandler(repo)

	req := withHookID(newReq(t, http.MethodDelete, hookXPath, nil), hookIDX)
	w := httptest.NewRecorder()
	h.Delete(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf(statusFmt, w.Code)
	}
	if _, ok := repo.hooks[hookIDX]; ok {
		t.Error("hook not deleted")
	}
}

func TestHookDeleteError(t *testing.T) {
	repo := newFakeHookRepo()
	repo.delErr = errors.New("locked")
	h := newHookHandler(repo)

	req := withHookID(newReq(t, http.MethodDelete, hookXPath, nil), hookIDX)
	w := httptest.NewRecorder()
	h.Delete(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf(statusFmt, w.Code)
	}
}

// ─── Test (synthetic delivery) ───────────────────────────────────────────────

func TestHookTest_HappyPath(t *testing.T) {
	repo := newFakeHookRepo()
	h := &HookHandler{hookRepo: repo, hooks: &stubHookTester{}}

	req := withHookID(newReq(t, http.MethodPost, "/hooks/x/test", nil), hookIDX)
	w := httptest.NewRecorder()
	h.Test(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestHookTest_NotFound(t *testing.T) {
	repo := newFakeHookRepo()
	h := &HookHandler{hookRepo: repo, hooks: &stubHookTester{err: store.ErrNotFound}}

	req := withHookID(newReq(t, http.MethodPost, "/hooks/missing/test", nil), missingHid)
	w := httptest.NewRecorder()
	h.Test(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf(statusFmt, w.Code)
	}
}

func TestHookTest_Unsupported(t *testing.T) {
	repo := newFakeHookRepo()
	h := &HookHandler{hookRepo: repo, hooks: &stubHookTester{err: hooks.ErrHookTestUnsupported}}

	req := withHookID(newReq(t, http.MethodPost, "/hooks/x/test", nil), hookIDX)
	w := httptest.NewRecorder()
	h.Test(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf(statusFmt, w.Code)
	}
}

func TestHookTest_DeliveryFailed(t *testing.T) {
	repo := newFakeHookRepo()
	h := &HookHandler{hookRepo: repo, hooks: &stubHookTester{err: errors.New("connection refused")}}

	req := withHookID(newReq(t, http.MethodPost, "/hooks/x/test", nil), hookIDX)
	w := httptest.NewRecorder()
	h.Test(w, req)
	if w.Code != http.StatusBadGateway {
		t.Fatalf(statusFmt, w.Code)
	}
}
