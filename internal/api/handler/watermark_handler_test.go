package handler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/watermarks"
)

// stubWatermarkSvc is a hand-rolled in-memory implementation of
// watermarkAssetService. The real *watermarks.Service is exercised by
// its own package's tests; here we only verify HTTP plumbing.
type stubWatermarkSvc struct {
	mu        sync.Mutex
	dir       string
	assets    map[domain.WatermarkFilename]*domain.WatermarkAsset
	saveErr   error
	deleteErr error
	getErr    error
	resolved  map[domain.WatermarkFilename]string // override path returned by ResolvePath
}

func newStubWatermarkSvc(t *testing.T) *stubWatermarkSvc {
	t.Helper()
	return &stubWatermarkSvc{
		dir:      t.TempDir(),
		assets:   map[domain.WatermarkFilename]*domain.WatermarkAsset{},
		resolved: map[domain.WatermarkFilename]string{},
	}
}

func (s *stubWatermarkSvc) Dir() string { return s.dir }

func (s *stubWatermarkSvc) List() []*domain.WatermarkAsset {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*domain.WatermarkAsset, 0, len(s.assets))
	for _, a := range s.assets {
		c := *a
		out = append(out, &c)
	}
	return out
}

func (s *stubWatermarkSvc) Get(name domain.WatermarkFilename) (*domain.WatermarkAsset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getErr != nil {
		return nil, s.getErr
	}
	a, ok := s.assets[name]
	if !ok {
		return nil, watermarks.ErrNotFound
	}
	c := *a
	return &c, nil
}

func (s *stubWatermarkSvc) ResolvePath(name domain.WatermarkFilename) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.resolved[name]; ok {
		return p, nil
	}
	if _, ok := s.assets[name]; !ok {
		return "", watermarks.ErrNotFound
	}
	return filepath.Join(s.dir, string(name)), nil
}

func (s *stubWatermarkSvc) Save(filename string, body io.Reader) (*domain.WatermarkAsset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return nil, s.saveErr
	}
	if _, err := io.Copy(io.Discard, body); err != nil {
		return nil, err
	}
	name := domain.WatermarkFilename(filename)
	a := &domain.WatermarkAsset{
		Filename:    name,
		ContentType: "image/png",
	}
	s.assets[name] = a
	return a, nil
}

func (s *stubWatermarkSvc) Delete(name domain.WatermarkFilename) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return s.deleteErr
	}
	if _, ok := s.assets[name]; !ok {
		return watermarks.ErrNotFound
	}
	delete(s.assets, name)
	return nil
}

func newWatermarkHandlerForTest(t *testing.T) (*WatermarkHandler, *stubWatermarkSvc, *fakeBus) {
	t.Helper()
	svc := newStubWatermarkSvc(t)
	bus := &fakeBus{}
	return &WatermarkHandler{svc: svc, bus: bus}, svc, bus
}

func wmReq(t *testing.T, method, path string, params map[string]string) *http.Request {
	t.Helper()
	r := httptest.NewRequestWithContext(t.Context(), method, path, http.NoBody)
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

// ─── List ────────────────────────────────────────────────────────────────────

func TestWatermarkHandler_List(t *testing.T) {
	t.Parallel()
	h, svc, _ := newWatermarkHandlerForTest(t)
	svc.assets["one.png"] = &domain.WatermarkAsset{Filename: "one.png"}

	w := httptest.NewRecorder()
	h.List(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/watermarks", nil))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"total":1`)
	assert.Contains(t, w.Body.String(), `"dir"`)
}

// ─── Get ─────────────────────────────────────────────────────────────────────

func TestWatermarkHandler_Get_Found(t *testing.T) {
	t.Parallel()
	h, svc, _ := newWatermarkHandlerForTest(t)
	svc.assets["logo.png"] = &domain.WatermarkAsset{Filename: "logo.png"}

	w := httptest.NewRecorder()
	h.Get(w, wmReq(t, http.MethodGet, "/watermarks/logo.png", map[string]string{"filename": "logo.png"}))
	require.Equal(t, http.StatusOK, w.Code)
}

func TestWatermarkHandler_Get_InvalidFilename(t *testing.T) {
	t.Parallel()
	h, _, _ := newWatermarkHandlerForTest(t)
	w := httptest.NewRecorder()
	h.Get(w, wmReq(t, http.MethodGet, "/watermarks/!!!", map[string]string{"filename": "!!!"}))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestWatermarkHandler_Get_NotFound(t *testing.T) {
	t.Parallel()
	h, _, _ := newWatermarkHandlerForTest(t)
	w := httptest.NewRecorder()
	h.Get(w, wmReq(t, http.MethodGet, "/watermarks/missing.png", map[string]string{"filename": "missing.png"}))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestWatermarkHandler_Get_BackendError(t *testing.T) {
	t.Parallel()
	h, svc, _ := newWatermarkHandlerForTest(t)
	svc.getErr = errors.New("disk gone")
	w := httptest.NewRecorder()
	h.Get(w, wmReq(t, http.MethodGet, "/watermarks/logo.png", map[string]string{"filename": "logo.png"}))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// ─── Raw ─────────────────────────────────────────────────────────────────────

func TestWatermarkHandler_Raw_HappyPath(t *testing.T) {
	t.Parallel()
	h, svc, _ := newWatermarkHandlerForTest(t)
	path := filepath.Join(svc.dir, "logo.png")
	require.NoError(t, os.WriteFile(path, []byte("png-data"), 0o644))
	svc.assets["logo.png"] = &domain.WatermarkAsset{Filename: "logo.png", ContentType: "image/png"}

	w := httptest.NewRecorder()
	h.Raw(w, wmReq(t, http.MethodGet, "/watermarks/logo.png/raw", map[string]string{"filename": "logo.png"}))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "png-data", w.Body.String())
	assert.Equal(t, "image/png", w.Header().Get("Content-Type"))
}

func TestWatermarkHandler_Raw_InvalidFilename(t *testing.T) {
	t.Parallel()
	h, _, _ := newWatermarkHandlerForTest(t)
	w := httptest.NewRecorder()
	h.Raw(w, wmReq(t, http.MethodGet, "/watermarks/!!!/raw", map[string]string{"filename": "!!!"}))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestWatermarkHandler_Raw_NotFound(t *testing.T) {
	t.Parallel()
	h, _, _ := newWatermarkHandlerForTest(t)
	w := httptest.NewRecorder()
	h.Raw(w, wmReq(t, http.MethodGet, "/watermarks/missing.png/raw", map[string]string{"filename": "missing.png"}))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// ─── Upload ──────────────────────────────────────────────────────────────────

// wmUploadReq builds a multipart upload with a fixed "logo.png" filename.
// The filename only matters to *watermarks.Service.Save which is stubbed in
// these tests, so a constant is fine.
func wmUploadReq(t *testing.T, body []byte) *http.Request {
	t.Helper()
	buf := &bytes.Buffer{}
	mw := multipart.NewWriter(buf)
	fw, err := mw.CreateFormFile("file", "logo.png")
	require.NoError(t, err)
	_, err = fw.Write(body)
	require.NoError(t, err)
	require.NoError(t, mw.Close())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/watermarks", buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

func TestWatermarkHandler_Upload_HappyPath(t *testing.T) {
	t.Parallel()
	h, _, bus := newWatermarkHandlerForTest(t)
	w := httptest.NewRecorder()
	h.Upload(w, wmUploadReq(t, []byte("png")))
	require.Equal(t, http.StatusCreated, w.Code)

	bus.mu.Lock()
	defer bus.mu.Unlock()
	require.Len(t, bus.events, 1)
	assert.Equal(t, domain.EventWatermarkAssetCreated, bus.events[0].Type)
}

func TestWatermarkHandler_Upload_MissingFileField(t *testing.T) {
	t.Parallel()
	h, _, _ := newWatermarkHandlerForTest(t)

	buf := &bytes.Buffer{}
	mw := multipart.NewWriter(buf)
	_, err := mw.CreateFormField("notfile")
	require.NoError(t, err)
	require.NoError(t, mw.Close())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/watermarks", buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	w := httptest.NewRecorder()
	h.Upload(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestWatermarkHandler_Upload_InvalidContent(t *testing.T) {
	t.Parallel()
	h, svc, _ := newWatermarkHandlerForTest(t)
	svc.saveErr = watermarks.ErrInvalidContent

	w := httptest.NewRecorder()
	h.Upload(w, wmUploadReq(t, []byte("not-an-image")))
	assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
}

func TestWatermarkHandler_Upload_AlreadyExists(t *testing.T) {
	t.Parallel()
	h, svc, _ := newWatermarkHandlerForTest(t)
	svc.saveErr = watermarks.ErrAlreadyExists

	w := httptest.NewRecorder()
	h.Upload(w, wmUploadReq(t, []byte("x")))
	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestWatermarkHandler_Upload_TooLarge(t *testing.T) {
	t.Parallel()
	h, svc, _ := newWatermarkHandlerForTest(t)
	svc.saveErr = watermarks.ErrTooLarge

	w := httptest.NewRecorder()
	h.Upload(w, wmUploadReq(t, []byte("x")))
	assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
}

func TestWatermarkHandler_Upload_GenericFailure(t *testing.T) {
	t.Parallel()
	h, svc, _ := newWatermarkHandlerForTest(t)
	svc.saveErr = errors.New("disk full")

	w := httptest.NewRecorder()
	h.Upload(w, wmUploadReq(t, []byte("x")))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// ─── Delete ──────────────────────────────────────────────────────────────────

func TestWatermarkHandler_Delete_HappyPath(t *testing.T) {
	t.Parallel()
	h, svc, bus := newWatermarkHandlerForTest(t)
	svc.assets["logo.png"] = &domain.WatermarkAsset{Filename: "logo.png"}

	w := httptest.NewRecorder()
	h.Delete(w, wmReq(t, http.MethodDelete, "/watermarks/logo.png", map[string]string{"filename": "logo.png"}))
	require.Equal(t, http.StatusNoContent, w.Code)

	bus.mu.Lock()
	defer bus.mu.Unlock()
	require.Len(t, bus.events, 1)
	assert.Equal(t, domain.EventWatermarkAssetDeleted, bus.events[0].Type)
}

func TestWatermarkHandler_Delete_InvalidFilename(t *testing.T) {
	t.Parallel()
	h, _, _ := newWatermarkHandlerForTest(t)
	w := httptest.NewRecorder()
	h.Delete(w, wmReq(t, http.MethodDelete, "/watermarks/!!!", map[string]string{"filename": "!!!"}))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestWatermarkHandler_Delete_NotFound(t *testing.T) {
	t.Parallel()
	h, _, _ := newWatermarkHandlerForTest(t)
	w := httptest.NewRecorder()
	h.Delete(w, wmReq(t, http.MethodDelete, "/watermarks/missing.png", map[string]string{"filename": "missing.png"}))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestWatermarkHandler_Delete_BackendError(t *testing.T) {
	t.Parallel()
	h, svc, _ := newWatermarkHandlerForTest(t)
	svc.assets["logo.png"] = &domain.WatermarkAsset{Filename: "logo.png"}
	svc.deleteErr = errors.New("disk gone")
	w := httptest.NewRecorder()
	h.Delete(w, wmReq(t, http.MethodDelete, "/watermarks/logo.png", map[string]string{"filename": "logo.png"}))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
