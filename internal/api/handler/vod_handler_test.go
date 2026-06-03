package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/vod"
)

// newVODHandlerForTest mirrors newRecHandlerForTest's pattern with
// fakeVODRepo (defined in config_yaml_test.go) + a fresh vod.Registry.
func newVODHandlerForTest(t *testing.T) (*VODHandler, *fakeVODRepo) {
	t.Helper()
	repo := newFakeVODRepo()
	return &VODHandler{
		repo:     repo,
		registry: vod.NewRegistry(),
	}, repo
}

// vodMountInTempDir builds a domain.VODMount whose Storage path points
// at a writable temp dir — tests for Create / ListFiles / Raw need a
// real path because the handler verifies writability up front.
func vodMountInTempDir(t *testing.T, name domain.VODName) *domain.VODMount {
	t.Helper()
	return &domain.VODMount{Name: name, Storage: t.TempDir()}
}

// ─── List ────────────────────────────────────────────────────────────────────

func TestVODHandler_List(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	require.NoError(t, repo.Save(t.Context(), vodMountInTempDir(t, "movies")))
	require.NoError(t, repo.Save(t.Context(), vodMountInTempDir(t, "shows")))

	w := httptest.NewRecorder()
	h.List(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/vod", nil))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"total":2`)
}

func TestVODHandler_List_RepoError(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	repo.listErr = errors.New("backend down")
	w := httptest.NewRecorder()
	h.List(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/vod", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// ─── Create ──────────────────────────────────────────────────────────────────

func TestVODHandler_Create_HappyPath(t *testing.T) {
	t.Parallel()
	h, _ := newVODHandlerForTest(t)
	mount := vodMountInTempDir(t, "movies")
	body, _ := json.Marshal(mount)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/vod", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)
	require.Equal(t, http.StatusCreated, w.Code)
}

func TestVODHandler_Create_InvalidName(t *testing.T) {
	t.Parallel()
	h, _ := newVODHandlerForTest(t)
	body := []byte(`{"name": "!@#$ illegal", "storage": "/tmp"}`)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/vod", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestVODHandler_Create_InvalidJSONReturns400(t *testing.T) {
	t.Parallel()
	h, _ := newVODHandlerForTest(t)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/vod", bytes.NewReader([]byte(`{nope`)))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestVODHandler_Create_DuplicateReturns409(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	mount := vodMountInTempDir(t, "movies")
	require.NoError(t, repo.Save(t.Context(), mount))

	body, _ := json.Marshal(mount)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/vod", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestVODHandler_Create_RepoSaveError(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	repo.saveErr = errors.New("disk full")

	mount := vodMountInTempDir(t, "movies")
	body, _ := json.Marshal(mount)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/vod", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestVODHandler_Create_BadStoragePath(t *testing.T) {
	t.Parallel()
	h, _ := newVODHandlerForTest(t)
	body := []byte(`{"name":"movies","storage":""}`)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/vod", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestVODHandler_Update_BadStoragePath(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	require.NoError(t, repo.Save(t.Context(), vodMountInTempDir(t, "movies")))
	body := []byte(`{"storage":""}`)
	req := chiReq(t, http.MethodPut, "/vod/movies", body, map[string]string{"name": "movies"})
	w := httptest.NewRecorder()
	h.Update(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestVODHandler_Update_RepoSaveError(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	require.NoError(t, repo.Save(t.Context(), vodMountInTempDir(t, "movies")))
	repo.saveErr = errors.New("disk full")

	updated := *vodMountInTempDir(t, "movies")
	body, _ := json.Marshal(updated)
	req := chiReq(t, http.MethodPut, "/vod/movies", body, map[string]string{"name": "movies"})
	w := httptest.NewRecorder()
	h.Update(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestVODHandler_Delete_RepoError(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	repo.delErr = errors.New("locked")

	req := chiReq(t, http.MethodDelete, "/vod/movies", nil, map[string]string{"name": "movies"})
	w := httptest.NewRecorder()
	h.Delete(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestVODHandler_Create_StorageNotWritable(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root bypasses mode-bit checks; cannot meaningfully test non-writable storage")
	}
	h, _ := newVODHandlerForTest(t)
	// Path that doesn't exist AND can't be created (parent dir is read-only).
	roParent := t.TempDir()
	require.NoError(t, os.Chmod(roParent, 0o500)) // read+execute, no write
	t.Cleanup(func() { _ = os.Chmod(roParent, 0o700) })

	mount := domain.VODMount{Name: "dead", Storage: filepath.Join(roParent, "nope")}
	body, _ := json.Marshal(mount)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/vod", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// ─── Get ─────────────────────────────────────────────────────────────────────

func TestVODHandler_Get_Found(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	mount := vodMountInTempDir(t, "movies")
	require.NoError(t, repo.Save(t.Context(), mount))

	w := httptest.NewRecorder()
	h.Get(w, chiReq(t, http.MethodGet, "/vod/movies", nil, map[string]string{"name": "movies"}))
	require.Equal(t, http.StatusOK, w.Code)
}

func TestVODHandler_Get_NotFound(t *testing.T) {
	t.Parallel()
	h, _ := newVODHandlerForTest(t)
	w := httptest.NewRecorder()
	h.Get(w, chiReq(t, http.MethodGet, "/vod/missing", nil, map[string]string{"name": "missing"}))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// Get with a name that doesn't pass repo's FindByName returns 404 — the
// validator is lenient because lookup itself enforces existence.
func TestVODHandler_Get_MissingReturns404(t *testing.T) {
	t.Parallel()
	h, _ := newVODHandlerForTest(t)
	w := httptest.NewRecorder()
	h.Get(w, chiReq(t, http.MethodGet, "/vod/anything", nil, map[string]string{"name": "anything"}))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// ─── Update ─────────────────────────────────────────────────────────────────

func TestVODHandler_Update_HappyPath(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	mount := vodMountInTempDir(t, "movies")
	require.NoError(t, repo.Save(t.Context(), mount))

	updated := *mount
	updated.Storage = t.TempDir()
	body, _ := json.Marshal(updated)
	req := chiReq(t, http.MethodPut, "/vod/movies", body, map[string]string{"name": "movies"})
	w := httptest.NewRecorder()
	h.Update(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestVODHandler_Update_NotFound(t *testing.T) {
	t.Parallel()
	h, _ := newVODHandlerForTest(t)
	body, _ := json.Marshal(vodMountInTempDir(t, "missing"))
	req := chiReq(t, http.MethodPut, "/vod/missing", body, map[string]string{"name": "missing"})
	w := httptest.NewRecorder()
	h.Update(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestVODHandler_Update_InvalidJSON(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	require.NoError(t, repo.Save(t.Context(), vodMountInTempDir(t, "movies")))
	req := chiReq(t, http.MethodPut, "/vod/movies", []byte(`{nope`), map[string]string{"name": "movies"})
	w := httptest.NewRecorder()
	h.Update(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// ─── Delete ─────────────────────────────────────────────────────────────────

func TestVODHandler_Delete_Existing(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	require.NoError(t, repo.Save(t.Context(), vodMountInTempDir(t, "movies")))

	req := chiReq(t, http.MethodDelete, "/vod/movies", nil, map[string]string{"name": "movies"})
	w := httptest.NewRecorder()
	h.Delete(w, req)
	assert.Equal(t, http.StatusNoContent, w.Code)
}

// Delete is idempotent — like Stream.Delete, repeating against a missing /
// invalid mount returns 204. The validation is forgiving because the
// downstream filesystem ops are no-ops when the path isn't tracked.
func TestVODHandler_Delete_InvalidIsIdempotent(t *testing.T) {
	t.Parallel()
	h, _ := newVODHandlerForTest(t)
	req := chiReq(t, http.MethodDelete, "/vod/missing", nil, map[string]string{"name": "missing"})
	w := httptest.NewRecorder()
	h.Delete(w, req)
	assert.Equal(t, http.StatusNoContent, w.Code)
}

// ─── ListFiles ───────────────────────────────────────────────────────────────

func TestVODHandler_ListFiles_HappyPath(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	mount := &domain.VODMount{Name: "movies", Storage: dir}
	require.NoError(t, repo.Save(t.Context(), mount))
	// Sync registry so the handler's ResolvePath finds the mount.
	require.NoError(t, h.resyncRegistry(t.Context()))

	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.mp4"), []byte("data"), 0o644))

	w := httptest.NewRecorder()
	h.ListFiles(w, chiReq(t, http.MethodGet, "/vod/movies/files", nil, map[string]string{"name": "movies"}))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "a.mp4")
}

func TestVODHandler_ListFiles_NotFound(t *testing.T) {
	t.Parallel()
	h, _ := newVODHandlerForTest(t)
	w := httptest.NewRecorder()
	h.ListFiles(w, chiReq(t, http.MethodGet, "/vod/missing/files", nil, map[string]string{"name": "missing"}))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestVODHandler_ListFiles_PathEscapesMount(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))

	req := chiReq(t, http.MethodGet, "/vod/movies/files?path=../escape", nil, map[string]string{"name": "movies"})
	w := httptest.NewRecorder()
	h.ListFiles(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// ─── Raw ─────────────────────────────────────────────────────────────────────

// chiRawReq builds a request with the wildcard "*" param chi uses for
// /vod/{name}/raw/* — Raw reads the file path via chi.URLParam(r, "*").
func chiRawReq(t *testing.T, name, sub string) *http.Request {
	t.Helper()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/vod/"+name+"/raw/"+sub, nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", name)
	rctx.URLParams.Add("*", sub)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

func TestVODHandler_Raw_HappyPath(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "clip.mp4"), []byte("payload"), 0o644))

	w := httptest.NewRecorder()
	h.Raw(w, chiRawReq(t, "movies", "clip.mp4"))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "payload", w.Body.String())
}

func TestVODHandler_Raw_MountNotFound(t *testing.T) {
	t.Parallel()
	h, _ := newVODHandlerForTest(t)
	w := httptest.NewRecorder()
	h.Raw(w, chiRawReq(t, "missing", "clip.mp4"))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestVODHandler_Raw_PathEscapes(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))

	w := httptest.NewRecorder()
	h.Raw(w, chiRawReq(t, "movies", "../etc/passwd"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestVODHandler_Raw_FileMissing(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))

	w := httptest.NewRecorder()
	h.Raw(w, chiRawReq(t, "movies", "ghost.mp4"))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestVODHandler_Raw_IsDirectoryReturns400(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "season1"), 0o755))

	w := httptest.NewRecorder()
	h.Raw(w, chiRawReq(t, "movies", "season1"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// ─── UploadFile ──────────────────────────────────────────────────────────────

// multipartUpload builds a multipart/form-data body with a single "file" part.
func multipartUpload(t *testing.T, filename string, body []byte) (io.Reader, string) {
	t.Helper()
	buf := &bytes.Buffer{}
	mw := multipart.NewWriter(buf)
	fw, err := mw.CreateFormFile("file", filename)
	require.NoError(t, err)
	_, err = fw.Write(body)
	require.NoError(t, err)
	require.NoError(t, mw.Close())
	return buf, mw.FormDataContentType()
}

func uploadReq(t *testing.T, name, query string, filename string, body []byte) *http.Request {
	t.Helper()
	r, ct := multipartUpload(t, filename, body)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/vod/"+name+"/files"+query, r)
	req.Header.Set("Content-Type", ct)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", name)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func TestVODHandler_UploadFile_HappyPath(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))

	w := httptest.NewRecorder()
	h.UploadFile(w, uploadReq(t, "movies", "", "trailer.mp4", []byte("vid-bytes")))
	require.Equal(t, http.StatusCreated, w.Code)

	stored, err := os.ReadFile(filepath.Join(dir, "trailer.mp4"))
	require.NoError(t, err)
	assert.Equal(t, []byte("vid-bytes"), stored)
}

func TestVODHandler_UploadFile_WithSubpath(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))

	w := httptest.NewRecorder()
	h.UploadFile(w, uploadReq(t, "movies", "?path=2026", "clip.mp4", []byte("x")))
	require.Equal(t, http.StatusCreated, w.Code)
	_, err := os.Stat(filepath.Join(dir, "2026", "clip.mp4"))
	assert.NoError(t, err)
}

func TestVODHandler_UploadFile_RejectsNonVideo(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))

	w := httptest.NewRecorder()
	h.UploadFile(w, uploadReq(t, "movies", "", "exploit.sh", []byte("rm -rf /")))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestVODHandler_UploadFile_DuplicateReturns409(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "trailer.mp4"), []byte("existing"), 0o644))

	w := httptest.NewRecorder()
	h.UploadFile(w, uploadReq(t, "movies", "", "trailer.mp4", []byte("new")))
	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestVODHandler_UploadFile_MountNotFound(t *testing.T) {
	t.Parallel()
	h, _ := newVODHandlerForTest(t)
	w := httptest.NewRecorder()
	h.UploadFile(w, uploadReq(t, "missing", "", "trailer.mp4", []byte("x")))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestVODHandler_UploadFile_MissingFileField(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))

	// Multipart body with a different field name.
	buf := &bytes.Buffer{}
	mw := multipart.NewWriter(buf)
	_, err := mw.CreateFormField("notfile")
	require.NoError(t, err)
	require.NoError(t, mw.Close())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/vod/movies/files", buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", "movies")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h.UploadFile(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// ─── DeleteFile ──────────────────────────────────────────────────────────────

func TestVODHandler_DeleteFile_HappyPath(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kill.mp4"), []byte("x"), 0o644))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/vod/movies/files/kill.mp4", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", "movies")
	rctx.URLParams.Add("*", "kill.mp4")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h.DeleteFile(w, req)
	require.Equal(t, http.StatusNoContent, w.Code)
	_, err := os.Stat(filepath.Join(dir, "kill.mp4"))
	assert.True(t, os.IsNotExist(err))
}

func TestVODHandler_DeleteFile_MountNotFound(t *testing.T) {
	t.Parallel()
	h, _ := newVODHandlerForTest(t)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/vod/missing/files/clip.mp4", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", "missing")
	rctx.URLParams.Add("*", "clip.mp4")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h.DeleteFile(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestVODHandler_DeleteFile_FileMissing(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/vod/movies/files/ghost.mp4", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", "movies")
	rctx.URLParams.Add("*", "ghost.mp4")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h.DeleteFile(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestVODHandler_DeleteFile_RejectsDirectory(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "season1"), 0o755))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/vod/movies/files/season1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", "movies")
	rctx.URLParams.Add("*", "season1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h.DeleteFile(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestVODHandler_DeleteFile_PathEscapes(t *testing.T) {
	t.Parallel()
	h, repo := newVODHandlerForTest(t)
	dir := t.TempDir()
	require.NoError(t, repo.Save(t.Context(), &domain.VODMount{Name: "movies", Storage: dir}))
	require.NoError(t, h.resyncRegistry(t.Context()))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/vod/movies/files/..%2Fpasswd", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("name", "movies")
	rctx.URLParams.Add("*", "../passwd")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h.DeleteFile(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}
