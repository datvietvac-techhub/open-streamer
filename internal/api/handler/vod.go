package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/samber/do/v2"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
	"github.com/datvietvac-techhub/open-streamer/internal/vod"
)

// uploadMaxBytes caps a single VOD upload. Hard-coded for now; if operators
// need to lift it, expose via config later. Multipart parsing buffers up to
// uploadMemoryBytes in RAM and spills the rest to a temp file.
const (
	uploadMaxBytes    = 10 << 30 // 10 GiB
	uploadMemoryBytes = 32 << 20 // 32 MiB
)

// VODHandler manages VOD mount registrations and serves live filesystem
// listings for the UI. The system never indexes file content — listings come
// straight from the host filesystem on every request.
type VODHandler struct {
	repo     store.VODMountRepository
	registry *vod.Registry
}

// NewVODHandler wires the handler from DI.
func NewVODHandler(i do.Injector) (*VODHandler, error) {
	return &VODHandler{
		repo:     do.MustInvoke[store.VODMountRepository](i),
		registry: do.MustInvoke[*vod.Registry](i),
	}, nil
}

// resyncRegistry reloads the in-memory registry from the persisted mount table.
// Called after every successful CRUD so the next ingest start sees the change.
func (h *VODHandler) resyncRegistry(ctx context.Context) error {
	mounts, err := h.repo.List(ctx)
	if err != nil {
		return err
	}
	h.registry.Sync(mounts)
	return nil
}

// List returns all registered VOD mounts.
//
// @Summary     List VOD mounts
// @Tags        vod
// @Produce     json
// @Success     200 {object} apidocs.VODMountList
// @Failure     500 {object} apidocs.ErrorBody
// @Router      /vod [get].
func (h *VODHandler) List(w http.ResponseWriter, r *http.Request) {
	mounts, err := h.repo.List(r.Context())
	if err != nil {
		serverError(w, r, "LIST_FAILED", "list vod mounts", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": mounts, "total": len(mounts)})
}

// Create registers a new VOD mount. Name must be unique; storage must be an
// absolute host path. The directory is not required to exist at create time —
// operators may provision it later — but a missing directory will surface as
// an error from List/Resolve until it is fixed.
//
// @Summary     Create VOD mount
// @Tags        vod
// @Accept      json
// @Produce     json
// @Param       body body domain.VODMount true "VOD mount"
// @Success     201 {object} apidocs.VODMountData
// @Failure     400 {object} apidocs.ErrorBody
// @Failure     409 {object} apidocs.ErrorBody
// @Failure     500 {object} apidocs.ErrorBody
// @Router      /vod [post].
func (h *VODHandler) Create(w http.ResponseWriter, r *http.Request) {
	var mount domain.VODMount
	if err := json.NewDecoder(r.Body).Decode(&mount); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_BODY", err.Error())
		return
	}
	if err := domain.ValidateVODName(string(mount.Name)); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_NAME", err.Error())
		return
	}
	if err := mount.ValidateStorage(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_STORAGE", err.Error())
		return
	}
	if err := ensureMountStorageWritable(mount.Storage); err != nil {
		writeError(w, http.StatusBadRequest, "STORAGE_NOT_WRITABLE",
			"vod mount storage is not writable by the service: "+err.Error())
		return
	}
	if _, err := h.repo.FindByName(r.Context(), mount.Name); err == nil {
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "vod mount already exists")
		return
	} else if !errors.Is(err, store.ErrNotFound) {
		serverError(w, r, "LOOKUP_FAILED", "lookup vod mount", err)
		return
	}
	if err := h.repo.Save(r.Context(), &mount); err != nil {
		serverError(w, r, "SAVE_FAILED", "create vod mount", err)
		return
	}
	if err := h.resyncRegistry(r.Context()); err != nil {
		serverError(w, r, "SYNC_FAILED", "resync vod registry after create", err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"data": mount})
}

// Get returns one mount by name.
//
// @Summary     Get VOD mount
// @Tags        vod
// @Produce     json
// @Param       name path string true "Mount name"
// @Success     200 {object} apidocs.VODMountData
// @Failure     404 {object} apidocs.ErrorBody
// @Router      /vod/{name} [get].
func (h *VODHandler) Get(w http.ResponseWriter, r *http.Request) {
	name := domain.VODName(chi.URLParam(r, "name"))
	mount, err := h.repo.FindByName(r.Context(), name)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": mount})
}

// Update replaces a mount's storage / comment. Name in URL wins over body.
//
// @Summary     Update VOD mount
// @Tags        vod
// @Accept      json
// @Produce     json
// @Param       name path string true "Mount name"
// @Param       body body domain.VODMount true "VOD mount"
// @Success     200 {object} apidocs.VODMountData
// @Failure     400 {object} apidocs.ErrorBody
// @Failure     404 {object} apidocs.ErrorBody
// @Failure     500 {object} apidocs.ErrorBody
// @Router      /vod/{name} [put].
func (h *VODHandler) Update(w http.ResponseWriter, r *http.Request) {
	name := domain.VODName(chi.URLParam(r, "name"))
	if _, err := h.repo.FindByName(r.Context(), name); err != nil {
		writeStoreError(w, r, err)
		return
	}
	var mount domain.VODMount
	if err := json.NewDecoder(r.Body).Decode(&mount); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_BODY", err.Error())
		return
	}
	mount.Name = name
	if err := mount.ValidateStorage(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_STORAGE", err.Error())
		return
	}
	if err := ensureMountStorageWritable(mount.Storage); err != nil {
		writeError(w, http.StatusBadRequest, "STORAGE_NOT_WRITABLE",
			"vod mount storage is not writable by the service: "+err.Error())
		return
	}
	if err := h.repo.Save(r.Context(), &mount); err != nil {
		serverError(w, r, "SAVE_FAILED", "update vod mount", err)
		return
	}
	if err := h.resyncRegistry(r.Context()); err != nil {
		serverError(w, r, "SYNC_FAILED", "resync vod registry after update", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": mount})
}

// Delete removes a mount. Files on disk are untouched.
//
// @Summary     Delete VOD mount
// @Tags        vod
// @Param       name path string true "Mount name"
// @Success     204 "No Content"
// @Failure     500 {object} apidocs.ErrorBody
// @Router      /vod/{name} [delete].
func (h *VODHandler) Delete(w http.ResponseWriter, r *http.Request) {
	name := domain.VODName(chi.URLParam(r, "name"))
	if err := h.repo.Delete(r.Context(), name); err != nil {
		serverError(w, r, "DELETE_FAILED", "delete vod mount", err)
		return
	}
	if err := h.resyncRegistry(r.Context()); err != nil {
		serverError(w, r, "SYNC_FAILED", "resync vod registry after delete", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ListFiles lists the contents of a directory inside a mount, one level deep.
// The optional ?path= query selects a subdirectory; defaults to the mount root.
// Listings are computed live from the host filesystem; the system never caches
// or indexes file content.
//
// @Summary     List files in a VOD mount
// @Tags        vod
// @Produce     json
// @Param       name path  string true  "Mount name"
// @Param       path query string false "Subdirectory (relative, no leading slash)"
// @Success     200 {object} apidocs.VODFileList
// @Failure     400 {object} apidocs.ErrorBody
// @Failure     404 {object} apidocs.ErrorBody
// @Failure     500 {object} apidocs.ErrorBody
// @Router      /vod/{name}/files [get].
func (h *VODHandler) ListFiles(w http.ResponseWriter, r *http.Request) {
	name := domain.VODName(chi.URLParam(r, "name"))
	subPath := r.URL.Query().Get("path")

	entries, err := h.registry.ListFiles(name, subPath)
	if err != nil {
		switch {
		case errors.Is(err, vod.ErrMountNotFound):
			writeError(w, http.StatusNotFound, "MOUNT_NOT_FOUND", err.Error())
		case errors.Is(err, vod.ErrPathEscapesMount):
			writeError(w, http.StatusBadRequest, "INVALID_PATH", err.Error())
		default:
			serverError(w, r, "LIST_FAILED", "list vod files", err)
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data":  entries,
		"total": len(entries),
		"path":  subPath,
	})
}

// Raw streams a single file from a VOD mount over HTTP. The browser hits
// this endpoint directly via the play_url returned by ListFiles. http.ServeFile
// handles Content-Type detection and Range requests so the browser's <video>
// tag can seek inside the file.
//
// @Summary     Stream a VOD file over HTTP
// @Description Returns the raw bytes of a file inside a VOD mount. Supports HTTP Range requests so an HTML5 video player can seek.
// @Tags        vod
// @Produce     octet-stream
// @Param       name path string true "Mount name"
// @Param       path path string true "File path inside the mount (forward-slash separated)"
// @Success     200 {file} file
// @Success     206 {file} file
// @Failure     400 {object} apidocs.ErrorBody
// @Failure     404 {object} apidocs.ErrorBody
// @Router      /vod/{name}/raw/{path} [get].
func (h *VODHandler) Raw(w http.ResponseWriter, r *http.Request) {
	name := domain.VODName(chi.URLParam(r, "name"))
	subPath := chi.URLParam(r, "*")

	abs, err := h.registry.ResolvePath(name, subPath)
	if err != nil {
		switch {
		case errors.Is(err, vod.ErrMountNotFound):
			writeError(w, http.StatusNotFound, "MOUNT_NOT_FOUND", err.Error())
		case errors.Is(err, vod.ErrPathEscapesMount):
			writeError(w, http.StatusBadRequest, "INVALID_PATH", err.Error())
		default:
			writeError(w, http.StatusBadRequest, "INVALID_PATH", err.Error())
		}
		return
	}

	// Stat first so directories and missing files return clean JSON errors
	// instead of http.ServeFile's text/html "404 page not found" response.
	info, err := os.Stat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			writeError(w, http.StatusNotFound, "FILE_NOT_FOUND", err.Error())
			return
		}
		serverError(w, r, "STAT_FAILED", "stat vod file for raw serve", err)
		return
	}
	if info.IsDir() {
		writeError(w, http.StatusBadRequest, "IS_DIRECTORY", "path refers to a directory")
		return
	}

	http.ServeFile(w, r, abs)
}

// UploadFile accepts a multipart upload and stores it inside a VOD mount.
// The form field "file" holds the bytes; the optional ?path= query selects a
// subdirectory under the mount root (created on demand). Only files with a
// known video extension are accepted; the destination filename is taken from
// the upload header and stripped to its base via filepath.Base. Existing files
// are not overwritten — clients must DELETE first.
//
// @Summary     Upload a file to a VOD mount
// @Tags        vod
// @Accept      multipart/form-data
// @Produce     json
// @Param       name path     string true  "Mount name"
// @Param       path query    string false "Subdirectory (relative, no leading slash)"
// @Param       file formData file   true  "Video file (mp4, mkv, mov, …)"
// @Success     201 {object} apidocs.VODFileData
// @Failure     400 {object} apidocs.ErrorBody
// @Failure     404 {object} apidocs.ErrorBody
// @Failure     409 {object} apidocs.ErrorBody
// @Failure     413 {object} apidocs.ErrorBody
// @Failure     500 {object} apidocs.ErrorBody
// @Router      /vod/{name}/files [post].
func (h *VODHandler) UploadFile(w http.ResponseWriter, r *http.Request) {
	name := domain.VODName(chi.URLParam(r, "name"))
	subPath := r.URL.Query().Get("path")

	r.Body = http.MaxBytesReader(w, r.Body, uploadMaxBytes)
	if err := r.ParseMultipartForm(uploadMemoryBytes); err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "UPLOAD_TOO_LARGE", err.Error())
		return
	}

	src, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_BODY", "missing 'file' field: "+err.Error())
		return
	}
	defer func() { _ = src.Close() }()

	filename := filepath.Base(header.Filename)
	if filename == "" || filename == "." || filename == string(filepath.Separator) {
		writeError(w, http.StatusBadRequest, "INVALID_FILENAME", "missing filename")
		return
	}
	if !vod.IsVideoFile(filename) {
		writeError(w, http.StatusBadRequest, "INVALID_FILENAME", "unsupported file extension")
		return
	}

	cleanSub := strings.TrimPrefix(strings.TrimSpace(subPath), "/")
	relPath := filepath.ToSlash(filepath.Join(cleanSub, filename))

	abs, err := h.registry.ResolvePath(name, relPath)
	if err != nil {
		switch {
		case errors.Is(err, vod.ErrMountNotFound):
			writeError(w, http.StatusNotFound, "MOUNT_NOT_FOUND", err.Error())
		case errors.Is(err, vod.ErrPathEscapesMount):
			writeError(w, http.StatusBadRequest, "INVALID_PATH", err.Error())
		default:
			writeError(w, http.StatusBadRequest, "INVALID_PATH", err.Error())
		}
		return
	}

	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		serverError(w, r, "MKDIR_FAILED", "create vod upload directory", err)
		return
	}

	if _, err := os.Stat(abs); err == nil {
		writeError(w, http.StatusConflict, "FILE_EXISTS", "file already exists")
		return
	} else if !os.IsNotExist(err) {
		serverError(w, r, "STAT_FAILED", "stat vod upload destination", err)
		return
	}

	written, err := writeUploadAtomic(abs, src)
	if err != nil {
		serverError(w, r, "UPLOAD_FAILED", "write vod upload", err)
		return
	}

	entry := vod.FileEntry{
		Name:      filename,
		Path:      relPath,
		Size:      written,
		PlayURL:   "/vod/" + string(name) + "/raw/" + relPath,
		IngestURL: "file://" + string(name) + "/" + relPath,
	}
	writeJSON(w, http.StatusCreated, map[string]any{"data": entry})
}

// DeleteFile removes a single file from a VOD mount. The wildcard path is the
// file's location relative to the mount root. Directories are refused — this
// endpoint is intentionally narrow so an operator typo cannot wipe a tree.
//
// @Summary     Delete a file from a VOD mount
// @Tags        vod
// @Param       name path string true "Mount name"
// @Param       path path string true "File path inside the mount (forward-slash separated)"
// @Success     204 "No Content"
// @Failure     400 {object} apidocs.ErrorBody
// @Failure     404 {object} apidocs.ErrorBody
// @Failure     500 {object} apidocs.ErrorBody
// @Router      /vod/{name}/files/{path} [delete].
func (h *VODHandler) DeleteFile(w http.ResponseWriter, r *http.Request) {
	name := domain.VODName(chi.URLParam(r, "name"))
	subPath := chi.URLParam(r, "*")

	abs, err := h.registry.ResolvePath(name, subPath)
	if err != nil {
		switch {
		case errors.Is(err, vod.ErrMountNotFound):
			writeError(w, http.StatusNotFound, "MOUNT_NOT_FOUND", err.Error())
		case errors.Is(err, vod.ErrPathEscapesMount):
			writeError(w, http.StatusBadRequest, "INVALID_PATH", err.Error())
		default:
			writeError(w, http.StatusBadRequest, "INVALID_PATH", err.Error())
		}
		return
	}

	info, err := os.Stat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			writeError(w, http.StatusNotFound, "FILE_NOT_FOUND", err.Error())
			return
		}
		serverError(w, r, "STAT_FAILED", "stat vod file for delete", err)
		return
	}
	if info.IsDir() {
		writeError(w, http.StatusBadRequest, "IS_DIRECTORY", "refusing to delete a directory")
		return
	}

	if err := os.Remove(abs); err != nil {
		serverError(w, r, "DELETE_FAILED", "remove vod file", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeUploadAtomic streams src to dst via a sibling .tmp file then renames it
// into place. The .tmp file is removed on any failure so a half-written upload
// never leaves debris next to the real file.
func writeUploadAtomic(dst string, src io.Reader) (int64, error) {
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return 0, err
	}
	written, copyErr := io.Copy(out, src)
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(tmp)
		if copyErr != nil {
			return 0, copyErr
		}
		return 0, closeErr
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	return written, nil
}

// ensureMountStorageWritable guarantees the mount's storage path exists and is
// writable by the running service. Behaviour:
//
//  1. MkdirAll(path, 0o755) — creates the directory tree if missing. No-op
//     when the directory already exists (regardless of its actual perms).
//  2. Write probe — creates a sentinel file inside the directory and removes
//     it. This catches the common "directory pre-created by root, service
//     runs as a different user" case that MkdirAll silently swallows.
//
// Returning a clear error here lets the API surface a descriptive 400 at
// mount-creation time, instead of failing per-upload later with a confusing
// "permission denied" deep in the file pipeline.
func ensureMountStorageWritable(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}
	probe, err := os.CreateTemp(path, ".open-streamer-write-probe-*")
	if err != nil {
		return fmt.Errorf("write probe: %w", err)
	}
	probeName := probe.Name()
	_ = probe.Close()
	_ = os.Remove(probeName)
	return nil
}
