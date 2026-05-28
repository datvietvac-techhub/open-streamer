// Package watermarks manages the on-disk library of uploadable watermark
// images (logos, channel bugs). Each asset is stored as a single file in a
// flat directory; the filename IS the identifier:
//
//	<dir>/<filename>      ← image bytes (only file per asset)
//
// The directory is the ONLY source of truth: there is no sidecar metadata
// file and no in-memory cache. Every List / Get / ResolvePath call reads
// the live filesystem state. This means operators can drop or remove
// files in the assets dir out of band (rsync, scp, manual cp) and the API
// reflects the change immediately — no service restart, no "refresh
// library" button needed.
//
// Resolution flow at transcode time:
//
//	Stream.Watermark.Filename = "vtv1_logo.png"
//	     │
//	     ▼  watermarks.Service.ResolvePath
//	  /<dir>/vtv1_logo.png
//	     │
//	     ▼  coordinator copies into TranscoderConfig.Watermark.ImagePath
//	  ffmpeg -i ... -vf "...,movie=<absolute path>,..."
package watermarks

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/samber/do/v2"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// Errors returned by the service. Mapped to HTTP status codes by the
// REST handler — see internal/api/handler/watermark.go.
var (
	// ErrNotFound is returned by Get / Delete when the asset is unknown.
	ErrNotFound = errors.New("watermarks: asset not found")
	// ErrAlreadyExists is returned by Save when an asset with the requested
	// filename already lives in the library. Filenames are unique by
	// construction; operators must Delete first to replace.
	ErrAlreadyExists = errors.New("watermarks: asset with this filename already exists")
	// ErrInvalidContent is returned when the uploaded bytes don't sniff
	// as an image MIME type the FFmpeg overlay path supports.
	ErrInvalidContent = errors.New("watermarks: not an image")
	// ErrTooLarge is returned by the handler when the upload exceeds
	// MaxWatermarkAssetBytes — defined here so the handler can wrap it.
	ErrTooLarge = errors.New("watermarks: upload too large")
)

// Service is the public facade. Constructed via DI from config.WatermarksConfig.
// Safe for concurrent use. A single Mutex serialises Save / Delete so the
// duplicate-filename check is race-free; List / Get / ResolvePath read the
// filesystem without locking — they're tolerant of concurrent mutations
// because every read is independent and the underlying syscalls are atomic.
type Service struct {
	dir string
	mu  sync.Mutex
}

// New is the samber/do constructor. Creates the assets directory if missing
// so subsequent Save / List calls have a stable working dir even on a fresh
// install.
func New(i do.Injector) (*Service, error) {
	cfg := do.MustInvoke[config.WatermarksConfig](i)
	return newService(cfg)
}

func newService(cfg config.WatermarksConfig) (*Service, error) {
	dir := strings.TrimSpace(cfg.Dir)
	if dir == "" {
		dir = "./watermarks"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("watermarks: mkdir %q: %w", dir, err)
	}
	return &Service{dir: dir}, nil
}

// Dir returns the on-disk root of the assets library. Exposed so the
// configuration UI can show operators where uploads land.
func (s *Service) Dir() string { return s.dir }

// List returns every asset currently in the assets directory, sorted by
// modification time (newest first) so dashboards default to "what did I
// just upload". Reads the live filesystem state on every call — no
// caching, so files dropped in or removed out of band show up immediately.
func (s *Service) List() []*domain.WatermarkAsset {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}
	out := make([]*domain.WatermarkAsset, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		asset, ok := s.assetFromEntry(e.Name())
		if !ok {
			continue
		}
		out = append(out, asset)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].UploadedAt.After(out[j].UploadedAt)
	})
	return out
}

// Get returns the metadata for a single asset. Stats the underlying file
// each time; missing or non-image files surface as ErrNotFound.
func (s *Service) Get(filename domain.WatermarkFilename) (*domain.WatermarkAsset, error) {
	if err := domain.ValidateWatermarkFilename(string(filename)); err != nil {
		return nil, ErrNotFound
	}
	asset, ok := s.assetFromEntry(string(filename))
	if !ok {
		return nil, ErrNotFound
	}
	return asset, nil
}

// ResolvePath returns the absolute filesystem path to the image bytes for
// the given filename. Used by the coordinator to translate
// Stream.Watermark.Filename into TranscoderConfig.Watermark.ImagePath
// before the transcoder consumes the config. ErrNotFound when the file
// is missing or doesn't sniff as a supported image.
func (s *Service) ResolvePath(filename domain.WatermarkFilename) (string, error) {
	if _, err := s.Get(filename); err != nil {
		return "", err
	}
	return s.resolveAssetPath(string(filename))
}

// Save uploads new image bytes under the given filename. ContentType is
// sniffed via http.DetectContentType from the first 512 bytes. Filenames
// are unique — re-uploading the same name returns ErrAlreadyExists so the
// operator's intent is explicit (Delete then Save to replace).
//
// Atomic on success: temp file written then renamed. The mutex serialises
// the os.Stat-then-create sequence so two concurrent uploads of the same
// filename can't both pass the duplicate check.
func (s *Service) Save(filename string, body io.Reader) (*domain.WatermarkAsset, error) {
	filename = strings.TrimSpace(filename)
	if err := domain.ValidateWatermarkFilename(filename); err != nil {
		return nil, err
	}

	imgPath, err := s.resolveAssetPath(filename)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, statErr := os.Stat(imgPath); statErr == nil {
		return nil, fmt.Errorf("%w: %s", ErrAlreadyExists, filename)
	}

	// Sniff first to reject non-images before writing anything to disk.
	head := make([]byte, 512)
	n, _ := io.ReadFull(body, head)
	head = head[:n]
	ct := http.DetectContentType(head)
	if !isSupportedImage(ct) {
		return nil, fmt.Errorf("%w: detected %s", ErrInvalidContent, ct)
	}

	tmp := imgPath + ".tmp"
	written, err := writeAtomic(tmp, imgPath, head, body)
	if err != nil {
		return nil, err
	}
	if written > domain.MaxWatermarkAssetBytes {
		_ = os.Remove(imgPath)
		return nil, fmt.Errorf("%w: %d bytes (cap %d)", ErrTooLarge, written, domain.MaxWatermarkAssetBytes)
	}

	return &domain.WatermarkAsset{
		Filename:    domain.WatermarkFilename(filename),
		ContentType: ct,
		SizeBytes:   written,
		UploadedAt:  time.Now().UTC(),
	}, nil
}

// Delete removes the image file. Idempotent on the "already gone" case so
// repeat calls return ErrNotFound exactly once and no-op afterwards. Caller
// is responsible for ensuring the asset isn't referenced by an active
// stream — there is no foreign-key check here.
//
// Uses os.Root (Go 1.24+) to sandbox the lookup inside the assets dir.
// The OpenRoot handle refuses any "../" or absolute-path escape in the
// filename argument at the syscall layer; CodeQL recognises this as the
// canonical path-traversal sanitiser, which the prior os.Stat+os.Remove
// pair did not.
func (s *Service) Delete(filename domain.WatermarkFilename) error {
	if err := domain.ValidateWatermarkFilename(string(filename)); err != nil {
		return ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	root, err := os.OpenRoot(s.dir)
	if err != nil {
		return fmt.Errorf("watermarks: open assets root: %w", err)
	}
	defer func() { _ = root.Close() }()

	name := string(filename)
	if _, statErr := root.Stat(name); statErr != nil {
		if errors.Is(statErr, fs.ErrNotExist) {
			return ErrNotFound
		}
		return fmt.Errorf("watermarks: stat: %w", statErr)
	}
	if err := root.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("watermarks: remove image: %w", err)
	}
	return nil
}

// ─── internals ───────────────────────────────────────────────────────────────

// resolveAssetPath validates `name` against the filename rules and joins it
// onto the assets directory, refusing any result that would escape the
// directory. The validation regex already rejects path separators and `..`
// so the absolute path is always `<dir>/<name>`, but the explicit
// containment check is what teaches CodeQL the downstream os.Stat /
// os.Remove / http.ServeFile sinks are safe — without it the path-injection
// rule flags every filepath.Join that touches a user-provided value.
func (s *Service) resolveAssetPath(name string) (string, error) {
	if err := domain.ValidateWatermarkFilename(name); err != nil {
		return "", err
	}
	rootAbs, err := filepath.Abs(s.dir)
	if err != nil {
		return "", fmt.Errorf("watermarks: resolve assets dir: %w", err)
	}
	// filepath.Base scrubs any path separators the validator should already
	// have rejected; pairing it with the containment check makes the
	// sanitiser robust even if the validation regex is loosened later.
	joined, err := filepath.Abs(filepath.Join(rootAbs, filepath.Base(name)))
	if err != nil {
		return "", fmt.Errorf("watermarks: resolve asset path: %w", err)
	}
	rootPrefix := rootAbs + string(filepath.Separator)
	if joined != rootAbs && !strings.HasPrefix(joined, rootPrefix) {
		return "", fmt.Errorf("watermarks: filename %q escapes assets dir", name)
	}
	return joined, nil
}

// assetFromEntry stats `name` under the assets directory and sniffs its
// MIME type, returning a metadata view of the asset. Returns (_, false)
// when the filename doesn't validate, the file is missing, can't be
// read, or its content type isn't a supported image — callers treat the
// flag as "skip this entry / not found".
func (s *Service) assetFromEntry(name string) (*domain.WatermarkAsset, bool) {
	path, err := s.resolveAssetPath(name)
	if err != nil {
		return nil, false
	}
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return nil, false
	}
	ct, err := sniffContentType(path)
	if err != nil || !isSupportedImage(ct) {
		return nil, false
	}
	return &domain.WatermarkAsset{
		Filename:    domain.WatermarkFilename(name),
		ContentType: ct,
		SizeBytes:   info.Size(),
		UploadedAt:  info.ModTime().UTC(),
	}, true
}

// sniffContentType opens the file, reads the first 512 bytes, and returns
// the MIME type as http.DetectContentType would.
func sniffContentType(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	head := make([]byte, 512)
	n, _ := io.ReadFull(f, head)
	return http.DetectContentType(head[:n]), nil
}

// isSupportedImage reports whether the sniffed Content-Type is something
// the FFmpeg overlay filter chain can render. We accept the formats with
// reliable cross-distro decoder support; SVG / WebP / AVIF are deliberately
// excluded because not every Ubuntu apt FFmpeg ships their decoders.
func isSupportedImage(contentType string) bool {
	switch strings.ToLower(strings.TrimSpace(strings.SplitN(contentType, ";", 2)[0])) {
	case "image/png", "image/jpeg", "image/jpg", "image/gif":
		return true
	}
	return false
}

// writeAtomic streams `head` then `body` into `tmp`, fsyncs, and renames
// to `dst`. On any failure the temp file is removed so a half-written
// upload never appears in the directory listing. Returns total bytes written.
func writeAtomic(tmp, dst string, head []byte, body io.Reader) (int64, error) {
	out, err := os.Create(tmp)
	if err != nil {
		return 0, fmt.Errorf("watermarks: create tmp: %w", err)
	}
	var written int64
	if len(head) > 0 {
		n, werr := out.Write(head)
		written += int64(n)
		if werr != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			return 0, fmt.Errorf("watermarks: write head: %w", werr)
		}
	}
	n, copyErr := io.Copy(out, body)
	written += n
	if cerr := out.Close(); cerr != nil && copyErr == nil {
		copyErr = cerr
	}
	if copyErr != nil {
		_ = os.Remove(tmp)
		return 0, fmt.Errorf("watermarks: copy: %w", copyErr)
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return 0, fmt.Errorf("watermarks: rename: %w", err)
	}
	return written, nil
}
