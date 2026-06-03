// Package vod implements the VOD mount registry.
//
// A VOD mount maps a logical name to a host filesystem directory. The system
// does not maintain a file index — file lookups (resolve a URL to an absolute
// path) and listings (enumerate files for the UI) are answered live against
// the host filesystem.
//
// Ingest URLs reference files via file://<mount>/<relative/path>; only files
// inside a registered mount can be played. Bare host paths and absolute file://
// URLs are rejected — the mount layer is the single point of policy.
package vod

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// ErrMountNotFound is returned when a URL references a mount that has not been registered.
var ErrMountNotFound = errors.New("vod: mount not found")

// ErrPathEscapesMount is returned when a resolved path falls outside its mount's storage root.
// This protects against ../ traversal in user-supplied URLs.
var ErrPathEscapesMount = errors.New("vod: path escapes mount root")

// ErrNotFileURL is returned when Resolve is called with a URL whose scheme is not file://.
var ErrNotFileURL = errors.New("vod: not a file:// url")

// FileEntry describes a single entry returned by ListFiles.
// Size is zero for directories. Empty for directories:
//   - PlayURL: HTTP path the browser hits to stream the file (Range-aware).
//   - IngestURL: the file:// URL that an ingestor stream input accepts.
type FileEntry struct {
	Name      string `json:"name"`
	Path      string `json:"path"` // path relative to the mount root, with forward slashes
	Size      int64  `json:"size"`
	IsDir     bool   `json:"is_dir"`
	ModTime   int64  `json:"mod_time_unix"`
	PlayURL   string `json:"play_url,omitempty"`
	IngestURL string `json:"ingest_url,omitempty"`
}

// videoExtensions is the allowlist of file extensions surfaced by ListFiles.
// Anything else (subtitles, sidecar JSON, system files…) is hidden from the
// client because it cannot be played by the ingestor anyway. Directories are
// always shown regardless of name so the user can navigate the tree.
var videoExtensions = map[string]struct{}{
	".mp4":  {},
	".m4v":  {},
	".mov":  {},
	".mkv":  {},
	".webm": {},
	".avi":  {},
	".flv":  {},
	".ts":   {},
	".mts":  {},
	".m2ts": {},
	".mpg":  {},
	".mpeg": {},
	".wmv":  {},
	".3gp":  {},
}

// IsVideoFile reports whether name has a known video extension.
func IsVideoFile(name string) bool {
	_, ok := videoExtensions[strings.ToLower(filepath.Ext(name))]
	return ok
}

// Registry holds the active mount table and answers Resolve / ListFiles queries.
// It is safe for concurrent use.
type Registry struct {
	mu     sync.RWMutex
	mounts map[domain.VODName]string // name → cleaned absolute storage path
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{mounts: make(map[domain.VODName]string)}
}

// Sync replaces the mount table with the given list. Invalid entries (empty
// name, non-absolute storage) are skipped silently — validation belongs to the
// API layer; here we just refuse to install a broken mount that would later
// fail to resolve anyway.
func (r *Registry) Sync(mounts []*domain.VODMount) {
	next := make(map[domain.VODName]string, len(mounts))
	for _, m := range mounts {
		if m == nil {
			continue
		}
		if domain.ValidateVODName(string(m.Name)) != nil {
			continue
		}
		storage := filepath.Clean(strings.TrimSpace(m.Storage))
		if !filepath.IsAbs(storage) {
			continue
		}
		next[m.Name] = storage
	}
	r.mu.Lock()
	r.mounts = next
	r.mu.Unlock()
}

// Names returns a sorted snapshot of the registered mount names. For diagnostics.
func (r *Registry) Names() []domain.VODName {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]domain.VODName, 0, len(r.mounts))
	for n := range r.mounts {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Storage returns the absolute storage path registered for name, or false if
// the mount is not registered.
func (r *Registry) Storage(name domain.VODName) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.mounts[name]
	return s, ok
}

// ResolvePath maps (mount name + relative subPath) to an absolute filesystem
// path inside the mount, applying the same traversal protection as Resolve.
// Used by the HTTP raw-file handler that serves files directly to browsers.
func (r *Registry) ResolvePath(name domain.VODName, subPath string) (string, error) {
	storage, ok := r.Storage(name)
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrMountNotFound, name)
	}
	clean := strings.TrimSpace(subPath)
	clean = strings.TrimPrefix(clean, "/")
	if clean == "" {
		return "", fmt.Errorf("vod: empty path under mount %q", name)
	}
	resolved := filepath.Clean(filepath.Join(storage, filepath.FromSlash(clean)))
	if !pathInside(resolved, storage) {
		return "", fmt.Errorf("%w: %q", ErrPathEscapesMount, subPath)
	}
	return resolved, nil
}

// Resolve maps a file:// URL to an absolute filesystem path inside a mount.
//
// Accepts URLs of the form:
//
//	file://<mount>/path/to/file.mp4[?loop=true|false]
//
// Loop default is TRUE — file:// inputs replay from the beginning when they
// reach EOF, treating the file as a continuous stream source. Operators who
// want one-shot playback must opt out with `?loop=false`. Rationale: in a
// streaming server, a file is almost always used as a fallback / standby /
// test source where unattended end-of-file means the stream is silently
// dead, which is rarely intended.
//
// Rejects bare paths, file:///absolute paths, and URLs whose host is not a
// registered mount. Also rejects resolved paths that escape the mount root
// (lexical check after Clean — symlink walks are intentionally not followed,
// since admins control both the mount table and the storage tree).
func (r *Registry) Resolve(rawURL string) (path string, loop bool, err error) {
	if !strings.HasPrefix(rawURL, "file://") {
		return "", false, ErrNotFileURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", false, fmt.Errorf("vod: parse url: %w", err)
	}
	if u.Host == "" {
		return "", false, fmt.Errorf("vod: url %q has no mount name (use file://<mount>/path)", rawURL)
	}

	name := domain.VODName(u.Host)
	storage, ok := r.Storage(name)
	if !ok {
		return "", false, fmt.Errorf("%w: %q", ErrMountNotFound, name)
	}

	// u.Path is the URL path (always starts with "/" when host is present and
	// path is non-empty). filepath.Clean normalises ".." and duplicate slashes;
	// the prefix check below catches any remaining attempt to escape.
	rel := strings.TrimPrefix(u.Path, "/")
	if rel == "" {
		return "", false, fmt.Errorf("vod: url %q has no file path", rawURL)
	}
	resolved := filepath.Clean(filepath.Join(storage, filepath.FromSlash(rel)))
	if !pathInside(resolved, storage) {
		return "", false, fmt.Errorf("%w: %q resolves outside %q", ErrPathEscapesMount, rawURL, storage)
	}

	return resolved, parseLoopParam(u.Query().Get("loop")), nil
}

// parseLoopParam interprets the `loop` query parameter on file:// URLs.
// Default (param empty) is true; explicit false/0/no opts out. Anything else
// also defaults to true to avoid silent surprises from typos.
func parseLoopParam(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "false", "0", "no", "off":
		return false
	default:
		return true
	}
}

// ListFiles enumerates the contents of subPath inside mount name.
// subPath is interpreted with the same rules as Resolve (must stay inside the
// mount root). Pass "" or "/" to list the mount root.
//
// Only directories and files with a known video extension are returned —
// sidecar files (subtitles, JSON metadata, dotfiles…) are hidden because the
// ingestor cannot play them. Each video file carries a PlayURL the client
// can hand back to the ingestor as-is.
//
// Listing is non-recursive: one directory level at a time, sorted by name.
// Returns ErrMountNotFound when the mount is unknown, ErrPathEscapesMount
// when subPath tries to traverse outside the mount, or a wrapped os error
// when the underlying filesystem read fails.
func (r *Registry) ListFiles(name domain.VODName, subPath string) ([]FileEntry, error) {
	storage, ok := r.Storage(name)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrMountNotFound, name)
	}

	clean := strings.TrimSpace(subPath)
	clean = strings.TrimPrefix(clean, "/")
	target := storage
	if clean != "" {
		target = filepath.Clean(filepath.Join(storage, filepath.FromSlash(clean)))
		if !pathInside(target, storage) {
			return nil, fmt.Errorf("%w: %q", ErrPathEscapesMount, subPath)
		}
	}

	dirEntries, err := os.ReadDir(target)
	if err != nil {
		return nil, fmt.Errorf("vod: list %q: %w", target, err)
	}

	out := make([]FileEntry, 0, len(dirEntries))
	for _, e := range dirEntries {
		if !e.IsDir() && !IsVideoFile(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			// Stat failed (file removed between ReadDir and Info); skip silently.
			continue
		}
		rel := filepath.ToSlash(filepath.Join(clean, e.Name()))
		entry := FileEntry{
			Name:    e.Name(),
			Path:    rel,
			IsDir:   e.IsDir(),
			ModTime: info.ModTime().Unix(),
		}
		if !e.IsDir() {
			entry.Size = info.Size()
			entry.PlayURL = "/vod/" + string(name) + "/raw/" + rel
			entry.IngestURL = "file://" + string(name) + "/" + rel
		}
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsDir != out[j].IsDir {
			return out[i].IsDir // directories first
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// pathInside reports whether child is the same as, or a descendant of, parent.
// Both arguments must be Clean'd absolute paths.
func pathInside(child, parent string) bool {
	if child == parent {
		return true
	}
	sep := string(filepath.Separator)
	if !strings.HasSuffix(parent, sep) {
		parent += sep
	}
	return strings.HasPrefix(child, parent)
}
