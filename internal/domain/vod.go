package domain

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// MaxVODNameLen bounds the length of a VOD mount name used as a URL host component.
const MaxVODNameLen = 64

var vodNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// VODName identifies a VOD mount. It appears as the URL host in ingest sources
// (e.g. file://<name>/path/to/file.mp4), so it must be URL-host-safe.
type VODName string

// ValidateVODName reports whether s is a non-empty, URL-host-safe VOD mount name.
func ValidateVODName(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return errors.New("vod name is required")
	}
	if len(s) > MaxVODNameLen {
		return fmt.Errorf("vod name exceeds max length %d", MaxVODNameLen)
	}
	if !vodNamePattern.MatchString(s) {
		return errors.New("vod name must contain only a-z, A-Z, 0-9, _ and -")
	}
	return nil
}

// VODMount registers a host filesystem directory as a named media library.
// The system does not maintain a file index; file lookups and listings are
// resolved live against Storage.
//
// Ingest URLs take the form file://<Name>/<relative/path/inside/storage>.
// Any path that would escape Storage (via "..", absolute components, or
// symlink traversal) must be rejected by the resolver.
type VODMount struct {
	Name    VODName `json:"name" yaml:"name"`
	Storage string  `json:"storage" yaml:"storage"`
	Comment string  `json:"comment,omitempty" yaml:"comment,omitempty"`
}

// ValidateStorage checks that Storage is a syntactically valid absolute path.
// The directory itself is not required to exist at validation time — operators
// may create it later — but the path must be absolute to avoid surprises from
// the server's working directory.
func (m *VODMount) ValidateStorage() error {
	s := strings.TrimSpace(m.Storage)
	if s == "" {
		return errors.New("storage is required")
	}
	if !filepath.IsAbs(s) {
		return errors.New("storage must be an absolute path")
	}
	// Defense-in-depth (S-5): refuse to mount a sensitive system directory (or
	// any ancestor/descendant of one) as a media library — combined with the
	// raw-serve video allowlist this keeps config/secrets/system files out of
	// reach even on a misconfiguration.
	clean := filepath.Clean(s)
	for _, sys := range []string{"/etc", "/sys", "/proc", "/boot", "/root", "/dev", "/run"} {
		if pathInside(clean, sys) || pathInside(sys, clean) {
			return fmt.Errorf("storage %q overlaps sensitive system directory %q", s, sys)
		}
	}
	return nil
}

// pathInside reports whether child is parent or a descendant of it. Both must be
// Clean'd absolute paths. Duplicated from internal/vod (a trivial helper) so the
// domain layer stays import-free of internal packages.
func pathInside(child, parent string) bool {
	if child == parent {
		return true
	}
	if !strings.HasSuffix(parent, string(filepath.Separator)) {
		parent += string(filepath.Separator)
	}
	return strings.HasPrefix(child, parent)
}
