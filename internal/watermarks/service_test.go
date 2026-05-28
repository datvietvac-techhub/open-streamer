package watermarks

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datvietvac-techhub/open-streamer/config"
)

// pngBytes is a minimal valid 1×1 PNG so http.DetectContentType returns
// "image/png". Used by every Save() test; sniff sees only the first 512
// bytes so this is enough to clear the validator.
var pngBytes = []byte{
	0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
	0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
	0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
	0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41,
	0x54, 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00,
	0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00,
	0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae,
	0x42, 0x60, 0x82,
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	return mustNewServiceForTest(t.TempDir())
}

// mustNewServiceForTest builds a Service rooted at dir without going
// through the do.Injector, so unit tests don't need DI plumbing.
func mustNewServiceForTest(dir string) *Service {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	s, err := newService(config.WatermarksConfig{Dir: dir})
	if err != nil {
		panic(err)
	}
	return s
}

func TestServiceSaveListGet(t *testing.T) {
	s := newTestService(t)

	a, err := s.Save("logo.png", bytes.NewReader(pngBytes))
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if a.Filename != "logo.png" {
		t.Fatalf("Filename = %q, want logo.png", a.Filename)
	}
	if a.ContentType != "image/png" {
		t.Errorf("ContentType = %q, want image/png", a.ContentType)
	}
	if a.SizeBytes != int64(len(pngBytes)) {
		t.Errorf("SizeBytes = %d, want %d", a.SizeBytes, len(pngBytes))
	}

	// Round-trip via Get.
	got, err := s.Get(a.Filename)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Filename != a.Filename {
		t.Errorf("Get returned wrong filename")
	}

	// List should contain it.
	listed := s.List()
	if len(listed) != 1 || listed[0].Filename != a.Filename {
		t.Errorf("List = %+v, want one matching asset", listed)
	}
}

func TestServiceResolvePath(t *testing.T) {
	s := newTestService(t)
	a, err := s.Save("logo.png", bytes.NewReader(pngBytes))
	if err != nil {
		t.Fatal(err)
	}
	path, err := s.ResolvePath(a.Filename)
	if err != nil {
		t.Fatalf("ResolvePath: %v", err)
	}
	if !strings.HasSuffix(path, "logo.png") {
		t.Errorf("expected logo.png suffix, got %q", path)
	}
	if !filepath.IsAbs(path) {
		t.Errorf("expected absolute path, got %q", path)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("file not on disk: %v", err)
	}
	if st.Size() != int64(len(pngBytes)) {
		t.Errorf("size mismatch: %d vs %d", st.Size(), len(pngBytes))
	}
}

func TestServiceRejectsNonImage(t *testing.T) {
	s := newTestService(t)
	_, err := s.Save("evil.txt", bytes.NewReader([]byte("just plain text")))
	if !errors.Is(err, ErrInvalidContent) {
		t.Errorf("want ErrInvalidContent, got %v", err)
	}
}

func TestServiceRejectsInvalidFilename(t *testing.T) {
	s := newTestService(t)
	cases := []string{
		"../evil.png",    // path traversal
		"logo",           // no extension
		"logo.tar.gz",    // multiple dots
		".gitignore",     // hidden
		"with space.png", // disallowed char
		"with/slash.png", // disallowed char
	}
	for _, name := range cases {
		if _, err := s.Save(name, bytes.NewReader(pngBytes)); err == nil {
			t.Errorf("expected error for filename %q, got nil", name)
		}
	}
}

func TestServiceRejectsDuplicateFilename(t *testing.T) {
	s := newTestService(t)
	if _, err := s.Save("logo.png", bytes.NewReader(pngBytes)); err != nil {
		t.Fatal(err)
	}
	_, err := s.Save("logo.png", bytes.NewReader(pngBytes))
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("want ErrAlreadyExists, got %v", err)
	}
}

func TestServiceDelete(t *testing.T) {
	s := newTestService(t)
	a, err := s.Save("logo.png", bytes.NewReader(pngBytes))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(a.Filename); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(a.Filename); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete: want ErrNotFound, got %v", err)
	}

	// Repeat delete is also ErrNotFound, never an error.
	if err := s.Delete(a.Filename); !errors.Is(err, ErrNotFound) {
		t.Errorf("repeat Delete: want ErrNotFound, got %v", err)
	}
	// File should be gone from disk.
	if _, err := os.Stat(filepath.Join(s.Dir(), string(a.Filename))); !os.IsNotExist(err) {
		t.Errorf("file still on disk after Delete: %v", err)
	}
}

func TestServiceRebuildCacheFromDisk(t *testing.T) {
	dir := t.TempDir()
	// First instance uploads an asset.
	{
		s := mustNewServiceForTest(dir)
		if _, err := s.Save("a.png", bytes.NewReader(pngBytes)); err != nil {
			t.Fatal(err)
		}
	}
	// Fresh instance pointed at the same dir picks up the file.
	s2 := mustNewServiceForTest(dir)
	got := s2.List()
	if len(got) != 1 || got[0].Filename != "a.png" {
		t.Errorf("rebuild missed file: %+v", got)
	}
	if got[0].ContentType != "image/png" {
		t.Errorf("rebuild missed content type sniff: %+v", got[0])
	}
}

// The directory is the source of truth: an asset dropped in via rsync / scp
// out of band must show up in List on the very next call, without any
// "reload library" trigger. Tests the no-cache invariant.
func TestServiceListSeesExternalDrops(t *testing.T) {
	s := newTestService(t)

	// Empty initially.
	if got := s.List(); len(got) != 0 {
		t.Fatalf("fresh service: want empty list, got %+v", got)
	}

	// Drop a file directly into the dir (simulates `cp logo.png /var/lib/.../watermarks/`).
	mustWrite(t, filepath.Join(s.Dir(), "external.png"), pngBytes)

	got := s.List()
	if len(got) != 1 || got[0].Filename != "external.png" {
		t.Errorf("after external drop: want [external.png], got %+v", got)
	}

	// Same for Get + ResolvePath — both must surface the externally-dropped file.
	if _, err := s.Get("external.png"); err != nil {
		t.Errorf("Get on external file: %v", err)
	}
	if _, err := s.ResolvePath("external.png"); err != nil {
		t.Errorf("ResolvePath on external file: %v", err)
	}

	// Remove externally — Get must surface ErrNotFound on the very next call.
	if err := os.Remove(filepath.Join(s.Dir(), "external.png")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("external.png"); !errors.Is(err, ErrNotFound) {
		t.Errorf("after external remove: want ErrNotFound, got %v", err)
	}
	if got := s.List(); len(got) != 0 {
		t.Errorf("after external remove: want empty list, got %+v", got)
	}
}

func TestServiceRebuildSkipsJunk(t *testing.T) {
	dir := t.TempDir()
	// Pre-seed dir with files the validator should reject.
	mustWrite(t, filepath.Join(dir, ".hidden"), []byte("x"))
	mustWrite(t, filepath.Join(dir, "logo.tar.gz"), []byte("x"))
	mustWrite(t, filepath.Join(dir, "valid.png"), pngBytes)

	s := mustNewServiceForTest(dir)
	got := s.List()
	if len(got) != 1 || got[0].Filename != "valid.png" {
		t.Errorf("expected only valid.png, got %+v", got)
	}
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
