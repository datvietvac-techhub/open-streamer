package vod_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/vod"
)

// testMountName is the canonical mount used by every test fixture.
const testMountName = "vod"

func newRegistryWithMount(t *testing.T) (*vod.Registry, string) {
	t.Helper()
	dir := t.TempDir()
	r := vod.NewRegistry()
	r.Sync([]*domain.VODMount{{Name: testMountName, Storage: dir}})
	return r, dir
}

func TestRegistry_ResolveValidURL(t *testing.T) {
	t.Parallel()
	r, dir := newRegistryWithMount(t)

	got, loop, err := r.Resolve("file://vod/sub/clip.mp4")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	want := filepath.Join(dir, "sub", "clip.mp4")
	if got != want {
		t.Errorf("path=%q want %q", got, want)
	}
	// loop defaults to TRUE for file:// — file is treated as a continuous
	// source, EOF replays from start. Opt-out via ?loop=false.
	if !loop {
		t.Error("loop should default to true")
	}
}

func TestRegistry_ResolveLoopQuery(t *testing.T) {
	t.Parallel()
	r, _ := newRegistryWithMount(t)

	_, loop, err := r.Resolve("file://vod/clip.mp4?loop=true")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !loop {
		t.Error("loop=true must be honoured")
	}
}

// Operators who want one-shot playback opt out via ?loop=false. Also covers
// the alternate falsey forms (0, no, off) for ergonomics.
func TestRegistry_ResolveLoopOptOut(t *testing.T) {
	t.Parallel()
	r, _ := newRegistryWithMount(t)

	for _, falsey := range []string{"false", "0", "no", "off", "FALSE", " false "} {
		t.Run(falsey, func(t *testing.T) {
			t.Parallel()
			_, loop, err := r.Resolve("file://vod/clip.mp4?loop=" + falsey)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if loop {
				t.Errorf("loop=%q must opt out", falsey)
			}
		})
	}
}

func TestRegistry_ResolveUnknownMount(t *testing.T) {
	t.Parallel()
	r := vod.NewRegistry()

	_, _, err := r.Resolve("file://missing/clip.mp4")
	if !errors.Is(err, vod.ErrMountNotFound) {
		t.Fatalf("err=%v want ErrMountNotFound", err)
	}
}

func TestRegistry_ResolveBarePathRejected(t *testing.T) {
	t.Parallel()
	r := vod.NewRegistry()

	_, _, err := r.Resolve("/etc/passwd")
	if !errors.Is(err, vod.ErrNotFileURL) {
		t.Fatalf("err=%v want ErrNotFileURL", err)
	}
}

func TestRegistry_ResolveAbsoluteFileURLRejected(t *testing.T) {
	t.Parallel()
	r := vod.NewRegistry()

	// file:///etc/passwd has empty host — must be rejected because we no
	// longer support free-form host paths.
	_, _, err := r.Resolve("file:///etc/passwd")
	if err == nil {
		t.Fatal("absolute file:// must be rejected when host is empty")
	}
}

func TestRegistry_ResolveTraversalRejected(t *testing.T) {
	t.Parallel()
	r, _ := newRegistryWithMount(t)

	_, _, err := r.Resolve("file://vod/../../etc/passwd")
	if !errors.Is(err, vod.ErrPathEscapesMount) {
		t.Fatalf("err=%v want ErrPathEscapesMount", err)
	}
}

func TestRegistry_ResolveEmptyPath(t *testing.T) {
	t.Parallel()
	r, _ := newRegistryWithMount(t)

	_, _, err := r.Resolve("file://vod/")
	if err == nil {
		t.Fatal("empty path must be rejected")
	}
}

func TestRegistry_SyncSkipsInvalidEntries(t *testing.T) {
	t.Parallel()
	r := vod.NewRegistry()
	r.Sync([]*domain.VODMount{
		nil,
		{Name: "", Storage: "/tmp"},               // empty name
		{Name: "bad name", Storage: "/tmp"},       // invalid chars
		{Name: "rel", Storage: "relative/path"},   // not absolute
		{Name: "ok", Storage: "/var/media"},       // valid
		{Name: "ok2", Storage: "  /var/movies  "}, // trimmed
	})

	if got := len(r.Names()); got != 2 {
		t.Errorf("registered=%d want 2", got)
	}
	if s, ok := r.Storage("ok2"); !ok || s != "/var/movies" {
		t.Errorf("ok2 storage=%q ok=%v", s, ok)
	}
}

func TestRegistry_ListFilesSortsDirsFirst(t *testing.T) {
	t.Parallel()
	r, dir := newRegistryWithMount(t)
	mustWrite(t, filepath.Join(dir, "a.mp4"), "a")
	mustWrite(t, filepath.Join(dir, "b.mp4"), "bb")
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}

	entries, err := r.ListFiles("vod", "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 3 || !entries[0].IsDir || entries[0].Name != "sub" {
		t.Fatalf("entries=%+v", entries)
	}
	if entries[1].Name != "a.mp4" || entries[1].Size != 1 {
		t.Errorf("a entry=%+v", entries[1])
	}
}

func TestRegistry_ListFilesSubdir(t *testing.T) {
	t.Parallel()
	r, dir := newRegistryWithMount(t)
	if err := os.MkdirAll(filepath.Join(dir, "movies"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, "movies", "x.mp4"), "x")

	entries, err := r.ListFiles("vod", "movies")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 1 || entries[0].Path != "movies/x.mp4" {
		t.Errorf("entries=%+v", entries)
	}
	if entries[0].PlayURL != "/vod/vod/raw/movies/x.mp4" {
		t.Errorf("play_url=%q", entries[0].PlayURL)
	}
	if entries[0].IngestURL != "file://vod/movies/x.mp4" {
		t.Errorf("ingest_url=%q", entries[0].IngestURL)
	}
}

func TestRegistry_ListFilesFiltersNonVideo(t *testing.T) {
	t.Parallel()
	r, dir := newRegistryWithMount(t)
	mustWrite(t, filepath.Join(dir, "movie.mp4"), "v")
	mustWrite(t, filepath.Join(dir, "Notes.txt"), "n")    // skipped
	mustWrite(t, filepath.Join(dir, "subtitle.srt"), "s") // skipped
	mustWrite(t, filepath.Join(dir, "clip.MKV"), "k")     // upper-case ext kept
	if err := os.Mkdir(filepath.Join(dir, "more"), 0o755); err != nil {
		t.Fatal(err)
	}

	entries, err := r.ListFiles("vod", "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name)
	}
	want := []string{"more", "clip.MKV", "movie.mp4"}
	if len(names) != len(want) {
		t.Fatalf("entries=%v want %v", names, want)
	}
	for i, n := range want {
		if names[i] != n {
			t.Errorf("entry[%d]=%q want %q", i, names[i], n)
		}
	}
}

func TestRegistry_ListFilesPlayURLForFiles(t *testing.T) {
	t.Parallel()
	r, dir := newRegistryWithMount(t)
	mustWrite(t, filepath.Join(dir, "a.mp4"), "a")
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}

	entries, err := r.ListFiles("vod", "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, e := range entries {
		if e.IsDir && e.PlayURL != "" {
			t.Errorf("dir %q must have empty play_url, got %q", e.Name, e.PlayURL)
		}
		if !e.IsDir && e.PlayURL != "/vod/vod/raw/"+e.Path {
			t.Errorf("file %q play_url=%q", e.Name, e.PlayURL)
		}
	}
}

func TestRegistry_ListFilesTraversalRejected(t *testing.T) {
	t.Parallel()
	r, _ := newRegistryWithMount(t)

	_, err := r.ListFiles("vod", "../..")
	if !errors.Is(err, vod.ErrPathEscapesMount) {
		t.Fatalf("err=%v want ErrPathEscapesMount", err)
	}
}

func TestRegistry_ListFilesUnknownMount(t *testing.T) {
	t.Parallel()
	r := vod.NewRegistry()

	_, err := r.ListFiles("missing", "")
	if !errors.Is(err, vod.ErrMountNotFound) {
		t.Fatalf("err=%v want ErrMountNotFound", err)
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
