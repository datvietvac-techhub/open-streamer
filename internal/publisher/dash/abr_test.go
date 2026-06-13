package dash

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestABRMaster_FlushOnStopWritesManifest — the simplest happy path:
// register one shard, Stop, expect the file on disk.
func TestABRMaster_FlushOnStopWritesManifest(t *testing.T) {
	dir := t.TempDir()
	rootPath := filepath.Join(dir, "index.mpd")
	m := NewABRMaster(rootPath, "test", 2*time.Second, 6)

	m.UpdateShard(ShardSnapshot{
		Slug:              "track_1",
		AvailabilityStart: time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC),
		Video:             sampleVideoRep("v0"),
		Audio:             sampleAudioRep(),
	})
	m.Stop()

	data, err := os.ReadFile(rootPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	s := string(data)
	for _, want := range []string{
		`type="dynamic"`,
		`contentType="video"`,
		`contentType="audio"`,
		`id="v0"`,
		`id="a0"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("manifest missing %q", want)
		}
	}
}

// TestABRMaster_MultiShardEmitsMultipleVideoReps — N shards → N video
// <Representation>s sharing one AdaptationSet.
func TestABRMaster_MultiShardEmitsMultipleVideoReps(t *testing.T) {
	dir := t.TempDir()
	rootPath := filepath.Join(dir, "index.mpd")
	m := NewABRMaster(rootPath, "test", 2*time.Second, 6)

	// 3 video shards; only the first packs audio.
	m.UpdateShard(ShardSnapshot{
		Slug:              "track_1",
		AvailabilityStart: time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC),
		Video:             sampleVideoRep("v0"),
		Audio:             sampleAudioRep(),
	})
	m.UpdateShard(ShardSnapshot{
		Slug:  "track_2",
		Video: sampleVideoRep("v1"),
	})
	m.UpdateShard(ShardSnapshot{
		Slug:  "track_3",
		Video: sampleVideoRep("v2"),
	})
	m.Stop()

	data, err := os.ReadFile(rootPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var parsed mpdRoot
	if err := xml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	ass := parsed.Periods[0].AdaptationSets
	if len(ass) != 2 {
		t.Fatalf("AdaptationSets count = %d, want 2 (video + audio)", len(ass))
	}
	var videoAS *mpdAdaptationSet
	for i := range ass {
		if ass[i].ContentType == "video" {
			videoAS = &ass[i]
		}
	}
	if videoAS == nil {
		t.Fatal("no video AdaptationSet")
	}
	if n := len(videoAS.Representations); n != 3 {
		t.Errorf("video Representations count = %d, want 3", n)
	}
}

// TestABRMaster_ShardsSortedByStableSlugOrder — the master writes
// representations in slug-sorted order regardless of UpdateShard
// arrival order.
func TestABRMaster_ShardsSortedByStableSlugOrder(t *testing.T) {
	dir := t.TempDir()
	rootPath := filepath.Join(dir, "index.mpd")
	m := NewABRMaster(rootPath, "test", 2*time.Second, 6)

	// Push in reverse order: track_3 first.
	m.UpdateShard(ShardSnapshot{Slug: "track_3", Video: sampleVideoRep("v2"), AvailabilityStart: time.Now()})
	m.UpdateShard(ShardSnapshot{Slug: "track_1", Video: sampleVideoRep("v0"), Audio: sampleAudioRep(), AvailabilityStart: time.Now()})
	m.UpdateShard(ShardSnapshot{Slug: "track_2", Video: sampleVideoRep("v1"), AvailabilityStart: time.Now()})
	m.Stop()

	data, _ := os.ReadFile(rootPath)
	var parsed mpdRoot
	_ = xml.Unmarshal(data, &parsed)
	var videoAS *mpdAdaptationSet
	for i := range parsed.Periods[0].AdaptationSets {
		if parsed.Periods[0].AdaptationSets[i].ContentType == "video" {
			videoAS = &parsed.Periods[0].AdaptationSets[i]
		}
	}
	if videoAS == nil {
		t.Fatal("no video AdaptationSet")
	}
	wantOrder := []string{"v0", "v1", "v2"}
	for i, r := range videoAS.Representations {
		if r.ID != wantOrder[i] {
			t.Errorf("rep[%d].ID = %q, want %q", i, r.ID, wantOrder[i])
		}
	}
}

// TestABRMaster_StopOnEmptyIsNoop — Stop with no shards should not
// write a manifest (nothing useful to serve).
func TestABRMaster_StopOnEmptyIsNoop(t *testing.T) {
	dir := t.TempDir()
	rootPath := filepath.Join(dir, "index.mpd")
	m := NewABRMaster(rootPath, "test", 2*time.Second, 6)
	m.Stop()

	if _, err := os.Stat(rootPath); !os.IsNotExist(err) {
		t.Errorf("expected no manifest file, got err=%v", err)
	}
}

// TestABRMaster_UpdateAfterStopIsNoop — calls past Stop are ignored
// (the typical teardown ordering puts external goroutines past Stop
// but they may still try a final UpdateShard).
func TestABRMaster_UpdateAfterStopIsNoop(t *testing.T) {
	dir := t.TempDir()
	rootPath := filepath.Join(dir, "index.mpd")
	m := NewABRMaster(rootPath, "test", 2*time.Second, 6)
	m.Stop()
	// This should not panic, not write, not schedule a flush.
	m.UpdateShard(ShardSnapshot{Slug: "track_1", Video: sampleVideoRep("v0")})

	if _, err := os.Stat(rootPath); !os.IsNotExist(err) {
		t.Error("UpdateShard after Stop should not create a manifest")
	}
}

// TestABRMaster_DebouncesRapidUpdates — multiple UpdateShard calls
// within 150 ms should coalesce into a single flush. Verified by
// observing the file mtime stabilises after the burst.
func TestABRMaster_DebouncesRapidUpdates(t *testing.T) {
	dir := t.TempDir()
	rootPath := filepath.Join(dir, "index.mpd")
	m := NewABRMaster(rootPath, "test", 2*time.Second, 6)
	defer m.Stop()

	// Burst of updates within debounce window.
	for i := 0; i < 5; i++ {
		m.UpdateShard(ShardSnapshot{
			Slug:              "track_1",
			AvailabilityStart: time.Now(),
			Video:             sampleVideoRep("v0"),
		})
	}
	// File should NOT exist yet (debounce hasn't fired).
	if _, err := os.Stat(rootPath); !os.IsNotExist(err) {
		t.Error("file written before debounce timer expired")
	}

	// Wait past debounce + a small buffer.
	time.Sleep(300 * time.Millisecond)
	if _, err := os.Stat(rootPath); err != nil {
		t.Errorf("expected manifest written after debounce, got %v", err)
	}
}

// TestCombineSnapshots_NoTracksReturnsNil — defensive.
func TestCombineSnapshots_NoTracksReturnsNil(t *testing.T) {
	snaps := []ShardSnapshot{{Slug: "track_1"}}
	in := combineSnapshots(snaps, 2*time.Second, 6)
	if in != nil {
		t.Errorf("expected nil, got %+v", in)
	}
}

// TestCombineSnapshots_FirstNonZeroASTWins — the first shard's
// non-zero AST becomes the manifest's AST; later shards don't
// overwrite (they all converge on the same value in practice, but
// the deterministic rule here makes test data simple).
func TestCombineSnapshots_FirstNonZeroASTWins(t *testing.T) {
	t1 := time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 11, 1, 0, 0, 0, time.UTC)
	snaps := []ShardSnapshot{
		{Slug: "track_1", AvailabilityStart: t1, Video: sampleVideoRep("v0")},
		{Slug: "track_2", AvailabilityStart: t2, Video: sampleVideoRep("v1")},
	}
	in := combineSnapshots(snaps, 2*time.Second, 6)
	if in == nil {
		t.Fatal("combineSnapshots returned nil")
	}
	if !in.AvailabilityStart.Equal(t1) {
		t.Errorf("AvailabilityStart = %v, want %v", in.AvailabilityStart, t1)
	}
}

// TestABRMaster_SharedAST — the ladder AST is set once by the first shard to
// flush and returned unchanged to every later caller, so all shards anchor to
// one availabilityStart regardless of flush order (B-4).
func TestABRMaster_SharedAST(t *testing.T) {
	m := NewABRMaster(filepath.Join(t.TempDir(), "index.mpd"), "test", 2*time.Second, 6)
	first := time.Date(2026, 6, 13, 10, 0, 0, 0, time.UTC)
	later := first.Add(900 * time.Millisecond)

	if got := m.SharedAST(first); !got.Equal(first) {
		t.Fatalf("first SharedAST = %v, want %v", got, first)
	}
	// A shard flushing ~1s later must still receive the FIRST shard's AST.
	if got := m.SharedAST(later); !got.Equal(first) {
		t.Errorf("later SharedAST = %v, want %v (AST must not change)", got, first)
	}

	// A zero candidate never sets the value; the first non-zero one does.
	m2 := NewABRMaster(filepath.Join(t.TempDir(), "index.mpd"), "test", 2*time.Second, 6)
	if got := m2.SharedAST(time.Time{}); !got.IsZero() {
		t.Errorf("zero candidate set AST to %v, want zero", got)
	}
	if got := m2.SharedAST(later); !got.Equal(later) {
		t.Errorf("first non-zero candidate %v should set AST; got %v", later, got)
	}
}

// sampleVideoRep returns a baseline TrackManifest for a video rendition.
func sampleVideoRep(repID string) *TrackManifest {
	return &TrackManifest{
		RepID:        repID,
		Codec:        "avc1.4D4028",
		Bandwidth:    5_000_000,
		Width:        1920,
		Height:       1080,
		Timescale:    90000,
		InitFile:     repID + "/init_v.mp4",
		MediaPattern: repID + "/seg_v_$Number%05d$.m4s",
		StartNumber:  1,
		Segments: []SegmentEntry{
			{StartTicks: 0, DurTicks: 180_000},
		},
	}
}

// sampleAudioRep returns a baseline audio TrackManifest.
func sampleAudioRep() *TrackManifest {
	return &TrackManifest{
		RepID:        "a0",
		Codec:        "mp4a.40.2",
		Bandwidth:    128_000,
		SampleRate:   48000,
		Timescale:    48000,
		InitFile:     "init_a.mp4",
		MediaPattern: "seg_a_$Number%05d$.m4s",
		StartNumber:  1,
		Segments: []SegmentEntry{
			{StartTicks: 0, DurTicks: 96_000},
		},
	}
}
