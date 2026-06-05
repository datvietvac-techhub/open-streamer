package dash

import (
	"encoding/xml"
	"strings"
	"testing"
	"time"
)

// TestBuildManifest_NilInput — defensive error path.
func TestBuildManifest_NilInput(t *testing.T) {
	if _, err := BuildManifest(nil); err == nil {
		t.Error("expected error from BuildManifest(nil)")
	}
}

// TestBuildManifest_EmptyTracks — both tracks have no segments → nil
// output (caller skips the write rather than publishing an empty MPD).
func TestBuildManifest_EmptyTracks(t *testing.T) {
	in := &ManifestInput{
		SegDur: 2 * time.Second,
		Window: 6,
	}
	got, err := BuildManifest(in)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil bytes for empty tracks, got %d bytes", len(got))
	}
}

// TestBuildManifest_ContainsVideoAndAudio — both tracks populated;
// output has one video and one audio AdaptationSet with the expected
// codec strings.
func TestBuildManifest_ContainsVideoAndAudio(t *testing.T) {
	in := sampleManifestInput()
	data, err := BuildManifest(in)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("empty output")
	}
	s := string(data)
	for _, want := range []string{
		`<?xml version="1.0" encoding="UTF-8"?>`,
		`xmlns="urn:mpeg:dash:schema:mpd:2011"`,
		`type="dynamic"`,
		`contentType="video"`,
		`contentType="audio"`,
		`codecs="avc1.4D4028"`,
		`codecs="mp4a.40.2"`,
		`width="1920"`,
		`height="1080"`,
		`audioSamplingRate="48000"`,
		`startNumber="10"`,
		`initialization="init_v.mp4"`,
		`initialization="init_a.mp4"`,
		`media="seg_v_$Number%05d$.m4s"`,
		`media="seg_a_$Number%05d$.m4s"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("manifest missing substring %q", want)
		}
	}
}

// TestBuildManifest_SegmentTimelineHasExplicitT — every <S> entry has
// an explicit `t=`. We emit it on every segment so that the MPD's view
// of each segment's start exactly matches the segment file's tfdt —
// the inferred-cumulative form would drift away from wallclock-based
// tfdt anchoring when per-segment durs don't fill the inter-write gap.
func TestBuildManifest_SegmentTimelineHasExplicitT(t *testing.T) {
	in := sampleManifestInput()
	data, err := BuildManifest(in)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}

	var parsed mpdRoot
	if err := xml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(parsed.Periods) == 0 {
		t.Fatal("no Period in parsed MPD")
	}
	for _, as := range parsed.Periods[0].AdaptationSets {
		for _, rep := range as.Representations {
			tl := rep.SegmentTemplate.Timeline
			if tl == nil || len(tl.S) == 0 {
				t.Errorf("%s: missing/empty SegmentTimeline", rep.ID)
				continue
			}
			for i, s := range tl.S {
				if s.T == nil {
					t.Errorf("%s: <S>[%d] missing t attr (every segment must carry explicit t=)", rep.ID, i)
				}
			}
		}
	}
}

// TestBuildManifest_VideoOnly — audio absent → manifest still works
// with a single AdaptationSet.
func TestBuildManifest_VideoOnly(t *testing.T) {
	in := sampleManifestInput()
	in.Audio = nil
	data, err := BuildManifest(in)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
	var parsed mpdRoot
	if err := xml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if n := len(parsed.Periods[0].AdaptationSets); n != 1 {
		t.Errorf("AdaptationSets count = %d, want 1 (video only)", n)
	}
}

// TestBuildManifest_AudioOnly — symmetric: video absent.
func TestBuildManifest_AudioOnly(t *testing.T) {
	in := sampleManifestInput()
	in.Video = nil
	data, err := BuildManifest(in)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
	var parsed mpdRoot
	if err := xml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if n := len(parsed.Periods[0].AdaptationSets); n != 1 {
		t.Errorf("AdaptationSets count = %d, want 1 (audio only)", n)
	}
}

// TestBuildManifest_ABRMultipleVideoReps — one video AdaptationSet
// containing multiple Representations. The audio AdaptationSet stays
// single (audio is shared across ABR variants).
func TestBuildManifest_ABRMultipleVideoReps(t *testing.T) {
	in := sampleManifestInput()
	// Add a second rep at 720p with lower bandwidth.
	in.Video = append(in.Video, TrackManifest{
		RepID:        "v1",
		Codec:        "avc1.4D401F",
		Bandwidth:    2_500_000,
		Width:        1280,
		Height:       720,
		Timescale:    90000,
		InitFile:     "v1/init_v.mp4",
		MediaPattern: "v1/seg_v_$Number%05d$.m4s",
		StartNumber:  10,
		Segments: []SegmentEntry{
			{StartTicks: 900_000, DurTicks: 180_000},
			{StartTicks: 1_080_000, DurTicks: 180_000},
		},
	})

	data, err := BuildManifest(in)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}

	var parsed mpdRoot
	if err := xml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	// Expect: 2 AdaptationSets total (1 video, 1 audio); the video AS
	// should have 2 Representations.
	ass := parsed.Periods[0].AdaptationSets
	if len(ass) != 2 {
		t.Fatalf("AdaptationSets count = %d, want 2", len(ass))
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
	if n := len(videoAS.Representations); n != 2 {
		t.Errorf("video Representations count = %d, want 2", n)
	}
	// Both rep IDs should appear.
	ids := map[string]bool{}
	for _, r := range videoAS.Representations {
		ids[r.ID] = true
	}
	for _, want := range []string{"v0", "v1"} {
		if !ids[want] {
			t.Errorf("missing video rep %q", want)
		}
	}
}

// TestBuildManifest_DurationsISO — segDur-derived attrs use ISO-8601
// PT...S form. Values: minBuffer=2×segDur, suggestedPresDelay=3×segDur,
// maxSegmentDuration=3×segDur, minUpdate=1×segDur,
// timeShiftBufferDepth=window×segDur.
func TestBuildManifest_DurationsISO(t *testing.T) {
	in := sampleManifestInput()
	in.SegDur = 4 * time.Second
	in.Window = 6
	data, _ := BuildManifest(in)
	s := string(data)
	for _, want := range []string{
		`minBufferTime="PT8S"`,
		`suggestedPresentationDelay="PT12S"`,
		`maxSegmentDuration="PT12S"`,
		`minimumUpdatePeriod="PT4S"`,
		`timeShiftBufferDepth="PT24S"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("missing duration attr %q", want)
		}
	}
}

// TestBuildManifest_DurationsAdaptToObserved — when the longest
// observed segment is bigger than 3× segDur (long-GOP source where the
// segmenter's safety-net cut emitted a giant segment), MinBuffer and
// MaxSegmentDuration grow to match so the MPD's announced timing
// matches what the player actually has to fetch. SuggestedPresentation
// Delay stays at 3× segDur — that's a separate live-edge knob.
func TestBuildManifest_DurationsAdaptToObserved(t *testing.T) {
	in := sampleManifestInput()
	in.SegDur = 2 * time.Second
	// 10 s observed video segment at 90 kHz = 900 000 ticks.
	in.Video[0].Segments = []SegmentEntry{
		{StartTicks: 0, DurTicks: 900_000}, // 10 s
		{StartTicks: 900_000, DurTicks: 540_000},
	}
	data, _ := BuildManifest(in)
	s := string(data)
	for _, want := range []string{
		`minBufferTime="PT10S"`,             // = max(2×2, 10) = 10
		`maxSegmentDuration="PT10S"`,        // = max(3×2, 10) = 10
		`suggestedPresentationDelay="PT6S"`, // unchanged at 3 × segDur
	} {
		if !strings.Contains(s, want) {
			t.Errorf("missing adapted duration attr %q in:\n%s", want, s)
		}
	}
}

// TestBuildManifest_AvailabilityStartTimeRFC3339 — AST is RFC3339 UTC.
func TestBuildManifest_AvailabilityStartTimeRFC3339(t *testing.T) {
	ast := time.Date(2026, 5, 11, 8, 52, 5, 0, time.UTC)
	in := sampleManifestInput()
	in.AvailabilityStart = ast

	data, _ := BuildManifest(in)
	want := `availabilityStartTime="2026-05-11T08:52:05Z"`
	if !strings.Contains(string(data), want) {
		t.Errorf("missing %q", want)
	}
}

// TestBuildManifest_TFDTValuesMatchInput — explicit timeline t attribute
// equals SegmentEntry.StartTicks.
func TestBuildManifest_TFDTValuesMatchInput(t *testing.T) {
	in := sampleManifestInput()
	// Replace video segments with known ticks.
	in.Video[0].Segments = []SegmentEntry{
		{StartTicks: 9_000_000, DurTicks: 180_000},
		{StartTicks: 9_180_000, DurTicks: 180_000},
	}
	in.Audio.Segments = []SegmentEntry{
		{StartTicks: 4_800_000, DurTicks: 96_000},
	}
	data, _ := BuildManifest(in)
	s := string(data)
	for _, want := range []string{`<S t="9000000"`, `<S t="4800000"`} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in manifest XML", want)
		}
	}
}

// sampleManifestInput returns a baseline two-track ManifestInput for
// tests to mutate.
// TestBuildManifest_StaticMode — a static (VOD-window) MPD advertises the
// duration + the on-demand-compatible profile and OMITS every live-edge attr.
func TestBuildManifest_StaticMode(t *testing.T) {
	in := sampleManifestInput()
	in.Static = true
	in.MediaPresentationDuration = 6 * time.Second
	in.Video[0].PTO = 900_000 // 90000-scale
	in.Audio.PTO = 480_000    // 48000-scale
	data, err := BuildManifest(in)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
	s := string(data)
	for _, want := range []string{
		`type="static"`,
		`profiles="urn:mpeg:dash:profile:isoff-main:2011"`,
		`mediaPresentationDuration="PT6S"`,
		`duration="PT6S"`, // Period@duration
		`presentationTimeOffset="900000"`,
		`presentationTimeOffset="480000"`,
		`minBufferTime=`, // required even for static
	} {
		if !strings.Contains(s, want) {
			t.Errorf("static manifest missing substring %q\n%s", want, s)
		}
	}
	for _, unwanted := range []string{
		`type="dynamic"`,
		`availabilityStartTime=`,
		`minimumUpdatePeriod=`,
		`timeShiftBufferDepth=`,
		`publishTime=`,
		`suggestedPresentationDelay=`,
		`<UTCTiming`,
		`isoff-live`,
	} {
		if strings.Contains(s, unwanted) {
			t.Errorf("static manifest must NOT contain %q\n%s", unwanted, s)
		}
	}
}

// TestBuildManifest_LivePTOOmitted — the live (dynamic) path leaves PTO at 0,
// which must be omitted from the XML so existing live output is unchanged.
func TestBuildManifest_LivePTOOmitted(t *testing.T) {
	data, err := BuildManifest(sampleManifestInput())
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
	s := string(data)
	if strings.Contains(s, "presentationTimeOffset=") {
		t.Errorf("live manifest must omit presentationTimeOffset\n%s", s)
	}
	if strings.Contains(s, "mediaPresentationDuration=") {
		t.Errorf("live manifest must omit mediaPresentationDuration\n%s", s)
	}
	if !strings.Contains(s, `type="dynamic"`) || !strings.Contains(s, "isoff-live") {
		t.Errorf("live manifest must stay dynamic/isoff-live\n%s", s)
	}
}

func sampleManifestInput() *ManifestInput {
	return &ManifestInput{
		AvailabilityStart: time.Date(2026, 5, 11, 8, 52, 5, 0, time.UTC),
		PublishTime:       time.Date(2026, 5, 11, 8, 53, 5, 0, time.UTC),
		SegDur:            2 * time.Second,
		Window:            6,
		Video: []TrackManifest{{
			RepID:        "v0",
			Codec:        "avc1.4D4028",
			Bandwidth:    5_000_000,
			Width:        1920,
			Height:       1080,
			Timescale:    90000,
			InitFile:     "init_v.mp4",
			MediaPattern: "seg_v_$Number%05d$.m4s",
			StartNumber:  10,
			Segments: []SegmentEntry{
				{StartTicks: 900_000, DurTicks: 180_000},
				{StartTicks: 1_080_000, DurTicks: 180_000},
				{StartTicks: 1_260_000, DurTicks: 180_000},
			},
		}},
		Audio: &TrackManifest{
			RepID:        "a0",
			Codec:        "mp4a.40.2",
			Bandwidth:    128_000,
			SampleRate:   48000,
			Timescale:    48000,
			InitFile:     "init_a.mp4",
			MediaPattern: "seg_a_$Number%05d$.m4s",
			StartNumber:  10,
			Segments: []SegmentEntry{
				{StartTicks: 480_000, DurTicks: 96_000},
				{StartTicks: 576_000, DurTicks: 96_000},
				{StartTicks: 672_000, DurTicks: 96_000},
			},
		},
	}
}
