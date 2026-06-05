package blob

import (
	"fmt"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/publisher/dash"
)

// Default codec / bandwidth strings used when the catalog profile doesn't carry
// them (passthrough recordings record no codec metadata).
const (
	defaultVideoCodec = "avc1.4d401f"
	dashAudioCodec    = "mp4a.40.2"
	defaultVideoBW    = 2_500_000
	defaultAudioBW    = 128_000
	defaultAudioTS    = 48_000
)

// RenderMPD builds a static (VOD-window) DASH manifest for a resolved timeshift
// window. Every covered video profile becomes a Representation in one video
// AdaptationSet; the audio-source profile (cat.AudioProfile) becomes the single
// shared audio AdaptationSet.
//
// All Representations share ONE presentation origin — T0, the earliest video
// fragment tick across the included profiles — applied per-track via
// SegmentTemplate@presentationTimeOffset (video in 90000, audio rescaled to its
// own timescale). This maps the absolute archive tfdt back to presentation 0 and
// keeps the same wallclock instant at the same presentation second on both
// tracks, so A/V (and ABR switching) stay aligned without re-stamping any bytes.
//
// Fragment URLs use the $Time$ form (dvrt-<profile>-<track>-$Time$.m4s): the
// player substitutes each segment's exact <S t=…> tick, which
// reader.FragmentByTime resolves back to the stored record by exact-tick match.
//
// windows holds one entry per profile id (as returned by Reader.Query); a
// profile with no video in the window is dropped. Returns an error when no
// profile covers the window.
func RenderMPD(cat *Catalog, windows map[string]*Window, segDur time.Duration) ([]byte, error) {
	var (
		videoReps []dash.TrackManifest
		t0, tEnd  uint64
		haveT0    bool
		vTS       uint32
	)
	for _, p := range orderedProfiles(cat) {
		w := windows[p.ID]
		if w == nil || len(w.Video) == 0 {
			continue
		}
		first := w.Video[0].MediaTicks
		last := w.Video[len(w.Video)-1]
		if !haveT0 || first < t0 {
			t0 = first
		}
		if end := last.MediaTicks + last.DurTicks; end > tEnd {
			tEnd = end
		}
		haveT0 = true
		if w.VideoTimescale != 0 {
			vTS = w.VideoTimescale
		}
		videoReps = append(videoReps, dash.TrackManifest{
			RepID:        p.ID,
			Codec:        codecOrDefault(p.Codec, defaultVideoCodec),
			Bandwidth:    bandwidthOrDefault(p.Bandwidth, defaultVideoBW),
			Width:        p.Width,
			Height:       p.Height,
			Timescale:    tsOrDefault(w.VideoTimescale, videoTimescale),
			InitFile:     fmt.Sprintf("dvri-%s-0.mp4", p.ID),
			MediaPattern: fmt.Sprintf("dvrt-%s-0-$Time$.m4s", p.ID),
			StartNumber:  1,
			Segments:     trackSegments(w.Video),
		})
	}
	if !haveT0 {
		return nil, fmt.Errorf("blob: no profile covers the requested window")
	}
	if vTS == 0 {
		vTS = videoTimescale
	}
	for i := range videoReps {
		videoReps[i].PTO = t0 // shared origin across every video rep
	}

	in := &dash.ManifestInput{
		Static:                    true,
		SegDur:                    segDur,
		MediaPresentationDuration: ticksToDuration(tEnd-t0, vTS),
		Video:                     videoReps,
	}
	if aw := windows[cat.AudioProfile]; aw != nil && len(aw.Audio) > 0 {
		aTS := aw.AudioTimescale
		if aTS == 0 {
			aTS = defaultAudioTS
		}
		in.Audio = &dash.TrackManifest{
			RepID:        "a",
			Codec:        dashAudioCodec,
			Bandwidth:    defaultAudioBW,
			SampleRate:   int(aTS),
			Timescale:    aTS,
			InitFile:     fmt.Sprintf("dvri-%s-1.mp4", cat.AudioProfile),
			MediaPattern: fmt.Sprintf("dvrt-%s-1-$Time$.m4s", cat.AudioProfile),
			StartNumber:  1,
			PTO:          t0 * uint64(aTS) / uint64(vTS), // same instant as video T0, in audio ticks
			Segments:     trackSegments(aw.Audio),
		}
	}
	return dash.BuildManifest(in)
}

// trackSegments maps fragment refs to manifest segment entries; the <S t=…>
// values are the records' exact MediaTicks so the $Time$ round-trip is lossless.
func trackSegments(refs []FragmentRef) []dash.SegmentEntry {
	out := make([]dash.SegmentEntry, 0, len(refs))
	for _, f := range refs {
		out = append(out, dash.SegmentEntry{StartTicks: f.MediaTicks, DurTicks: f.DurTicks})
	}
	return out
}

// ticksToDuration converts a tick span at timescale ts to a time.Duration with
// millisecond precision (multiply-by-1000 first keeps full-day windows exact).
func ticksToDuration(ticks uint64, ts uint32) time.Duration {
	if ts == 0 {
		return 0
	}
	return time.Duration(ticks*1000/uint64(ts)) * time.Millisecond //nolint:gosec // bounded window span
}

func codecOrDefault(c, def string) string {
	if c == "" {
		return def
	}
	return c
}

func bandwidthOrDefault(b, def int) int {
	if b <= 0 {
		return def
	}
	return b
}

func tsOrDefault(ts, def uint32) uint32 {
	if ts == 0 {
		return def
	}
	return ts
}
