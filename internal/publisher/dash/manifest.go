package dash

import (
	"encoding/xml"
	"fmt"
	"strings"
	"time"
)

// manifest.go — MPD XML generation.
//
// The MPD format follows the MPEG-DASH ISO BMFF Live profile. Schema
// reference: ISO/IEC 23009-1, Annex K (the "live" profile,
// urn:mpeg:dash:profile:isoff-live:2011).
//
// The XML structs are intentionally a SUBSET of the full DASH schema —
// we only emit attributes the player needs (dashjs / Shaka / mpv).

// ManifestInput is the value-shaped input to BuildManifest. The packager
// fills this from its own per-stream state before each MPD rewrite.
type ManifestInput struct {
	// AvailabilityStart is the wallclock anchor: the moment media-time-0
	// became available. Players use it to compute live-edge as
	// (now − AST) − liveDelay.
	AvailabilityStart time.Time

	// PublishTime should be the current wallclock when the manifest is
	// generated. Players use it to detect changes (delta between fetches).
	PublishTime time.Time

	// SegDur is the target segment duration. Used to derive MinBuffer,
	// SuggestedPresentationDelay, MaxSegmentDuration, MinUpdatePeriod
	// per common DASH live conventions (multiples of segDur).
	SegDur time.Duration

	// Window is the number of segments visible in each track's
	// SegmentTimeline. Player needs timeShiftBufferDepth ≈
	// Window × SegDur to allow seeking within the window.
	Window int

	// Video is one or more Representations within a single video
	// AdaptationSet. Length 1 for single-rendition streams; length N
	// for ABR ladders. Empty (nil or zero-length) when the stream
	// carries no video.
	Video []TrackManifest

	// Audio is the single audio Representation, shared across ABR
	// renditions. Nil for video-only streams. (DASH does support
	// multiple audio AdaptationSets — different languages — but this
	// packager emits at most one.)
	Audio *TrackManifest

	// Static selects a bounded VOD-window (type="static") MPD instead of
	// the default live (type="dynamic") one. Used by the DVR blob-archive
	// timeshift surface: the window is fully available, so the manifest
	// carries mediaPresentationDuration and omits every live-edge attr
	// (availabilityStartTime / minimumUpdatePeriod / timeShiftBufferDepth /
	// publishTime / suggestedPresentationDelay / UTCTiming). The live
	// packager leaves this false, so its output is unchanged.
	Static bool

	// MediaPresentationDuration is the static window's total presentation
	// span (after applying each track's presentationTimeOffset). Only read
	// when Static is true.
	MediaPresentationDuration time.Duration
}

// TrackManifest describes one track's manifest data. Same struct shape
// for video and audio; codec-specific attrs sit alongside common ones.
type TrackManifest struct {
	// RepID is the Representation ID emitted as `id="..."`. Convention:
	// "v0" for the primary video rep, "a0" for audio. ABR ladders use
	// "v0", "v1", "v2" — the abr.go layer assembles those.
	RepID string

	// Codec is the codecs="..." string, e.g. "avc1.4D4028" or "mp4a.40.2".
	Codec string

	// Bandwidth is the bandwidth="..." attribute in bps. For audio,
	// the common 128_000 default applies; for video, derived from the
	// transcoder profile or measured from segments.
	Bandwidth int

	// Width / Height (video only). Zero for audio.
	Width  int
	Height int

	// SampleRate (audio only). Zero for video.
	SampleRate int

	// Timescale is the SegmentTemplate@timescale value. 90000 for
	// video; the audio sample rate (e.g. 48000) for audio.
	Timescale uint32

	// InitFile is the relative filename of the init segment ("init_v.mp4"
	// or "init_a.mp4"). The manifest serves it as
	// initialization="$initFile".
	InitFile string

	// MediaPattern is the SegmentTemplate@media value, e.g.
	// "seg_v_$Number%05d$.m4s". $Number$ resolves to StartNumber +
	// segment index.
	MediaPattern string

	// StartNumber is the first segment's number — the player computes
	// segment URLs as `pattern.replace($Number$, StartNumber + i)`.
	// MUST equal the actual filename suffix of Segments[0].
	StartNumber uint64

	// Segments is the sliding-window contents: each entry is one
	// segment's start tick + dur tick. The first entry's StartTicks
	// becomes the explicit `<S t="...">`; subsequent entries use only
	// `<S d="...">` (duration), with the player accumulating.
	Segments []SegmentEntry

	// PTO is the SegmentTemplate@presentationTimeOffset in this track's
	// timescale. Set for a static archive window whose fragments carry an
	// absolute (large, non-zero) tfdt: the player maps a segment at media
	// time t to presentation time (t − PTO)/timescale, so PTO = the first
	// emitted fragment's media time anchors the window to presentation 0.
	// Zero (the live default) is omitted from the XML.
	PTO uint64
}

// SegmentEntry is one segment's position + duration in track timescale.
type SegmentEntry struct {
	StartTicks uint64
	DurTicks   uint64
}

// longestObservedSegSec scans every track's segment list and returns the
// longest segment duration in whole seconds (rounded up). Used by
// buildMPDDoc to tune MinBuffer and MaxSegmentDuration so the MPD's
// announced timing matches what the player will actually fetch.
//
// Returns 0 when the input has no segments yet — caller falls back to
// the segDur-based defaults.
func longestObservedSegSec(in *ManifestInput) int {
	maxSec := 0
	scan := func(t *TrackManifest) {
		if t == nil || t.Timescale == 0 {
			return
		}
		for _, s := range t.Segments {
			// Round UP so a 5960 ms segment doesn't get announced as 5 s.
			secs := int((s.DurTicks + uint64(t.Timescale) - 1) / uint64(t.Timescale))
			if secs > maxSec {
				maxSec = secs
			}
		}
	}
	for i := range in.Video {
		scan(&in.Video[i])
	}
	scan(in.Audio)
	return maxSec
}

// BuildManifest serialises the MPD XML for a live DASH stream. Returns
// the bytes ready to atomically write to disk.
//
// Returns nil, nil when both tracks are empty (no segments to advertise
// yet) — the caller skips the write rather than publishing an empty MPD.
func BuildManifest(in *ManifestInput) ([]byte, error) {
	if in == nil {
		return nil, fmt.Errorf("manifest: nil input")
	}
	doc := buildMPDDoc(in)
	if doc == nil {
		return nil, nil
	}

	header := []byte(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	body, err := xml.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("manifest: marshal: %w", err)
	}
	return append(header, body...), nil
}

// buildMPDDoc constructs the in-memory MPD document. Returns nil when
// neither track has any segments yet.
//
// MPD timing attrs are derived from BOTH the configured segDur AND the
// largest observed segment dur on the current window. Configured-only
// values would lie to the player when a source's GOP forces segments
// well above segDur: a 2 s `SegDur` configured with an 8 s observed
// GOP would advertise `minBufferTime="PT4S"` while each new segment
// the player fetches is 8 s long, forcing the player to underrun its
// announced buffer at the first hand-off. Adapting to observed dur
// keeps the announcement honest; the player buffers enough to swallow
// the next segment before starting.
func buildMPDDoc(in *ManifestInput) *mpdRoot {
	segSec := int(in.SegDur.Seconds())
	if segSec < 1 {
		segSec = 1
	}
	maxObservedSec := longestObservedSegSec(in)
	minBufferSec := segSec * 2
	if maxObservedSec > minBufferSec {
		minBufferSec = maxObservedSec
	}
	maxSegDurSec := segSec * 3
	if maxObservedSec > maxSegDurSec {
		maxSegDurSec = maxObservedSec
	}
	doc := &mpdRoot{
		XMLNS:              "urn:mpeg:dash:schema:mpd:2011",
		MinBuffer:          durationISO(minBufferSec),
		MaxSegmentDuration: durationISO(maxSegDurSec),
		Periods:            []mpdPeriod{{ID: "0", Start: "PT0S"}},
	}
	if in.Static {
		// Bounded archive window — a fully-available VOD-like presentation.
		// Advertise the duration and OMIT every live-edge attr: a static MPD
		// carrying minimumUpdatePeriod (or an isoff-live profile) makes
		// players treat the window as a live stream and chase a non-existent
		// live edge. presentationTimeOffset on each Representation anchors the
		// absolute tfdt back to presentation 0.
		doc.Type = "static"
		doc.Profiles = "urn:mpeg:dash:profile:isoff-main:2011"
		doc.MediaPresentationDuration = durationISOFractional(in.MediaPresentationDuration)
		doc.Periods[0].Duration = doc.MediaPresentationDuration
	} else {
		doc.Type = "dynamic"
		doc.Profiles = "urn:mpeg:dash:profile:isoff-live:2011"
		doc.SuggestedPresentationDelay = durationISO(segSec * 3)
		doc.AvailabilityStartTime = formatRFC3339(in.AvailabilityStart)
		doc.MinUpdate = durationISO(segSec)
		doc.BufferDepth = durationISO(segSec * in.Window)
		doc.PublishTime = formatRFC3339(in.PublishTime)
		doc.UTCTiming = &mpdUTCTiming{
			SchemeIDURI: "urn:mpeg:dash:utc:direct:2014",
			Value:       formatRFC3339(in.PublishTime),
		}
	}
	per := &doc.Periods[0]

	if as := buildVideoAdaptationSet(in.Video); as != nil {
		per.AdaptationSets = append(per.AdaptationSets, *as)
	}
	if as := buildAudioAdaptationSet(in.Audio); as != nil {
		per.AdaptationSets = append(per.AdaptationSets, *as)
	}
	if len(per.AdaptationSets) == 0 {
		return nil
	}
	return doc
}

// buildVideoAdaptationSet emits the video AdaptationSet with one or
// more Representations. Returns nil when no rep has segments yet.
//
// ABR ladders pass multiple reps; single-rendition streams pass one.
// All reps share the AdaptationSet-level mimeType / segmentAlignment /
// startWithSAP attributes; per-rep attrs (codec, bandwidth, width,
// height, SegmentTemplate) sit inside each <Representation>.
func buildVideoAdaptationSet(reps []TrackManifest) *mpdAdaptationSet {
	mpdReps := make([]mpdRepresentation, 0, len(reps))
	for _, t := range reps {
		if len(t.Segments) == 0 {
			continue
		}
		mpdReps = append(mpdReps, mpdRepresentation{
			ID:        t.RepID,
			MimeType:  "video/mp4",
			Codecs:    t.Codec,
			Bandwidth: t.Bandwidth,
			Width:     t.Width,
			Height:    t.Height,
			SegmentTemplate: mpdSegmentTemplate{
				Timescale:      int(t.Timescale), //nolint:gosec // timescale fits int
				PTO:            t.PTO,
				Initialization: t.InitFile,
				Media:          t.MediaPattern,
				StartNumber:    int(t.StartNumber), //nolint:gosec // segment numbers fit int for live durations
				Timeline:       buildSegTimeline(t.Segments),
			},
		})
	}
	if len(mpdReps) == 0 {
		return nil
	}
	return &mpdAdaptationSet{
		ID:               "0",
		ContentType:      "video",
		MimeType:         "video/mp4",
		SegmentAlignment: "true",
		StartWithSAP:     "1",
		Representations:  mpdReps,
	}
}

// buildAudioAdaptationSet emits the audio AdaptationSet, or nil when no
// segments are available.
func buildAudioAdaptationSet(t *TrackManifest) *mpdAdaptationSet {
	if t == nil || len(t.Segments) == 0 {
		return nil
	}
	asr := t.SampleRate
	return &mpdAdaptationSet{
		ID:               "1",
		ContentType:      "audio",
		MimeType:         "audio/mp4",
		SegmentAlignment: "true",
		StartWithSAP:     "1",
		Representations: []mpdRepresentation{{
			ID:                t.RepID,
			MimeType:          "audio/mp4",
			Codecs:            t.Codec,
			Bandwidth:         t.Bandwidth,
			AudioSamplingRate: &asr,
			SegmentTemplate: mpdSegmentTemplate{
				Timescale:      int(t.Timescale), //nolint:gosec // timescale fits int
				PTO:            t.PTO,
				Initialization: t.InitFile,
				Media:          t.MediaPattern,
				StartNumber:    int(t.StartNumber), //nolint:gosec // segment numbers fit int for live durations
				Timeline:       buildSegTimeline(t.Segments),
			},
		}},
	}
}

// buildSegTimeline emits the SegmentTimeline child with an EXPLICIT
// `t=` on every entry.
//
// The DASH spec allows omitting `t=` on entries after the first — the
// player then accumulates `prev_t + prev_d` for each subsequent segment,
// implying a contiguous timeline. That's correct when per-segment durs
// exactly sum to the inter-write wallclock delta. With wallclock-based
// tfdt and sample-count-based durs (the AAC 1024-sample frame rule),
// the actual segment files can carry a tfdt that's AHEAD of the
// implied-cumulative value — e.g. an under-emitting audio track writes
// 0.5 s of content every 2 s of wallclock, leaving a 1.5 s gap between
// the segment-file tfdt and the next segment's start.
//
// Without explicit `t=`, the player's view of where each segment lives
// diverges from the segment file's actual tfdt. Players that strictly
// validate this (dashjs in some modes, Shaka) refuse to play. Emitting
// explicit `t=` keeps the MPD's view of each segment's start exactly
// equal to the segment file's tfdt.
func buildSegTimeline(segs []SegmentEntry) *mpdSegTimeline {
	if len(segs) == 0 {
		return nil
	}
	tl := &mpdSegTimeline{S: make([]mpdSTimeline, 0, len(segs))}
	for _, s := range segs {
		if s.DurTicks == 0 {
			continue
		}
		t := s.StartTicks
		tl.S = append(tl.S, mpdSTimeline{T: &t, D: s.DurTicks})
	}
	return tl
}

// durationISO returns the ISO-8601 duration form (e.g. PT4S) for a
// whole-second value. Zero produces "PT0S".
func durationISO(seconds int) string {
	return fmt.Sprintf("PT%dS", seconds)
}

// durationISOFractional returns the ISO-8601 duration form with millisecond
// precision (e.g. PT3600.04S) so a static window's mediaPresentationDuration
// doesn't truncate the seekable tail to a whole second. Trailing zeros in the
// fraction are trimmed; a whole-second value emits no decimal point.
func durationISOFractional(d time.Duration) string {
	if d <= 0 {
		return "PT0S"
	}
	ms := d.Milliseconds()
	sec, frac := ms/1000, ms%1000
	if frac == 0 {
		return fmt.Sprintf("PT%dS", sec)
	}
	return fmt.Sprintf("PT%d.%sS", sec, strings.TrimRight(fmt.Sprintf("%03d", frac), "0"))
}

// formatRFC3339 returns t in UTC RFC3339 form, or empty when t is zero.
func formatRFC3339(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// ─── MPD XML structs (private — exposed only via BuildManifest) ──────

type mpdRoot struct {
	XMLName                    xml.Name      `xml:"MPD"`
	XMLNS                      string        `xml:"xmlns,attr"`
	Type                       string        `xml:"type,attr"`
	Profiles                   string        `xml:"profiles,attr"`
	MinBuffer                  string        `xml:"minBufferTime,attr"`
	MediaPresentationDuration  string        `xml:"mediaPresentationDuration,attr,omitempty"`
	SuggestedPresentationDelay string        `xml:"suggestedPresentationDelay,attr,omitempty"`
	MaxSegmentDuration         string        `xml:"maxSegmentDuration,attr,omitempty"`
	AvailabilityStartTime      string        `xml:"availabilityStartTime,attr,omitempty"`
	MinUpdate                  string        `xml:"minimumUpdatePeriod,attr,omitempty"`
	BufferDepth                string        `xml:"timeShiftBufferDepth,attr,omitempty"`
	PublishTime                string        `xml:"publishTime,attr,omitempty"`
	Periods                    []mpdPeriod   `xml:"Period"`
	UTCTiming                  *mpdUTCTiming `xml:"UTCTiming,omitempty"`
}

type mpdUTCTiming struct {
	SchemeIDURI string `xml:"schemeIdUri,attr"`
	Value       string `xml:"value,attr"`
}

type mpdPeriod struct {
	ID             string             `xml:"id,attr"`
	Start          string             `xml:"start,attr"`
	Duration       string             `xml:"duration,attr,omitempty"`
	AdaptationSets []mpdAdaptationSet `xml:"AdaptationSet"`
}

type mpdAdaptationSet struct {
	ID               string              `xml:"id,attr"`
	ContentType      string              `xml:"contentType,attr"`
	MimeType         string              `xml:"mimeType,attr"`
	SegmentAlignment string              `xml:"segmentAlignment,attr"`
	StartWithSAP     string              `xml:"startWithSAP,attr"`
	Representations  []mpdRepresentation `xml:"Representation"`
}

type mpdRepresentation struct {
	ID                string             `xml:"id,attr"`
	MimeType          string             `xml:"mimeType,attr"`
	Codecs            string             `xml:"codecs,attr"`
	Bandwidth         int                `xml:"bandwidth,attr"`
	Width             int                `xml:"width,attr,omitempty"`
	Height            int                `xml:"height,attr,omitempty"`
	AudioSamplingRate *int               `xml:"audioSamplingRate,attr,omitempty"`
	SegmentTemplate   mpdSegmentTemplate `xml:"SegmentTemplate"`
}

type mpdSegmentTemplate struct {
	Timescale      int             `xml:"timescale,attr"`
	PTO            uint64          `xml:"presentationTimeOffset,attr,omitempty"`
	Initialization string          `xml:"initialization,attr"`
	Media          string          `xml:"media,attr"`
	StartNumber    int             `xml:"startNumber,attr"`
	Timeline       *mpdSegTimeline `xml:"SegmentTimeline"`
}

type mpdSegTimeline struct {
	S []mpdSTimeline `xml:"S"`
}

type mpdSTimeline struct {
	T *uint64 `xml:"t,attr,omitempty"`
	D uint64  `xml:"d,attr"`
}
