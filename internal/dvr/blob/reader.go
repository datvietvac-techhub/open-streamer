package blob

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// FragmentRef locates one stored fragment for serving. ByteOffset/ByteLen point
// into the hour blob (.cmfv for video, .cmfa for audio); the reader slices those
// bytes verbatim — no re-mux.
type FragmentRef struct {
	Track         uint8
	HourStem      string // absolute path stem (…/p0/YYYY/MM/DD/HH) for reading bytes
	HourCompact   string // "YYYYMMDDHH" — the URL hour token
	SeqInHour     int    // 0-based index of this record within its track+hour
	ByteOffset    int64
	ByteLen       int
	MediaTicks    uint64
	DurTicks      uint64
	WallTimeMs    int64
	Keyframe      bool
	Discontinuity bool
}

// Window is the resolved timeshift slice for one profile.
type Window struct {
	Profile        string
	Video          []FragmentRef
	Audio          []FragmentRef
	VideoTimescale uint32
	AudioTimescale uint32
	PDT0           time.Time // wall time of the first video fragment
}

// Reader serves timeshift from one stream's archive. Construct per request
// (cheap); it holds a catalog snapshot loaded from disk.
type Reader struct {
	streamDir string
	cat       *Catalog
}

// NewReader loads the stream's catalog. Returns an error if no blob archive
// exists for the stream dir.
func NewReader(streamDir string) (*Reader, error) {
	cat, ok, err := LoadCatalog(streamDir)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("blob: no archive at %s", streamDir)
	}
	return &Reader{streamDir: streamDir, cat: cat}, nil
}

// Catalog returns the loaded catalog snapshot.
func (br *Reader) Catalog() *Catalog { return br.cat }

// Query resolves the [from, from+dur) window for profileID into a Window of
// fragment refs. dur <= 0 means "to the end of the recording". Video is
// keyframe-snapped (start at the last IDR ≤ from); audio is paired by media-time
// overlap so the player has samples covering the first video frame.
func (br *Reader) Query(profileID string, from time.Time, dur time.Duration) (*Window, error) {
	pd, ok := br.cat.Profile(profileID)
	if !ok {
		return nil, fmt.Errorf("blob: unknown profile %q", profileID)
	}
	profDir := profileDirPath(br.streamDir, profileID)

	fromMs := from.UTC().UnixMilli()
	endMs := int64(1<<62 - 1)
	if dur > 0 {
		endMs = fromMs + dur.Milliseconds()
	}
	fromTicks := br.cat.WallToMediaTicks(fromMs)
	endTicks := uint64(1<<63 - 1)
	if dur > 0 {
		endTicks = br.cat.WallToMediaTicks(endMs)
	}

	var allV, allA []FragmentRef
	var vTS, aTS uint32
	for _, hr := range pd.Hours {
		if hr.WallToMs < fromMs || hr.WallFromMs > endMs {
			continue // hour outside the window
		}
		stem := filepath.Join(profDir, filepath.FromSlash(hr.Hour))
		hdr, recs, err := ReadRanges(stem+".ranges", hourBlobSize(stem))
		if err != nil {
			return nil, err
		}
		if hdr.VideoTimescale != 0 {
			vTS = hdr.VideoTimescale
		}
		if hdr.AudioTimescale != 0 {
			aTS = hdr.AudioTimescale
		}
		compact := strings.ReplaceAll(hr.Hour, "/", "")
		var vSeq, aSeq int
		for _, r := range recs {
			ref := FragmentRef{
				Track: r.Track, HourStem: stem, HourCompact: compact,
				ByteOffset: int64(r.ByteOffset), ByteLen: int(r.ByteLen), //nolint:gosec // bounded
				MediaTicks: r.MediaTicks, DurTicks: uint64(r.DurTicks), WallTimeMs: r.WallTimeMs,
				Keyframe: r.Keyframe, Discontinuity: r.Discontinuity,
			}
			if r.Track == TrackVideo {
				ref.SeqInHour = vSeq
				vSeq++
				allV = append(allV, ref)
			} else {
				ref.SeqInHour = aSeq
				aSeq++
				allA = append(allA, ref)
			}
		}
	}

	w := &Window{Profile: profileID, VideoTimescale: vTS, AudioTimescale: aTS}
	w.Video = snapVideo(allV, fromTicks, endTicks)
	if len(w.Video) > 0 {
		w.PDT0 = time.UnixMilli(w.Video[0].WallTimeMs).UTC()
		startTicks := w.Video[0].MediaTicks
		// audio media ticks are in the audio timescale; convert the video-clock
		// window bounds for the overlap test.
		w.Audio = pairAudio(allA, startTicks, endTicks, vTS, aTS)
	}
	return w, nil
}

// snapVideo keeps video refs from the last keyframe ≤ fromTicks up to (but not
// including) endTicks.
func snapVideo(refs []FragmentRef, fromTicks, endTicks uint64) []FragmentRef {
	start := 0
	for i, r := range refs {
		if r.MediaTicks > fromTicks {
			break
		}
		if r.Keyframe {
			start = i
		}
	}
	out := make([]FragmentRef, 0, len(refs))
	for i := start; i < len(refs); i++ {
		if refs[i].MediaTicks >= endTicks {
			break
		}
		out = append(out, refs[i])
	}
	return out
}

// pairAudio selects audio fragments overlapping [startTicks, endTicks) where
// those bounds are in the VIDEO timescale; they are converted to audio ticks.
func pairAudio(refs []FragmentRef, startTicksV, endTicksV uint64, vTS, aTS uint32) []FragmentRef {
	if aTS == 0 || vTS == 0 {
		return nil
	}
	startA := startTicksV * uint64(aTS) / uint64(vTS)
	var endA uint64 = 1<<63 - 1
	if endTicksV < 1<<62 {
		endA = endTicksV * uint64(aTS) / uint64(vTS)
	}
	out := make([]FragmentRef, 0, len(refs))
	for _, r := range refs {
		if r.MediaTicks+r.DurTicks <= startA || r.MediaTicks >= endA {
			continue
		}
		out = append(out, r)
	}
	return out
}

// ReadFragment returns the exact stored bytes of one fragment.
func (br *Reader) ReadFragment(ref FragmentRef) ([]byte, error) {
	ext := ".cmfv"
	if ref.Track == TrackAudio {
		ext = ".cmfa"
	}
	bf, err := openBlobFile(ref.HourStem + ext)
	if err != nil {
		return nil, err
	}
	defer func() { _ = bf.Close() }()
	return bf.ReadSlice(ref.ByteOffset, ref.ByteLen)
}

// FragmentByHour resolves a fragment addressed by (hourCompact, track, seq) and
// returns its bytes. Used by the fragment HTTP route.
func (br *Reader) FragmentByHour(profileID, hourCompact string, track uint8, seq int) ([]byte, error) {
	stem, err := br.hourStemFromCompact(profileID, hourCompact)
	if err != nil {
		return nil, err
	}
	_, recs, err := ReadRanges(stem+".ranges", hourBlobSize(stem))
	if err != nil {
		return nil, err
	}
	idx := 0
	for _, r := range recs {
		if r.Track != track {
			continue
		}
		if idx == seq {
			ext := ".cmfv"
			if track == TrackAudio {
				ext = ".cmfa"
			}
			bf, err := openBlobFile(stem + ext)
			if err != nil {
				return nil, err
			}
			defer func() { _ = bf.Close() }()
			return bf.ReadSlice(int64(r.ByteOffset), int(r.ByteLen)) //nolint:gosec // bounded
		}
		idx++
	}
	return nil, fmt.Errorf("blob: fragment %s-%d-%d not found", hourCompact, track, seq)
}

// ReadInit returns the init bytes ([0, initLen)) for a profile's track, taken
// from the newest hour that has the track.
func (br *Reader) ReadInit(profileID string, track uint8) ([]byte, error) {
	pd, ok := br.cat.Profile(profileID)
	if !ok || len(pd.Hours) == 0 {
		return nil, fmt.Errorf("blob: no hours for profile %q", profileID)
	}
	profDir := profileDirPath(br.streamDir, profileID)
	hr := pd.Hours[len(pd.Hours)-1]
	stem := filepath.Join(profDir, filepath.FromSlash(hr.Hour))
	hdr, _, err := ReadRanges(stem+".ranges", hourBlobSize(stem))
	if err != nil {
		return nil, err
	}
	ext, initLen := ".cmfv", int64(hdr.VideoInitLen)
	if track == TrackAudio {
		ext, initLen = ".cmfa", int64(hdr.AudioInitLen)
	}
	if initLen == 0 {
		return nil, fmt.Errorf("blob: no %s init for profile %q", ext, profileID)
	}
	bf, err := openBlobFile(stem + ext)
	if err != nil {
		return nil, err
	}
	defer func() { _ = bf.Close() }()
	return bf.ReadSlice(0, int(initLen))
}

func (br *Reader) hourStemFromCompact(profileID, hourCompact string) (string, error) {
	if len(hourCompact) != 10 {
		return "", fmt.Errorf("blob: bad hour token %q", hourCompact)
	}
	for _, c := range hourCompact {
		if c < '0' || c > '9' {
			return "", fmt.Errorf("blob: bad hour token %q", hourCompact)
		}
	}
	rel := filepath.Join(hourCompact[0:4], hourCompact[4:6], hourCompact[6:8], hourCompact[8:10])
	return filepath.Join(profileDirPath(br.streamDir, profileID), rel), nil
}

// hourBlobSize is the .cmfv size used to bound-check video records; audio
// records are bounded by the .cmfa size, but ReadRanges takes one bound — the
// video blob is always the larger/primary, and recovery already truncated any
// torn tail, so the persisted records are durable in both blobs.
func hourBlobSize(stem string) int64 {
	v := fileSize(stem + ".cmfv")
	if a := fileSize(stem + ".cmfa"); a > v {
		return a
	}
	return v
}
