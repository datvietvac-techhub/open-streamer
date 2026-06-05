package blob

import (
	"strconv"
	"strings"
)

// Flat URL filename scheme (single path segment, no '/'), so blob timeshift
// fragments/inits route through the catch-all media dispatcher as <code>/<file>:
//
//	dvrf-<profile>-<hourCompact>-<track>-<seq>.m4s   one fragment (HLS, by hour+seq)
//	dvrt-<profile>-<track>-<mediaTicks>.m4s          one fragment (DASH, by $Time$)
//	dvri-<profile>-<track>.mp4                       a track's init
//
// profile is "pN", hourCompact is "YYYYMMDDHH", track is 0 (video) or 1 (audio).
// The HLS playlist addresses fragments by (hour, seq); the DASH SegmentTemplate
// uses $Time$, so its media URLs carry the fragment's exact media-time tick —
// reader.FragmentByTime resolves that back to the stored record.
const (
	fragPrefix     = "dvrf-"
	timeFragPrefix = "dvrt-"
	initPrefix     = "dvri-"
	fragSuffix     = ".m4s"
	initSuffix     = ".mp4"
)

// IsBlobMediaFile reports whether file is a blob fragment or init request.
func IsBlobMediaFile(file string) bool {
	return strings.HasPrefix(file, fragPrefix) ||
		strings.HasPrefix(file, timeFragPrefix) ||
		strings.HasPrefix(file, initPrefix)
}

// ParseFragmentName decodes a dvrf-… fragment filename.
func ParseFragmentName(file string) (profile, hourCompact string, track uint8, seq int, ok bool) {
	if !strings.HasPrefix(file, fragPrefix) || !strings.HasSuffix(file, fragSuffix) {
		return "", "", 0, 0, false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(file, fragPrefix), fragSuffix)
	parts := strings.Split(body, "-")
	if len(parts) != 4 {
		return "", "", 0, 0, false
	}
	tr, okT := parseTrack(parts[2])
	sq, errS := strconv.Atoi(parts[3])
	if !validProfile(parts[0]) || !validHourCompact(parts[1]) || !okT || errS != nil || sq < 0 {
		return "", "", 0, 0, false
	}
	return parts[0], parts[1], tr, sq, true
}

// ParseTimeFragmentName decodes a dvrt-… DASH fragment filename addressed by
// media-time tick: dvrt-<profile>-<track>-<mediaTicks>.m4s. mediaTicks is the
// fragment's exact tfdt in its track's timescale (video: 90000, audio: sample
// rate) — the value reader.FragmentByTime matches against the stored record.
func ParseTimeFragmentName(file string) (profile string, track uint8, mediaTicks uint64, ok bool) {
	if !strings.HasPrefix(file, timeFragPrefix) || !strings.HasSuffix(file, fragSuffix) {
		return "", 0, 0, false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(file, timeFragPrefix), fragSuffix)
	parts := strings.Split(body, "-")
	if len(parts) != 3 {
		return "", 0, 0, false
	}
	tr, okT := parseTrack(parts[1])
	ticks, errT := strconv.ParseUint(parts[2], 10, 64)
	if !validProfile(parts[0]) || !okT || errT != nil {
		return "", 0, 0, false
	}
	return parts[0], tr, ticks, true
}

// ParseInitName decodes a dvri-… init filename.
func ParseInitName(file string) (profile string, track uint8, ok bool) {
	if !strings.HasPrefix(file, initPrefix) || !strings.HasSuffix(file, initSuffix) {
		return "", 0, false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(file, initPrefix), initSuffix)
	parts := strings.Split(body, "-")
	if len(parts) != 2 {
		return "", 0, false
	}
	tr, okT := parseTrack(parts[1])
	if !validProfile(parts[0]) || !okT {
		return "", 0, false
	}
	return parts[0], tr, true
}

func parseTrack(s string) (uint8, bool) {
	switch s {
	case "0":
		return TrackVideo, true
	case "1":
		return TrackAudio, true
	default:
		return 0, false
	}
}

// validProfile accepts the "pN" profile-id form.
func validProfile(s string) bool {
	if len(s) < 2 || s[0] != 'p' {
		return false
	}
	for _, c := range s[1:] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// validHourCompact accepts exactly 10 digits ("YYYYMMDDHH").
func validHourCompact(s string) bool {
	if len(s) != 10 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
