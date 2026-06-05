package blob

import (
	"fmt"
	"math"
	"strings"
	"time"
)

const pdtLayout = "2006-01-02T15:04:05.000Z07:00"

// RenderMaster builds the HLS multivariant playlist: one video variant per
// profile plus a shared audio rendition group pointing at the audio-source
// profile. params is the timeshift query string ("from=…&dur=…") carried onto
// every child URL.
func RenderMaster(cat *Catalog, params string) string {
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n")
	if cat.AudioProfile != "" {
		fmt.Fprintf(&b, "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"audio\",DEFAULT=YES,AUTOSELECT=YES,URI=\"index.m3u8?%s&profile=%s&track=a\"\n",
			params, cat.AudioProfile)
	}
	for _, p := range orderedProfiles(cat) {
		bw := p.Bandwidth
		if bw == 0 {
			bw = 2_500_000
		}
		codec := p.Codec
		if codec == "" {
			codec = "avc1.4d401f"
		}
		audio := ""
		if cat.AudioProfile != "" {
			audio = fmt.Sprintf(",CODECS=\"%s,mp4a.40.2\",AUDIO=\"aud\"", codec)
		} else {
			audio = fmt.Sprintf(",CODECS=\"%s\"", codec)
		}
		fmt.Fprintf(&b, "#EXT-X-STREAM-INF:BANDWIDTH=%d%s", bw, audio)
		if p.Width > 0 && p.Height > 0 {
			fmt.Fprintf(&b, ",RESOLUTION=%dx%d", p.Width, p.Height)
		}
		b.WriteString("\n")
		fmt.Fprintf(&b, "index.m3u8?%s&profile=%s&track=v\n", params, p.ID)
	}
	return b.String()
}

// orderedProfiles returns the catalog profiles with the best one first (the
// player's default variant).
func orderedProfiles(cat *Catalog) []ProfileDesc {
	if cat.BestProfile == "" {
		return cat.Profiles
	}
	out := make([]ProfileDesc, 0, len(cat.Profiles))
	for _, p := range cat.Profiles {
		if p.ID == cat.BestProfile {
			out = append(out, p)
		}
	}
	for _, p := range cat.Profiles {
		if p.ID != cat.BestProfile {
			out = append(out, p)
		}
	}
	return out
}

// RenderMediaPlaylist builds a VOD fMP4 media playlist for one track of a
// resolved window. Fragment URIs are relative ("dvr/<profile>/…") so they
// resolve against the playlist URL.
func RenderMediaPlaylist(w *Window, track uint8) string {
	frags, ts, trackTok := w.Video, w.VideoTimescale, "0"
	if track == TrackAudio {
		frags, ts, trackTok = w.Audio, w.AudioTimescale, "1"
	}
	if ts == 0 {
		ts = 1
	}
	// Flat single-segment URIs (no '/') so they route through the catch-all
	// media dispatcher as <code>/<file>: init = dvri-<profile>-<track>.mp4,
	// fragment = dvrf-<profile>-<hourCompact>-<track>-<seq>.m4s.
	initURI := fmt.Sprintf("dvri-%s-%s.mp4", w.Profile, trackTok)

	var maxDur float64
	for _, f := range frags {
		if d := float64(f.DurTicks) / float64(ts); d > maxDur {
			maxDur = d
		}
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", int(math.Ceil(maxDur)))
	b.WriteString("#EXT-X-PLAYLIST-TYPE:VOD\n")
	fmt.Fprintf(&b, "#EXT-X-MAP:URI=%q\n", initURI)
	for i, f := range frags {
		if f.Discontinuity && i > 0 {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
			fmt.Fprintf(&b, "#EXT-X-MAP:URI=%q\n", initURI)
		}
		if i == 0 || f.Discontinuity {
			fmt.Fprintf(&b, "#EXT-X-PROGRAM-DATE-TIME:%s\n", time.UnixMilli(f.WallTimeMs).UTC().Format(pdtLayout))
		}
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n", float64(f.DurTicks)/float64(ts))
		fmt.Fprintf(&b, "dvrf-%s-%s-%s-%d.m4s\n", w.Profile, f.HourCompact, trackTok, f.SeqInHour)
	}
	b.WriteString("#EXT-X-ENDLIST\n")
	return b.String()
}
