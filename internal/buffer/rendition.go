package buffer

import (
	"fmt"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// RenditionBufferID is the Buffer Hub id for one transcoded video profile (ABR ladder).
// Slug is always VideoTrackSlug(i) for the profile index i in the ladder.
func RenditionBufferID(code domain.StreamCode, slug string) domain.StreamCode {
	return domain.StreamCode("$r$" + string(code) + "$" + slug)
}

// VideoTrackSlug returns the stable path segment for the profile at index (0-based): track_1, track_2, ….
func VideoTrackSlug(index int) string {
	return fmt.Sprintf("track_%d", index+1)
}

// RenditionPlayout describes one ladder rung for publisher routing.
type RenditionPlayout struct {
	Slug        string
	BufferID    domain.StreamCode
	Width       int
	Height      int
	BitrateKbps int
}

// RenditionsForTranscoder returns ladder entries when transcoding is active (non-external).
// When video.copy is true or video.profiles is empty, a single passthrough rendition
// is returned (one worker: copy from raw ingest, no ABR ladder).
func RenditionsForTranscoder(code domain.StreamCode, tc *domain.TranscoderConfig) []RenditionPlayout {
	if tc == nil {
		return nil
	}
	// Both video and audio copy → raw MPEG-TS passes through without FFmpeg; no rendition buffers needed.
	if tc.Video.Copy && tc.Audio.Copy {
		return nil
	}
	if tc.Video.Copy || len(tc.Video.Profiles) == 0 {
		slug := VideoTrackSlug(0)
		return []RenditionPlayout{{
			Slug:        slug,
			BufferID:    RenditionBufferID(code, slug),
			Width:       0,
			Height:      0,
			BitrateKbps: 0,
		}}
	}
	out := make([]RenditionPlayout, 0, len(tc.Video.Profiles))
	for i, p := range tc.Video.Profiles {
		slug := VideoTrackSlug(i)
		br := p.Bitrate
		if br <= 0 {
			br = 2500
		}
		out = append(out, RenditionPlayout{
			Slug:        slug,
			BufferID:    RenditionBufferID(code, slug),
			Width:       p.Width,
			Height:      p.Height,
			BitrateKbps: br,
		})
	}
	return out
}

// BestRenditionIndex picks the highest resolution (then bitrate) for single-bitrate protocols.
func BestRenditionIndex(rends []RenditionPlayout) int {
	if len(rends) == 0 {
		return 0
	}
	best := 0
	bestScore := rends[0].Width*rends[0].Height*1000 + rends[0].BitrateKbps
	for i := 1; i < len(rends); i++ {
		sc := rends[i].Width*rends[i].Height*1000 + rends[i].BitrateKbps
		if sc > bestScore {
			bestScore = sc
			best = i
		}
	}
	return best
}

// PlaybackBufferID returns the buffer subscribers should use for single-rendition outputs
// (RTSP, RTMP play, SRT, push, DVR) — best ladder rung when transcoding, else the main stream code.
func PlaybackBufferID(code domain.StreamCode, tc *domain.TranscoderConfig) domain.StreamCode {
	rends := RenditionsForTranscoder(code, tc)
	if len(rends) == 0 {
		return code
	}
	return rends[BestRenditionIndex(rends)].BufferID
}

// BandwidthBps returns EXT-X-STREAM-INF BANDWIDTH (bits/s).
func (r RenditionPlayout) BandwidthBps() int {
	if r.BitrateKbps <= 0 {
		return 2_500_000
	}
	return r.BitrateKbps * 1000
}
