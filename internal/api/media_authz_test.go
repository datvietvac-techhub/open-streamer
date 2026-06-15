package api

import (
	"testing"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// TestStripABRTrackSlug — media auth must key on the parent stream code, not on
// a per-rendition track_<N> subdir, so a token minted for <code> covers every
// ABR rendition served at /<code>/track_<N>/…. Regression for the adversarial
// finding where ABR segments were authed under "<code>/track_N" (token verify
// + per-stream policy both missed).
func TestStripABRTrackSlug(t *testing.T) {
	t.Parallel()
	cases := []struct{ in, want string }{
		{"ch1/track_1", "ch1"},
		{"ch1/track_12", "ch1"},
		{"region/north/live/track_2", "region/north/live"}, // multi-segment code
		{"ch1", "ch1"},                                     // no slug
		{"ch1/index.m3u8", "ch1/index.m3u8"},               // not a track slug — left intact (file split happens earlier)
		{"ch1/track_x", "ch1/track_x"},                     // non-numeric → not a slug
		{"ch1/track_", "ch1/track_"},                       // empty number → not a slug
		{"track_1", "track_1"},                             // no parent code → unchanged
	}
	for _, c := range cases {
		if got := string(stripABRTrackSlug(domain.StreamCode(c.in))); got != c.want {
			t.Errorf("stripABRTrackSlug(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
