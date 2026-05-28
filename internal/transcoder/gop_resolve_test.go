package transcoder

import (
	"testing"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// TestResolveGopFrames_Precedence locks the resolver's documented
// fallback chain (profile override → global frame-count → encoder
// default) so an accidental swap of priorities or off-by-one in the
// framerate math doesn't silently regress the segment cadence on
// every transcoded stream.
func TestResolveGopFrames_Precedence(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name string
		p    Profile
		tc   *domain.TranscoderConfig
		want int32
	}{
		{
			name: "profile keyframe interval wins over global gop",
			p:    Profile{KeyframeInterval: 4, Framerate: 25},
			tc:   &domain.TranscoderConfig{Global: domain.TranscoderGlobalConfig{GOP: 100, FPS: 25}},
			want: 100, // 4s * 25fps = 100 frames; matches global by coincidence here, but path is the profile override
		},
		{
			name: "profile-only at 30fps yields 60 frames for 2s",
			p:    Profile{KeyframeInterval: 2, Framerate: 30},
			tc:   nil,
			want: 60,
		},
		{
			name: "profile zero falls back to global gop frames",
			p:    Profile{KeyframeInterval: 0, Framerate: 25},
			tc:   &domain.TranscoderConfig{Global: domain.TranscoderGlobalConfig{GOP: 100, FPS: 25}},
			want: 100,
		},
		{
			name: "profile zero + framerate zero uses global fps for conversion",
			p:    Profile{KeyframeInterval: 0, Framerate: 0},
			tc:   &domain.TranscoderConfig{Global: domain.TranscoderGlobalConfig{GOP: 50, FPS: 25}},
			want: 50,
		},
		{
			name: "all zero returns 0 = encoder default",
			p:    Profile{},
			tc:   nil,
			want: 0,
		},
		{
			name: "global gop set but profile uses framerate from itself",
			p:    Profile{KeyframeInterval: 3, Framerate: 60},
			tc:   &domain.TranscoderConfig{Global: domain.TranscoderGlobalConfig{GOP: 100, FPS: 25}},
			want: 180, // 3s * 60fps
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := resolveGopFrames(tc.p, tc.tc)
			if got != tc.want {
				t.Fatalf("resolveGopFrames = %d, want %d", got, tc.want)
			}
		})
	}
}
