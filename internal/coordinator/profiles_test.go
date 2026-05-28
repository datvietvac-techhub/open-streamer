package coordinator

import (
	"testing"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/transcoder"
	"github.com/stretchr/testify/require"
)

// Regression: coordinator must not hardcode "libx264"/"fast" as profile defaults
// when codec/preset are unset — those values short-circuit normalizeVideoEncoder
// and silently override Global.HW=nvenc, producing CPU-encoded output despite
// GPU configuration.

func TestTranscoderProfilesFromDomain_NilVideoReturnsPassthrough(t *testing.T) {
	t.Parallel()
	got := transcoderProfilesFromDomain(nil)
	require.Len(t, got, 1, "nil video → exactly one passthrough profile")
	require.Empty(t, got[0].Codec, "passthrough must leave codec empty for HW routing")
	require.Empty(t, got[0].Preset, "passthrough must leave preset empty")
}

func TestTranscoderProfilesFromDomain_VideoCopyReturnsPassthrough(t *testing.T) {
	t.Parallel()
	video := &domain.VideoTranscodeConfig{Copy: true}
	got := transcoderProfilesFromDomain(video)
	require.Len(t, got, 1)
	require.Empty(t, got[0].Codec)
	require.Empty(t, got[0].Preset)
}

func TestTranscoderProfilesFromDomain_EmptyProfilesReturnsPassthrough(t *testing.T) {
	t.Parallel()
	video := &domain.VideoTranscodeConfig{Profiles: nil}
	got := transcoderProfilesFromDomain(video)
	require.Len(t, got, 1)
	require.Empty(t, got[0].Codec)
}

// Core regression case: profile with empty codec must propagate "" so the
// transcoder layer can route based on Global.HW. Previous code hardcoded
// "libx264" here, which made HW=nvenc a no-op.
func TestTranscoderProfilesFromDomain_EmptyCodecPreservedForHWRouting(t *testing.T) {
	t.Parallel()
	video := &domain.VideoTranscodeConfig{
		Profiles: []domain.VideoProfile{
			{Width: 1920, Height: 1080, Bitrate: 4500},
		},
	}
	got := transcoderProfilesFromDomain(video)
	require.Len(t, got, 1)
	require.Empty(t, got[0].Codec, "empty codec must NOT be replaced with libx264")
	require.Empty(t, got[0].Preset, "empty preset must NOT be replaced with fast")
}

func TestTranscoderProfilesFromDomain_ExplicitCodecAndPresetPreserved(t *testing.T) {
	t.Parallel()
	video := &domain.VideoTranscodeConfig{
		Profiles: []domain.VideoProfile{
			{Codec: "h264_nvenc", Preset: "p5", Width: 1280, Height: 720, Bitrate: 2500},
			{Codec: "libx265", Preset: "veryfast", Width: 854, Height: 480, Bitrate: 800},
		},
	}
	got := transcoderProfilesFromDomain(video)
	require.Len(t, got, 2)
	require.Equal(t, "h264_nvenc", got[0].Codec)
	require.Equal(t, "p5", got[0].Preset)
	require.Equal(t, "libx265", got[1].Codec)
	require.Equal(t, "veryfast", got[1].Preset)
}

func TestTranscoderProfilesFromDomain_CodecCopyTreatedAsEmpty(t *testing.T) {
	t.Parallel()
	video := &domain.VideoTranscodeConfig{
		Profiles: []domain.VideoProfile{
			{Codec: domain.VideoCodecCopy, Width: 1280, Height: 720, Bitrate: 2500},
		},
	}
	got := transcoderProfilesFromDomain(video)
	require.Len(t, got, 1)
	require.Empty(t, got[0].Codec, `codec="copy" at profile level is meaningless when video.copy=false; treat as empty so HW routing works`)
}

func TestTranscoderProfilesFromDomain_BitrateDefaultWhenZero(t *testing.T) {
	t.Parallel()
	video := &domain.VideoTranscodeConfig{
		Profiles: []domain.VideoProfile{
			{Width: 1280, Height: 720, Bitrate: 0},
		},
	}
	got := transcoderProfilesFromDomain(video)
	require.Equal(t, "2500k", got[0].Bitrate)
}

func TestTranscoderProfilesFromDomain_BitrateExplicit(t *testing.T) {
	t.Parallel()
	video := &domain.VideoTranscodeConfig{
		Profiles: []domain.VideoProfile{
			{Width: 1920, Height: 1080, Bitrate: 4500},
		},
	}
	got := transcoderProfilesFromDomain(video)
	require.Equal(t, "4500k", got[0].Bitrate)
}

// Make sure non-defaulted fields (dimensions, framerate, GOP, max bitrate,
// codec profile/level) round-trip through the mapping unchanged.
func TestTranscoderProfilesFromDomain_AllFieldsPropagated(t *testing.T) {
	t.Parallel()
	video := &domain.VideoTranscodeConfig{
		Profiles: []domain.VideoProfile{{
			Codec:            "h264",
			Preset:           "p4",
			Width:            1920,
			Height:           1080,
			Bitrate:          5000,
			MaxBitrate:       7500,
			Framerate:        29.97,
			KeyframeInterval: 2,
			Profile:          "high",
			Level:            "4.1",
		}},
	}
	got := transcoderProfilesFromDomain(video)
	require.Equal(t, transcoder.Profile{
		Codec:            "h264",
		Preset:           "p4",
		Width:            1920,
		Height:           1080,
		Bitrate:          "5000k",
		MaxBitrate:       7500,
		Framerate:        29.97,
		KeyframeInterval: 2,
		CodecProfile:     "high",
		CodecLevel:       "4.1",
	}, got[0])
}

// New-field round-trip: Bframes/Refs/SAR/ResizeMode added in v0.0.6 must
// reach the transcoder layer unchanged so buildFFmpegArgs can emit them.
func TestTranscoderProfilesFromDomain_NewFieldsPropagated(t *testing.T) {
	t.Parallel()
	bf := 0
	refs := 3
	video := &domain.VideoTranscodeConfig{
		Profiles: []domain.VideoProfile{{
			Width: 1280, Height: 720, Bitrate: 2500,
			Bframes:    &bf,
			Refs:       &refs,
			SAR:        "1:1",
			ResizeMode: domain.ResizeModeCrop,
		}},
	}
	got := transcoderProfilesFromDomain(video)
	require.Len(t, got, 1)
	require.NotNil(t, got[0].Bframes)
	require.Equal(t, 0, *got[0].Bframes)
	require.NotNil(t, got[0].Refs)
	require.Equal(t, 3, *got[0].Refs)
	require.Equal(t, "1:1", got[0].SAR)
	require.Equal(t, "crop", got[0].ResizeMode)
}

// Pointer fields must propagate as nil (encoder default) when domain leaves them unset.
func TestTranscoderProfilesFromDomain_NilPointersStayNil(t *testing.T) {
	t.Parallel()
	video := &domain.VideoTranscodeConfig{
		Profiles: []domain.VideoProfile{{Width: 1280, Height: 720, Bitrate: 2500}},
	}
	got := transcoderProfilesFromDomain(video)
	require.Nil(t, got[0].Bframes, "unset Bframes must stay nil — encoder picks its own default")
	require.Nil(t, got[0].Refs, "unset Refs must stay nil")
	require.Empty(t, got[0].SAR)
	require.Empty(t, got[0].ResizeMode)
}

func TestTranscoderProfilesFromDomain_MultipleProfilesPreserveOrder(t *testing.T) {
	t.Parallel()
	video := &domain.VideoTranscodeConfig{
		Profiles: []domain.VideoProfile{
			{Width: 1920, Height: 1080, Bitrate: 4500},
			{Width: 1280, Height: 720, Bitrate: 2500},
			{Width: 854, Height: 480, Bitrate: 1200},
		},
	}
	got := transcoderProfilesFromDomain(video)
	require.Len(t, got, 3)
	require.Equal(t, 1920, got[0].Width)
	require.Equal(t, 1280, got[1].Width)
	require.Equal(t, 854, got[2].Width)
}

func TestSingleOriginCopyProfile_LeavesCodecEmptyForHWRouting(t *testing.T) {
	t.Parallel()
	got := singleOriginCopyProfile()
	require.Len(t, got, 1)
	require.Empty(t, got[0].Codec, "passthrough profile must let buildFFmpegArgs route on HW")
	require.Empty(t, got[0].Preset)
	require.Equal(t, "2500k", got[0].Bitrate)
}

// shouldRunTranscoder gates whether FFmpeg spawns at all. The full matrix:
//
//	transcoder | video.copy | audio.copy | spawn FFmpeg?
//	-----------+------------+------------+--------------
//	nil        |    n/a     |    n/a     | no
//	non-nil    |   true     |   true     | no  (passthrough — both copy)
//	non-nil    |   true     |   false    | yes (audio re-encode only)
//	non-nil    |   false    |   true     | yes (video re-encode only)
//	non-nil    |   false    |   false    | yes (both re-encode)
func TestShouldRunTranscoder(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		tc   *domain.TranscoderConfig
		want bool
	}{
		{
			name: "nil transcoder → no spawn",
			tc:   nil,
			want: false,
		},
		{
			name: "video copy + audio copy → no spawn (raw passthrough)",
			tc: &domain.TranscoderConfig{
				Video: domain.VideoTranscodeConfig{Copy: true},
				Audio: domain.AudioTranscodeConfig{Copy: true},
			},
			want: false,
		},
		{
			name: "video copy + audio re-encode → spawn",
			tc: &domain.TranscoderConfig{
				Video: domain.VideoTranscodeConfig{Copy: true},
				Audio: domain.AudioTranscodeConfig{Copy: false, Codec: domain.AudioCodecAAC},
			},
			want: true,
		},
		{
			name: "video re-encode + audio copy → spawn",
			tc: &domain.TranscoderConfig{
				Video: domain.VideoTranscodeConfig{Copy: false, Profiles: []domain.VideoProfile{{Width: 1280, Height: 720}}},
				Audio: domain.AudioTranscodeConfig{Copy: true},
			},
			want: true,
		},
		{
			name: "video re-encode + audio re-encode → spawn",
			tc: &domain.TranscoderConfig{
				Video: domain.VideoTranscodeConfig{Copy: false, Profiles: []domain.VideoProfile{{Width: 1920, Height: 1080}}},
				Audio: domain.AudioTranscodeConfig{Copy: false, Codec: domain.AudioCodecAAC},
			},
			want: true,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := shouldRunTranscoder(&domain.Stream{Transcoder: tt.tc})
			require.Equal(t, tt.want, got)
		})
	}
}

func TestShouldRunTranscoder_NilStream(t *testing.T) {
	t.Parallel()
	require.False(t, shouldRunTranscoder(nil))
}
