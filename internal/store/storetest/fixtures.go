// Package storetest provides shared test fixtures for all store implementations.
package storetest

import (
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// NewFullStream returns a Stream with every field populated.
func NewFullStream(code domain.StreamCode) *domain.Stream {
	return &domain.Stream{
		Code:        code,
		Name:        "Test Stream",
		Description: "A test stream with all fields set",
		Tags:        []string{"live", "test", "hd"},
		StreamKey:   "secret-key-123",
		Disabled:    false,
		Inputs: []domain.Input{
			{
				URL:      "rtmp://source.example.com/live/key1",
				Priority: 0,
				Headers:  map[string]string{"Authorization": "Bearer tok1"},
				Params:   map[string]string{"passphrase": "abc"},
				Net: domain.InputNetConfig{
					TimeoutSec: 10,
				},
			},
			{
				URL:      "rtmp://backup.example.com/live/key2",
				Priority: 1,
				Net:      domain.InputNetConfig{},
			},
		},
		Transcoder: &domain.TranscoderConfig{
			Video: domain.VideoTranscodeConfig{
				Copy: false,
				Profiles: []domain.VideoProfile{
					{
						Width:            1920,
						Height:           1080,
						Bitrate:          4000,
						MaxBitrate:       5000,
						Framerate:        30,
						KeyframeInterval: 2,
						Codec:            domain.VideoCodecH264,
						Preset:           "veryfast",
						Profile:          "high",
						Level:            "4.1",
					},
					{
						Width:            1280,
						Height:           720,
						Bitrate:          2000,
						MaxBitrate:       2500,
						Framerate:        30,
						KeyframeInterval: 2,
						Codec:            domain.VideoCodecH264,
						Preset:           "veryfast",
						Profile:          "main",
						Level:            "4.0",
					},
				},
			},
			Audio: domain.AudioTranscodeConfig{
				Copy:       false,
				Codec:      domain.AudioCodecAAC,
				Bitrate:    128,
				SampleRate: 48000,
				Channels:   2,
				Language:   "en",
				Normalize:  true,
			},
			Decoder: domain.DecoderConfig{Name: "h264_cuvid"},
			Global: domain.TranscoderGlobalConfig{
				HW:       domain.HWAccelNVENC,
				FPS:      30,
				GOP:      60,
				DeviceID: 0,
			},
		},
		Protocols: &domain.OutputProtocols{
			HLS:  true,
			DASH: true,
			RTSP: false,
			RTMP: true,
			SRT:  false,
		},
		Push: []domain.PushDestination{
			{
				URL:             "rtmp://rtmp.example.com/live2/live-key",
				Enabled:         true,
				TimeoutSec:      10,
				RetryTimeoutSec: 5,
				Limit:           3,
				Comment:         "Live stream",
			},
		},
		DVR: &domain.StreamDVRConfig{
			Enabled:         true,
			RetentionSec:    86400,
			SegmentDuration: 4,
			StoragePath:     "/data/dvr/test",
			MaxSizeGB:       10,
		},
	}
}

// NewFullRecording returns a Recording with every field populated, including StoppedAt.
func NewFullRecording(id domain.RecordingID, code domain.StreamCode) *domain.Recording {
	stoppedAt := time.Date(2026, 1, 2, 6, 0, 0, 0, time.UTC)
	return &domain.Recording{
		ID:         id,
		StreamCode: code,
		StartedAt:  time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		StoppedAt:  &stoppedAt,
		Status:     domain.RecordingStatusRecording,
		SegmentDir: "/data/dvr/test/segments",
	}
}

// NewFullTemplate returns a Template with every config-like field populated.
// Useful for round-trip tests that need to verify nothing is silently
// dropped by serialisation.
func NewFullTemplate(code domain.TemplateCode) *domain.Template {
	return &domain.Template{
		Code:        code,
		Name:        "Profile A",
		Description: "Full template fixture",
		Transcoder: &domain.TranscoderConfig{
			Video: domain.VideoTranscodeConfig{
				Profiles: []domain.VideoProfile{{
					Width:   1280,
					Height:  720,
					Bitrate: 2000,
					Codec:   domain.VideoCodecH264,
				}},
			},
		},
		Protocols: &domain.OutputProtocols{HLS: true, DASH: true},
		Push: []domain.PushDestination{{
			URL:     "rtmp://target.example.com/live/key",
			Enabled: true,
		}},
		DVR: &domain.StreamDVRConfig{Enabled: true, RetentionSec: 3600},
	}
}

// NewFullHook returns a Hook with every field populated.
func NewFullHook(id domain.HookID) *domain.Hook {
	return &domain.Hook{
		ID:         id,
		Name:       "My Webhook",
		Type:       domain.HookTypeHTTP,
		Target:     "https://hooks.example.com/events",
		Secret:     "hmac-secret-xyz",
		EventTypes: []domain.EventType{"stream.started", "stream.stopped", "recording.started"},
		Enabled:    true,
		MaxRetries: 5,
		TimeoutSec: 15,
	}
}
