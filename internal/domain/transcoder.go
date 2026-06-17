package domain

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// HWAccel selects the hardware acceleration backend for encoding/decoding.
type HWAccel string

// HWAccel values map to FFmpeg hardware device options.
const (
	HWAccelNone         HWAccel = "none"         // CPU only (libx264, libx265)
	HWAccelNVENC        HWAccel = "nvenc"        // NVIDIA GPU (h264_nvenc, hevc_nvenc)
	HWAccelVAAPI        HWAccel = "vaapi"        // Intel/AMD GPU via VA-API (Linux)
	HWAccelVideoToolbox HWAccel = "videotoolbox" // Apple GPU (macOS)
	HWAccelQSV          HWAccel = "qsv"          // Intel Quick Sync Video
)

// VideoCodec identifies the video compression format.
type VideoCodec string

// VideoCodec values name supported output video codecs.
const (
	VideoCodecH264  VideoCodec = "h264" // AVC — widest device support
	VideoCodecH265  VideoCodec = "h265" // HEVC — ~50% smaller than H.264
	VideoCodecAV1   VideoCodec = "av1"  // royalty-free, best compression (high CPU)
	VideoCodecMPEG2 VideoCodec = "mp2v" // MPEG-2 Part 2 — DVB legacy contribution feeds, transmitter chains
	VideoCodecCopy  VideoCodec = "copy" // passthrough — no re-encode
)

// AudioCodec identifies the audio compression format.
type AudioCodec string

// AudioCodec values name supported output audio codecs.
const (
	AudioCodecAAC  AudioCodec = "aac"  // default for HLS/DASH
	AudioCodecMP2  AudioCodec = "mp2a" // MPEG-1/2 Audio Layer II — DVB broadcast contribution feeds
	AudioCodecMP3  AudioCodec = "mp3"  // legacy compatibility
	AudioCodecAC3  AudioCodec = "ac3"  // Dolby Digital — broadcast use
	AudioCodecEAC3 AudioCodec = "eac3" // Dolby Digital Plus — premium HD/UHD broadcast (5.1+)
	AudioCodecCopy AudioCodec = "copy" // passthrough — no re-encode
)

// ResizeMode controls how the source frame is fitted into the output dimensions.
type ResizeMode string

// ResizeMode values match the common operator-tooling resize modes. "" defaults to ResizeModePad.
const (
	ResizeModePad     ResizeMode = "pad"     // letterbox: keep aspect, fill remainder with black
	ResizeModeCrop    ResizeMode = "crop"    // fill: keep aspect, crop excess
	ResizeModeStretch ResizeMode = "stretch" // distort: scale to W:H, ignore source aspect
	ResizeModeFit     ResizeMode = "fit"     // keep aspect, no padding (output may be smaller than W:H)
)

// InterlaceMode selects deinterlacing behavior for the source.
type InterlaceMode string

// InterlaceMode values map to yadif/yadif_cuda parameters. "" disables the filter.
const (
	InterlaceAuto        InterlaceMode = "auto"        // detect parity each frame
	InterlaceTopField    InterlaceMode = "tff"         // top field first (BBC HD, most ATSC)
	InterlaceBottomField InterlaceMode = "bff"         // bottom field first (legacy DV)
	InterlaceProgressive InterlaceMode = "progressive" // assert source is progressive — skip filter
)

// VideoProfile is a single rendition in the ABR (Adaptive Bitrate) ladder.
// The Transcoder produces one FFmpeg output per profile.
// Stable rendition ids are derived from slice order: track_1, track_2, track_3, … (1-based).
type VideoProfile struct {
	// Width and Height define the output resolution.
	// Set to 0 to keep the source dimensions (Width=0 & Height=0 = no scaling).
	Width  int `json:"width" yaml:"width"`
	Height int `json:"height" yaml:"height"`

	// Bitrate is the target video bitrate in kbps. 0 = encoder auto.
	Bitrate int `json:"bitrate" yaml:"bitrate"`

	// MaxBitrate caps the peak bitrate in kbps (CBR/VBR ceiling). 0 = no cap.
	MaxBitrate int `json:"max_bitrate" yaml:"max_bitrate"`

	// Framerate is the output frame rate (fps). 0 = match source.
	Framerate float64 `json:"framerate" yaml:"framerate"`

	// KeyframeInterval is the GOP size in seconds.
	// Must match or be a multiple of the HLS/DASH segment duration.
	KeyframeInterval int `json:"keyframe_interval" yaml:"keyframe_interval"`

	Codec VideoCodec `json:"codec" yaml:"codec"`

	// Preset controls the encoder speed/quality tradeoff.
	// libx264: "ultrafast" | "superfast" | "veryfast" | "faster" | "fast" | "medium" | "slow" | "veryslow"
	// NVENC:   "p1" (fastest) .. "p7" (highest quality)
	Preset string `json:"preset" yaml:"preset"`

	// Profile controls the H.264/H.265 encoding profile.
	// "baseline" | "main" | "high" (H.264); "main" | "main10" (H.265)
	Profile string `json:"profile" yaml:"profile"`

	// Level controls the H.264/H.265 encoding level.
	// Common: "3.1", "4.0", "4.1", "4.2", "5.0", "5.1"
	Level string `json:"level" yaml:"level"`

	// Bframes is the number of consecutive B-frames the encoder may emit.
	// nil = encoder default; 0 = explicit none (low-latency live);
	// 2-3 = typical VOD; NVENC HW B-ref pyramid is auto-enabled when >0.
	Bframes *int `json:"bframes,omitempty" yaml:"bframes,omitempty"`

	// Refs is the number of reference frames. nil = encoder default.
	// Higher = better compression at cost of CPU/latency. NVENC has its own caps.
	Refs *int `json:"refs,omitempty" yaml:"refs,omitempty"`

	// SAR is the output Sample Aspect Ratio, "N:M". "" = inherit from source.
	// Use "1:1" for square pixels (web); "16:11", "59:54" etc. for anamorphic.
	SAR string `json:"sar,omitempty" yaml:"sar,omitempty"`

	// ResizeMode chooses how the source is fitted to Width/Height.
	// "" defaults to ResizeModePad.
	ResizeMode ResizeMode `json:"resize_mode,omitempty" yaml:"resize_mode,omitempty"`
}

// AudioConfig defines the audio encoding settings applied to all output profiles.
type AudioConfig struct {
	Codec AudioCodec `json:"codec" yaml:"codec"`

	// Bitrate is the audio bitrate in kbps. Typical: 128 (stereo), 192 (high quality).
	Bitrate int `json:"bitrate" yaml:"bitrate"`

	// SampleRate is the output sample rate in Hz. Typical: 44100, 48000.
	SampleRate int `json:"sample_rate" yaml:"sample_rate"`

	// Channels: 1 = mono, 2 = stereo, 6 = 5.1 surround.
	Channels int `json:"channels" yaml:"channels"`

	// Language is the ISO 639-1 code embedded in HLS/DASH metadata, e.g. "en", "vi".
	Language string `json:"language" yaml:"language"`

	// Normalize applies EBU R128 loudness normalization (-23 LUFS).
	// Useful for broadcast compliance.
	Normalize bool `json:"normalize" yaml:"normalize"`
}

// ParseAudioVolume converts an AudioConfig.Volume spec into a linear gain
// factor applied per audio sample (output = factor × input). It accepts the
// same two forms as the playout volume control:
//
//   - a decibel string with a "dB" suffix ("+9dB", "-6dB", "0dB") →
//     factor = 10^(dB/20); and
//   - a plain linear multiplier ("2", "0.5", "1") → factor = the value.
//
// Empty (or whitespace) returns 1.0 (unity, no change). A linear value must be
// >= 0; a negative linear multiplier is rejected (it would invert phase rather
// than attenuate — use a dB value or a fraction to make audio quieter).
func ParseAudioVolume(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 1, nil
	}
	if l := strings.ToLower(s); strings.HasSuffix(l, "db") {
		db, err := strconv.ParseFloat(strings.TrimSpace(s[:len(s)-2]), 64)
		if err != nil {
			return 0, fmt.Errorf("audio volume: invalid dB value %q: %w", s, err)
		}
		return math.Pow(10, db/20), nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("audio volume: invalid value %q: %w", s, err)
	}
	if f < 0 {
		return 0, fmt.Errorf("audio volume: linear multiplier must be >= 0, got %q", s)
	}
	return f, nil
}

// DecoderConfig defines decoder behavior.
type DecoderConfig struct {
	// Name is the FFmpeg decoder name.
	// "" = let FFmpeg choose automatically.
	// Examples: "h264_cuvid", "h264_qsv".
	Name string `json:"name,omitempty" yaml:"name,omitempty"`
}

// TranscoderGlobalConfig holds global transcoder parameters.
type TranscoderGlobalConfig struct {
	// HW selects the acceleration backend.
	HW HWAccel `json:"hw" yaml:"hw"`

	// FPS sets output framerate. 0 = source/default.
	FPS int `json:"fps" yaml:"fps"`

	// GOP sets keyframe interval in frames. 0 = encoder default.
	GOP int `json:"gop" yaml:"gop"`

	// DeviceID selects hardware device index.
	DeviceID int `json:"deviceid" yaml:"deviceid"`
}

// VideoTranscodeConfig defines video transcoding behavior.
type VideoTranscodeConfig struct {
	// Copy copies origin video without re-encoding.
	Copy bool `json:"copy" yaml:"copy"`

	// Interlace selects the deinterlace pre-filter applied before scaling.
	// Applies in the transcoder's video pipeline, before scaling.
	// "" disables the filter; use ResizeModeProgressive to assert progressive source.
	Interlace InterlaceMode `json:"interlace,omitempty" yaml:"interlace,omitempty"`

	// Profiles defines ABR renditions when re-encoding.
	Profiles []VideoProfile `json:"profiles,omitempty" yaml:"profiles,omitempty"`
}

// AudioTranscodeConfig defines audio transcoding behavior.
type AudioTranscodeConfig struct {
	// Copy copies origin audio without re-encoding.
	Copy bool `json:"copy" yaml:"copy"`

	Codec AudioCodec `json:"codec" yaml:"codec"`

	// Bitrate is the audio bitrate in kbps.
	Bitrate int `json:"bitrate" yaml:"bitrate"`

	// SampleRate is output sample rate in Hz.
	SampleRate int `json:"sample_rate" yaml:"sample_rate"`

	// Channels: 1 = mono, 2 = stereo, 6 = 5.1.
	Channels int `json:"channels" yaml:"channels"`

	// Language is ISO 639-1 code, e.g. "en", "vi".
	Language string `json:"language" yaml:"language"`

	// Normalize applies EBU R128 loudness normalization.
	Normalize bool `json:"normalize" yaml:"normalize"`

	// Volume sets the output audio gain on the re-encode path
	// (Copy=false; passthrough audio cannot be gained). The value is either a
	// plain linear multiplier ("2", "0.5" → output = value × input) or a
	// decibel string ("+9dB", "-6dB" → output level = input ± dB). Both reduce
	// to one linear factor via ParseAudioVolume. Empty / "1" / "0dB" = unity
	// (no change). Boosting can clip; samples are clamped to full scale.
	Volume string `json:"volume,omitempty" yaml:"volume,omitempty"`
}

// TranscoderConfig is the complete transcoding configuration for a stream.
//
// The Mode + ExtraArgs fields were removed in the native-transcoder
// migration (see internal/transcoder/native). The native pipeline has a
// single process topology — decoder ephemeral per input, encoder long-
// lived per stream — so there is nothing for Mode to select. ExtraArgs
// was FFmpeg-CLI-specific and has no equivalent under in-process libav.
type TranscoderConfig struct {
	Video   VideoTranscodeConfig   `json:"video" yaml:"video"`
	Audio   AudioTranscodeConfig   `json:"audio" yaml:"audio"`
	Decoder DecoderConfig          `json:"decoder" yaml:"decoder"`
	Global  TranscoderGlobalConfig `json:"global" yaml:"global"`

	// Watermark is a runtime-only field populated by the coordinator from
	// Stream.Watermark before each transcoder.Start. Not persisted on the
	// transcoder section (json/yaml "-") so the API surface keeps
	// Stream.Watermark as the single source of truth.
	Watermark *WatermarkConfig `json:"-" yaml:"-"`
}
