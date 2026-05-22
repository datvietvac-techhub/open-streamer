package handler

import (
	"net/http"

	"github.com/ntt0601zcoder/open-streamer/internal/domain"
)

// configDefaultsResponse mirrors the user-configurable surface of
// GlobalConfig + Stream-level config and reports the implicit value the
// server applies when the field is left zero / empty.
//
// UI fetches this once on app init and uses each value as form field
// placeholder text — so users see "1024" instead of literal "default".
//
// Shape choice: NESTED to match GlobalConfig / Stream JSON paths exactly,
// so frontend can do `defaults.transcoder.audio.bitrate_k` without
// remapping. Field names are snake_case to match the existing config
// JSON tags.
//
// Fields intentionally NOT included:
//   - Encoder-internal defaults (preset, profile, level, gop, framerate)
//     where empty → no flag → encoder picks. Frontend should render
//     "encoder default" placeholder for these.
//   - Source pass-through fields (audio.channels, audio.sample_rate, fps)
//     where 0 = match source. Frontend renders "match source".
//   - Sentinel zeros (dvr.retention_sec=forever, push.limit=unlimited)
//     where the zero itself carries semantic meaning.
//   - Required fields (publisher.{hls,dash}.dir, hooks.worker_count) that
//     have no default — server rejects empty.
//   - Dead config fields (input.net.read_timeout_sec, reconnect_*) that
//     are declared in domain but not consumed by any service.
type configDefaultsResponse struct {
	Buffer struct {
		Capacity int `json:"capacity"`
	} `json:"buffer"`

	Manager struct {
		InputPacketTimeoutSec int `json:"input_packet_timeout_sec"`
	} `json:"manager"`

	Publisher struct {
		HLS  liveSegmentDefaults `json:"hls"`
		DASH liveSegmentDefaults `json:"dash"`
	} `json:"publisher"`

	Hook struct {
		MaxRetries int `json:"max_retries"`
		TimeoutSec int `json:"timeout_sec"`
		// Batch* defaults apply to HTTP hooks only — file hooks always
		// write one event per line. The dispatcher's mergeBatchConfig
		// uses the same Default* constants, so the API surface and the
		// runtime fallback can never drift.
		BatchMaxItems         int `json:"batch_max_items"`
		BatchFlushIntervalSec int `json:"batch_flush_interval_sec"`
		BatchMaxQueueItems    int `json:"batch_max_queue_items"`
	} `json:"hook"`

	Push struct {
		TimeoutSec      int `json:"timeout_sec"`
		RetryTimeoutSec int `json:"retry_timeout_sec"`
	} `json:"push"`

	DVR struct {
		// SegmentDuration is in seconds.
		SegmentDuration int `json:"segment_duration"`
		// StoragePathTemplate uses {streamCode} as the placeholder for
		// the stream code — frontend must substitute it client-side.
		StoragePathTemplate string `json:"storage_path_template"`
	} `json:"dvr"`

	Transcoder struct {
		FFmpegPath string `json:"ffmpeg_path"`
		Video      struct {
			BitrateK   int               `json:"bitrate_k"`
			ResizeMode domain.ResizeMode `json:"resize_mode"`
			// Codec is the codec family used when the user leaves
			// VideoProfile.Codec empty. EncoderByCodecHW resolves it (or
			// any explicit codec) to the actual FFmpeg encoder name based
			// on the stream's Global.HW selection — frontend looks up
			// `EncoderByCodecHW[codec][hw]` to render the placeholder.
			Codec            string                       `json:"codec"`
			EncoderByCodecHW map[string]map[string]string `json:"encoder_by_codec_hw"`
		} `json:"video"`
		Audio struct {
			Codec    domain.AudioCodec `json:"codec"`
			BitrateK int               `json:"bitrate_k"`
		} `json:"audio"`
		Global struct {
			HW       domain.HWAccel `json:"hw"`
			DeviceID int            `json:"deviceid"`
		} `json:"global"`
	} `json:"transcoder"`

	Listeners struct {
		RTMP listenerDefaults     `json:"rtmp"`
		RTSP rtspListenerDefaults `json:"rtsp"`
		SRT  srtListenerDefaults  `json:"srt"`
	} `json:"listeners"`

	Ingestor struct {
		HLSPlaylistTimeoutSec int `json:"hls_playlist_timeout_sec"`
		HLSSegmentTimeoutSec  int `json:"hls_segment_timeout_sec"`
		HLSMaxSegmentBuffer   int `json:"hls_max_segment_buffer"`
		// Per-protocol pull timeouts the server fills when a stream's
		// `input.net.timeout_sec` is left zero. UI uses these as the
		// placeholder value on the per-input Timeout (s) field, picking
		// the right one based on the input URL scheme.
		RTMPTimeoutSec int `json:"rtmp_timeout_sec"`
		RTSPTimeoutSec int `json:"rtsp_timeout_sec"`
	} `json:"ingestor"`
}

type liveSegmentDefaults struct {
	LiveSegmentSec int  `json:"live_segment_sec"`
	LiveWindow     int  `json:"live_window"`
	LiveHistory    int  `json:"live_history"`
	LiveEphemeral  bool `json:"live_ephemeral"`
}

type listenerDefaults struct {
	Port       int    `json:"port"`
	ListenHost string `json:"listen_host"`
}

type rtspListenerDefaults struct {
	Port       int    `json:"port"`
	ListenHost string `json:"listen_host"`
	Transport  string `json:"transport"`
}

type srtListenerDefaults struct {
	Port       int    `json:"port"`
	ListenHost string `json:"listen_host"`
	LatencyMS  int    `json:"latency_ms"`
}

// supportedHWAccels lists the hwaccel values the codec routing table is
// built for. Order is fixed so the JSON output is deterministic across
// runs (Go map iteration is random — without this the response would
// flap and break frontend caching).
var supportedHWAccels = []domain.HWAccel{
	domain.HWAccelNone,
	domain.HWAccelNVENC,
	domain.HWAccelVAAPI,
	domain.HWAccelQSV,
	domain.HWAccelVideoToolbox,
}

// supportedCodecFamilies is the input axis of the routing table. Each
// family maps to a fully-resolved encoder name per HW backend.
var supportedCodecFamilies = []domain.VideoCodec{
	domain.VideoCodecH264,
	domain.VideoCodecH265,
	domain.VideoCodecAV1,
	domain.VideoCodecMPEG2,
}

// buildEncoderByCodecHW computes the codec×HW → FFmpeg encoder name
// mapping using domain.ResolveVideoEncoder as the single source of truth.
// Frontend reads the result to render the codec placeholder per stream:
//
//	encoder := defaults.video.encoder_by_codec_hw[codec || default_codec][hw]
func buildEncoderByCodecHW() map[string]map[string]string {
	out := make(map[string]map[string]string, len(supportedCodecFamilies))
	for _, codec := range supportedCodecFamilies {
		row := make(map[string]string, len(supportedHWAccels))
		for _, hw := range supportedHWAccels {
			row[string(hw)] = domain.ResolveVideoEncoder(codec, hw)
		}
		out[string(codec)] = row
	}
	return out
}

// buildConfigDefaults constructs the response from the constants in
// internal/domain/defaults.go. Pure function — no state, no IO — so the
// handler is trivially testable and the response is deterministic.
//
// Listener ports + RTSP transport + HW + audio codec + resize mode are
// reported here even though no service consumes constants for them
// directly (they are well-known protocol defaults). Operators / UI need
// these too; defining them inline rather than in defaults.go keeps the
// domain package free of "API-only" constants.
func buildConfigDefaults() configDefaultsResponse {
	const (
		// Well-known protocol ports — IANA assignments / convention.
		rtmpPort      = 1935
		rtspPort      = 554
		srtPort       = 9999
		rtspTransport = "tcp"
	)

	var resp configDefaultsResponse

	resp.Buffer.Capacity = domain.DefaultBufferCapacity
	resp.Manager.InputPacketTimeoutSec = domain.DefaultInputPacketTimeoutSec

	resp.Publisher.HLS = liveSegmentDefaults{
		LiveSegmentSec: domain.DefaultLiveSegmentSec,
		LiveWindow:     domain.DefaultLiveWindow,
		LiveHistory:    domain.DefaultLiveHistory,
		LiveEphemeral:  false,
	}
	resp.Publisher.DASH = liveSegmentDefaults{
		LiveSegmentSec: domain.DefaultLiveSegmentSec,
		LiveWindow:     domain.DefaultLiveWindow,
		LiveHistory:    domain.DefaultLiveHistory,
		LiveEphemeral:  false,
	}

	resp.Hook.MaxRetries = domain.DefaultHookMaxRetries
	resp.Hook.TimeoutSec = domain.DefaultHookTimeoutSec
	resp.Hook.BatchMaxItems = domain.DefaultHookBatchMaxItems
	resp.Hook.BatchFlushIntervalSec = domain.DefaultHookBatchFlushIntervalSec
	resp.Hook.BatchMaxQueueItems = domain.DefaultHookBatchMaxQueueItems

	resp.Push.TimeoutSec = domain.DefaultPushTimeoutSec
	resp.Push.RetryTimeoutSec = domain.DefaultPushRetryTimeoutSec

	resp.DVR.SegmentDuration = domain.DefaultDVRSegmentDuration
	resp.DVR.StoragePathTemplate = domain.DefaultDVRRoot + "/{streamCode}"

	resp.Transcoder.FFmpegPath = domain.DefaultFFmpegPath
	resp.Transcoder.Video.BitrateK = domain.DefaultVideoBitrateK
	resp.Transcoder.Video.ResizeMode = domain.ResizeModePad
	resp.Transcoder.Video.Codec = string(domain.VideoCodecH264)
	resp.Transcoder.Video.EncoderByCodecHW = buildEncoderByCodecHW()
	resp.Transcoder.Audio.Codec = domain.AudioCodecAAC
	resp.Transcoder.Audio.BitrateK = domain.DefaultAudioBitrateK
	resp.Transcoder.Global.HW = domain.HWAccelNone
	resp.Transcoder.Global.DeviceID = 0

	resp.Listeners.RTMP = listenerDefaults{
		Port:       rtmpPort,
		ListenHost: domain.DefaultListenHost,
	}
	resp.Listeners.RTSP = rtspListenerDefaults{
		Port:       rtspPort,
		ListenHost: domain.DefaultListenHost,
		Transport:  rtspTransport,
	}
	resp.Listeners.SRT = srtListenerDefaults{
		Port:       srtPort,
		ListenHost: domain.DefaultListenHost,
		LatencyMS:  domain.DefaultSRTLatencyMS,
	}

	resp.Ingestor.HLSPlaylistTimeoutSec = domain.DefaultHLSPlaylistTimeoutSec
	resp.Ingestor.HLSSegmentTimeoutSec = domain.DefaultHLSSegmentTimeoutSec
	resp.Ingestor.HLSMaxSegmentBuffer = domain.DefaultHLSMaxSegmentBuffer
	resp.Ingestor.RTMPTimeoutSec = domain.DefaultRTMPTimeoutSec
	resp.Ingestor.RTSPTimeoutSec = domain.DefaultRTSPTimeoutSec

	return resp
}

// GetConfigDefaults returns the implicit values the server applies when
// the corresponding configuration field is left zero / empty by the user.
//
// Frontend usage: fetch once on app init, cache, use as form field
// placeholder text so users see the actual default value (e.g. "1024",
// "aac", "h264") instead of a generic "default" word.
//
// Note: a few fields with context-aware resolution (video codec name
// depending on HW backend) are NOT included here — they need the caller's
// current HW selection to compute. Frontend handles those client-side.
//
// @Summary     Get system configuration defaults.
// @Description Static values the server fills in for unset configuration fields. Use as form placeholders so users see real defaults instead of "default" text.
// @Tags        system
// @Produce     json
// @Success     200 {object} handler.configDefaultsResponse
// @Router      /config/defaults [get].
func (h *ConfigHandler) GetConfigDefaults(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, buildConfigDefaults())
}
