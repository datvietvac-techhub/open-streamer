// Package apidocs holds OpenAPI response/request shapes referenced from swag comments.
package apidocs

import (
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/manager"
	"github.com/datvietvac-techhub/open-streamer/internal/vod"
	"github.com/datvietvac-techhub/open-streamer/pkg/version"
)

// ErrorBody matches handler writeError JSON shape.
type ErrorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// StreamResponse is the API representation of a stream returned by GET /streams
// and GET /streams/{code}. The persisted configuration is embedded at the top
// level; everything runtime-computed lives under a single `runtime` envelope:
//
//   - runtime.status            — coordinator-resolved lifecycle state
//   - runtime.pipeline_active   — true when the manager has registered a pipeline
//   - runtime.inputs[]          — per-input health + last 5 degradation errors
//   - runtime.transcoder        — transcoder subprocess state: status, restart count, last 5 crash errors, rendition list
//
// `runtime` is always present (never null), even when the pipeline is not
// running — clients can rely on it as a single root for all live data.
type StreamResponse struct {
	domain.Stream
	Runtime manager.RuntimeStatus `json:"runtime"`
}

// StreamData wraps a single stream response.
type StreamData struct {
	Data StreamResponse `json:"data"`
}

// StreamPutRequest is request body for PUT /streams/{code}.
// code is taken from path param.
type StreamPutRequest struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`

	StreamKey string `json:"stream_key"`

	Disabled bool `json:"disabled"`

	Inputs []domain.Input `json:"inputs"`

	Transcoder *domain.TranscoderConfig `json:"transcoder,omitempty"`
	Protocols  domain.OutputProtocols   `json:"protocols"`
	Push       []domain.PushDestination `json:"push"`
	DVR        *domain.StreamDVRConfig  `json:"dvr,omitempty"`
	Watermark  *domain.WatermarkConfig  `json:"watermark,omitempty"`
	Thumbnail  *domain.ThumbnailConfig  `json:"thumbnail,omitempty"`
}

// StreamList wraps listed streams.
type StreamList struct {
	Data  []StreamResponse `json:"data"`
	Total int              `json:"total"`
}

// InputSwitchRequest is the request body for POST /streams/{code}/switch.
type InputSwitchRequest struct {
	Priority int `json:"priority"`
}

// ConfigData wraps the GET /config response.
type ConfigData struct {
	Version            version.Info               `json:"version"`
	HWAccels           []domain.HWAccel           `json:"hw_accels"`
	VideoCodecs        []domain.VideoCodec        `json:"video_codecs"`
	AudioCodecs        []domain.AudioCodec        `json:"audio_codecs"`
	OutputProtocols    []string                   `json:"output_protocols"`
	StreamStatuses     []domain.StreamStatus      `json:"stream_statuses"`
	WatermarkTypes     []domain.WatermarkType     `json:"watermark_types"`
	WatermarkPositions []domain.WatermarkPosition `json:"watermark_positions"`
	Ports              ConfigPorts                `json:"ports"`
	GlobalConfig       *domain.GlobalConfig       `json:"global_config"`
}

// ConfigUpdateResponse wraps the POST /config response.
type ConfigUpdateResponse struct {
	GlobalConfig *domain.GlobalConfig `json:"global_config"`
	Ports        ConfigPorts          `json:"ports"`
}

// ConfigPorts exposes listener port configuration so the UI can build output URLs.
type ConfigPorts struct {
	HTTPAddr string `json:"http_addr"`
	RTSPPort int    `json:"rtsp_port"`
	RTMPPort int    `json:"rtmp_port"`
	SRTPort  int    `json:"srt_port"`
}

// ErrorResponse wraps an error body for swagger documentation.
type ErrorResponse = ErrorBody

// StreamActionData is returned by POST .../restart.
type StreamActionData struct {
	Data StreamActionInner `json:"data"`
}

// StreamActionInner holds status string for start/stop responses.
type StreamActionInner struct {
	Status string `json:"status"`
}

// InputData wraps inputs array.
type InputData struct {
	Data []domain.Input `json:"data"`
}

// RecordingData wraps a recording.
type RecordingData struct {
	Data *domain.Recording `json:"data"`
}

// RecordingList wraps recordings for a stream.
type RecordingList struct {
	Data  []*domain.Recording `json:"data"`
	Total int                 `json:"total"`
}

// HookData wraps a hook.
type HookData struct {
	Data *domain.Hook `json:"data"`
}

// HookList wraps hooks.
type HookList struct {
	Data  []*domain.Hook `json:"data"`
	Total int            `json:"total"`
}

// WatermarkAssetList wraps the watermark library list response.
type WatermarkAssetList struct {
	Data  []*domain.WatermarkAsset `json:"data"`
	Total int                      `json:"total"`
	Dir   string                   `json:"dir"`
}

// WatermarkAssetData wraps a single watermark asset response.
type WatermarkAssetData struct {
	Data *domain.WatermarkAsset `json:"data"`
}

// SessionList wraps the play-sessions list response.
// Mirrors the JSON shape emitted by handler.SessionHandler.List /
// ListByStream so swag can resolve {object} apidocs.SessionList.
type SessionList struct {
	Sessions   []*domain.PlaySession `json:"sessions"`
	TotalCount int                   `json:"total_count"`
	Stats      SessionStats          `json:"stats"`
}

// SessionStats mirrors sessions.Stats — duplicated here so apidocs stays
// dependency-free of the sessions package (avoids any future cycle).
type SessionStats struct {
	Active          int   `json:"active"`
	OpenedTotal     int64 `json:"opened_total"`
	ClosedTotal     int64 `json:"closed_total"`
	IdleClosedTotal int64 `json:"idle_closed_total"`
	KickedTotal     int64 `json:"kicked_total"`
}

// HookTestInner is the payload for hook test success.
type HookTestInner struct {
	Status string `json:"status"`
	At     string `json:"at"`
}

// HookTestData is returned by POST .../test.
type HookTestData struct {
	Data HookTestInner `json:"data"`
}

// VODMountData wraps a single VOD mount.
type VODMountData struct {
	Data *domain.VODMount `json:"data"`
}

// VODMountList wraps listed VOD mounts.
type VODMountList struct {
	Data  []*domain.VODMount `json:"data"`
	Total int                `json:"total"`
}

// VODFileList wraps a directory listing inside a VOD mount.
type VODFileList struct {
	Data  []vod.FileEntry `json:"data"`
	Total int             `json:"total"`
	Path  string          `json:"path"`
}

// VODFileData wraps a single uploaded VOD file.
type VODFileData struct {
	Data vod.FileEntry `json:"data"`
}
