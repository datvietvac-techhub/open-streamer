package domain

import "time"

// RecordingID is the unique identifier for a DVR recording.
// Always equals the stream code — one recording per stream.
type RecordingID string

// RecordingStatus represents the lifecycle state of a recording.
type RecordingStatus string

// RecordingStatus values.
const (
	RecordingStatusRecording RecordingStatus = "recording"
	RecordingStatusStopped   RecordingStatus = "stopped"
	RecordingStatusFailed    RecordingStatus = "failed"
)

// DVRGap represents a period of signal loss or server downtime.
type DVRGap struct {
	From     time.Time     `json:"from" yaml:"from"`         // wall time gap started
	To       time.Time     `json:"to" yaml:"to"`             // wall time recording resumed
	Duration time.Duration `json:"duration" yaml:"duration"` // To - From
}

// DVRIndex is the on-disk metadata index for a stream's DVR recording.
// Written atomically to {SegmentDir}/index.json after every segment flush.
//
// Deliberately lightweight — no per-segment details.
// Per-segment timeline (wall time, duration, discontinuity) lives in playlist.m3u8
// via #EXT-X-PROGRAM-DATE-TIME and #EXTINF tags.
type DVRIndex struct {
	StreamCode    StreamCode `json:"stream_code" yaml:"stream_code"`
	StartedAt     time.Time  `json:"started_at" yaml:"started_at"`
	LastSegmentAt time.Time  `json:"last_segment_at,omitempty" yaml:"last_segment_at,omitempty"`

	SegmentCount   int   `json:"segment_count" yaml:"segment_count"`
	TotalSizeBytes int64 `json:"total_size_bytes" yaml:"total_size_bytes"`

	// Gaps is the list of known signal-loss / server-restart interruptions.
	Gaps []DVRGap `json:"gaps,omitempty" yaml:"gaps,omitempty"`
}

// Recording represents the lifecycle metadata for a DVR recording session.
// ID equals StreamCode — one persistent recording per stream.
// Segment data lives in DVRIndex (index.json on disk), not here.
type Recording struct {
	ID         RecordingID     `json:"id" yaml:"id"`
	StreamCode StreamCode      `json:"stream_code" yaml:"stream_code"`
	StartedAt  time.Time       `json:"started_at" yaml:"started_at"`
	StoppedAt  *time.Time      `json:"stopped_at,omitempty" yaml:"stopped_at,omitempty"`
	Status     RecordingStatus `json:"status" yaml:"status"`

	// SegmentDir is the absolute path to the directory holding TS files,
	// playlist.m3u8, and index.json.
	SegmentDir string `json:"segment_dir" yaml:"segment_dir"`
}

// StreamDVRConfig overrides the global DVR settings for a specific stream.
type StreamDVRConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled"`

	// RetentionSec is the retention window in seconds.
	// 0 = keep forever.
	RetentionSec int `json:"retention_sec" yaml:"retention_sec"`

	// SegmentDuration overrides the global segment length in seconds.
	// 0 = use default (4s).
	SegmentDuration int `json:"segment_duration" yaml:"segment_duration"`

	// StoragePath overrides the default DVR root directory for this stream.
	// "" = use "./dvr/{streamCode}".
	StoragePath string `json:"storage_path" yaml:"storage_path"`

	// MaxSizeGB caps total disk usage. Oldest segments pruned when exceeded.
	// 0 = no limit.
	MaxSizeGB float64 `json:"max_size_gb" yaml:"max_size_gb"`

	// Profiles selects which renditions the CMAF blob archive records:
	// "" or "best" = the best rendition only (default); "all" = every rendition
	// in the ABR ladder.
	Profiles string `json:"profiles" yaml:"profiles"`
}
