package native

import (
	"context"

	"github.com/ntt0601zcoder/open-streamer/internal/domain"
)

// Config is the per-stream pipeline configuration handed from the parent
// transcoder.Service to the native runner. Fields mirror the relevant
// subset of domain.TranscoderConfig (no FFmpeg-CLI plumbing).
type Config struct {
	StreamCode  domain.StreamCode
	RawIngestID domain.StreamCode
	Targets     []Target
	HWBackend   HWBackend
	Watermark   *domain.WatermarkConfig
	Audio       AudioConfig
}

// HWBackend names the encode acceleration the pipeline should use. Probe
// the host (P1) and fall back to CPU when the chosen backend is missing.
type HWBackend string

// HWBackend values.
const (
	HWBackendCPU          HWBackend = "cpu"          // libx264 / libx265
	HWBackendNVENC        HWBackend = "nvenc"        // NVIDIA
	HWBackendVAAPI        HWBackend = "vaapi"        // Intel / AMD on Linux
	HWBackendVideoToolbox HWBackend = "videotoolbox" // macOS
	HWBackendQSV          HWBackend = "qsv"          // Intel Quick Sync
)

// Target describes one rendition the encoder must emit.
type Target struct {
	BufferID       domain.StreamCode
	Index          int
	Width, Height  int
	BitrateKbps    int
	MaxBitrateKbps int
	Framerate      float64
	GOPSeconds     int
	Codec          string
	Preset         string
	Profile        string
	Level          string
	Bframes        *int
	Refs           *int
	SAR            string
	ResizeMode     string
}

// AudioConfig is the audio side of the pipeline.
type AudioConfig struct {
	Copy       bool
	Codec      string
	BitrateK   int
	SampleRate int
	Channels   int
	Normalize  bool
}

// Runner is the public entry point for a per-stream encoder pipeline.
// One Runner per stream code, owned by the open-streamer-transcoder
// subprocess. Methods are NOT safe to call concurrently — the subprocess
// gRPC server serialises them onto the runner's loop.
type Runner interface {
	// Run blocks until ctx is cancelled or the encoder dies fatally.
	// Emits encoded MPEG-TS chunks via the supplied sink for each
	// configured target (keyed by Target.Index).
	Run(ctx context.Context, ingestPackets <-chan []byte, sink Sink) error

	// SwitchInput tears down the active decoder + scaler and rebuilds
	// them against the new input source. Encoder is unaffected — that's
	// the entire point of the split.
	SwitchInput(newRawIngestID domain.StreamCode) error
}

// Sink receives encoded packets from the Runner. Implementations push
// them back over the gRPC stream to the parent process which writes them
// into the buffer hub for each rendition.
type Sink interface {
	Write(targetIndex int, ts []byte) error
}

// NewRunner constructs a Runner for the given config. P0 stub returns
// ErrNotImplemented; P1 will wire the encoder, P2 the decoder + scaler.
func NewRunner(_ Config) (Runner, error) {
	return nil, ErrNotImplemented
}
