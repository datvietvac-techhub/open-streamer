package domain

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// MaxStreamCodeLen is the maximum length of a user-defined stream code.
const MaxStreamCodeLen = 128

// streamCodePattern allows alphanumerics, dash, underscore, and slash.
// Slashes let operators namespace streams as `region/channel` for routing
// hierarchies; the route layer treats the whole prefix as one opaque key.
var streamCodePattern = regexp.MustCompile(`^[A-Za-z0-9_/-]+$`)

// StreamCode is the user-assigned primary key for a stream.
// Allowed characters: a-z, A-Z, 0-9, underscore, dash, slash.
type StreamCode string

// ValidateStreamCode reports whether s is a non-empty valid stream code.
func ValidateStreamCode(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return errors.New("stream code is required")
	}
	if len(s) > MaxStreamCodeLen {
		return fmt.Errorf("stream code exceeds max length %d", MaxStreamCodeLen)
	}
	if !streamCodePattern.MatchString(s) {
		return errors.New("stream code must contain only A-Z, a-z, 0-9, '_', '-', and '/'")
	}
	if strings.HasPrefix(s, "/") || strings.HasSuffix(s, "/") {
		return errors.New("stream code must not start or end with '/'")
	}
	if strings.Contains(s, "//") {
		return errors.New("stream code must not contain consecutive '/'")
	}
	return nil
}

// StreamStatus represents the lifecycle state of a stream.
type StreamStatus string

// StreamStatus values are used by the stream manager and API.
const (
	StatusIdle     StreamStatus = "idle"
	StatusActive   StreamStatus = "active"
	StatusDegraded StreamStatus = "degraded"
	StatusStopped  StreamStatus = "stopped"
)

// Stream is the central domain entity.
// It describes everything needed to ingest, process, and deliver a live stream.
type Stream struct {
	// Code is the unique key chosen by the user ([a-zA-Z0-9_]).
	Code StreamCode `json:"code" yaml:"code"`

	// Template references a Template by its code; the template's config-like
	// fields fill in any field this stream leaves at its zero value. nil
	// means no template inheritance — the stream stands alone. See
	// ResolveStream in template.go for the merge rules.
	Template *TemplateCode `json:"template,omitempty" yaml:"template,omitempty"`

	Name        string   `json:"name" yaml:"name"`
	Description string   `json:"description" yaml:"description"`
	Tags        []string `json:"tags" yaml:"tags"`

	// StreamKey is used to authenticate RTMP/SRT push ingest.
	StreamKey string `json:"stream_key" yaml:"stream_key"`

	// PlaybackPolicy binds this stream to a named media-auth Policy by its
	// code (see domain.Policy). Empty means no policy — the stream is public
	// (allow-all). The referenced policy carries the full rule set (token
	// requirement + secret, IP/country/UA/domain allow-deny lists). See
	// internal/mediaauth.
	PlaybackPolicy PolicyCode `json:"playback_policy,omitempty" yaml:"playback_policy,omitempty"`

	// Status is the runtime lifecycle state.
	// It is never persisted — always computed from the coordinator's in-memory
	// state and overlaid by the API layer before returning responses to clients.
	Status StreamStatus `json:"-" yaml:"-"`

	// Disabled when true excludes the stream from server bootstrap and rejects pipeline Start.
	Disabled bool `json:"disabled" yaml:"disabled"`

	// Inputs are the available ingest sources ordered by Priority.
	// The Stream Manager monitors health and switches between them on failure.
	Inputs []Input `json:"inputs" yaml:"inputs"`

	// Transcoder controls encoding/decoding settings.
	// nil means no transcoding for this stream.
	Transcoder *TranscoderConfig `json:"transcoder,omitempty" yaml:"transcoder,omitempty"`

	// Protocols defines which delivery protocols are opened for this stream.
	// nil means the field is unset and ResolveStream inherits the template's
	// Protocols (or leaves the resolved value nil when no template applies —
	// publisher treats nil as "no protocols enabled"). An explicit non-nil
	// pointer — including the zero value &OutputProtocols{} — is an
	// operator-asserted override and beats template inheritance.
	Protocols *OutputProtocols `json:"protocols,omitempty" yaml:"protocols,omitempty"`

	// Push is the list of external destinations the server actively pushes to.
	// Each entry defines one push target (social media live ingest, CDN relay, etc.).
	Push []PushDestination `json:"push" yaml:"push"`

	// DVR overrides the global DVR settings for this specific stream.
	// If nil, the global config is used (when DVR is enabled globally).
	DVR *StreamDVRConfig `json:"dvr,omitempty" yaml:"dvr,omitempty"`

	// Watermark is an optional text or image overlay applied before encoding.
	Watermark *WatermarkConfig `json:"watermark,omitempty" yaml:"watermark,omitempty"`

	// Thumbnail controls periodic screenshot generation for preview images.
	Thumbnail *ThumbnailConfig `json:"thumbnail,omitempty" yaml:"thumbnail,omitempty"`
}

// ValidateInputPriorities enforces that input priorities are contiguous and sorted.
// For N inputs, expected priorities are exactly 0..N-1 in ascending order.
func (s *Stream) ValidateInputPriorities() error {
	if s == nil {
		return nil
	}
	for i, in := range s.Inputs {
		if in.Priority != i {
			return fmt.Errorf("input priority must be %d at index %d", i, i)
		}
	}
	return nil
}

// ValidateUniqueInputs enforces that inputs in one stream are not duplicated.
// Two inputs are considered duplicates if their URL (trimmed) is identical.
func (s *Stream) ValidateUniqueInputs() error {
	if s == nil {
		return nil
	}
	seen := make(map[string]int, len(s.Inputs))
	for i, in := range s.Inputs {
		key := strings.TrimSpace(in.URL)
		if prev, ok := seen[key]; ok {
			return fmt.Errorf("duplicate input URL at indexes %d and %d", prev, i)
		}
		seen[key] = i
	}
	return nil
}
