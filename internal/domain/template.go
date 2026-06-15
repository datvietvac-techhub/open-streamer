package domain

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// MaxTemplateCodeLen is the maximum length of a template code.
const MaxTemplateCodeLen = 128

// templateCodePattern allows alphanumerics, dash, and underscore. Unlike
// stream codes, templates have no `/` namespacing — they live in a flat
// namespace because they are referenced by streams (Stream.Template), not
// mounted on a media URL path.
var templateCodePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// TemplateCode is the user-assigned primary key for a template.
// Allowed characters: a-z, A-Z, 0-9, underscore, dash.
type TemplateCode string

// ValidateTemplateCode reports whether s is a non-empty valid template code.
func ValidateTemplateCode(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return errors.New("template code is required")
	}
	if len(s) > MaxTemplateCodeLen {
		return fmt.Errorf("template code exceeds max length %d", MaxTemplateCodeLen)
	}
	if !templateCodePattern.MatchString(s) {
		return errors.New("template code must contain only A-Z, a-z, 0-9, '_', and '-'")
	}
	return nil
}

// Template is a reusable bundle of stream configuration that can be
// inherited by multiple streams. Per-stream identity that the template
// does NOT carry: Code (each stream has its own unique key) and Disabled
// (a runtime toggle that belongs to the stream lifecycle, not the
// shared profile). Every other Stream field has a parallel here so a
// stream pointing at a template can leave it at the zero value and
// inherit the template's setting (see ResolveStream).
//
// Prefixes additionally enable auto-publish: when an encoder pushes to a
// path matching any template prefix and that template carries an Inputs
// list with a publish:// source, the server materialises a runtime stream
// on the fly (no config record needed).
type Template struct {
	// Code is the unique key chosen by the operator.
	Code TemplateCode `json:"code" yaml:"code"`

	// Name and Description are template-level metadata. Name surfaces in
	// the API for human-readable lists; Description carries the rationale
	// behind the template's settings. Streams inheriting this template
	// keep their own Name / Description fields — the template metadata is
	// for operator-facing tooling, not for downstream consumers.
	Name        string `json:"name" yaml:"name"`
	Description string `json:"description" yaml:"description"`

	// Tags propagate to inheriting streams when the stream leaves Tags
	// empty. Useful for grouping every stream that follows a common
	// profile under one operational label.
	Tags []string `json:"tags,omitempty" yaml:"tags,omitempty"`

	// StreamKey is the shared push-authentication secret for streams that
	// inherit this template. Empty means no secret is templated and each
	// stream may set its own (or none).
	StreamKey string `json:"stream_key,omitempty" yaml:"stream_key,omitempty"`

	// PlaybackPolicy is the media-auth Policy code inherited by streams
	// referencing this template (see domain.Policy). Empty = no policy
	// (inheriting streams stay public unless they set their own).
	PlaybackPolicy PolicyCode `json:"playback_policy,omitempty" yaml:"playback_policy,omitempty"`

	// Prefixes is the list of URL-path prefixes that trigger auto-publish.
	// When an encoder pushes to a path whose first segment(s) match any
	// prefix here AND this template has at least one publish:// input, a
	// runtime stream is created on the fly. Prefixes must not overlap any
	// other template's prefix (validated at save time).
	Prefixes []string `json:"prefixes,omitempty" yaml:"prefixes,omitempty"`

	// Inputs are inherited by streams that leave Inputs empty. A template
	// carrying a publish:// input is also the trigger that makes Prefixes
	// matching produce a runtime stream — without an input source there
	// is nothing for the runtime stream to subscribe to.
	Inputs []Input `json:"inputs,omitempty" yaml:"inputs,omitempty"`

	// Transcoder controls encoding/decoding settings. nil means no
	// transcoding for streams inheriting this template (unless they set
	// their own Transcoder).
	Transcoder *TranscoderConfig `json:"transcoder,omitempty" yaml:"transcoder,omitempty"`

	// Protocols defines which delivery protocols are opened. nil means the
	// template doesn't dictate protocols — inheriting streams must declare
	// their own, or the resolved value stays nil (no protocols enabled).
	Protocols *OutputProtocols `json:"protocols,omitempty" yaml:"protocols,omitempty"`

	// Push is the list of external destinations the server actively pushes to.
	Push []PushDestination `json:"push" yaml:"push"`

	// DVR overrides the global DVR settings. Streams that inherit this
	// template inherit the DVR configuration unless they set their own.
	DVR *StreamDVRConfig `json:"dvr,omitempty" yaml:"dvr,omitempty"`

	// Watermark is an optional text or image overlay applied before encoding.
	Watermark *WatermarkConfig `json:"watermark,omitempty" yaml:"watermark,omitempty"`

	// Thumbnail controls periodic screenshot generation.
	Thumbnail *ThumbnailConfig `json:"thumbnail,omitempty" yaml:"thumbnail,omitempty"`
}

// MaxTemplatePrefixLen is the maximum length of a single prefix string.
const MaxTemplatePrefixLen = 128

// templatePrefixPattern matches the same character set as a stream code
// (without requiring a leading `/`). Empty prefixes and any prefix
// containing `..` or characters outside the allowed set are rejected.
var templatePrefixPattern = regexp.MustCompile(`^[A-Za-z0-9_/-]+$`)

// ValidatePrefix reports whether s is a non-empty, well-formed prefix.
// Format mirrors the stream-code character set so a prefix may resemble
// any path a stream code could occupy.
func ValidatePrefix(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return errors.New("template prefix is required")
	}
	if len(s) > MaxTemplatePrefixLen {
		return fmt.Errorf("template prefix exceeds max length %d", MaxTemplatePrefixLen)
	}
	if !templatePrefixPattern.MatchString(s) {
		return errors.New("template prefix must contain only A-Z, a-z, 0-9, '_', '-', and '/'")
	}
	if strings.Contains(s, "//") {
		return errors.New("template prefix must not contain consecutive '/'")
	}
	if strings.HasPrefix(s, "/") {
		return errors.New("template prefix must not start with '/'")
	}
	return nil
}

// ValidatePrefixes reports whether the prefixes within a single template
// are individually well-formed and do not duplicate each other.
// Cross-template overlap is checked separately at the repository layer
// — see PrefixOverlapError / CheckPrefixOverlap.
func ValidatePrefixes(prefixes []string) error {
	seen := make(map[string]int, len(prefixes))
	for i, p := range prefixes {
		if err := ValidatePrefix(p); err != nil {
			return fmt.Errorf("prefix[%d]: %w", i, err)
		}
		norm := normalisePrefix(p)
		if prev, ok := seen[norm]; ok {
			return fmt.Errorf("prefix[%d] %q duplicates prefix[%d]", i, p, prev)
		}
		seen[norm] = i
	}
	return nil
}

// normalisePrefix strips trailing slashes so `live/` and `live` compare
// equal during overlap detection. The match logic later still respects
// segment boundaries — see PrefixMatches.
func normalisePrefix(s string) string {
	return strings.TrimRight(strings.TrimSpace(s), "/")
}

// PrefixMatches reports whether `path` starts with `prefix` on a segment
// boundary. Either `prefix` may be supplied with or without a trailing
// slash — `live/foo/bar` matches both `live` and `live/`, while it does
// NOT match `liv` or `livestream`. Empty prefix is rejected because that
// would catch every push.
func PrefixMatches(prefix, path string) bool {
	prefix = normalisePrefix(prefix)
	if prefix == "" {
		return false
	}
	path = strings.TrimPrefix(strings.TrimSpace(path), "/")
	if path == prefix {
		return true
	}
	return strings.HasPrefix(path, prefix+"/")
}

// PrefixesOverlap reports whether two prefixes would both accept some
// shared path. Two prefixes overlap when one is a path-prefix of the
// other (or they are equal). Used at save time to keep the routing
// namespace partitioned.
func PrefixesOverlap(a, b string) bool {
	a = normalisePrefix(a)
	b = normalisePrefix(b)
	if a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	// `a` is a path-prefix of `b` iff `b` starts with `a + "/"`.
	if strings.HasPrefix(b, a+"/") {
		return true
	}
	if strings.HasPrefix(a, b+"/") {
		return true
	}
	return false
}

// ResolveStream returns the effective configuration of a stream after applying
// its template. The non-inheritable identity is Code (each stream owns its
// key) and Disabled (a per-stream runtime toggle). Every other field uses
// the stream's value when non-zero and falls back to the template's value:
//
//   - Pointer fields (Transcoder, DVR, Watermark, Thumbnail): nil = inherit.
//   - Slice fields (Inputs, Tags, Push): len == 0 = inherit.
//   - Struct fields (Protocols): all-zero = inherit.
//   - String fields (Name, Description, StreamKey): "" = inherit.
//
// If the stream has no Template reference, or the template lookup returns
// nil, the stream is returned unchanged (caller may safely pass nil for
// tpl). The returned pointer is always a copy when a template merge
// happens — the caller is free to mutate it without affecting the
// persisted stream. When no merge is performed the original pointer is
// returned.
func ResolveStream(s *Stream, tpl *Template) *Stream {
	if s == nil || s.Template == nil || tpl == nil {
		return s
	}
	out := *s
	if out.Name == "" && tpl.Name != "" {
		out.Name = tpl.Name
	}
	if out.Description == "" && tpl.Description != "" {
		out.Description = tpl.Description
	}
	if len(out.Tags) == 0 && len(tpl.Tags) > 0 {
		out.Tags = tpl.Tags
	}
	if out.StreamKey == "" && tpl.StreamKey != "" {
		out.StreamKey = tpl.StreamKey
	}
	if out.PlaybackPolicy == "" && tpl.PlaybackPolicy != "" {
		out.PlaybackPolicy = tpl.PlaybackPolicy
	}
	if len(out.Inputs) == 0 && len(tpl.Inputs) > 0 {
		out.Inputs = tpl.Inputs
	}
	if out.Transcoder == nil && tpl.Transcoder != nil {
		out.Transcoder = tpl.Transcoder
	}
	if out.Protocols == nil && tpl.Protocols != nil {
		out.Protocols = tpl.Protocols
	}
	if len(out.Push) == 0 && len(tpl.Push) > 0 {
		out.Push = tpl.Push
	}
	if out.DVR == nil && tpl.DVR != nil {
		out.DVR = tpl.DVR
	}
	if out.Watermark == nil && tpl.Watermark != nil {
		out.Watermark = tpl.Watermark
	}
	if out.Thumbnail == nil && tpl.Thumbnail != nil {
		out.Thumbnail = tpl.Thumbnail
	}
	return &out
}

// TemplateAcceptsPush reports whether the template is wired to receive
// auto-publish (has at least one publish:// input). Required by the
// runtime-stream router: a prefix match alone is not enough to materialise
// a stream — there must be a source for the runtime stream to subscribe
// to. Returns false on nil template.
func TemplateAcceptsPush(tpl *Template) bool {
	if tpl == nil {
		return false
	}
	for _, in := range tpl.Inputs {
		if strings.HasPrefix(strings.TrimSpace(in.URL), "publish://") {
			return true
		}
	}
	return false
}
