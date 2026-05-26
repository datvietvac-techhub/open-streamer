package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateTemplateCode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		in      string
		wantErr string
	}{
		{"empty", "", "required"},
		{"whitespace only", "   ", "required"},
		{"alphanumeric ok", "profile_1", ""},
		{"dash ok", "profile-A", ""},
		{"slash rejected", "region/north", "must contain only"},
		{"space rejected", "profile A", "must contain only"},
		{"too long", strings.Repeat("a", MaxTemplateCodeLen+1), "exceeds max length"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTemplateCode(tc.in)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

// ResolveStream: a stream with no Template reference must come back unchanged
// (same pointer) so callers can rely on identity to avoid unnecessary copies.
func TestResolveStream_NoTemplateReturnsSamePointer(t *testing.T) {
	t.Parallel()
	s := &Stream{Code: "live"}
	got := ResolveStream(s, &Template{Code: "profile-a", Transcoder: &TranscoderConfig{}})
	assert.Same(t, s, got, "stream with no Template reference must not be merged")
}

// Templates are nil → nothing to merge. Callers (handler, coordinator) rely on
// this so a missing/deleted template falls through to the raw stream.
func TestResolveStream_NilTemplateReturnsSamePointer(t *testing.T) {
	t.Parallel()
	code := TemplateCode("profile-a")
	s := &Stream{Code: "live", Template: &code}
	assert.Same(t, s, ResolveStream(s, nil))
}

// Pointer fields (Transcoder / DVR / Watermark / Thumbnail) follow the
// "nil = inherit" rule.
func TestResolveStream_InheritsPointerFieldsWhenNil(t *testing.T) {
	t.Parallel()
	code := TemplateCode("profile-a")
	tpl := &Template{
		Code:       "profile-a",
		Transcoder: &TranscoderConfig{Global: TranscoderGlobalConfig{HW: HWAccelNVENC}},
		DVR:        &StreamDVRConfig{},
		Watermark:  &WatermarkConfig{},
		Thumbnail:  &ThumbnailConfig{},
	}
	s := &Stream{Code: "live", Template: &code}
	out := ResolveStream(s, tpl)

	require.NotSame(t, s, out, "merge must produce a copy so the persisted stream is not mutated")
	assert.Same(t, tpl.Transcoder, out.Transcoder)
	assert.Same(t, tpl.DVR, out.DVR)
	assert.Same(t, tpl.Watermark, out.Watermark)
	assert.Same(t, tpl.Thumbnail, out.Thumbnail)
	assert.Nil(t, s.Transcoder, "original stream must stay untouched")
}

// Non-nil stream values override the template even when the template would
// fill them in. The override rule is field-level: setting Transcoder on the
// stream does NOT cause DVR to also be taken from the stream.
func TestResolveStream_StreamPointerOverridesTemplate(t *testing.T) {
	t.Parallel()
	code := TemplateCode("profile-a")
	tplTc := &TranscoderConfig{Global: TranscoderGlobalConfig{HW: HWAccelNVENC}}
	streamTc := &TranscoderConfig{Global: TranscoderGlobalConfig{HW: HWAccelNone}}
	tpl := &Template{
		Code:       "profile-a",
		Transcoder: tplTc,
		DVR:        &StreamDVRConfig{},
	}
	s := &Stream{Code: "live", Template: &code, Transcoder: streamTc}

	out := ResolveStream(s, tpl)
	assert.Same(t, streamTc, out.Transcoder, "stream's Transcoder wins")
	assert.Same(t, tpl.DVR, out.DVR, "untouched fields still inherit")
}

// Slice fields (Push) use len == 0 as the inherit signal.
func TestResolveStream_InheritsEmptySlices(t *testing.T) {
	t.Parallel()
	code := TemplateCode("profile-a")
	tpl := &Template{
		Code: "profile-a",
		Push: []PushDestination{{URL: "rtmp://example/a"}},
	}
	s := &Stream{Code: "live", Template: &code}
	out := ResolveStream(s, tpl)
	require.Len(t, out.Push, 1)
	assert.Equal(t, "rtmp://example/a", out.Push[0].URL)
}

func TestResolveStream_NonEmptyStreamSliceOverridesTemplate(t *testing.T) {
	t.Parallel()
	code := TemplateCode("profile-a")
	tpl := &Template{
		Code: "profile-a",
		Push: []PushDestination{{URL: "rtmp://example/a"}},
	}
	s := &Stream{
		Code:     "live",
		Template: &code,
		Push:     []PushDestination{{URL: "rtmp://example/b"}},
	}
	out := ResolveStream(s, tpl)
	require.Len(t, out.Push, 1)
	assert.Equal(t, "rtmp://example/b", out.Push[0].URL)
}

// Protocols is now *OutputProtocols. nil on the stream means "inherit from
// template"; any non-nil value (even &OutputProtocols{}) is an operator
// assertion and replaces the template's value entirely.
func TestResolveStream_InheritsNilProtocols(t *testing.T) {
	t.Parallel()
	code := TemplateCode("profile-a")
	tpl := &Template{Code: "profile-a", Protocols: &OutputProtocols{HLS: true, DASH: true}}
	s := &Stream{Code: "live", Template: &code}
	out := ResolveStream(s, tpl)
	require.NotNil(t, out.Protocols)
	assert.Equal(t, OutputProtocols{HLS: true, DASH: true}, *out.Protocols)
}

func TestResolveStream_PartialProtocolsOverrideTemplate(t *testing.T) {
	t.Parallel()
	code := TemplateCode("profile-a")
	tpl := &Template{Code: "profile-a", Protocols: &OutputProtocols{HLS: true, DASH: true}}
	s := &Stream{
		Code:      "live",
		Template:  &code,
		Protocols: &OutputProtocols{RTMP: true},
	}
	out := ResolveStream(s, tpl)
	require.NotNil(t, out.Protocols)
	assert.Equal(t, OutputProtocols{RTMP: true}, *out.Protocols,
		"a non-nil pointer on the stream replaces the template's protocols entirely")
}

// Explicit empty &OutputProtocols{} on the stream is an operator assertion
// "no protocols enabled" — it must NOT trip the inherit branch.
func TestResolveStream_ExplicitEmptyProtocolsBeatsTemplate(t *testing.T) {
	t.Parallel()
	code := TemplateCode("profile-a")
	tpl := &Template{Code: "profile-a", Protocols: &OutputProtocols{HLS: true, DASH: true}}
	s := &Stream{Code: "live", Template: &code, Protocols: &OutputProtocols{}}
	out := ResolveStream(s, tpl)
	require.NotNil(t, out.Protocols)
	assert.Equal(t, OutputProtocols{}, *out.Protocols,
		"&OutputProtocols{} is explicit-disable, not inherit")
}

// Stream-owned values continue to win over template values even after
// the merge surface widened to cover Name/Description/Tags/StreamKey
// /Inputs. Only Code and Disabled are non-inheritable now.
func TestResolveStream_StreamValuesWinOverTemplate(t *testing.T) {
	t.Parallel()
	code := TemplateCode("profile-a")
	tpl := &Template{
		Code:        "profile-a",
		Name:        "Template Name",
		Description: "Template Description",
		Tags:        []string{"templated"},
		StreamKey:   "template-secret",
		Inputs:      []Input{{URL: "publish://"}},
	}
	s := &Stream{
		Code:        "live",
		Template:    &code,
		Name:        "Stream Name",
		Description: "Stream Description",
		Tags:        []string{"stream"},
		StreamKey:   "stream-secret",
		Inputs:      []Input{{URL: "rtmp://encoder/live/key"}},
	}
	out := ResolveStream(s, tpl)
	assert.Equal(t, "Stream Name", out.Name)
	assert.Equal(t, "Stream Description", out.Description)
	assert.Equal(t, []string{"stream"}, out.Tags)
	assert.Equal(t, "stream-secret", out.StreamKey)
	require.Len(t, out.Inputs, 1)
	assert.Equal(t, "rtmp://encoder/live/key", out.Inputs[0].URL)
}

// Streams that leave string / slice fields at their zero value inherit
// the corresponding template field. Mirrors the existing rule for
// Transcoder / DVR / etc.
func TestResolveStream_InheritsEmptyStringSliceFields(t *testing.T) {
	t.Parallel()
	code := TemplateCode("profile-a")
	tpl := &Template{
		Code:        "profile-a",
		Name:        "From Template",
		Description: "Template-described",
		Tags:        []string{"news", "vn"},
		StreamKey:   "shared-secret",
		Inputs:      []Input{{URL: "publish://"}},
	}
	s := &Stream{Code: "live", Template: &code}
	out := ResolveStream(s, tpl)

	assert.Equal(t, "From Template", out.Name)
	assert.Equal(t, "Template-described", out.Description)
	assert.Equal(t, []string{"news", "vn"}, out.Tags)
	assert.Equal(t, "shared-secret", out.StreamKey)
	require.Len(t, out.Inputs, 1)
	assert.Equal(t, "publish://", out.Inputs[0].URL)
}

// ─── prefix primitives ───────────────────────────────────────────────────────

func TestValidatePrefix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		in      string
		wantErr string
	}{
		{"empty rejected", "", "required"},
		{"single segment ok", "live", ""},
		{"trailing slash ok", "live/", ""},
		{"multi-segment ok", "region/north", ""},
		{"leading slash rejected", "/live", "must not start"},
		{"double slash rejected", "live//foo", "consecutive"},
		{"bad chars rejected", "live+ext", "must contain only"},
		{"too long", strings.Repeat("a", MaxTemplatePrefixLen+1), "exceeds max length"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePrefix(tc.in)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

func TestValidatePrefixes_RejectsDuplicates(t *testing.T) {
	t.Parallel()
	// `live` and `live/` normalise to the same canonical prefix so the
	// duplicate detector must catch them.
	err := ValidatePrefixes([]string{"live", "live/"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicates")
}

func TestPrefixMatches(t *testing.T) {
	t.Parallel()
	cases := []struct {
		prefix, path string
		want         bool
	}{
		{"live", "live/foo", true},
		{"live/", "live/foo", true},
		{"live", "live", true},
		{"live", "livestream/foo", false},
		{"live", "lives/foo", false},
		{"region/north", "region/north/sports/foo", true},
		{"region/north", "region/north", true},
		{"region/north", "region/south", false},
		{"", "anything", false},
	}
	for _, tc := range cases {
		t.Run(tc.prefix+"_vs_"+tc.path, func(t *testing.T) {
			assert.Equal(t, tc.want, PrefixMatches(tc.prefix, tc.path))
		})
	}
}

func TestPrefixesOverlap(t *testing.T) {
	t.Parallel()
	cases := []struct {
		a, b string
		want bool
	}{
		{"live", "live", true},
		{"live", "live/", true},                    // canonicalise
		{"live", "live/sports", true},              // a is prefix of b
		{"live/sports", "live", true},              // b is prefix of a
		{"live", "lives", false},                   // distinct segments
		{"region/north", "region/south", false},    // sibling
		{"region/north", "region/north/foo", true}, // nested
		{"region/north/foo", "region/north", true}, // reversed
		{"a/b/c", "a/x/c", false},                  // diverge mid-tree
		{"", "live", false},                        // empty never overlaps
	}
	for _, tc := range cases {
		t.Run(tc.a+"_vs_"+tc.b, func(t *testing.T) {
			assert.Equal(t, tc.want, PrefixesOverlap(tc.a, tc.b))
		})
	}
}

func TestTemplateAcceptsPush(t *testing.T) {
	t.Parallel()
	assert.False(t, TemplateAcceptsPush(nil))
	assert.False(t, TemplateAcceptsPush(&Template{}))
	assert.False(t, TemplateAcceptsPush(&Template{Inputs: []Input{{URL: "rtmp://a/b"}}}))
	assert.True(t, TemplateAcceptsPush(&Template{Inputs: []Input{{URL: "publish://"}}}))
	assert.True(t, TemplateAcceptsPush(&Template{Inputs: []Input{
		{URL: "rtmp://a/b"},
		{URL: "publish://"},
	}}))
}
