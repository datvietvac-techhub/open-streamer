package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

func mkStreamWithInputs(code string, inputs ...string) *domain.Stream {
	s := &domain.Stream{Code: domain.StreamCode(code)}
	for i, u := range inputs {
		s.Inputs = append(s.Inputs, domain.Input{Priority: i, URL: u})
	}
	return s
}

func mkABRUpstream(code string, profiles int) *domain.Stream {
	pp := make([]domain.VideoProfile, profiles)
	for i := range pp {
		pp[i] = domain.VideoProfile{Width: 1920 - i*640, Height: 1080 - i*360, Bitrate: 4500 - i*1500}
	}
	return &domain.Stream{
		Code: domain.StreamCode(code),
		Transcoder: &domain.TranscoderConfig{
			Video: domain.VideoTranscodeConfig{Profiles: pp},
		},
	}
}

// Pure-network input lists must pass through validation untouched —
// the validator is a no-op for streams that don't reference copy://.
func TestValidateCopyConfig_PureNetworkAllowed(t *testing.T) {
	t.Parallel()
	proposed := mkStreamWithInputs("a", "rtmp://origin/a")
	require.Nil(t, validateCopyConfigOn(proposed, nil))
}

// Malformed copy:// URL must fail at INVALID_COPY_URL with the input index
// in the message — frontend can highlight the bad input directly.
func TestValidateCopyConfig_RejectsMalformedURL(t *testing.T) {
	t.Parallel()
	proposed := mkStreamWithInputs("a",
		"rtmp://origin/a",
		"copy://", // missing target — copy://-grammar violation
	)
	err := validateCopyConfigOn(proposed, nil)
	require.NotNil(t, err)
	require.Equal(t, "INVALID_COPY_URL", err.code)
	require.Contains(t, err.message, "inputs[1]")
}

// Self-copy short-circuits the shape validator before cycle detection,
// giving a clearer message than "cycle: A → A".
func TestValidateCopyConfig_RejectsSelfCopy(t *testing.T) {
	t.Parallel()
	proposed := mkStreamWithInputs("a", "copy://a")
	err := validateCopyConfigOn(proposed, nil)
	require.NotNil(t, err)
	require.Equal(t, "INVALID_COPY_SHAPE", err.code)
	require.Contains(t, err.message, "self-copy")
}

// ABR upstream + fallback input → SHAPE error with actionable hint.
func TestValidateCopyConfig_RejectsABRWithFallback(t *testing.T) {
	t.Parallel()
	upstream := mkABRUpstream("up", 3)
	proposed := mkStreamWithInputs("dn",
		"copy://up",
		"rtmp://backup/stream",
	)
	err := validateCopyConfigOn(proposed, []*domain.Stream{upstream})
	require.NotNil(t, err)
	require.Equal(t, "INVALID_COPY_SHAPE", err.code)
	require.Contains(t, err.message, "must be the only input")
}

// ABR-copy as sole input + no local transcoder = the supported v1 case.
func TestValidateCopyConfig_AllowsABRCopyAsSoleInput(t *testing.T) {
	t.Parallel()
	upstream := mkABRUpstream("up", 2)
	proposed := mkStreamWithInputs("dn", "copy://up")
	require.Nil(t, validateCopyConfigOn(proposed, []*domain.Stream{upstream}))
}

// Missing upstream is NOT a write-time error (upstream may be created
// later). Coordinator catches it at start time as a hard error.
func TestValidateCopyConfig_AllowsMissingUpstream(t *testing.T) {
	t.Parallel()
	proposed := mkStreamWithInputs("dn", "copy://ghost")
	require.Nil(t, validateCopyConfigOn(proposed, nil))
}

// B-6: copy:// shape validation must resolve the upstream's template before
// classifying it. An upstream that inherits its ABR transcoder from a
// template looks single-stream on its raw record; without resolution the
// "ABR-copy must be the sole input" rule is silently skipped and the
// misconfig only surfaces later as a runtime blackout.
func TestValidateCopyConfig_ResolvesTemplateUpstream(t *testing.T) {
	t.Parallel()
	tplCode := domain.TemplateCode("abr-tpl")
	tr := newFakeTemplateRepo()
	require.NoError(t, tr.Save(context.Background(), &domain.Template{
		Code: tplCode,
		Transcoder: &domain.TranscoderConfig{
			Video: domain.VideoTranscodeConfig{Profiles: []domain.VideoProfile{
				{Width: 1920, Height: 1080, Bitrate: 4500},
				{Width: 1280, Height: 720, Bitrate: 3000},
			}},
		},
	}))
	repo := newFakeStreamRepoFull()
	repo.seed(&domain.Stream{Code: "up", Template: &tplCode}) // ABR inherited; raw record looks single-stream
	h := &StreamHandler{streamRepo: repo, templateRepo: tr}

	// copy://up + a fallback input → the ABR-copy is NOT the sole input.
	proposed := &domain.Stream{Code: "down", Inputs: []domain.Input{
		{URL: "copy://up"},
		{URL: "udp://example.invalid:1234"},
	}}
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/streams/down", nil)
	vErr := h.validateCopyConfig(req, proposed)
	require.NotNil(t, vErr, "ABR-copy with a fallback must be rejected once the template is resolved (B-6)")
	require.Equal(t, "INVALID_COPY_SHAPE", vErr.code)
}
