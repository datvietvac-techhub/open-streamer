package handler

import (
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
