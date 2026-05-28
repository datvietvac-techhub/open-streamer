package handler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

func mkMixerInput(video, audio string) domain.Input {
	return domain.Input{Priority: 0, URL: "mixer://" + video + "," + audio}
}

// Pure-network input lists pass through validation untouched —
// validator is a no-op for streams that don't reference mixer://.
func TestValidateMixerConfig_PureNetworkAllowed(t *testing.T) {
	t.Parallel()
	proposed := mkStreamWithInputs("a", "rtmp://origin/a")
	require.Nil(t, validateMixerConfigOn(proposed, nil))
}

// Malformed mixer:// URL must fail at INVALID_MIXER_URL with the input index
// in the message.
func TestValidateMixerConfig_RejectsMalformedURL(t *testing.T) {
	t.Parallel()
	proposed := mkStreamWithInputs("mix", "mixer://only_one_code")
	err := validateMixerConfigOn(proposed, nil)
	require.NotNil(t, err)
	require.Equal(t, "INVALID_MIXER_URL", err.code)
	require.Contains(t, err.message, "inputs[0]")
}

// Self-mix surfaces as INVALID_MIXER_SHAPE with self-mix message.
func TestValidateMixerConfig_RejectsSelfMix(t *testing.T) {
	t.Parallel()
	proposed := &domain.Stream{
		Code:   "a",
		Inputs: []domain.Input{mkMixerInput("a", "radio")},
	}
	err := validateMixerConfigOn(proposed, []*domain.Stream{{Code: "radio"}})
	require.NotNil(t, err)
	require.Equal(t, "INVALID_MIXER_SHAPE", err.code)
	require.Contains(t, err.message, "self-mix")
}

// mixer:// + fallback input → SHAPE error with sole-input hint.
func TestValidateMixerConfig_RejectsFallbackInput(t *testing.T) {
	t.Parallel()
	proposed := &domain.Stream{
		Code: "mix",
		Inputs: []domain.Input{
			mkMixerInput("cam", "radio"),
			{Priority: 1, URL: "rtmp://backup/mix"},
		},
	}
	err := validateMixerConfigOn(proposed, []*domain.Stream{
		{Code: "cam"},
		{Code: "radio"},
	})
	require.NotNil(t, err)
	require.Equal(t, "INVALID_MIXER_SHAPE", err.code)
	require.Contains(t, err.message, "must be the only input")
}

// Single + single upstream as sole input = supported v1 case.
func TestValidateMixerConfig_AllowsSingleSinglePair(t *testing.T) {
	t.Parallel()
	proposed := &domain.Stream{
		Code:   "mix",
		Inputs: []domain.Input{mkMixerInput("cam", "radio")},
	}
	require.Nil(t, validateMixerConfigOn(proposed, []*domain.Stream{
		{Code: "cam"},
		{Code: "radio"},
	}))
}

// Missing upstream is NOT a write-time error — runtime catches it.
func TestValidateMixerConfig_AllowsMissingUpstreams(t *testing.T) {
	t.Parallel()
	proposed := &domain.Stream{
		Code:   "mix",
		Inputs: []domain.Input{mkMixerInput("ghost_v", "ghost_a")},
	}
	require.Nil(t, validateMixerConfigOn(proposed, nil))
}
