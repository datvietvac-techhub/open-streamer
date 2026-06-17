package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ParseAudioVolume accepts a linear multiplier OR a dB string and reduces both
// to a single linear gain factor; empty/unity inputs are 1.0 and bad inputs err.
func TestParseAudioVolume(t *testing.T) {
	t.Parallel()
	ok := []struct {
		in   string
		want float64
	}{
		{"", 1},          // default → unity
		{"1", 1},         // linear unity
		{"0dB", 1},       // dB unity
		{"+0dB", 1},      // dB unity, signed
		{"2", 2},         // linear ×2
		{"0.5", 0.5},     // linear ×0.5
		{"0", 0},         // linear mute
		{"+6dB", 1.9953}, // +6 dB ≈ ×2
		{"-6dB", 0.5012}, // -6 dB ≈ ×0.5
		{"+9dB", 2.8184}, // +9 dB ≈ ×2.82
		{"  +9dB ", 2.8184},
		{"3DB", 1.4125}, // case-insensitive suffix
	}
	for _, tc := range ok {
		got, err := ParseAudioVolume(tc.in)
		require.NoError(t, err, "input %q", tc.in)
		assert.InDelta(t, tc.want, got, 0.001, "input %q", tc.in)
	}

	bad := []string{"abc", "5x", "-2", "1.2.3", "dB", "+dB", "loud"}
	for _, in := range bad {
		_, err := ParseAudioVolume(in)
		assert.Error(t, err, "input %q should be rejected", in)
	}
}
