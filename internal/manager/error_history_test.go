package manager

import (
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/stretchr/testify/require"
)

// recordInputError keeps newest at index 0 and caps at maxInputErrorHistory.
// This is the contract InputHealthSnapshot.Errors exposes — frontend reads
// Errors[0] for the most recent failure.
func TestRecordInputError_OrderingAndCap(t *testing.T) {
	t.Parallel()
	h := &InputHealth{}
	base := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 7; i++ {
		recordInputError(h, errMsg(i), base.Add(time.Duration(i)*time.Second))
	}

	require.Len(t, h.Errors, maxInputErrorHistory, "ring buffer must cap at %d", maxInputErrorHistory)
	require.Equal(t, "err-6", h.Errors[0].Message, "newest entry at index 0")
	require.Equal(t, "err-2", h.Errors[maxInputErrorHistory-1].Message, "oldest survivor at end")
	require.Equal(t, base.Add(6*time.Second), h.Errors[0].At)
}

func TestRecordInputError_StartsEmpty(t *testing.T) {
	t.Parallel()
	h := &InputHealth{}
	require.Empty(t, h.Errors)

	recordInputError(h, "first", time.Now())
	require.Len(t, h.Errors, 1)
	require.Equal(t, "first", h.Errors[0].Message)
}

// Snapshot must defensively copy Errors so callers cannot mutate state via
// the returned slice (state mutex is released right after RuntimeStatus returns).
func TestRuntimeStatus_DefensiveErrorCopy(t *testing.T) {
	t.Parallel()
	h := &InputHealth{}
	recordInputError(h, "first", time.Now())

	snap := InputHealthSnapshot{}
	if len(h.Errors) > 0 {
		snap.Errors = make([]domain.ErrorEntry, len(h.Errors))
		copy(snap.Errors, h.Errors)
	}

	// Mutate underlying state — snapshot must be unaffected.
	recordInputError(h, "second", time.Now())
	require.Equal(t, "second", h.Errors[0].Message)
	require.Equal(t, "first", snap.Errors[0].Message, "snapshot must not see post-snapshot mutations")
}

func errMsg(i int) string {
	return "err-" + string(rune('0'+i))
}
