package coordinator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
)

// newDegradationTestCoordinator returns a Coordinator wired with only
// the deps the degradation handlers touch (degradation map + a no-op
// event bus). Avoids spinning up buffer/manager/transcoder/publisher
// just to test the status reconciliation logic.
func newDegradationTestCoordinator() *Coordinator {
	return &Coordinator{
		degradation: make(map[domain.StreamCode]*streamDegradation),
		// workers=0 → Publish is a synchronous no-op (no subscribers).
		bus: events.New(0, 1),
	}
}

// streamDegradation.any() must report TRUE when any flag is set so
// StreamStatus can derive Degraded from a presence-of-entry check.
// FALSE only when every flag is cleared — caller drops the map entry
// in that case so healthy streams stay allocation-free.
func TestStreamDegradation_AnyFlag(t *testing.T) {
	t.Parallel()
	d := &streamDegradation{}
	assert.False(t, d.any(), "all-zero must report no degradation")

	d.inputsExhausted = true
	assert.True(t, d.any(), "inputsExhausted must drive degraded")

	d.inputsExhausted = false
	d.transcoderUnhealthy = true
	assert.True(t, d.any(), "transcoderUnhealthy must drive degraded")

	d.inputsExhausted = true
	assert.True(t, d.any(), "both flags set must still drive degraded")

	d.inputsExhausted = false
	d.transcoderUnhealthy = false
	assert.False(t, d.any(), "all flags cleared must report no degradation")
}

// updateDegradation must prune the map entry when every flag clears,
// so healthy streams don't accumulate state. Critical for long-running
// servers with many short-lived streams.
func TestUpdateDegradation_PrunesOnAllClear(t *testing.T) {
	t.Parallel()
	c := newDegradationTestCoordinator()

	c.updateDegradation("s1", func(d *streamDegradation) {
		d.transcoderUnhealthy = true
	})
	require.Contains(t, c.degradation, domain.StreamCode("s1"))

	c.updateDegradation("s1", func(d *streamDegradation) {
		d.transcoderUnhealthy = false
	})
	_, present := c.degradation["s1"]
	assert.False(t, present, "entry must be dropped when all flags clear")
}

// Reconciliation: stream is degraded if EITHER source is unhealthy.
// Recovery from one source while the other is still broken keeps the
// stream degraded. The bug we are fixing: input-only signal couldn't
// reflect transcoder crash loops.
func TestStreamDegradation_TwoSourceReconciliation(t *testing.T) {
	t.Parallel()
	c := newDegradationTestCoordinator()

	// Both sources broken → degraded.
	c.handleAllInputsExhausted("s1")
	c.handleTranscoderUnhealthy("s1", "ffmpeg crashed: encoder rejected preset")
	assert.NotNil(t, c.degradation["s1"])
	assert.True(t, c.degradation["s1"].inputsExhausted)
	assert.True(t, c.degradation["s1"].transcoderUnhealthy)

	// Inputs recover but transcoder still crashing → still degraded.
	c.handleInputRestored("s1")
	require.NotNil(t, c.degradation["s1"], "transcoder still unhealthy → entry kept")
	assert.False(t, c.degradation["s1"].inputsExhausted)
	assert.True(t, c.degradation["s1"].transcoderUnhealthy,
		"transcoder flag must persist across input recovery")

	// Transcoder recovers → all clear, entry dropped.
	c.handleTranscoderHealthy("s1")
	_, present := c.degradation["s1"]
	assert.False(t, present, "all sources healthy → no degradation entry")
}

// Symmetric path: transcoder recovers first, inputs still exhausted.
func TestStreamDegradation_TranscoderRecoversFirst(t *testing.T) {
	t.Parallel()
	c := newDegradationTestCoordinator()

	c.handleAllInputsExhausted("s1")
	c.handleTranscoderUnhealthy("s1", "crash")
	c.handleTranscoderHealthy("s1")

	require.NotNil(t, c.degradation["s1"], "inputs still exhausted → entry kept")
	assert.True(t, c.degradation["s1"].inputsExhausted)
	assert.False(t, c.degradation["s1"].transcoderUnhealthy)
}

// clearDegradation (called from Start paths) must wipe BOTH flags so a
// fresh pipeline for a previously-broken stream code starts clean.
func TestClearDegradation_WipesBothFlags(t *testing.T) {
	t.Parallel()
	c := newDegradationTestCoordinator()
	c.handleAllInputsExhausted("s1")
	c.handleTranscoderUnhealthy("s1", "crash")
	c.clearDegradation("s1")
	_, present := c.degradation["s1"]
	assert.False(t, present, "Start must reset both degradation sources")
}

// Toggling the same flag idempotently must not duplicate map entries
// or leak into the other flag.
func TestUpdateDegradation_IdempotentToggle(t *testing.T) {
	t.Parallel()
	c := newDegradationTestCoordinator()
	c.handleTranscoderUnhealthy("s1", "first")
	c.handleTranscoderUnhealthy("s1", "second")
	c.handleTranscoderUnhealthy("s1", "third")
	assert.True(t, c.degradation["s1"].transcoderUnhealthy)
	assert.False(t, c.degradation["s1"].inputsExhausted,
		"transcoder fires must not pollute the input flag")
}
