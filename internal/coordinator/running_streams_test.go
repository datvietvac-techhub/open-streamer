package coordinator

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// RunningStreams must merge streams from all three pipeline tracks
// (manager-supervised + ABR copy + ABR mixer) and dedupe so the same
// code never appears twice — the runtime restarter would otherwise stop
// the same pipeline midway through a fresh start.
func TestCoordinator_RunningStreams_MergesAndDedupes(t *testing.T) {
	t.Parallel()
	mgr := newSpyMgr()
	mgr.registered["normal"] = true
	mgr.registered["shared"] = true // also lives in abrCopies — must dedupe

	c := &Coordinator{
		mgr: mgr,
		abrCopies: map[domain.StreamCode]*abrCopyEntry{
			"copyA":  {},
			"shared": {}, // intentional overlap
		},
		abrMixers: map[domain.StreamCode]*abrMixerEntry{
			"mixA": {},
		},
	}

	got := c.RunningStreams()
	codes := make([]string, len(got))
	for i, code := range got {
		codes[i] = string(code)
	}
	sort.Strings(codes)
	assert.Equal(t, []string{"copyA", "mixA", "normal", "shared"}, codes)
}

func TestCoordinator_RunningStreams_EmptyWhenNothingRunning(t *testing.T) {
	t.Parallel()
	c := &Coordinator{
		mgr:       newSpyMgr(),
		abrCopies: map[domain.StreamCode]*abrCopyEntry{},
		abrMixers: map[domain.StreamCode]*abrMixerEntry{},
	}
	assert.Empty(t, c.RunningStreams())
}
