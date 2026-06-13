package blob

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// D-1: a restart must not wipe the record of pre-restart hours. mergePriorCoverage
// carries prior Hours/Available/Gaps into the fresh catalog so retention sees (and
// prunes) them instead of orphaning the blobs and growing disk without bound.
func TestMergePriorCoverage(t *testing.T) {
	t.Parallel()
	prev := &Catalog{
		Format: CatalogFormat,
		Profiles: []ProfileDesc{
			{ID: "p0", Hours: []HourRecord{{Hour: "2026/06/13/10", Sealed: true, SizeBytes: 100}}, Available: []MediaWindow{{FromMs: 1, ToMs: 2}}},
			{ID: "p9-removed", Hours: []HourRecord{{Hour: "2026/06/13/09", Sealed: true, SizeBytes: 50}}},
		},
		Gaps: []Gap{{FromMs: 5, ToMs: 6, Reason: "drop"}},
	}
	cat := &Catalog{Format: CatalogFormat, Profiles: []ProfileDesc{{ID: "p0"}, {ID: "p1"}}}

	mergePriorCoverage(cat, prev)

	require.Len(t, cat.Profiles[0].Hours, 1, "p0 inherits prior hours")
	require.Equal(t, int64(100), cat.Profiles[0].Hours[0].SizeBytes)
	require.Len(t, cat.Profiles[0].Available, 1, "p0 inherits prior available windows")
	require.Empty(t, cat.Profiles[1].Hours, "new profile p1 has no prior hours")

	var removed *ProfileDesc
	for i := range cat.Profiles {
		if cat.Profiles[i].ID == "p9-removed" {
			removed = &cat.Profiles[i]
		}
	}
	require.NotNil(t, removed, "a profile only in the old catalog is carried forward so its hours stay prunable")
	require.Len(t, removed.Hours, 1)
	require.Len(t, cat.Gaps, 1, "gaps carried forward")
}
