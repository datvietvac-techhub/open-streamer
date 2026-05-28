package autopublish

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// matcher must locate the owning template for any path that begins at a
// segment boundary of one of the registered prefixes — and refuse a path
// that nests a prefix mid-segment (the classic `live` vs `livestream`
// trap).
func TestMatcher_LongestMatchWins(t *testing.T) {
	t.Parallel()
	m := newMatcher([]*domain.Template{
		{Code: "wide", Prefixes: []string{"region"}},
		{Code: "specific", Prefixes: []string{"region/north"}},
	})

	owner, ok := m.match("region/north/foo")
	require.True(t, ok)
	assert.Equal(t, domain.TemplateCode("specific"), owner,
		"longer prefix wins when both could match")

	owner, ok = m.match("region/south/foo")
	require.True(t, ok)
	assert.Equal(t, domain.TemplateCode("wide"), owner,
		"falls back to the shorter prefix when the longer one diverges")
}

func TestMatcher_SegmentBoundary(t *testing.T) {
	t.Parallel()
	m := newMatcher([]*domain.Template{{Code: "live", Prefixes: []string{"live"}}})

	cases := []struct {
		path string
		want bool
	}{
		{"live/foo/bar", true},
		{"live", true},
		{"live/", true},
		{"livestream/foo", false},
		{"liver/foo", false},
		{"", false},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			_, ok := m.match(tc.path)
			assert.Equal(t, tc.want, ok)
		})
	}
}

// A matcher built from nil templates or templates with no prefixes is
// safe to query and returns "no match" for everything. Hot-swap path
// relies on this — between the moment the last template is deleted and
// the next RefreshTemplates call, a few pushes may briefly hit the
// matcher with no entries.
func TestMatcher_EmptyMatcherNeverMatches(t *testing.T) {
	t.Parallel()
	m := newMatcher(nil)
	_, ok := m.match("anything")
	assert.False(t, ok)

	m = newMatcher([]*domain.Template{{Code: "empty"}, nil})
	_, ok = m.match("anything")
	assert.False(t, ok)
}
