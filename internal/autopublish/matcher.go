// Package autopublish materialises runtime streams when a push handshake
// arrives on a path that matches a template's prefix list.
//
// A template can declare zero or more URL-path prefixes. When an encoder
// pushes to <protocol>://host/<path> and <path> begins on a segment
// boundary at one of those prefixes, the server looks up the owning
// template, verifies it carries a publish:// input, and synthesises a
// runtime stream whose code is the full incoming path. Runtime streams
// are NEVER persisted — they live in memory only. An idle reaper stops
// a runtime stream when no packet has reached the buffer hub for 30 s.
package autopublish

import (
	"sort"
	"strings"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// matcher snapshots the prefix → template-code map at one point in time.
// Lookups are O(n) linear scans — N is bounded by total prefixes across
// all templates which, in practice, is small (tens to low hundreds).
// Prefixes are sorted by descending length so the longest match wins
// first even though cross-template overlap is forbidden at save time
// (defence in depth against a brief inconsistency during a hot swap).
type matcher struct {
	entries []matcherEntry
}

type matcherEntry struct {
	prefix string // canonical: no leading/trailing '/', no consecutive '/'
	owner  domain.TemplateCode
}

func newMatcher(templates []*domain.Template) *matcher {
	m := &matcher{}
	for _, t := range templates {
		if t == nil {
			continue
		}
		for _, p := range t.Prefixes {
			canon := canonPrefix(p)
			if canon == "" {
				continue
			}
			m.entries = append(m.entries, matcherEntry{prefix: canon, owner: t.Code})
		}
	}
	sort.Slice(m.entries, func(i, j int) bool {
		return len(m.entries[i].prefix) > len(m.entries[j].prefix)
	})
	return m
}

// match returns the owning template for a push path, or false if no
// template prefix matches. Matching is segment-aware: prefix "live"
// matches path "live/foo/bar" but not "livestream/foo".
func (m *matcher) match(path string) (domain.TemplateCode, bool) {
	path = canonPath(path)
	if path == "" {
		return "", false
	}
	for _, e := range m.entries {
		if path == e.prefix || strings.HasPrefix(path, e.prefix+"/") {
			return e.owner, true
		}
	}
	return "", false
}

// canonPrefix / canonPath strip leading and trailing slashes so segment-
// boundary comparison reduces to a plain string match. Whitespace is
// stripped too — the API surface validates input but the matcher is
// also reachable from internal paths (testing, future ingest formats).
func canonPrefix(p string) string {
	p = strings.TrimSpace(p)
	p = strings.Trim(p, "/")
	return p
}

func canonPath(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "/")
	return p
}
