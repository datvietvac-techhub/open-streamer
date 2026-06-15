package push

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// rtmpRouteKey reconstructs the URL path from `app` + `streamName`, strips
// any leading `live/`, and rejects bare single-segment paths (no `live/`
// prefix and no '/') so encoders can't accidentally hit a 1-segment stream
// via `rtmp://host/<code>`.
func TestRTMPRouteKey(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		app        string
		streamName string
		want       string
	}{
		{"canonical 1-segment via live app", "live", "foo", "foo"},
		{"2-segment app/streamName", "foo", "bar", "foo/bar"},
		{"3-segment app holds two", "foo/bar", "baz", "foo/bar/baz"},
		{"non-canonical live prefix on multi-segment", "live", "foo/bar", "foo/bar"},
		{"bare 1-segment via app only", "foo", "", ""},
		{"bare 1-segment via streamName only", "", "foo", ""},
		{"empty both", "", "", ""},
		{"whitespace trimmed", "  live  ", "  foo  ", "foo"},
		{"live alone rejected", "live", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, rtmpRouteKey(tc.app, tc.streamName))
		})
	}
}

// pushSecretFromQuery pulls the `key` param off the publish-URL query.
func TestPushSecretFromQuery(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, rawQuery, want string
	}{
		{"empty", "", ""},
		{"key present", "key=s3cr3t", "s3cr3t"},
		{"key among others", "a=1&key=s3cr3t&b=2", "s3cr3t"},
		{"no key param", "token=x", ""},
		{"malformed", "%zz", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, pushSecretFromQuery(tc.rawQuery))
		})
	}
}

// pushKeyOK gates a publish against the stream's configured StreamKey (S-8):
// no resolver or an empty configured key allows; otherwise the secret must
// match in constant time.
func TestPushKeyOK(t *testing.T) {
	t.Parallel()

	t.Run("nil resolver allows (auth disabled)", func(t *testing.T) {
		s := &RTMPServer{}
		assert.True(t, s.pushKeyOK("foo", ""))
		assert.True(t, s.pushKeyOK("foo", "anything"))
	})

	t.Run("empty configured key allows (opted out)", func(t *testing.T) {
		s := &RTMPServer{}
		s.SetStreamKeyResolver(func(domain.StreamCode) string { return "" })
		assert.True(t, s.pushKeyOK("foo", ""))
		assert.True(t, s.pushKeyOK("foo", "whatever"))
	})

	t.Run("configured key requires exact match", func(t *testing.T) {
		s := &RTMPServer{}
		s.SetStreamKeyResolver(func(domain.StreamCode) string { return "s3cr3t" })
		assert.True(t, s.pushKeyOK("foo", "s3cr3t"))
		assert.False(t, s.pushKeyOK("foo", "wrong"))
		assert.False(t, s.pushKeyOK("foo", ""))
	})
}
