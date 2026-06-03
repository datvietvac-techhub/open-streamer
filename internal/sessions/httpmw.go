package sessions

import (
	"net/http"
	"path/filepath"
	"strings"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// HTTPMiddleware returns a net/http middleware that records a session hit
// for every successful HLS / DASH manifest or segment request served from
// /<stream_code>/<file> — the URL layout the API server's media catch-all
// dispatches. Stream codes may contain '/' so the parser treats everything
// up to the LAST '/' as the code and the trailing segment as the file.
//
// Behaviour:
//   - Only file extensions known to the media layer are counted: .m3u8 / .ts
//     for HLS; .mpd / .m4s / .mp4 for DASH. Other paths (admin APIs, .json
//     metadata, /healthz) pass through untouched.
//   - Bytes counted are exactly what was written to the wire — we wrap the
//     ResponseWriter and read its byte counter.
//   - Non-2xx responses still record a hit (so a broken segment URL still
//     reveals the viewer in the dashboard) but credit zero bytes.
//   - DVR (timeshift) playback is detected by the presence of any
//     timeshift query param (from / delay / dur / ago) and tagged so
//     dashboards distinguish archive viewers from live-edge.
func HTTPMiddleware(t Tracker) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			proto := protocolForPath(r.URL.Path)
			if proto == "" {
				next.ServeHTTP(w, r)
				return
			}
			code := streamCodeFromPath(r.URL.Path)
			if code == "" {
				next.ServeHTTP(w, r)
				return
			}

			cw := &countingWriter{ResponseWriter: w}
			next.ServeHTTP(cw, r)

			// Only credit bytes when the response succeeded — error pages
			// don't represent media a player rendered.
			var delta int64
			if cw.status >= 200 && cw.status < 300 {
				delta = cw.written
			}
			hit := NewHTTPHit(r, code, proto, delta)
			if isDVRPlayback(r) {
				hit.DVR = true
			}
			t.TrackHTTP(r.Context(), hit)
		})
	}
}

// streamCodeFromPath returns everything between the leading '/' and the
// LAST '/' — i.e. the stream-code prefix under the catch-all layout
// /<code>/<file>. With slashes-allowed codes the prefix may itself span
// multiple segments (e.g. "region/north/live").
//
// DASH + HLS ABR put per-rendition segments under a `track_<N>` subdir
// (`buffer.VideoTrackSlug` is the source of truth — `track_1`, `track_2`,
// …), so the raw path-prefix split would catch the rendition slug as
// part of the stream code. That makes a single viewer pulling N
// renditions look like N distinct sessions in the tracker; this helper
// strips a trailing `track_<digits>` segment so the parent stream owns
// the session. The slug is a closed namespace inside this codebase, so
// the strip can't collide with an operator-defined code unless they
// deliberately namespace one segment as `track_<N>` (and even then it
// would only conflict for the ABR / track_<N> rendition layout).
func streamCodeFromPath(p string) domain.StreamCode {
	p = strings.TrimPrefix(p, "/")
	i := strings.LastIndexByte(p, '/')
	if i <= 0 {
		return ""
	}
	code := p[:i]
	if j := strings.LastIndexByte(code, '/'); j > 0 && isABRTrackSlug(code[j+1:]) {
		code = code[:j]
	}
	return domain.StreamCode(code)
}

// isABRTrackSlug reports whether s matches `track_<positive digits>` —
// the path segment buffer.VideoTrackSlug emits for ABR renditions.
// Kept inline (not imported from internal/buffer) so the sessions
// package stays leaf-level in the import graph.
func isABRTrackSlug(s string) bool {
	const prefix = "track_"
	if !strings.HasPrefix(s, prefix) {
		return false
	}
	rest := s[len(prefix):]
	if rest == "" {
		return false
	}
	for _, c := range rest {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// isDVRPlayback reports whether the request carries any timeshift query
// parameter. Kept in sync with the api server dispatcher's equivalent
// helper.
func isDVRPlayback(r *http.Request) bool {
	q := r.URL.Query()
	return q.Has("from") || q.Has("delay") || q.Has("dur") || q.Has("ago")
}

// protocolForPath maps a request path's extension to its SessionProto.
// Returns "" for extensions outside the HLS/DASH delivery set.
func protocolForPath(p string) domain.SessionProto {
	switch strings.ToLower(filepath.Ext(p)) {
	case ".m3u8", ".ts":
		return domain.SessionProtoHLS
	case ".mpd", ".m4s", ".mp4":
		return domain.SessionProtoDASH
	default:
		return ""
	}
}

// countingWriter is a minimal http.ResponseWriter wrapper that tallies bytes
// written and remembers the status code. We avoid go-chi's middleware.WrapResponseWriter
// here because it allocates a couple of small structs per request; on a busy
// HLS edge that adds up. Net/http guarantees Write is called serially per
// request so the counters need no locks.
type countingWriter struct {
	http.ResponseWriter
	status  int
	written int64
}

// WriteHeader records the status before delegating so the middleware can
// distinguish 2xx vs error responses without re-reading from the wire.
func (c *countingWriter) WriteHeader(code int) {
	c.status = code
	c.ResponseWriter.WriteHeader(code)
}

func (c *countingWriter) Write(b []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	n, err := c.ResponseWriter.Write(b)
	c.written += int64(n)
	return n, err
}
