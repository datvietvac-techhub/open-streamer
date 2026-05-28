package sessions

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

func newRouterWithTracker(t *testing.T, tr Tracker) http.Handler {
	t.Helper()
	r := chi.NewRouter()
	r.Use(HTTPMiddleware(tr))
	// Use catch-all routes to mirror the api server's media catch-all
	// layout (codes may contain '/' so single-segment {code} won't do).
	r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/abc/index.m3u8":
			_, _ = w.Write([]byte("#EXTM3U\n"))
		case "/abc/seg0.ts", "/region/north/abc/seg0.ts":
			w.Header().Set("Content-Length", "16")
			_, _ = w.Write([]byte("0123456789ABCDEF"))
		case "/abc/index.mpd":
			_, _ = w.Write([]byte("<MPD/>"))
		case "/abc/missing.ts":
			http.Error(w, "no", http.StatusNotFound)
		case "/healthz":
			_, _ = w.Write([]byte("ok"))
		default:
			http.NotFound(w, r)
		}
	})
	return r
}

func TestHTTPMiddlewareRecordsHLSHit(t *testing.T) {
	s := newTestService(t)
	h := newRouterWithTracker(t, s)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/abc/seg0.ts", nil)
	req.RemoteAddr = "1.2.3.4:9999"
	req.Header.Set("User-Agent", "vlc")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	all := s.List(Filter{})
	if len(all) != 1 {
		t.Fatalf("got %d sessions, want 1", len(all))
	}
	got := all[0]
	if got.Protocol != domain.SessionProtoHLS {
		t.Errorf("proto=%s, want hls", got.Protocol)
	}
	if got.IP != "1.2.3.4" {
		t.Errorf("ip=%s, want 1.2.3.4", got.IP)
	}
	if got.Bytes != 16 {
		t.Errorf("bytes=%d, want 16", got.Bytes)
	}
	if got.DVR {
		t.Errorf("live request should not be tagged DVR")
	}
}

func TestHTTPMiddlewareDASHDetection(t *testing.T) {
	s := newTestService(t)
	h := newRouterWithTracker(t, s)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/abc/index.mpd", nil)
	req.RemoteAddr = "5.6.7.8:1234"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	got := s.List(Filter{})
	if len(got) != 1 || got[0].Protocol != domain.SessionProtoDASH {
		t.Fatalf("expected one DASH session, got %+v", got)
	}
}

func TestHTTPMiddlewareSkipsNonMedia(t *testing.T) {
	s := newTestService(t)
	h := newRouterWithTracker(t, s)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := len(s.List(Filter{})); got != 0 {
		t.Errorf("non-media path created %d sessions, want 0", got)
	}
}

func TestHTTPMiddlewareCreditsZeroBytesOnError(t *testing.T) {
	s := newTestService(t)
	h := newRouterWithTracker(t, s)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/abc/missing.ts", nil)
	req.RemoteAddr = "9.9.9.9:1"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	got := s.List(Filter{})
	if len(got) != 1 {
		t.Fatalf("got %d sessions", len(got))
	}
	if got[0].Bytes != 0 {
		t.Errorf("Bytes=%d on 404 response, want 0", got[0].Bytes)
	}
}

func TestHTTPMiddlewareDisabledTrackerNoop(t *testing.T) {
	s := newService(config.SessionsConfig{Enabled: false}, nil, NullGeoIP{})
	h := newRouterWithTracker(t, s)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/abc/seg0.ts", nil)
	req.RemoteAddr = "1.2.3.4:1"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("middleware broke response when disabled: %d", rec.Code)
	}
	if got := s.Stats().Active; got != 0 {
		t.Errorf("Active=%d when disabled, want 0", got)
	}
}

// Codes containing '/' are kept whole — the middleware must take
// everything up to the LAST '/' as the code, not just the first segment.
func TestHTTPMiddlewareSupportsNestedCodes(t *testing.T) {
	s := newTestService(t)
	h := newRouterWithTracker(t, s)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/region/north/abc/seg0.ts", nil)
	req.RemoteAddr = "1.2.3.4:1"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	all := s.List(Filter{})
	if len(all) != 1 {
		t.Fatalf("got %d sessions, want 1", len(all))
	}
	if got := all[0].StreamCode; got != "region/north/abc" {
		t.Errorf("stream_code=%s, want region/north/abc", got)
	}
}

// DASH + HLS ABR put per-rendition segments under a `track_<N>` subdir.
// The session belongs to the parent stream — a viewer pulling 1080p +
// 720p + 480p must show up as ONE session, not three. Before this fix
// streamCodeFromPath swallowed `/track_<N>` into the stream code, so
// each rendition opened its own session record with a distinct
// fingerprint (the fingerprint hash includes the stream code).
func TestHTTPMiddlewareCollapsesABRTrackHits(t *testing.T) {
	s := newTestService(t)
	h := newRouterWithTracker(t, s)

	const remote = "1.2.3.4:1"
	const ua = "Mozilla/5.0"
	for _, p := range []string{
		"/thanh_hoa/track_1/init.mp4",
		"/thanh_hoa/track_1/seg-0.m4s",
		"/thanh_hoa/track_2/init.mp4",
		"/thanh_hoa/track_2/seg-0.m4s",
	} {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, p, nil)
		req.RemoteAddr = remote
		req.Header.Set("User-Agent", ua)
		h.ServeHTTP(httptest.NewRecorder(), req)
	}

	all := s.List(Filter{})
	if len(all) != 1 {
		t.Fatalf("got %d sessions, want 1 (rendition hits must collapse onto the parent stream)", len(all))
	}
	if got := all[0].StreamCode; got != "thanh_hoa" {
		t.Errorf("stream_code=%s, want thanh_hoa (rendition slug must be stripped)", got)
	}
}

// streamCodeFromPath is the path-parsing primitive that the middleware
// relies on; cover the edge cases directly so a future refactor that
// rewrites the helper without changing the middleware still surfaces
// the regression as a unit-test failure.
func TestStreamCodeFromPath(t *testing.T) {
	cases := []struct {
		path string
		want domain.StreamCode
	}{
		// Flat single-segment codes — file at depth 1.
		{"/news/index.m3u8", "news"},
		{"/news/seg0.ts", "news"},
		// Multi-segment namespaced codes — file at deeper depth.
		{"/region/north/news/index.m3u8", "region/north/news"},
		{"/region/north/news/seg0.ts", "region/north/news"},
		// ABR rendition layout — `/track_<N>` must be stripped.
		{"/news/track_1/init.mp4", "news"},
		{"/news/track_2/seg-3.m4s", "news"},
		{"/region/north/news/track_1/seg-3.m4s", "region/north/news"},
		// Names that look like a track slug but aren't — left alone.
		{"/news/trackfoo/seg-3.m4s", "news/trackfoo"},
		{"/news/track_/seg-3.m4s", "news/track_"},
		{"/news/track_x1/seg-3.m4s", "news/track_x1"},
		// Pathological: no `/` after code → no code (no file to serve anyway).
		{"/news", ""},
		{"", ""},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			if got := streamCodeFromPath(tc.path); got != tc.want {
				t.Errorf("streamCodeFromPath(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}

// Timeshift query params (from / delay / dur / ago) flip the hit into
// DVR=true so the dashboard can split archive vs live-edge viewers —
// replaces the dropped DVRHTTPMiddleware route group.
func TestHTTPMiddlewareTagsDVRFromQueryParams(t *testing.T) {
	cases := []string{"from", "delay", "dur", "ago"}
	for _, key := range cases {
		t.Run(key, func(t *testing.T) {
			s := newTestService(t)
			h := newRouterWithTracker(t, s)

			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet,
				"/abc/index.m3u8?"+key+"=1", nil)
			req.RemoteAddr = "1.2.3.4:1"
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			all := s.List(Filter{})
			if len(all) != 1 {
				t.Fatalf("got %d sessions", len(all))
			}
			if !all[0].DVR {
				t.Errorf("expected DVR=true for ?%s=1", key)
			}
		})
	}
}

// A live + DVR pair from the same client must be two distinct sessions
// (the dvr bool is part of the session fingerprint).
func TestDVRHitDoesNotCollideWithLiveSession(t *testing.T) {
	s := newTestService(t)
	h := newRouterWithTracker(t, s)

	const ua = "vlc"
	const ip = "1.2.3.4:1"

	liveReq := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/abc/seg0.ts", nil)
	liveReq.RemoteAddr = ip
	liveReq.Header.Set("User-Agent", ua)
	h.ServeHTTP(httptest.NewRecorder(), liveReq)

	dvrReq := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/abc/index.m3u8?from=1700000000", nil)
	dvrReq.RemoteAddr = ip
	dvrReq.Header.Set("User-Agent", ua)
	h.ServeHTTP(httptest.NewRecorder(), dvrReq)

	all := s.List(Filter{})
	if len(all) != 2 {
		t.Fatalf("got %d sessions, want 2 (live + dvr distinct)", len(all))
	}
	var liveSeen, dvrSeen bool
	for _, sess := range all {
		if sess.DVR {
			dvrSeen = true
		} else {
			liveSeen = true
		}
	}
	if !liveSeen || !dvrSeen {
		t.Errorf("expected both live and dvr sessions, got liveSeen=%v dvrSeen=%v", liveSeen, dvrSeen)
	}
}
