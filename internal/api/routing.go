package api

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/go-chi/chi/v5"
)

// Stream codes can contain `/` (used as a namespace separator) so the
// admin endpoint `/streams/...` and the media endpoint `/<code>/...` can no
// longer rely on a single-segment chi `{code}` param. Instead the router
// mounts a catch-all and the helpers below split the captured suffix into a
// (code, action) or (code, filename) pair.

// Reserved admin actions under /streams/<code>/. Stream codes that happen to
// END with one of these segments still work — the code is the prefix and the
// final segment is treated as the action.
const (
	streamActionRestart = "restart"
	streamActionSwitch  = "switch"
)

// Reserved media actions under /<code>/. These are dispatched by exact-name
// match against the final path segment. Everything else falls through to the
// extension-based file dispatcher.
const (
	mediaActionMPEGTS          = "mpegts"
	mediaFileHLSIndex          = "index.m3u8"
	mediaFileDASHIndex         = "index.mpd"
	mediaFileRecordingStatusJS = "recording_status.json"
)

// splitTailAction splits suffix on the LAST '/' boundary. The tail is one of
// the supplied known actions; otherwise tail is "" and head is the entire
// suffix. Used by the streams admin dispatcher to peel a `/restart` or
// `/switch` off the back of a multi-segment stream code.
//
// Examples (with actions {"restart","switch"}):
//
//	"live"                  → head="live",                action=""
//	"live/restart"          → head="live",                action="restart"
//	"region/north/live"     → head="region/north/live",   action=""
//	"region/north/live/restart" → head="region/north/live", action="restart"
//	"live/restart/extra"    → head="live/restart/extra",  action=""   (no match)
func splitTailAction(suffix string, actions ...string) (head, action string) {
	idx := strings.LastIndex(suffix, "/")
	if idx < 0 {
		return suffix, ""
	}
	tail := suffix[idx+1:]
	for _, a := range actions {
		if tail == a {
			return suffix[:idx], a
		}
	}
	return suffix, ""
}

// splitCodeAndFile splits a media-namespace suffix into (code, file). The
// file is always the final path segment; everything before is the (possibly
// slash-containing) stream code. Returns ok=false when there's no '/' at
// all — a single-segment URL like /live has no file to serve.
func splitCodeAndFile(suffix string) (code, file string, ok bool) {
	idx := strings.LastIndex(suffix, "/")
	if idx < 0 {
		return "", "", false
	}
	return suffix[:idx], suffix[idx+1:], true
}

// sanitizeSubpath rejects empty inputs and path traversal. Returns the
// cleaned suffix with any leading '/' stripped. The streams + media
// dispatchers both validate input through this gate before deciding which
// branch to take, so traversal can't sneak past via a known-action prefix.
func sanitizeSubpath(suffix string) (string, bool) {
	suffix = strings.TrimPrefix(suffix, "/")
	if suffix == "" {
		return "", false
	}
	// Reject anything that would resolve outside the namespace once
	// filepath.Clean canonicalises it. filepath.Clean turns `a/../b` into
	// `b`, but the presence of `..` segments in the raw input is itself a
	// red flag — the API never legitimately needs them. Same for absolute
	// paths after Clean.
	if strings.Contains(suffix, "..") {
		return "", false
	}
	cleaned := filepath.Clean(suffix)
	if cleaned == "." || strings.HasPrefix(cleaned, "/") || strings.HasPrefix(cleaned, "..") {
		return "", false
	}
	return suffix, true
}

// setURLParam injects (or overwrites) a chi URL param on the request's route
// context so downstream handlers that read chi.URLParam(r, key) see the
// value the catch-all dispatcher just parsed. Returns the original request
// — chi mutates the context's UrlParams in place — but the call signature
// matches `r = setURLParam(r, ...)` for symmetry with WithContext code.
func setURLParam(r *http.Request, key, value string) *http.Request {
	rctx := chi.RouteContext(r.Context())
	if rctx == nil {
		rctx = chi.NewRouteContext()
		r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
	}
	rctx.URLParams.Add(key, value)
	return r
}
