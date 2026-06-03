// Package version exposes build-time injected version metadata.
//
// Variables are populated via -ldflags at build time:
//
//	go build -ldflags="\
//	    -X github.com/datvietvac-techhub/open-streamer/pkg/version.Version=v0.0.7 \
//	    -X github.com/datvietvac-techhub/open-streamer/pkg/version.Commit=abc1234 \
//	    -X github.com/datvietvac-techhub/open-streamer/pkg/version.BuiltAt=2026-04-23T15:00:00Z"
//
// Defaults below ("dev"/"unknown"/"") apply when running via `go run` or for
// unstamped local builds. The Makefile `build` target wires all three.
package version

// Version is the release tag (vMAJ.MIN.PATCH), short SHA fallback, or "dev".
var Version = "dev"

// Commit is the git commit SHA the binary was built from, or "unknown".
var Commit = "unknown"

// BuiltAt is the UTC RFC3339 timestamp of the build, or empty.
var BuiltAt = ""

// Info bundles all version fields for JSON exposure (e.g. GET /config).
type Info struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	BuiltAt string `json:"built_at,omitempty"`
}

// Get returns the current build metadata as a value (safe to embed in
// JSON responses without exposing the underlying mutable globals).
func Get() Info {
	return Info{Version: Version, Commit: Commit, BuiltAt: BuiltAt}
}

// UserAgent is the canonical product identifier this binary uses on
// outbound requests (HLS / HTTP-TS / RTSP pulls). Downstream servers
// surface this string in their play-session dashboards, so a stable
// "Open-Streamer/<version>" form makes our pulls identifiable in
// other operators' tooling.
//
// Format follows RFC 9110 §10.1.5 product-token convention
// "<product>/<product-version>". When Version is the placeholder "dev"
// (unstamped local build) we still emit it — operators want to see
// that a dev build is calling them rather than guess.
func UserAgent() string {
	return "Open-Streamer/" + Version
}
