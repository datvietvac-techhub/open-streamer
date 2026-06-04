package domain

import "time"

// SessionProto identifies the network protocol carrying a play session.
// It mirrors the on-the-wire transport — application-layer flavours
// (e.g. browser-embedded HLS.js vs native Safari) are not distinguished.
type SessionProto string

// SessionProto values for each playback protocol Open-Streamer serves.
const (
	SessionProtoHLS    SessionProto = "hls"
	SessionProtoDASH   SessionProto = "dash"
	SessionProtoRTMP   SessionProto = "rtmp"
	SessionProtoSRT    SessionProto = "srt"
	SessionProtoRTSP   SessionProto = "rtsp"
	SessionProtoMPEGTS SessionProto = "mpegts"
)

// SessionNamedBy describes how the session's UserName was resolved.
// Mirrors the common `named_by` field so external dashboards expecting that
// vocabulary keep working.
type SessionNamedBy string

// SessionNamedBy values.
const (
	SessionNamedByToken       SessionNamedBy = "token"       // resolved from a `token` query param
	SessionNamedByConfig      SessionNamedBy = "config"      // resolved from server-side config (future: signed URL)
	SessionNamedByFingerprint SessionNamedBy = "fingerprint" // synthesised hash of ip+ua+stream
)

// SessionCloseReason is set on PlaySession.CloseReason when the session ends.
type SessionCloseReason string

// SessionCloseReason values.
const (
	SessionCloseIdle        SessionCloseReason = "idle"         // no activity within the configured idle window
	SessionCloseMaxLifetime SessionCloseReason = "max_lifetime" // total lifetime exceeded the configured cap
	SessionCloseClient      SessionCloseReason = "client_gone"  // TCP/UDP peer closed (RTMP/SRT/RTSP)
	SessionCloseShutdown    SessionCloseReason = "shutdown"     // server shutting down
	SessionCloseKicked      SessionCloseReason = "kicked"       // operator force-closed via API
)

// PlaySession is the audit record for one client watching a stream.
// It is created when the first activity for a given (stream, fingerprint)
// pair is observed and stays live until the idle timer expires or the
// transport-level connection closes.
//
// All time fields are wall-clock UTC; default Go RFC3339 JSON encoding.
type PlaySession struct {
	// ID uniquely identifies the session across the server's lifetime. For
	// segment protocols (HLS/DASH) it is the deterministic fingerprint hash
	// (so reconnects within the idle window collapse onto one session); for
	// connection-bound protocols (RTMP/SRT/RTSP) it is a random UUID.
	ID string `json:"id"`

	// StreamCode is the foreign key to Stream.Code.
	StreamCode StreamCode `json:"stream_code"`

	// Protocol is the wire protocol used to deliver this session.
	Protocol SessionProto `json:"proto"`

	// IP is the remote peer address (no port). For HTTP-based protocols this
	// is the X-Forwarded-For head when present, otherwise the RemoteAddr.
	IP string `json:"ip"`

	// UserAgent is the browser/player User-Agent (HTTP) or the equivalent
	// flashVer field for RTMP. May be empty.
	UserAgent string `json:"user_agent,omitempty"`

	// Referer is the HTTP Referer when present (HLS/DASH only).
	Referer string `json:"referer,omitempty"`

	// QueryString is the raw query of the FIRST request that opened the
	// session (HLS/DASH) — useful when token/abr-variant info is encoded there.
	QueryString string `json:"query_string,omitempty"`

	// Token is the value of `?token=` from the first request, when present.
	// When set, NamedBy is SessionNamedByToken.
	Token string `json:"token,omitempty"`

	// UserName is a human label. Resolved from token claims if available,
	// otherwise the fingerprint hash short form. Empty when no identity could
	// be resolved.
	UserName string `json:"user_name,omitempty"`

	// NamedBy records how UserName was resolved.
	NamedBy SessionNamedBy `json:"named_by,omitempty"`

	// Country is an ISO 3166-1 alpha-2 country code from the configured
	// GeoIP resolver, or "" when GeoIP is disabled / lookup failed.
	Country string `json:"country,omitempty"`

	// Bytes is the cumulative bytes sent to this client since session open.
	// HLS/DASH: sum of segment + manifest response bodies. RTMP/SRT: write
	// bytes from the publisher's outbound pipeline.
	Bytes int64 `json:"bytes"`

	// Secure is true when the underlying transport was TLS / SRTS / RTMPS.
	Secure bool `json:"secure"`

	// DVR is true when the playlist request carried any timeshift query
	// parameter (from / delay / dur / ago) — false for live-edge hits. Set
	// by sessions.HTTPMiddleware after detecting the query shape. The flag
	// also participates in the session fingerprint, so a viewer watching
	// both live and timeshift gets two distinct session records.
	DVR bool `json:"dvr"`

	// OpenedAt is the time the first activity for this session was observed.
	OpenedAt time.Time `json:"opened_at"`

	// StartedAt is when the first byte of media was delivered (segment data
	// for HLS/DASH, first frame after RTMP play handshake). Zero before
	// media flows.
	StartedAt time.Time `json:"started_at,omitempty"`

	// UpdatedAt is the time of the most recent activity. Idle reaper closes
	// sessions whose UpdatedAt is older than (now - idle_timeout).
	UpdatedAt time.Time `json:"updated_at"`

	// ClosedAt is set when the session ends. nil while active.
	ClosedAt *time.Time `json:"closed_at,omitempty"`

	// CloseReason explains why the session ended. Empty while active.
	CloseReason SessionCloseReason `json:"close_reason,omitempty"`
}

// Duration returns the elapsed time between OpenedAt and either ClosedAt
// (when set) or now. Zero when OpenedAt is the zero value.
func (s *PlaySession) Duration() time.Duration {
	if s.OpenedAt.IsZero() {
		return 0
	}
	end := s.UpdatedAt
	if s.ClosedAt != nil {
		end = *s.ClosedAt
	}
	if end.Before(s.OpenedAt) {
		return 0
	}
	return end.Sub(s.OpenedAt)
}

// Active reports whether the session is still considered open (ClosedAt unset).
func (s *PlaySession) Active() bool {
	return s.ClosedAt == nil
}

// Session-related event types emitted on the bus when sessions open / close.
// Hooks can subscribe to persist analytics, send notifications, etc.
const (
	EventSessionOpened EventType = "session.opened"
	EventSessionClosed EventType = "session.closed"
)
