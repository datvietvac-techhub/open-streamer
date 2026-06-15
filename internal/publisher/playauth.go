package publisher

// playauth.go — media-plane (playback) authorization glue for the publisher's
// non-HTTP protocols (RTMP / SRT / RTSP). The HTTP protocols (HLS / DASH /
// MPEGTS) are gated in the api dispatcher. See internal/mediaauth (B / S-13).

import (
	"log/slog"
	"net"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/mediaauth"
)

// SetMediaAuthorizer wires the playback authorizer. nil disables media auth
// (every playback request is allowed) — the default.
func (s *Service) SetMediaAuthorizer(a *mediaauth.Authorizer) { s.mediaAuth = a }

// PlaybackPolicy returns a RUNNING stream's bound media-auth Policy code ("" =
// no policy → public) and whether the stream is currently running. O(1)
// in-memory lookup, used as the first tier of the authorizer's PolicyResolver so
// the live hot path never reads the store. The `running` flag lets the caller
// fall back to the store for a STOPPED stream (whose DVR archive is still
// served) instead of mistaking its empty in-memory binding for "no policy".
func (s *Service) PlaybackPolicy(code domain.StreamCode) (policy domain.PolicyCode, running bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ss, ok := s.streams[code]; ok {
		return ss.playbackPolicy, true
	}
	return "", false
}

// PushStreamKey returns a RUNNING stream's configured push-ingest secret
// (its resolved StreamKey; "" = no secret → push unauthenticated for that
// stream) and whether the stream is currently running. O(1) in-memory lookup,
// the first tier of the RTMP server's push-auth resolver (S-8) so the connect
// path avoids a store read for live streams — including auto-publish runtime
// streams that have no on-disk record. The `running` flag lets the caller fall
// back to the store + template for a stopped/configured stream.
func (s *Service) PushStreamKey(code domain.StreamCode) (streamKey string, running bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ss, ok := s.streams[code]; ok {
		return ss.streamKey, true
	}
	return "", false
}

// playAllowed runs the media-auth chain for one playback request. Returns true
// (allow) when no authorizer is wired or media auth is disabled. Denials are
// logged with the (non-leaky) reason.
func (s *Service) playAllowed(code domain.StreamCode, proto, addr, token, ua, referer string) bool {
	if s.mediaAuth == nil {
		return true
	}
	d := s.mediaAuth.Authorize(mediaauth.AuthRequest{
		Code:      code,
		ClientIP:  ipFromAddr(addr),
		Token:     token,
		UserAgent: ua,
		Referer:   referer,
	})
	if !d.Allow {
		slog.Info("publisher: playback denied", "stream_code", code, "proto", proto, "reason", d.Reason, "remote", addr)
	}
	return d.Allow
}

// ipFromAddr extracts the IP from a "host:port" (or bare host) address string.
func ipFromAddr(addr string) net.IP {
	if addr == "" {
		return nil
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return net.ParseIP(host)
	}
	return net.ParseIP(addr)
}
