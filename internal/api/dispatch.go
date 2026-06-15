package api

import (
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/dvr/blob"
	"github.com/datvietvac-techhub/open-streamer/internal/mediaauth"
	"github.com/datvietvac-techhub/open-streamer/internal/mediaserve"
)

// mediaAllowed runs the media-plane authorization chain for an HTTP playback
// request (HLS/DASH/MPEGTS). Returns true (allow) when no authorizer is wired
// or media auth is disabled. The client IP comes from r.RemoteAddr, which the
// RealIP middleware has already set from the trusted proxy's forwarded header.
func (s *Server) mediaAllowed(r *http.Request, code domain.StreamCode) bool {
	if s.mediaAuth == nil {
		return true
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	d := s.mediaAuth.Authorize(mediaauth.AuthRequest{
		// Key auth on the parent stream: ABR renditions live under
		// /<code>/track_<N>/… so a token minted for <code> must cover them.
		Code:      stripABRTrackSlug(code),
		ClientIP:  net.ParseIP(host),
		Token:     r.URL.Query().Get("token"),
		UserAgent: r.UserAgent(),
		Referer:   r.Referer(),
	})
	if !d.Allow {
		slog.Info("api: playback denied", "stream_code", code, "reason", d.Reason, "remote", r.RemoteAddr)
	}
	return d.Allow
}

// stripABRTrackSlug removes a trailing "/track_<N>" rendition segment so media
// auth keys on the parent stream code — a token minted for <code> covers all
// of its ABR renditions (served at /<code>/track_<N>/…). Mirrors the sessions
// tracker's path handling; the slug is a closed internal namespace.
func stripABRTrackSlug(code domain.StreamCode) domain.StreamCode {
	s := string(code)
	if i := strings.LastIndexByte(s, '/'); i > 0 && isABRTrackSlug(s[i+1:]) {
		return domain.StreamCode(s[:i])
	}
	return code
}

func isABRTrackSlug(s string) bool {
	const prefix = "track_"
	if !strings.HasPrefix(s, prefix) || len(s) == len(prefix) {
		return false
	}
	for _, c := range s[len(prefix):] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// dispatchStreamsSubpath handles every URL under /streams/<...>. The chi
// catch-all captures the suffix after /streams/ in the "*" URL param.
//
// Layout:
//
//	/streams/<code>           — GET (status), POST (put), DELETE
//	/streams/<code>/restart   — POST
//	/streams/<code>/switch    — POST (force-switch ingest priority)
//
// <code> may contain '/' (namespacing) so the trailing segment is peeled
// off only when it matches a reserved action. A code that happens to END
// in /restart or /switch must be addressed via the corresponding action —
// there is no escape syntax — but that's an acceptable namespace constraint
// for an admin API.
func (s *Server) dispatchStreamsSubpath() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		suffix, ok := sanitizeSubpath(chi.URLParam(r, "*"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		code, action := splitTailAction(suffix, streamActionRestart, streamActionSwitch)
		if err := domain.ValidateStreamCode(code); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r = setURLParam(r, "code", code)

		switch action {
		case streamActionRestart:
			if r.Method != http.MethodPost {
				w.Header().Set("Allow", http.MethodPost)
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			s.streamH.Restart(w, r)
		case streamActionSwitch:
			if r.Method != http.MethodPost {
				w.Header().Set("Allow", http.MethodPost)
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			s.streamH.SwitchInput(w, r)
		default:
			switch r.Method {
			case http.MethodGet:
				s.streamH.Get(w, r)
			case http.MethodPost:
				s.streamH.Put(w, r)
			case http.MethodDelete:
				s.streamH.Delete(w, r)
			default:
				w.Header().Set("Allow", strings.Join(
					[]string{http.MethodGet, http.MethodPost, http.MethodDelete}, ", "))
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			}
		}
	}
}

// dispatchMedia handles every URL of shape /<code>/<file-or-action>. Mounted
// last in the router so it only fires for paths not claimed by /streams,
// /sessions, /hooks, /watermarks, /vod, /config, /metrics, /swagger, etc.
//
// Tail dispatch:
//
//	mpegts                 — live MPEG-TS over HTTP (publisher subscription)
//	recording_status.json  — CMAF DVR metadata JSON (status, range, size, gaps)
//	index.m3u8 + DVR query — CMAF timeshift slice (from/delay/dur/ago params)
//	index.m3u8 (no query)  — live HLS playlist (file served from disk)
//	index.mpd              — DASH manifest (file served from disk)
//	<*.ts|*.m4s|*.mp4>     — segment file served from disk
//
// Unknown tails or codes that fail validation return 404 so the catch-all
// can't be used to enumerate the host's directory tree.
func (s *Server) dispatchMedia() http.HandlerFunc {
	roots := mediaserve.Roots{HLSDir: s.hlsDir, DASHDir: s.dashDir}
	return func(w http.ResponseWriter, r *http.Request) {
		suffix, ok := sanitizeSubpath(chi.URLParam(r, "*"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		code, file, ok := splitCodeAndFile(suffix)
		if !ok {
			http.NotFound(w, r)
			return
		}
		if err := domain.ValidateStreamCode(code); err != nil {
			http.NotFound(w, r)
			return
		}
		// Media-plane authorization (token / IP / country / UA / referer) for
		// all HTTP playback (HLS / DASH / MPEGTS) — B / S-13. No-op when media
		// auth is disabled.
		if !s.mediaAllowed(r, domain.StreamCode(code)) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		r = setURLParam(r, "code", code)

		switch file {
		case mediaActionMPEGTS:
			if s.pub == nil {
				http.NotFound(w, r)
				return
			}
			s.pub.HandleMPEGTS()(w, r)
			return
		case mediaFileRecordingStatusJS:
			s.blobH.RecordingStatusJSON(w, r)
			return
		case mediaFileHLSIndex:
			// DVR timeshift is served from the CMAF blob archive.
			if isDVRPlaybackRequest(r) && s.blobH.IsBlob(r, domain.StreamCode(code)) {
				s.blobH.ServeTimeshift(w, r)
				return
			}
		case mediaFileDASHIndex:
			if isDVRPlaybackRequest(r) && s.blobH.IsBlob(r, domain.StreamCode(code)) {
				s.blobH.ServeMPD(w, r)
				return
			}
		}
		// Blob-archive timeshift inits/fragments carry a flat dvri-/dvrf-/dvrt-
		// name (dvrf- = HLS hour+seq, dvrt- = DASH $Time$).
		if blob.IsBlobMediaFile(file) {
			r = setURLParam(r, "file", file)
			switch {
			case strings.HasPrefix(file, "dvri-"):
				s.blobH.ServeInit(w, r)
			case strings.HasPrefix(file, "dvrt-"):
				s.blobH.ServeTimeFragment(w, r)
			default:
				s.blobH.ServeFragment(w, r)
			}
			return
		}
		// Fall-through: serve the file by extension. index.m3u8 / index.mpd
		// land here when no DVR query params are present; live segments always
		// land here.
		roots.ServeFile(w, r, code, file)
	}
}

// isDVRPlaybackRequest mirrors sessions.isDVRPlayback. Kept here to avoid
// cross-package coupling for what is a one-line query check.
func isDVRPlaybackRequest(r *http.Request) bool {
	q := r.URL.Query()
	return q.Has("from") || q.Has("delay") || q.Has("dur") || q.Has("ago")
}
