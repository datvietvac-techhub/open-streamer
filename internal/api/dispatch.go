package api

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/dvr/blob"
	"github.com/datvietvac-techhub/open-streamer/internal/mediaserve"
)

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
//	recording_status.json  — DVR metadata JSON (segment_count, dvr_range, …)
//	index.m3u8 + DVR query — timeshift slice (from/delay/dur/ago params)
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
			s.recordingH.RecordingStatusJSON(w, r)
			return
		case mediaFileHLSIndex:
			if isDVRPlaybackRequest(r) {
				// Blob-archive recordings serve via the CMAF handler; legacy
				// .ts recordings via the per-segment handler.
				if s.blobH.IsBlob(r, domain.StreamCode(code)) {
					s.blobH.ServeTimeshift(w, r)
				} else {
					s.recordingH.ServeTimeshift(w, r)
				}
				return
			}
		}
		// Blob-archive timeshift inits/fragments carry a flat dvri-/dvrf- name.
		if blob.IsBlobMediaFile(file) {
			r = setURLParam(r, "file", file)
			if strings.HasPrefix(file, "dvri-") {
				s.blobH.ServeInit(w, r)
			} else {
				s.blobH.ServeFragment(w, r)
			}
			return
		}
		// Legacy DVR timeshift segments carry the dvr_ prefix and live in the
		// recording store, not the sliding live-HLS window — route them to the
		// DVR handler so they don't 404 against the live media roots.
		if strings.HasPrefix(file, "dvr_") {
			r = setURLParam(r, "file", file)
			s.recordingH.ServeSegment(w, r)
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
