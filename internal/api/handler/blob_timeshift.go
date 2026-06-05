package handler

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/samber/do/v2"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/dvr/blob"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

// BlobTimeshiftHandler serves CMAF blob-archive timeshift: the multivariant
// master, per-profile/track media playlists, inits, and fragments. It coexists
// with the legacy RecordingHandler — the dispatcher routes here when a stream's
// recording is a blob archive (catalog.json present).
type BlobTimeshiftHandler struct {
	recRepo store.RecordingRepository
}

// NewBlobTimeshiftHandler constructs the handler and registers it with DI.
func NewBlobTimeshiftHandler(i do.Injector) (*BlobTimeshiftHandler, error) {
	return &BlobTimeshiftHandler{recRepo: do.MustInvoke[store.RecordingRepository](i)}, nil
}

// IsBlob reports whether the stream's recording is a blob archive (so the
// dispatcher can pick this handler over the legacy .ts one).
func (h *BlobTimeshiftHandler) IsBlob(r *http.Request, code domain.StreamCode) bool {
	rec, err := h.recRepo.FindByID(r.Context(), domain.RecordingID(code))
	if err != nil || rec.SegmentDir == "" {
		return false
	}
	return blob.HasCatalog(rec.SegmentDir)
}

// reader resolves the per-stream archive reader for the request.
func (h *BlobTimeshiftHandler) reader(r *http.Request) (*blob.Reader, bool) {
	code := domain.StreamCode(chi.URLParam(r, "code"))
	if err := domain.ValidateStreamCode(string(code)); err != nil {
		return nil, false
	}
	rec, err := h.recRepo.FindByID(r.Context(), domain.RecordingID(code))
	if err != nil || rec.SegmentDir == "" {
		return nil, false
	}
	br, err := blob.NewReader(rec.SegmentDir)
	if err != nil {
		return nil, false
	}
	return br, true
}

// ServeTimeshift serves the master playlist (no `profile` param) or a
// per-profile, per-track media playlist (`profile=pN[&track=a]`).
func (h *BlobTimeshiftHandler) ServeTimeshift(w http.ResponseWriter, r *http.Request) {
	br, ok := h.reader(r)
	if !ok {
		writeError(w, http.StatusNotFound, "NO_DATA", msgRecordingNoData)
		return
	}
	cat := br.Catalog()
	q := r.URL.Query()
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "no-cache")

	if q.Get("profile") == "" {
		_, _ = w.Write([]byte(blob.RenderMaster(cat, timeshiftParams(q))))
		return
	}
	start, ok := parseTimeshiftStart(r, time.UnixMilli(cat.RecordingMediaOriginUnixMs).UTC())
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMS", "from must be Unix seconds; delay/ago non-negative")
		return
	}
	dur, ok := parseTimeshiftDuration(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMS", "dur must be a positive number of seconds")
		return
	}
	win, err := br.Query(q.Get("profile"), start, dur)
	if err != nil {
		writeError(w, http.StatusNotFound, "NO_SEGMENTS_IN_RANGE", err.Error())
		return
	}
	track := blob.TrackVideo
	if q.Get("track") == "a" {
		track = blob.TrackAudio
	}
	_, _ = w.Write([]byte(blob.RenderMediaPlaylist(win, track)))
}

// ServeInit serves a track init (dvri-<profile>-<track>.mp4).
func (h *BlobTimeshiftHandler) ServeInit(w http.ResponseWriter, r *http.Request) {
	br, ok := h.reader(r)
	if !ok {
		http.NotFound(w, r)
		return
	}
	profile, track, okN := blob.ParseInitName(chi.URLParam(r, "file"))
	if !okN {
		http.NotFound(w, r)
		return
	}
	data, err := br.ReadInit(profile, track)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "video/mp4")
	_, _ = w.Write(data)
}

// ServeFragment serves one fragment (dvrf-<profile>-<hour>-<track>-<seq>.m4s).
func (h *BlobTimeshiftHandler) ServeFragment(w http.ResponseWriter, r *http.Request) {
	br, ok := h.reader(r)
	if !ok {
		http.NotFound(w, r)
		return
	}
	profile, hourC, track, seq, okN := blob.ParseFragmentName(chi.URLParam(r, "file"))
	if !okN {
		http.NotFound(w, r)
		return
	}
	data, err := br.FragmentByHour(profile, hourC, track, seq)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "video/mp4")
	_, _ = w.Write(data)
}

// timeshiftParams rebuilds the from/dur/delay/ago query for the master's child
// URLs (profile/track are appended by the renderer).
func timeshiftParams(q url.Values) string {
	parts := make([]string, 0, 4)
	for _, k := range []string{"from", "dur", "delay", "ago"} {
		if v := q.Get(k); v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	return strings.Join(parts, "&")
}
