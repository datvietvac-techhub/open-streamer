package handler

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
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

// Migrate converts a stopped legacy `.ts` recording into the CMAF blob archive
// in place (POST /streams/{code}/migrate). The caller must ensure the recording
// is stopped first — migration is an offline replay. Optional query params:
// `prune=1` deletes the legacy files on success; `segdur=<sec>` overrides the
// fragment target duration.
func (h *BlobTimeshiftHandler) Migrate(w http.ResponseWriter, r *http.Request) {
	code := domain.StreamCode(chi.URLParam(r, "code"))
	rec, err := h.recRepo.FindByID(r.Context(), domain.RecordingID(code))
	if err != nil || rec.SegmentDir == "" {
		writeError(w, http.StatusNotFound, "NO_DATA", msgRecordingNoData)
		return
	}
	if !blob.IsLegacyRecording(rec.SegmentDir) {
		writeError(w, http.StatusConflict, "NOT_LEGACY", "no un-migrated legacy recording for this stream")
		return
	}
	opts := blob.MigrateOptions{StreamCode: string(code), Prune: r.URL.Query().Get("prune") == "1"}
	if v := r.URL.Query().Get("segdur"); v != "" {
		if sec, perr := strconv.Atoi(v); perr == nil && sec > 0 {
			opts.SegDur = time.Duration(sec) * time.Second
		}
	}
	res, err := blob.Migrate(context.WithoutCancel(r.Context()), rec.SegmentDir, opts)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "MIGRATE_FAILED", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": res})
}

// ServeMPD serves the static DASH manifest for the timeshift window. It queries
// every profile for the requested [from, from+dur) slice and hands the resolved
// per-profile windows to blob.RenderMPD (one video AdaptationSet across covered
// profiles + the shared audio AdaptationSet). DASH timeshift exists only for
// blob archives; the dispatcher gates this behind IsBlob.
func (h *BlobTimeshiftHandler) ServeMPD(w http.ResponseWriter, r *http.Request) {
	br, ok := h.reader(r)
	if !ok {
		writeError(w, http.StatusNotFound, "NO_DATA", msgRecordingNoData)
		return
	}
	cat := br.Catalog()
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
	windows := make(map[string]*blob.Window, len(cat.Profiles))
	for _, p := range cat.Profiles {
		win, err := br.Query(p.ID, start, dur)
		if err != nil {
			continue
		}
		windows[p.ID] = win
	}
	mpd, err := blob.RenderMPD(cat, windows, blob.DefaultSegDur)
	if err != nil {
		writeError(w, http.StatusNotFound, "NO_SEGMENTS_IN_RANGE", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/dash+xml")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(mpd)
}

// ServeTimeFragment serves one DASH fragment addressed by media-time tick
// (dvrt-<profile>-<track>-<ticks>.m4s).
func (h *BlobTimeshiftHandler) ServeTimeFragment(w http.ResponseWriter, r *http.Request) {
	br, ok := h.reader(r)
	if !ok {
		http.NotFound(w, r)
		return
	}
	profile, track, ticks, okN := blob.ParseTimeFragmentName(chi.URLParam(r, "file"))
	if !okN {
		http.NotFound(w, r)
		return
	}
	data, err := br.FragmentByTime(profile, track, ticks)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "video/mp4")
	_, _ = w.Write(data)
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
