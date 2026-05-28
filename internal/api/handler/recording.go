package handler

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/dvr"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/samber/do/v2"
)

// msgRecordingNoData is the user-facing reason returned whenever a stream
// has no recording payload yet — either the Recording row exists with an
// empty SegmentDir (writer never armed) or its index.json hasn't been
// written. Shared so the wording stays consistent across all DVR routes.
const msgRecordingNoData = "recording has no data yet"

// dvrIndexReader narrows *dvr.Service to the on-disk-read methods this
// handler uses, so tests can stub out the DVR service without wiring a
// full buffer + bus + metrics graph behind it. *dvr.Service satisfies
// implicitly.
type dvrIndexReader interface {
	LoadIndex(segDir string) (*domain.DVRIndex, error)
	ParsePlaylist(segDir string) ([]dvr.SegmentMeta, error)
}

// RecordingHandler serves DVR metadata and timeshift playlists under
// /<code>/recording_status.json and /<code>/index.m3u8?<timeshift-params>.
// The standalone /recordings/{rid} CRUD surface was dropped — recordings
// are 1:1 with streams and addressed by stream code throughout.
type RecordingHandler struct {
	dvr     dvrIndexReader
	recRepo store.RecordingRepository
}

// NewRecordingHandler creates a RecordingHandler and registers it with the DI injector.
func NewRecordingHandler(i do.Injector) (*RecordingHandler, error) {
	return &RecordingHandler{
		dvr:     do.MustInvoke[*dvr.Service](i),
		recRepo: do.MustInvoke[store.RecordingRepository](i),
	}, nil
}

// RecordingStatusJSON returns full DVR metadata for a stream's recording:
// dvr_range, gaps, segment count, total size, lifecycle status. Served at
// the file path /<code>/recording_status.json so players and ops scripts
// can fetch it from the same namespace as the playlist.
//
// @Summary  DVR recording status
// @Tags     dvr
// @Produce  json
// @Param    code path string true "Stream code (the recording ID)"
// @Success  200 {object} map[string]any
// @Failure  404 {object} apidocs.ErrorBody
// @Router   /{code}/recording_status.json [get].
func (h *RecordingHandler) RecordingStatusJSON(w http.ResponseWriter, r *http.Request) {
	code := domain.StreamCode(chi.URLParam(r, "code"))
	if err := domain.ValidateStreamCode(string(code)); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_STREAM_CODE", err.Error())
		return
	}

	rec, err := h.recRepo.FindByID(r.Context(), domain.RecordingID(code))
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	if rec.SegmentDir == "" {
		writeError(w, http.StatusNotFound, "NO_DATA", msgRecordingNoData)
		return
	}

	idx, err := h.dvr.LoadIndex(rec.SegmentDir)
	if err != nil {
		serverError(w, r, "INDEX_READ_FAILED", "load dvr index", err)
		return
	}
	if idx == nil {
		writeError(w, http.StatusNotFound, "NO_DATA", msgRecordingNoData)
		return
	}

	type dvrRange struct {
		StartedAt     time.Time `json:"started_at"`
		LastSegmentAt time.Time `json:"last_segment_at,omitempty"`
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": map[string]any{
			"stream_code": rec.StreamCode,
			"status":      rec.Status,
			"dvr_range": dvrRange{
				StartedAt:     idx.StartedAt,
				LastSegmentAt: idx.LastSegmentAt,
			},
			"gaps":             idx.Gaps,
			"segment_count":    idx.SegmentCount,
			"total_size_bytes": idx.TotalSizeBytes,
		},
	})
}

// ServeTimeshift builds a dynamic VOD M3U8 sliced over a wall-clock window,
// invoked by the media catch-all when /<code>/index.m3u8 carries any DVR
// timeshift query params. The live playlist (no params) is served directly
// from disk by mediaserve and never reaches this handler.
//
// Query parameters:
//   - from=<unix_seconds>  absolute start time
//   - delay=<seconds>      start `delay` seconds before now (live-edge offset)
//   - ago=<seconds>        alias of delay
//   - dur=<seconds>        clip length; omit for "everything from start"
//
// Precedence: `from` > `delay` > `ago`. When none of the start params are
// set the handler treats it as "from the beginning of the recording".
//
// @Summary  HLS timeshift playlist (DVR)
// @Tags     dvr
// @Produce  plain
// @Param    code  path  string true  "Stream code"
// @Param    from  query int    false "Absolute start (Unix seconds)"
// @Param    delay query int    false "Start N seconds before now"
// @Param    ago   query int    false "Alias of delay"
// @Param    dur   query int    false "Clip duration in seconds"
// @Success  200 {string} string "VOD m3u8"
// @Failure  400 {object} apidocs.ErrorBody
// @Failure  404 {object} apidocs.ErrorBody
// @Router   /{code}/index.m3u8 [get].
func (h *RecordingHandler) ServeTimeshift(w http.ResponseWriter, r *http.Request) {
	code := domain.StreamCode(chi.URLParam(r, "code"))
	if err := domain.ValidateStreamCode(string(code)); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_STREAM_CODE", err.Error())
		return
	}
	rec, err := h.recRepo.FindByID(r.Context(), domain.RecordingID(code))
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	if rec.SegmentDir == "" {
		writeError(w, http.StatusNotFound, "NO_DATA", msgRecordingNoData)
		return
	}

	segments, err := h.dvr.ParsePlaylist(rec.SegmentDir)
	if err != nil || len(segments) == 0 {
		writeError(w, http.StatusNotFound, "NO_PLAYLIST", "playlist not yet available")
		return
	}

	startTime, ok := parseTimeshiftStart(r, segments[0].WallTime)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMS",
			"from must be Unix seconds; delay/ago must be non-negative numbers")
		return
	}

	windowDur, ok := parseTimeshiftDuration(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMS",
			"dur must be a positive number of seconds")
		return
	}

	window := sliceSegments(segments, startTime, windowDur)
	if len(window) == 0 {
		writeError(w, http.StatusNotFound, "NO_SEGMENTS_IN_RANGE",
			"no segments found for the requested time range")
		return
	}

	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(renderTimeshiftPlaylist(window)))
}

// parseTimeshiftStart returns the absolute start time selected by the
// caller's timeshift params. Order: from > delay > ago > recordingStart.
// Returns ok=false on malformed numbers.
func parseTimeshiftStart(r *http.Request, recordingStart time.Time) (time.Time, bool) {
	q := r.URL.Query()
	if raw := q.Get("from"); raw != "" {
		secs, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || secs < 0 {
			return time.Time{}, false
		}
		return time.Unix(secs, 0), true
	}
	for _, key := range []string{"delay", "ago"} {
		if raw := q.Get(key); raw != "" {
			secs, err := strconv.ParseFloat(raw, 64)
			if err != nil || secs < 0 {
				return time.Time{}, false
			}
			return time.Now().Add(-time.Duration(secs * float64(time.Second))), true
		}
	}
	return recordingStart, true
}

// parseTimeshiftDuration parses the optional `dur` window. Zero means
// "from start to end of available recording".
func parseTimeshiftDuration(r *http.Request) (time.Duration, bool) {
	raw := r.URL.Query().Get("dur")
	if raw == "" {
		return 0, true
	}
	secs, err := strconv.ParseFloat(raw, 64)
	if err != nil || secs <= 0 {
		return 0, false
	}
	return time.Duration(secs * float64(time.Second)), true
}

// sliceSegments returns the contiguous run of segments whose wall-time
// interval intersects [startTime, startTime+windowDur). windowDur=0 means
// "no upper bound — include everything from startTime onward".
func sliceSegments(segments []dvr.SegmentMeta, startTime time.Time, windowDur time.Duration) []dvr.SegmentMeta {
	endTime := startTime.Add(windowDur)
	out := make([]dvr.SegmentMeta, 0, len(segments))
	for _, seg := range segments {
		if seg.WallTime.IsZero() {
			continue
		}
		segEnd := seg.WallTime.Add(seg.Duration)
		if segEnd.Before(startTime) || segEnd.Equal(startTime) {
			continue
		}
		if windowDur > 0 && seg.WallTime.After(endTime) {
			break
		}
		out = append(out, seg)
	}
	return out
}

// renderTimeshiftPlaylist emits a VOD-style M3U8 for the given slice.
func renderTimeshiftPlaylist(window []dvr.SegmentMeta) string {
	maxSec := 1.0
	for _, seg := range window {
		if d := seg.Duration.Seconds(); d > maxSec {
			maxSec = d
		}
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:3\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", int(math.Ceil(maxSec)))
	b.WriteString("#EXT-X-PLAYLIST-TYPE:VOD\n")
	b.WriteByte('\n')

	needDateTime := true
	for _, seg := range window {
		if seg.Discontinuity {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
			needDateTime = true
		}
		if needDateTime && !seg.WallTime.IsZero() {
			fmt.Fprintf(&b, "#EXT-X-PROGRAM-DATE-TIME:%s\n",
				seg.WallTime.UTC().Format("2006-01-02T15:04:05.000Z"))
			needDateTime = false
		}
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n", seg.Duration.Seconds())
		fmt.Fprintf(&b, "%06d.ts\n", seg.Index)
	}
	b.WriteString("#EXT-X-ENDLIST\n")
	return b.String()
}
