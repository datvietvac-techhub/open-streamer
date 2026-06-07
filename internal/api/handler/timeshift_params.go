package handler

import (
	"net/http"
	"strconv"
	"time"
)

// timeshift_params.go — shared parsing for DVR timeshift query params, used by
// the CMAF blob timeshift handler.

// msgRecordingNoData is the user-facing reason returned whenever a stream has no
// recording payload yet (no Recording row, empty SegmentDir, or no catalog).
// Shared so the wording stays consistent across DVR routes.
const msgRecordingNoData = "recording has no data yet"

// parseTimeshiftStart returns the absolute start time selected by the caller's
// timeshift params. Order: from > delay > ago > recordingStart. Returns
// ok=false on malformed numbers.
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

// parseTimeshiftDuration parses the optional `dur` window. Zero means "from
// start to end of the available recording".
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
