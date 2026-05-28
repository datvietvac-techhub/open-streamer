package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// GET /config/defaults must echo every constant from internal/domain/defaults.go
// — frontend uses it as the source of truth for form placeholders.
func TestGetConfigDefaultsShape(t *testing.T) {
	t.Parallel()
	h := &ConfigHandler{}

	w := httptest.NewRecorder()
	h.GetConfigDefaults(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/config/defaults", nil))
	require.Equal(t, http.StatusOK, w.Code)

	var got configDefaultsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))

	assert.Equal(t, domain.DefaultBufferCapacity, got.Buffer.Capacity)
	assert.Equal(t, domain.DefaultInputPacketTimeoutSec, got.Manager.InputPacketTimeoutSec)

	assert.Equal(t, domain.DefaultLiveSegmentSec, got.Publisher.HLS.LiveSegmentSec)
	assert.Equal(t, domain.DefaultLiveWindow, got.Publisher.HLS.LiveWindow)
	assert.Equal(t, domain.DefaultLiveHistory, got.Publisher.HLS.LiveHistory)
	assert.False(t, got.Publisher.HLS.LiveEphemeral)
	assert.Equal(t, domain.DefaultLiveSegmentSec, got.Publisher.DASH.LiveSegmentSec)
	assert.False(t, got.Publisher.DASH.LiveEphemeral)

	assert.Equal(t, domain.DefaultHookMaxRetries, got.Hook.MaxRetries)
	assert.Equal(t, domain.DefaultHookTimeoutSec, got.Hook.TimeoutSec)
	assert.Equal(t, domain.DefaultHookBatchMaxItems, got.Hook.BatchMaxItems)
	assert.Equal(t, domain.DefaultHookBatchFlushIntervalSec, got.Hook.BatchFlushIntervalSec)
	assert.Equal(t, domain.DefaultHookBatchMaxQueueItems, got.Hook.BatchMaxQueueItems)

	assert.Equal(t, domain.DefaultPushTimeoutSec, got.Push.TimeoutSec)
	assert.Equal(t, domain.DefaultPushRetryTimeoutSec, got.Push.RetryTimeoutSec)

	assert.Equal(t, domain.DefaultDVRSegmentDuration, got.DVR.SegmentDuration)

	assert.Equal(t, domain.DefaultVideoBitrateK, got.Transcoder.Video.BitrateK)
	assert.Equal(t, domain.ResizeModePad, got.Transcoder.Video.ResizeMode)
	assert.Equal(t, domain.AudioCodecAAC, got.Transcoder.Audio.Codec)
	assert.Equal(t, domain.DefaultAudioBitrateK, got.Transcoder.Audio.BitrateK)
	assert.Equal(t, domain.HWAccelNone, got.Transcoder.Global.HW)
	assert.Equal(t, 0, got.Transcoder.Global.DeviceID)

	assert.Equal(t, 1935, got.Listeners.RTMP.Port)
	assert.Equal(t, domain.DefaultListenHost, got.Listeners.RTMP.ListenHost)
	assert.Equal(t, 554, got.Listeners.RTSP.Port)
	assert.Equal(t, domain.DefaultListenHost, got.Listeners.RTSP.ListenHost)
	assert.Equal(t, "tcp", got.Listeners.RTSP.Transport)
	assert.Equal(t, 9999, got.Listeners.SRT.Port)
	assert.Equal(t, domain.DefaultListenHost, got.Listeners.SRT.ListenHost)
	assert.Equal(t, domain.DefaultSRTLatencyMS, got.Listeners.SRT.LatencyMS)

	assert.Equal(t, domain.DefaultHLSPlaylistTimeoutSec, got.Ingestor.HLSPlaylistTimeoutSec)
	assert.Equal(t, domain.DefaultHLSSegmentTimeoutSec, got.Ingestor.HLSSegmentTimeoutSec)
	assert.Equal(t, domain.DefaultHLSMaxSegmentBuffer, got.Ingestor.HLSMaxSegmentBuffer)
	assert.Equal(t, domain.DefaultRTMPTimeoutSec, got.Ingestor.RTMPTimeoutSec)
	assert.Equal(t, domain.DefaultRTSPTimeoutSec, got.Ingestor.RTSPTimeoutSec)
}

// Codec routing table: empty codec + HW=nvenc must resolve to h264_nvenc;
// empty codec + HW=none must resolve to libx264. The DefaultCodec field
// tells frontend which family to look up when user hasn't picked one.
func TestGetConfigDefaults_CodecRoutingTable(t *testing.T) {
	t.Parallel()
	h := &ConfigHandler{}

	w := httptest.NewRecorder()
	h.GetConfigDefaults(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/config/defaults", nil))
	require.Equal(t, http.StatusOK, w.Code)

	var got configDefaultsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))

	assert.Equal(t, "h264", got.Transcoder.Video.Codec,
		"default codec family must be h264 — most-compatible across players")

	table := got.Transcoder.Video.EncoderByCodecHW
	require.Contains(t, table, "h264")
	require.Contains(t, table, "h265")
	require.Contains(t, table, "av1")
	require.Contains(t, table, "mp2v")
	require.NotContains(t, table, "vp9", "vp9 is no longer an exposed encoder option")

	// Pin every cell of the h264/h265 rows that frontend will touch most.
	assert.Equal(t, "libx264", table["h264"]["none"])
	assert.Equal(t, "h264_nvenc", table["h264"]["nvenc"])
	assert.Equal(t, "libx264", table["h264"]["vaapi"], "no implicit vaapi routing for empty/h264")
	assert.Equal(t, "libx264", table["h264"]["qsv"])
	assert.Equal(t, "libx264", table["h264"]["videotoolbox"])

	assert.Equal(t, "libx265", table["h265"]["none"])
	assert.Equal(t, "hevc_nvenc", table["h265"]["nvenc"])

	// AV1 / MPEG-2 ignore HW backend (CPU-only encoder paths).
	for _, hw := range []string{"none", "nvenc", "vaapi", "qsv", "videotoolbox"} {
		assert.Equal(t, "libsvtav1", table["av1"][hw], "av1 always libsvtav1 (no HW route)")
		assert.Equal(t, "mpeg2video", table["mp2v"][hw], "mp2v always mpeg2video (no HW route)")
	}
}

// StoragePathTemplate must use the {streamCode} placeholder so frontend
// can substitute it client-side per-stream.
func TestGetConfigDefaults_DVRStoragePathTemplate(t *testing.T) {
	t.Parallel()
	h := &ConfigHandler{}

	w := httptest.NewRecorder()
	h.GetConfigDefaults(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/config/defaults", nil))
	require.Equal(t, http.StatusOK, w.Code)

	var got configDefaultsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))

	assert.True(t, strings.HasPrefix(got.DVR.StoragePathTemplate, domain.DefaultDVRRoot+"/"),
		"template must start with the DVR root: %q", got.DVR.StoragePathTemplate)
	assert.Contains(t, got.DVR.StoragePathTemplate, "{streamCode}",
		"template must include {streamCode} placeholder for client substitution")
}

// Endpoint must be deterministic — same input, same bytes (no map
// iteration randomness leaking through). Frontend caching depends on it.
func TestGetConfigDefaults_Deterministic(t *testing.T) {
	t.Parallel()
	h := &ConfigHandler{}

	body := func() []byte {
		w := httptest.NewRecorder()
		h.GetConfigDefaults(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/config/defaults", nil))
		return w.Body.Bytes()
	}

	first := body()
	for i := 0; i < 10; i++ {
		assert.Equal(t, first, body(),
			"response must be byte-identical across calls (iteration %d)", i)
	}
}
