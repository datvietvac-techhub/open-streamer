package handler

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ProbeTranscoder is a P0 migration stub: invalid JSON → 400, anything
// else → 200 with a deprecation notice. Tests cover those two branches;
// the FFmpeg-binary-unusable and path-changed cases are gone because the
// stub no longer touches the filesystem or holds state.

func TestConfigHandler_ProbeTranscoder_InvalidJSON(t *testing.T) {
	t.Parallel()
	h := &ConfigHandler{}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost,
		"/config/transcoder/probe", bytes.NewReader([]byte("{nope")))
	w := httptest.NewRecorder()
	h.ProbeTranscoder(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestConfigHandler_ProbeTranscoder_StubReturnsOK(t *testing.T) {
	t.Parallel()
	h := &ConfigHandler{}
	body := []byte(`{"ffmpeg_path":"/anything"}`)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost,
		"/config/transcoder/probe", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ProbeTranscoder(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"ok":true`)
	assert.Contains(t, w.Body.String(), "migration")
}

// validateTranscoderPath always returns nil during the migration so save
// is never blocked by transcoder fields. Document the contract.
func TestConfigHandler_ValidateTranscoderPath_Stubbed(t *testing.T) {
	t.Parallel()
	h := &ConfigHandler{}
	require.Nil(t, h.validateTranscoderPath(t.Context(), nil))
	require.Nil(t, h.validateTranscoderPath(t.Context(), "anything"))
}
