package handler

import (
	"context"
	"encoding/json"
	"net/http"
)

// The transcoder capability probe is a no-op: the endpoint stays mounted
// (the UI keeps its "Test" button wired) but always reports "ok" with an
// empty backend set, and save-time validation does not block edits to
// transcoder fields. The native pipeline links libavcodec directly; a
// future in-process libav capability inventory could populate this.

// probeRequest is the body shape for POST /config/transcoder/probe.
// Path field kept for forward-compat with the UI; ignored server-side.
type probeRequest struct {
	FFmpegPath string `json:"ffmpeg_path"`
}

// probeStubResponse is the body returned by the no-op capability probe.
type probeStubResponse struct {
	OK     bool   `json:"ok"`
	Notice string `json:"notice"`
}

// ProbeTranscoder is a P0 migration stub. Always returns ok=true with a
// deprecation notice so operators see the migration state in the UI.
//
// @Summary     Probe transcoder capabilities (stub during native migration).
// @Description P0 stub — always returns ok=true. Real capability probe will be reintroduced once the native libav pipeline lands.
// @Tags        system
// @Accept      json
// @Produce     json
// @Success     200  {object} handler.probeStubResponse
// @Failure     400  {object} apidocs.ErrorBody
// @Router      /config/transcoder/probe [post].
func (h *ConfigHandler) ProbeTranscoder(w http.ResponseWriter, r *http.Request) {
	var body probeRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_BODY", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, probeStubResponse{
		OK:     true,
		Notice: "transcoder probe stubbed during FFmpeg→native migration; capability detection will return in a later phase",
	})
}

// validateTranscoderPath was the save-time gate for the FFmpeg binary.
// During the migration we accept any path so config edits never block.
func (h *ConfigHandler) validateTranscoderPath(_ context.Context, _ any) *putValidationError {
	return nil
}
