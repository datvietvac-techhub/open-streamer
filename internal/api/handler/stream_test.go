package handler

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

const (
	validationErrFmt = "validation err: %+v"
	rtmpInputA       = "rtmp://a"
)

func decodeBodyHelper(t *testing.T, code domain.StreamCode, cur *domain.Stream, exists bool, raw any) (*domain.Stream, *putValidationError) {
	t.Helper()
	body, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/streams/"+string(code), bytes.NewReader(body))
	return decodeStreamBody(req, code, cur, exists)
}

func TestDecodeStreamBodyNew(t *testing.T) {
	body := domain.Stream{Name: "hello"}
	got, vErr := decodeBodyHelper(t, "live1", nil, false, body)
	if vErr != nil {
		t.Fatalf("unexpected validation err: %+v", vErr)
	}
	if got.Code != "live1" || got.Name != "hello" {
		t.Errorf("body=%+v", got)
	}
}

func TestDecodeStreamBodyMergesExisting(t *testing.T) {
	cur := &domain.Stream{Code: "x", Name: "old", Disabled: true}
	got, vErr := decodeBodyHelper(t, "x", cur, true, map[string]any{"name": "new"})
	if vErr != nil {
		t.Fatalf(validationErrFmt, vErr)
	}
	if got.Name != "new" {
		t.Errorf("name not overwritten: %q", got.Name)
	}
	if !got.Disabled {
		t.Error("omitted Disabled field must keep prior value")
	}
}

func TestDecodeStreamBodyURLCodeWins(t *testing.T) {
	got, vErr := decodeBodyHelper(t, "url_code", nil, false,
		map[string]any{"code": "body_code"})
	if vErr != nil {
		t.Fatalf(validationErrFmt, vErr)
	}
	if got.Code != "url_code" {
		t.Errorf("URL code must win, got %q", got.Code)
	}
}

func TestDecodeStreamBodyInvalidJSON(t *testing.T) {
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/streams/x", bytes.NewReader([]byte("{bad")))
	_, vErr := decodeStreamBody(req, "x", nil, false)
	if vErr == nil || vErr.code != "INVALID_BODY" {
		t.Errorf("want INVALID_BODY, got %+v", vErr)
	}
}

func TestDecodeStreamBodyInvalidCode(t *testing.T) {
	_, vErr := decodeBodyHelper(t, "bad-code!", nil, false, map[string]any{})
	if vErr == nil || vErr.code != "INVALID_CODE" {
		t.Errorf("want INVALID_CODE, got %+v", vErr)
	}
}

func TestDecodeStreamBodyDuplicateInputs(t *testing.T) {
	body := map[string]any{
		"inputs": []map[string]any{
			{"url": rtmpInputA, "priority": 0},
			{"url": rtmpInputA, "priority": 1},
		},
	}
	_, vErr := decodeBodyHelper(t, "x", nil, false, body)
	if vErr == nil || vErr.code != "DUPLICATE_INPUT" {
		t.Errorf("want DUPLICATE_INPUT, got %+v", vErr)
	}
}

func TestDecodeStreamBodyInvalidPriority(t *testing.T) {
	body := map[string]any{
		"inputs": []map[string]any{
			{"url": rtmpInputA, "priority": 5}, // does not start at 0
		},
	}
	_, vErr := decodeBodyHelper(t, "x", nil, false, body)
	if vErr == nil || vErr.code != "INVALID_INPUT_PRIORITY" {
		t.Errorf("want INVALID_INPUT_PRIORITY, got %+v", vErr)
	}
}

// Regression: shallow-copying cur into base aliased pointer fields like
// Transcoder, so json.Decode on base mutated cur in-place and downstream
// ComputeDiff(cur, body) reported no change. The fix deep-clones cur first.
func TestDecodeStreamBodyDoesNotMutateExisting(t *testing.T) {
	cur := &domain.Stream{
		Code: "x",
		Transcoder: &domain.TranscoderConfig{
			Video: domain.VideoTranscodeConfig{
				Profiles: []domain.VideoProfile{{Width: 1280, Height: 720, Bitrate: 2500}},
			},
			Audio: domain.AudioTranscodeConfig{Copy: true},
		},
	}
	body := map[string]any{
		"transcoder": map[string]any{
			"video": map[string]any{
				"copy":     false,
				"profiles": []map[string]any{{"width": 1920, "height": 1080, "bitrate": 4500}},
			},
			"audio": map[string]any{"copy": true},
		},
	}
	got, vErr := decodeBodyHelper(t, "x", cur, true, body)
	if vErr != nil {
		t.Fatalf(validationErrFmt, vErr)
	}
	if cur.Transcoder == got.Transcoder {
		t.Fatal("cur.Transcoder and body.Transcoder must not share a pointer (deep clone failed)")
	}
	if cur.Transcoder.Video.Profiles[0].Width != 1280 {
		t.Errorf("cur was mutated: got width %d, want 1280", cur.Transcoder.Video.Profiles[0].Width)
	}
	if got.Transcoder.Video.Profiles[0].Width != 1920 {
		t.Errorf("body did not apply: got width %d, want 1920", got.Transcoder.Video.Profiles[0].Width)
	}
}
