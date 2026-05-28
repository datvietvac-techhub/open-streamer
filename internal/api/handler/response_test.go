package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

func TestWriteJSONStatusAndBody(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, 201, map[string]int{"x": 1})
	if w.Code != 201 {
		t.Errorf("status=%d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("content-type=%s", got)
	}
	var got map[string]int
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("body not JSON: %v", err)
	}
	if got["x"] != 1 {
		t.Errorf("body=%+v", got)
	}
}

func TestWriteErrorShape(t *testing.T) {
	w := httptest.NewRecorder()
	writeError(w, 422, "BAD_THING", "explanation")
	if w.Code != 422 {
		t.Errorf("status=%d", w.Code)
	}
	var got struct {
		Error struct {
			Code, Message string
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("body not JSON: %v", err)
	}
	if got.Error.Code != "BAD_THING" || got.Error.Message != "explanation" {
		t.Errorf("body=%+v", got)
	}
}

func TestWriteStoreErrorNotFoundReturns404(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/streams/x", nil)
	writeStoreError(w, r, store.ErrNotFound)
	if w.Code != 404 {
		t.Errorf("status=%d", w.Code)
	}
	var got struct {
		Error struct{ Code string } `json:"error"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got.Error.Code != "NOT_FOUND" {
		t.Errorf("code=%s", got.Error.Code)
	}
}

func TestWriteStoreErrorWrappedNotFound(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/streams/x", nil)
	writeStoreError(w, r, errors.New("wrapped"))
	if w.Code != 500 {
		t.Errorf("non-NotFound should be 500, got %d", w.Code)
	}
}
