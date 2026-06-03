package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/sessions"
)

const (
	testIP1            = "1.1.1.1"
	testIP2            = "2.2.2.2"
	sessionsRoutePath  = "/sessions"
	sessionByIDPathFmt = "/sessions/"
)

func newSessionHandlerWithRouter(t *testing.T) (http.Handler, *sessions.Service) {
	t.Helper()
	cfg := config.SessionsConfig{Enabled: true, IdleTimeoutSec: 30}
	svc := sessions.NewServiceForTesting(cfg, nil, sessions.NullGeoIP{})
	h := &SessionHandler{tracker: svc}

	r := chi.NewRouter()
	r.Get(sessionsRoutePath, h.List)
	r.Route("/sessions/{id}", func(r chi.Router) {
		r.Get("/", h.Get)
		r.Delete("/", h.Delete)
	})
	return r, svc
}

func TestSessionList(t *testing.T) {
	r, svc := newSessionHandlerWithRouter(t)

	// Seed via the public Tracker contract so the handler sees them via the
	// same path as production wiring.
	svc.TrackHTTP(context.Background(), sessions.HTTPHit{
		StreamCode: "live", Protocol: domain.SessionProtoHLS, IP: testIP1,
	})
	svc.TrackHTTP(context.Background(), sessions.HTTPHit{
		StreamCode: "other", Protocol: domain.SessionProtoDASH, IP: testIP2,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, sessionsRoutePath, nil)
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Sessions   []*domain.PlaySession `json:"sessions"`
		TotalCount int                   `json:"total_count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalCount != 2 || len(resp.Sessions) != 2 {
		t.Errorf("got total=%d len=%d, want 2", resp.TotalCount, len(resp.Sessions))
	}
}

// Per-stream listing is now /sessions?stream=<code>. The handler must
// scope the result to that stream and reject malformed codes with 400.
func TestSessionListFilterByStreamQuery(t *testing.T) {
	r, svc := newSessionHandlerWithRouter(t)

	svc.TrackHTTP(context.Background(), sessions.HTTPHit{
		StreamCode: "live", Protocol: domain.SessionProtoHLS, IP: testIP1,
	})
	svc.TrackHTTP(context.Background(), sessions.HTTPHit{
		StreamCode: "other", Protocol: domain.SessionProtoHLS, IP: testIP2,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, sessionsRoutePath+"?stream=live", nil)
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Sessions []*domain.PlaySession `json:"sessions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Sessions) != 1 || resp.Sessions[0].StreamCode != "live" {
		t.Errorf("got %+v, want one session for stream=live", resp.Sessions)
	}
}

// Codes containing '/' are now valid; ?stream=region/north/live must
// resolve to that exact stream rather than 400'ing on the slash.
func TestSessionListFilterByStreamQuery_SupportsNestedCode(t *testing.T) {
	r, svc := newSessionHandlerWithRouter(t)

	svc.TrackHTTP(context.Background(), sessions.HTTPHit{
		StreamCode: "region/north/live", Protocol: domain.SessionProtoHLS, IP: testIP1,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet,
		sessionsRoutePath+"?stream=region/north/live", nil)
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Sessions []*domain.PlaySession `json:"sessions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Sessions) != 1 || resp.Sessions[0].StreamCode != "region/north/live" {
		t.Errorf("got %+v, want session for region/north/live", resp.Sessions)
	}
}

func TestSessionListInvalidProto(t *testing.T) {
	r, _ := newSessionHandlerWithRouter(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, sessionsRoutePath+"?proto=bogus", nil)
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status=%d, want 400", rec.Code)
	}
}

func TestSessionGetAndDelete(t *testing.T) {
	r, svc := newSessionHandlerWithRouter(t)
	sess := svc.TrackHTTP(context.Background(), sessions.HTTPHit{
		StreamCode: "live", Protocol: domain.SessionProtoHLS, IP: testIP1,
	})
	url := sessionByIDPathFmt + sess.ID

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("get status=%d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodDelete, url, nil)
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d", rec.Code)
	}

	// Repeat delete → 404.
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), http.MethodDelete, url, nil)
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("repeat delete status=%d, want 404", rec.Code)
	}
}

func TestSessionGetMissing(t *testing.T) {
	r, _ := newSessionHandlerWithRouter(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, sessionByIDPathFmt+"does-not-exist", nil)
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status=%d, want 404", rec.Code)
	}
}

func TestSessionListInvalidStatus(t *testing.T) {
	r, _ := newSessionHandlerWithRouter(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, sessionsRoutePath+"?status=ghost", nil)
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status=%d, want 400", rec.Code)
	}
}

func TestSessionListInvalidLimit(t *testing.T) {
	r, _ := newSessionHandlerWithRouter(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, sessionsRoutePath+"?limit=-1", nil)
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status=%d, want 400", rec.Code)
	}
}

func TestSessionListLimitClampedToMax(t *testing.T) {
	r, _ := newSessionHandlerWithRouter(t)
	// Limit above sessionListMaxLimit is silently clamped — handler should
	// still succeed (no error), exercising the cap branch in parseSessionFilter.
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, sessionsRoutePath+"?limit=99999", nil)
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status=%d, want 200", rec.Code)
	}
}

func TestSessionListWithStatusFilter(t *testing.T) {
	r, _ := newSessionHandlerWithRouter(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, sessionsRoutePath+"?status=active", nil)
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status=%d, want 200", rec.Code)
	}
}

func TestSessionListWithProtoFilter(t *testing.T) {
	r, _ := newSessionHandlerWithRouter(t)
	for _, p := range []string{"hls", "dash", "rtmp", "srt", "rtsp"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, sessionsRoutePath+"?proto="+p, nil)
		r.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("proto=%s status=%d, want 200", p, rec.Code)
		}
	}
}

func TestSessionListWithStatusClosed(t *testing.T) {
	r, _ := newSessionHandlerWithRouter(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, sessionsRoutePath+"?status=closed", nil)
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status=%d, want 200", rec.Code)
	}
}

// A malformed ?stream=… must return 400 with the validator's reason,
// not silently swallow the filter into "every stream".
func TestSessionListFilterByStreamQuery_InvalidCode(t *testing.T) {
	r, _ := newSessionHandlerWithRouter(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, sessionsRoutePath+"?stream=!!!", nil)
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status=%d, want 400", rec.Code)
	}
}
