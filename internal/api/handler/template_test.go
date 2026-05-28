package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

// ─── fakes ───────────────────────────────────────────────────────────────────

// fakeTemplateRepo is a simple in-memory TemplateRepository for handler tests.
type fakeTemplateRepo struct {
	mu        sync.Mutex
	templates map[domain.TemplateCode]*domain.Template
}

func newFakeTemplateRepo() *fakeTemplateRepo {
	return &fakeTemplateRepo{templates: make(map[domain.TemplateCode]*domain.Template)}
}

func (r *fakeTemplateRepo) Save(_ context.Context, t *domain.Template) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *t
	r.templates[t.Code] = &cp
	return nil
}

func (r *fakeTemplateRepo) FindByCode(_ context.Context, code domain.TemplateCode) (*domain.Template, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.templates[code]; ok {
		cp := *t
		return &cp, nil
	}
	return nil, store.ErrNotFound
}

func (r *fakeTemplateRepo) List(_ context.Context) ([]*domain.Template, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*domain.Template, 0, len(r.templates))
	for _, t := range r.templates {
		cp := *t
		out = append(out, &cp)
	}
	return out, nil
}

func (r *fakeTemplateRepo) Delete(_ context.Context, code domain.TemplateCode) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.templates, code)
	return nil
}

// withTemplateCode adds {"code": value} to chi's URL params for the request.
func withTemplateCode(req *http.Request, val string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("code", val)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func newTemplateHandler(tr *fakeTemplateRepo, sr *fakeStreamRepo, c *stubCoord) *TemplateHandler {
	return &TemplateHandler{
		templateRepo: tr,
		streamRepo:   sr,
		coordinator:  c,
	}
}

// ─── List / Get ──────────────────────────────────────────────────────────────

func TestTemplate_ListReturnsTotal(t *testing.T) {
	tr := newFakeTemplateRepo()
	require.NoError(t, tr.Save(context.Background(), &domain.Template{Code: "a"}))
	require.NoError(t, tr.Save(context.Background(), &domain.Template{Code: "b"}))

	h := newTemplateHandler(tr, newFakeStreamRepo(), newStubCoord())
	req := newReq(t, http.MethodGet, "/templates", nil)
	rec := httptest.NewRecorder()

	h.List(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body struct {
		Data  []*domain.Template `json:"data"`
		Total int                `json:"total"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, 2, body.Total)
}

func TestTemplate_GetNotFound(t *testing.T) {
	h := newTemplateHandler(newFakeTemplateRepo(), newFakeStreamRepo(), newStubCoord())
	req := withTemplateCode(newReq(t, http.MethodGet, "/templates/missing", nil), "missing")
	rec := httptest.NewRecorder()

	h.Get(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestTemplate_GetInvalidCodeRejected(t *testing.T) {
	h := newTemplateHandler(newFakeTemplateRepo(), newFakeStreamRepo(), newStubCoord())
	req := withTemplateCode(newReq(t, http.MethodGet, "/templates/bad+code", nil), "bad+code")
	rec := httptest.NewRecorder()

	h.Get(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─── Put: create vs update + URL-param wins ──────────────────────────────────

func TestTemplate_PutCreatesNewReturns201(t *testing.T) {
	tr := newFakeTemplateRepo()
	h := newTemplateHandler(tr, newFakeStreamRepo(), newStubCoord())

	body, _ := json.Marshal(domain.Template{Name: "Profile A"})
	req := withTemplateCode(newReq(t, http.MethodPost, "/templates/profile_a", body), "profile_a")
	rec := httptest.NewRecorder()

	h.Put(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	stored, err := tr.FindByCode(t.Context(), "profile_a")
	require.NoError(t, err)
	assert.Equal(t, "Profile A", stored.Name)
	assert.Equal(t, domain.TemplateCode("profile_a"), stored.Code, "URL code must be persisted")
}

func TestTemplate_PutUpdatesExistingReturns200(t *testing.T) {
	tr := newFakeTemplateRepo()
	require.NoError(t, tr.Save(t.Context(), &domain.Template{Code: "profile_a", Name: "old"}))
	h := newTemplateHandler(tr, newFakeStreamRepo(), newStubCoord())

	body, _ := json.Marshal(domain.Template{Name: "new"})
	req := withTemplateCode(newReq(t, http.MethodPost, "/templates/profile_a", body), "profile_a")
	rec := httptest.NewRecorder()

	h.Put(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	stored, _ := tr.FindByCode(t.Context(), "profile_a")
	assert.Equal(t, "new", stored.Name)
}

func TestTemplate_PutURLParamOverridesBodyCode(t *testing.T) {
	tr := newFakeTemplateRepo()
	h := newTemplateHandler(tr, newFakeStreamRepo(), newStubCoord())

	// Body says "evil", URL says "profile_a" — URL wins so a rename via body
	// can't silently orphan dependent streams.
	body, _ := json.Marshal(domain.Template{Code: "evil", Name: "x"})
	req := withTemplateCode(newReq(t, http.MethodPost, "/templates/profile_a", body), "profile_a")
	rec := httptest.NewRecorder()

	h.Put(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	stored, err := tr.FindByCode(t.Context(), "profile_a")
	require.NoError(t, err)
	assert.Equal(t, domain.TemplateCode("profile_a"), stored.Code)
	_, err = tr.FindByCode(t.Context(), "evil")
	require.Error(t, err)
}

// ─── Put: hot reload ─────────────────────────────────────────────────────────

func TestTemplate_PutHotReloadsRunningDependents(t *testing.T) {
	tr := newFakeTemplateRepo()
	sr := newFakeStreamRepo()
	coord := newStubCoord()

	require.NoError(t, tr.Save(t.Context(), &domain.Template{
		Code:      "profile_a",
		Protocols: &domain.OutputProtocols{HLS: true},
	}))
	tplCode := domain.TemplateCode("profile_a")
	streamCode := domain.StreamCode("live")
	require.NoError(t, sr.Save(t.Context(), &domain.Stream{
		Code:     streamCode,
		Template: &tplCode,
	}))
	coord.runningByCode[streamCode] = true

	h := newTemplateHandler(tr, sr, coord)

	// New template enables DASH in addition to HLS.
	body, _ := json.Marshal(domain.Template{
		Protocols: &domain.OutputProtocols{HLS: true, DASH: true},
	})
	req := withTemplateCode(newReq(t, http.MethodPost, "/templates/profile_a", body), "profile_a")
	rec := httptest.NewRecorder()

	h.Put(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Len(t, coord.updatedCodes, 1, "running dependent must be hot-reloaded")
	assert.Equal(t, streamCode, coord.updatedCodes[0])
}

func TestTemplate_PutDoesNotReloadStoppedDependents(t *testing.T) {
	tr := newFakeTemplateRepo()
	sr := newFakeStreamRepo()
	coord := newStubCoord()

	require.NoError(t, tr.Save(t.Context(), &domain.Template{Code: "profile_a"}))
	tplCode := domain.TemplateCode("profile_a")
	require.NoError(t, sr.Save(t.Context(), &domain.Stream{Code: "live", Template: &tplCode}))
	// running map intentionally empty

	h := newTemplateHandler(tr, sr, coord)
	body, _ := json.Marshal(domain.Template{Name: "v2"})
	req := withTemplateCode(newReq(t, http.MethodPost, "/templates/profile_a", body), "profile_a")
	h.Put(httptest.NewRecorder(), req)

	assert.Empty(t, coord.updatedCodes, "stopped streams must not be touched at template update")
}

// ─── Delete: success + reference guard ──────────────────────────────────────

func TestTemplate_DeleteSuccess(t *testing.T) {
	tr := newFakeTemplateRepo()
	require.NoError(t, tr.Save(t.Context(), &domain.Template{Code: "profile_a"}))
	h := newTemplateHandler(tr, newFakeStreamRepo(), newStubCoord())

	req := withTemplateCode(newReq(t, http.MethodDelete, "/templates/profile_a", nil), "profile_a")
	rec := httptest.NewRecorder()

	h.Delete(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	_, err := tr.FindByCode(t.Context(), "profile_a")
	assert.True(t, errors.Is(err, store.ErrNotFound))
}

func TestTemplate_DeleteRejectedWhenReferenced(t *testing.T) {
	tr := newFakeTemplateRepo()
	sr := newFakeStreamRepo()
	require.NoError(t, tr.Save(t.Context(), &domain.Template{Code: "profile_a"}))
	tplCode := domain.TemplateCode("profile_a")
	require.NoError(t, sr.Save(t.Context(), &domain.Stream{Code: "live", Template: &tplCode}))

	h := newTemplateHandler(tr, sr, newStubCoord())

	req := withTemplateCode(newReq(t, http.MethodDelete, "/templates/profile_a", nil), "profile_a")
	rec := httptest.NewRecorder()
	h.Delete(rec, req)

	require.Equal(t, http.StatusConflict, rec.Code)
	var body struct {
		Error   string   `json:"error"`
		Streams []string `json:"streams"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "TEMPLATE_IN_USE", body.Error)
	assert.Equal(t, []string{"live"}, body.Streams)

	// Template must still exist after a rejected delete.
	_, err := tr.FindByCode(t.Context(), "profile_a")
	require.NoError(t, err)
}

func TestTemplate_DeleteMissingReturns404(t *testing.T) {
	h := newTemplateHandler(newFakeTemplateRepo(), newFakeStreamRepo(), newStubCoord())

	req := withTemplateCode(newReq(t, http.MethodDelete, "/templates/missing", nil), "missing")
	rec := httptest.NewRecorder()
	h.Delete(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// Prefix overlap with another template must reject the save before any
// store mutation. Otherwise the matcher would have two templates owning
// overlapping prefix space, breaking the auto-publish routing invariant.
func TestTemplate_PutRejectsOverlappingPrefix(t *testing.T) {
	tr := newFakeTemplateRepo()
	require.NoError(t, tr.Save(t.Context(), &domain.Template{
		Code:     "first",
		Prefixes: []string{"live"},
	}))

	h := newTemplateHandler(tr, newFakeStreamRepo(), newStubCoord())
	// `live/sports` is nested under `live` → overlaps.
	body, _ := json.Marshal(domain.Template{Prefixes: []string{"live/sports"}})
	req := withTemplateCode(newReq(t, http.MethodPost, "/templates/second", body), "second")
	rec := httptest.NewRecorder()
	h.Put(rec, req)

	require.Equal(t, http.StatusConflict, rec.Code)
	var resp struct {
		Error           string `json:"error"`
		ConflictingWith string `json:"conflicting_with"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "PREFIX_OVERLAP", resp.Error)
	assert.Equal(t, "first", resp.ConflictingWith)

	// Make sure the conflicting record was NOT persisted.
	_, err := tr.FindByCode(t.Context(), "second")
	require.Error(t, err)
}

// Updating an existing template with one of its OWN prior prefixes must
// succeed — self-overlap is allowed because the update replaces them.
func TestTemplate_PutSelfPrefixIsNotOverlap(t *testing.T) {
	tr := newFakeTemplateRepo()
	require.NoError(t, tr.Save(t.Context(), &domain.Template{
		Code:     "profile_a",
		Prefixes: []string{"live"},
	}))

	h := newTemplateHandler(tr, newFakeStreamRepo(), newStubCoord())
	body, _ := json.Marshal(domain.Template{Prefixes: []string{"live", "vod"}})
	req := withTemplateCode(newReq(t, http.MethodPost, "/templates/profile_a", body), "profile_a")
	rec := httptest.NewRecorder()
	h.Put(rec, req)

	require.Equal(t, http.StatusOK, rec.Code,
		"updating a template's own prefixes must not collide with itself")
}

// A prefix that fails ValidatePrefix at the handler boundary is a 400,
// not a 409 — distinguishes "malformed" from "in use".
func TestTemplate_PutRejectsMalformedPrefix(t *testing.T) {
	h := newTemplateHandler(newFakeTemplateRepo(), newFakeStreamRepo(), newStubCoord())
	body, _ := json.Marshal(domain.Template{Prefixes: []string{"/leading-slash"}})
	req := withTemplateCode(newReq(t, http.MethodPost, "/templates/profile_a", body), "profile_a")
	rec := httptest.NewRecorder()
	h.Put(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// Force the request body decoder onto an unparseable byte stream so the
// handler exercises the JSON-decode error branch.
func TestTemplate_PutInvalidJSONReturns400(t *testing.T) {
	h := newTemplateHandler(newFakeTemplateRepo(), newFakeStreamRepo(), newStubCoord())

	req := withTemplateCode(
		newReq(t, http.MethodPost, "/templates/profile_a", []byte("{not json")),
		"profile_a",
	)
	rec := httptest.NewRecorder()
	h.Put(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// Ensure the same request body that resembles a chi-style payload doesn't
// crash when the URL param is missing — the validation gate catches it.
func TestTemplate_PutInvalidCodeReturns400(t *testing.T) {
	h := newTemplateHandler(newFakeTemplateRepo(), newFakeStreamRepo(), newStubCoord())

	body := bytes.NewReader([]byte(`{"name":"x"}`))
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/templates/", body)
	rec := httptest.NewRecorder()
	h.Put(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
