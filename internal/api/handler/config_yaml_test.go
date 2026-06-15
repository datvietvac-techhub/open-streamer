package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
	"github.com/datvietvac-techhub/open-streamer/internal/vod"
)

const (
	yamlPath       = "/config/yaml"
	codeAlpha      = "alpha"
	codeBeta       = "beta"
	hookFileTarget = "/var/log/open-streamer/events.log"
	hookHTTPTarget = "https://x.test"
	srcRTMPURL     = "rtmp://src/a"
)

// --- in-memory test doubles ----------------------------------------------------

// fakeStreamRepo is an in-memory store.StreamRepository for tests.
type fakeStreamRepo struct {
	items   map[domain.StreamCode]*domain.Stream
	listErr error
	saveErr error
	delErr  error
}

func newFakeStreamRepo(seed ...*domain.Stream) *fakeStreamRepo {
	r := &fakeStreamRepo{items: make(map[domain.StreamCode]*domain.Stream)}
	for _, s := range seed {
		r.items[s.Code] = s
	}
	return r
}

func (r *fakeStreamRepo) Save(_ context.Context, s *domain.Stream) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	r.items[s.Code] = s
	return nil
}

func (r *fakeStreamRepo) FindByCode(_ context.Context, code domain.StreamCode) (*domain.Stream, error) {
	s, ok := r.items[code]
	if !ok {
		return nil, store.ErrNotFound
	}
	return s, nil
}

func (r *fakeStreamRepo) List(_ context.Context, _ store.StreamFilter) ([]*domain.Stream, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	out := make([]*domain.Stream, 0, len(r.items))
	for _, s := range r.items {
		out = append(out, s)
	}
	return out, nil
}

func (r *fakeStreamRepo) Delete(_ context.Context, code domain.StreamCode) error {
	if r.delErr != nil {
		return r.delErr
	}
	delete(r.items, code)
	return nil
}

// seedHooks builds a fakeHookRepo (defined in hook_test.go) prefilled with the
// given hooks so each test can express its starting state inline.
func seedHooks(initial ...*domain.Hook) *fakeHookRepo {
	r := newFakeHookRepo()
	for _, h := range initial {
		r.hooks[h.ID] = h
	}
	return r
}

// fakeCoord records every lifecycle call so tests can assert the exact diff.
type fakeCoord struct {
	running   map[domain.StreamCode]bool
	starts    []domain.StreamCode
	stops     []domain.StreamCode
	updates   []domain.StreamCode
	startErr  error
	updateErr error
}

func newFakeCoord(running ...domain.StreamCode) *fakeCoord {
	c := &fakeCoord{running: make(map[domain.StreamCode]bool)}
	for _, code := range running {
		c.running[code] = true
	}
	return c
}

func (c *fakeCoord) Start(_ context.Context, s *domain.Stream) error {
	if c.startErr != nil {
		return c.startErr
	}
	c.starts = append(c.starts, s.Code)
	c.running[s.Code] = true
	return nil
}

func (c *fakeCoord) Stop(_ context.Context, code domain.StreamCode) {
	c.stops = append(c.stops, code)
	delete(c.running, code)
}

func (c *fakeCoord) Update(_ context.Context, _, want *domain.Stream) error {
	if c.updateErr != nil {
		return c.updateErr
	}
	c.updates = append(c.updates, want.Code)
	return nil
}

func (c *fakeCoord) IsRunning(code domain.StreamCode) bool {
	return c.running[code]
}

// fakeVODRepo is an in-memory store.VODMountRepository for tests.
type fakeVODRepo struct {
	items   map[domain.VODName]*domain.VODMount
	listErr error
	saveErr error
	delErr  error
}

func newFakeVODRepo(seed ...*domain.VODMount) *fakeVODRepo {
	r := &fakeVODRepo{items: make(map[domain.VODName]*domain.VODMount)}
	for _, m := range seed {
		r.items[m.Name] = m
	}
	return r
}

func (r *fakeVODRepo) Save(_ context.Context, m *domain.VODMount) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	r.items[m.Name] = m
	return nil
}

func (r *fakeVODRepo) FindByName(_ context.Context, name domain.VODName) (*domain.VODMount, error) {
	if m, ok := r.items[name]; ok {
		return m, nil
	}
	return nil, store.ErrNotFound
}

func (r *fakeVODRepo) List(_ context.Context) ([]*domain.VODMount, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	out := make([]*domain.VODMount, 0, len(r.items))
	for _, m := range r.items {
		out = append(out, m)
	}
	return out, nil
}

func (r *fakeVODRepo) Delete(_ context.Context, name domain.VODName) error {
	if r.delErr != nil {
		return r.delErr
	}
	delete(r.items, name)
	return nil
}

// newTestHandler wires the fakes into a ConfigHandler the way the production
// DI wiring would, so each test starts from a clean slate.
func newTestHandler(
	rtm *fakeRuntimeManager,
	streamRepo *fakeStreamRepo,
	hookRepo *fakeHookRepo,
	coord *fakeCoord,
) *ConfigHandler {
	return &ConfigHandler{
		rtm:        rtm,
		streamRepo: streamRepo,
		hookRepo:   hookRepo,
		vodRepo:    newFakeVODRepo(),
		vods:       vod.NewRegistry(),
		coord:      coord,
	}
}

// putYAML is a tiny helper that builds a PUT /config/yaml request from a string.
func putYAML(t *testing.T, body string) *http.Request {
	t.Helper()
	return httptest.NewRequestWithContext(t.Context(), http.MethodPut, yamlPath, strings.NewReader(body))
}

// --- GET ----------------------------------------------------------------------

func TestGetConfigYAMLBundlesAllSections(t *testing.T) {
	rtm := &fakeRuntimeManager{cfg: &domain.GlobalConfig{
		Server: &config.ServerConfig{HTTPAddr: ":8080"},
	}}
	streams := newFakeStreamRepo(&domain.Stream{Code: codeAlpha})
	hooks := seedHooks(&domain.Hook{ID: "h1", Type: domain.HookTypeHTTP, Target: hookHTTPTarget})
	h := newTestHandler(rtm, streams, hooks, newFakeCoord())

	w := httptest.NewRecorder()
	h.GetConfigYAML(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, yamlPath, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != yamlContentType {
		t.Errorf("content-type=%s", got)
	}

	var back fullConfig
	if err := yaml.Unmarshal(w.Body.Bytes(), &back); err != nil {
		t.Fatalf("response not valid YAML: %v", err)
	}
	if back.GlobalConfig == nil || back.GlobalConfig.Server.HTTPAddr != ":8080" {
		t.Errorf("global_config not bundled: %+v", back.GlobalConfig)
	}
	if len(back.Streams) != 1 || back.Streams[0].Code != codeAlpha {
		t.Errorf("streams not bundled: %+v", back.Streams)
	}
	if len(back.Hooks) != 1 || back.Hooks[0].ID != "h1" {
		t.Errorf("hooks not bundled: %+v", back.Hooks)
	}
}

func TestGetConfigYAMLStreamListError(t *testing.T) {
	rtm := &fakeRuntimeManager{cfg: &domain.GlobalConfig{}}
	streams := newFakeStreamRepo()
	streams.listErr = errors.New("db down")
	h := newTestHandler(rtm, streams, newFakeHookRepo(), newFakeCoord())

	w := httptest.NewRecorder()
	h.GetConfigYAML(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, yamlPath, nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestGetConfigYAMLHookListError(t *testing.T) {
	rtm := &fakeRuntimeManager{cfg: &domain.GlobalConfig{}}
	hooks := newFakeHookRepo()
	hooks.listErr = errors.New("db down")
	h := newTestHandler(rtm, newFakeStreamRepo(), hooks, newFakeCoord())

	w := httptest.NewRecorder()
	h.GetConfigYAML(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, yamlPath, nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d", w.Code)
	}
}

// --- PUT happy path -----------------------------------------------------------

func TestReplaceConfigYAMLAppliesAllSections(t *testing.T) {
	rtm := &fakeRuntimeManager{cfg: &domain.GlobalConfig{
		Server: &config.ServerConfig{HTTPAddr: ":8080"},
	}}
	streams := newFakeStreamRepo()
	hooks := newFakeHookRepo()
	coord := newFakeCoord()
	h := newTestHandler(rtm, streams, hooks, coord)

	body := `global_config:
  server:
    http_addr: ":9999"
  buffer:
    capacity: 2048
streams:
  - code: alpha
    inputs:
      - url: rtmp://src/a
        priority: 0
hooks:
  - id: h1
    type: http
    target: https://example.com/webhook
    enabled: true
`
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	if rtm.applied == nil || rtm.applied.Server.HTTPAddr != ":9999" {
		t.Errorf("global config not applied: %+v", rtm.applied)
	}
	if _, ok := streams.items[codeAlpha]; !ok {
		t.Error("stream alpha not saved")
	}
	if _, ok := hooks.hooks["h1"]; !ok {
		t.Error("hook h1 not saved")
	}
	if len(coord.starts) != 1 || coord.starts[0] != codeAlpha {
		t.Errorf("expected one Start(alpha), got starts=%v", coord.starts)
	}
}

// --- diff/sync ---------------------------------------------------------------

func TestReplaceConfigYAMLStopsAndDeletesRemovedStream(t *testing.T) {
	streams := newFakeStreamRepo(&domain.Stream{
		Code:   codeAlpha,
		Inputs: []domain.Input{{URL: srcRTMPURL, Priority: 0}},
	})
	coord := newFakeCoord(codeAlpha)
	h := newTestHandler(&fakeRuntimeManager{cfg: &domain.GlobalConfig{}}, streams, newFakeHookRepo(), coord)

	// Body omits alpha entirely → must be stopped + deleted.
	body := "streams: []\n"
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if _, exists := streams.items[codeAlpha]; exists {
		t.Error("removed stream still in store")
	}
	if len(coord.stops) != 1 || coord.stops[0] != codeAlpha {
		t.Errorf("expected Stop(alpha), got stops=%v", coord.stops)
	}
}

func TestReplaceConfigYAMLUpdatesRunningStream(t *testing.T) {
	streams := newFakeStreamRepo(&domain.Stream{
		Code:   codeAlpha,
		Name:   "old",
		Inputs: []domain.Input{{URL: srcRTMPURL, Priority: 0}},
	})
	coord := newFakeCoord(codeAlpha)
	h := newTestHandler(&fakeRuntimeManager{cfg: &domain.GlobalConfig{}}, streams, newFakeHookRepo(), coord)

	body := `streams:
  - code: alpha
    name: new
    inputs:
      - url: rtmp://src/a
        priority: 0
`
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if streams.items[codeAlpha].Name != "new" {
		t.Errorf("name not updated: %q", streams.items[codeAlpha].Name)
	}
	if len(coord.updates) != 1 {
		t.Errorf("expected Update for running stream, got updates=%v starts=%v", coord.updates, coord.starts)
	}
}

func TestReplaceConfigYAMLStartsPreviouslyDisabledStream(t *testing.T) {
	streams := newFakeStreamRepo(&domain.Stream{
		Code:     codeAlpha,
		Disabled: true,
		Inputs:   []domain.Input{{URL: srcRTMPURL, Priority: 0}},
	})
	coord := newFakeCoord() // not running because was disabled
	h := newTestHandler(&fakeRuntimeManager{cfg: &domain.GlobalConfig{}}, streams, newFakeHookRepo(), coord)

	body := `streams:
  - code: alpha
    disabled: false
    inputs:
      - url: rtmp://src/a
        priority: 0
`
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if len(coord.starts) != 1 || coord.starts[0] != codeAlpha {
		t.Errorf("expected Start(alpha), got starts=%v", coord.starts)
	}
}

func TestReplaceConfigYAMLDoesNotStartDisabledNewStream(t *testing.T) {
	coord := newFakeCoord()
	streams := newFakeStreamRepo()
	h := newTestHandler(&fakeRuntimeManager{cfg: &domain.GlobalConfig{}}, streams, newFakeHookRepo(), coord)

	body := `streams:
  - code: alpha
    disabled: true
    inputs:
      - url: rtmp://src/a
        priority: 0
`
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if _, exists := streams.items[codeAlpha]; !exists {
		t.Error("disabled stream should still be saved")
	}
	if len(coord.starts) != 0 {
		t.Errorf("disabled stream must not start, got starts=%v", coord.starts)
	}
}

func TestReplaceConfigYAMLDeletesRemovedHook(t *testing.T) {
	hooks := seedHooks(
		&domain.Hook{ID: "h_keep", Type: domain.HookTypeHTTP, Target: "https://k.test"},
		&domain.Hook{ID: "h_drop", Type: domain.HookTypeHTTP, Target: "https://d.test"},
	)
	h := newTestHandler(&fakeRuntimeManager{cfg: &domain.GlobalConfig{}}, newFakeStreamRepo(), hooks, newFakeCoord())

	body := `hooks:
  - id: h_keep
    type: http
    target: https://k.test
`
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if _, exists := hooks.hooks["h_keep"]; !exists {
		t.Error("kept hook missing from store")
	}
	if _, exists := hooks.hooks["h_drop"]; exists {
		t.Error("dropped hook still present")
	}
}

// --- error paths --------------------------------------------------------------

func TestReplaceConfigYAMLEmptyBody(t *testing.T) {
	h := newTestHandler(&fakeRuntimeManager{cfg: &domain.GlobalConfig{}},
		newFakeStreamRepo(), newFakeHookRepo(), newFakeCoord())
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, "  \n  "))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestReplaceConfigYAMLInvalidYAML(t *testing.T) {
	h := newTestHandler(&fakeRuntimeManager{cfg: &domain.GlobalConfig{}},
		newFakeStreamRepo(), newFakeHookRepo(), newFakeCoord())
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, "global_config: [unbalanced"))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestReplaceConfigYAMLUnknownFieldsRejected(t *testing.T) {
	h := newTestHandler(&fakeRuntimeManager{cfg: &domain.GlobalConfig{}},
		newFakeStreamRepo(), newFakeHookRepo(), newFakeCoord())
	body := "global_config:\n  server:\n    http_addr: \":80\"\n    totally_made_up_field: 42\n"
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("typos must be caught, got status=%d", w.Code)
	}
}

func TestReplaceConfigYAMLValidationCollectsAllSections(t *testing.T) {
	h := newTestHandler(&fakeRuntimeManager{cfg: &domain.GlobalConfig{}},
		newFakeStreamRepo(), newFakeHookRepo(), newFakeCoord())
	body := `global_config:
  server:
    http_addr: ""
  buffer:
    capacity: -1
streams:
  - code: "bad code!!"
    inputs: []
hooks:
  - id: ""
    type: http
    target: not-a-url
`
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d body=%s", w.Code, w.Body.String())
	}
	var got struct {
		Error struct {
			Code   string       `json:"code"`
			Fields []fieldError `json:"fields"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("body not JSON: %v", err)
	}
	if got.Error.Code != "VALIDATION_FAILED" {
		t.Errorf("code=%s", got.Error.Code)
	}
	have := map[string]bool{}
	for _, f := range got.Error.Fields {
		have[f.Path] = true
	}
	for _, p := range []string{
		"global_config.server.http_addr",
		"global_config.buffer.capacity",
		"streams[0].code",
		"hooks[0].id",
		"hooks[0].target",
	} {
		if !have[p] {
			t.Errorf("missing validation error for path %q (got: %+v)", p, got.Error.Fields)
		}
	}
}

func TestReplaceConfigYAMLApplyGlobalError(t *testing.T) {
	rtm := &fakeRuntimeManager{
		cfg:      &domain.GlobalConfig{},
		applyErr: errors.New("disk full"),
	}
	h := newTestHandler(rtm, newFakeStreamRepo(), newFakeHookRepo(), newFakeCoord())
	body := "global_config:\n  server:\n    http_addr: \":8080\"\n"
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestReplaceConfigYAMLBodyTooLarge(t *testing.T) {
	h := newTestHandler(&fakeRuntimeManager{cfg: &domain.GlobalConfig{}},
		newFakeStreamRepo(), newFakeHookRepo(), newFakeCoord())
	huge := bytes.Repeat([]byte("a"), maxYAMLBodyBytes+1)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, yamlPath, bytes.NewReader(huge))
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestReplaceConfigYAMLStreamSaveError(t *testing.T) {
	streams := newFakeStreamRepo()
	streams.saveErr = errors.New("boom")
	h := newTestHandler(&fakeRuntimeManager{cfg: &domain.GlobalConfig{}},
		streams, newFakeHookRepo(), newFakeCoord())
	body := `streams:
  - code: alpha
    inputs:
      - url: rtmp://src/a
        priority: 0
`
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestReplaceConfigYAMLStreamStartError(t *testing.T) {
	coord := newFakeCoord()
	coord.startErr = errors.New("port busy")
	h := newTestHandler(&fakeRuntimeManager{cfg: &domain.GlobalConfig{}},
		newFakeStreamRepo(), newFakeHookRepo(), coord)
	body := `streams:
  - code: alpha
    inputs:
      - url: rtmp://src/a
        priority: 0
`
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

// --- direct validator tests ---------------------------------------------------

func TestValidateGlobalConfigPassesForValid(t *testing.T) {
	cfg := &domain.GlobalConfig{
		Server:    &config.ServerConfig{HTTPAddr: ":8080"},
		Buffer:    &config.BufferConfig{Capacity: 2000},
		Listeners: &config.ListenersConfig{RTMP: config.RTMPListenerConfig{Enabled: true, Port: 1936}},
		Hooks:     &config.HooksConfig{WorkerCount: 2},
		Log:       &config.LogConfig{Level: "info", Format: "json"},
	}
	if errs := validateGlobalConfig(cfg); len(errs) != 0 {
		t.Errorf("valid config flagged errors: %+v", errs)
	}
}

func TestValidateGlobalConfigCORSWildcardWithCredentials(t *testing.T) {
	cfg := &domain.GlobalConfig{
		Server: &config.ServerConfig{
			HTTPAddr: ":8080",
			CORS: config.CORSConfig{
				AllowedOrigins:   []string{"*"},
				AllowCredentials: true,
			},
		},
	}
	errs := validateGlobalConfig(cfg)
	found := false
	for _, e := range errs {
		if strings.Contains(e.Path, "allow_credentials") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected allow_credentials error, got %+v", errs)
	}
}

func TestValidateGlobalConfigSameDirHLSDASH(t *testing.T) {
	cfg := &domain.GlobalConfig{
		Publisher: &config.PublisherConfig{
			HLS:  config.PublisherHLSConfig{Dir: "/x"},
			DASH: config.PublisherDASHConfig{Dir: "/x"},
		},
	}
	errs := validateGlobalConfig(cfg)
	found := false
	for _, e := range errs {
		if e.Path == "global_config.publisher.dash.dir" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected dash.dir collision error, got %+v", errs)
	}
}

func TestValidateGlobalConfigListenerPortRequiredWhenEnabled(t *testing.T) {
	cfg := &domain.GlobalConfig{
		Listeners: &config.ListenersConfig{
			RTMP: config.RTMPListenerConfig{Enabled: true, Port: 0},
			SRT:  config.SRTListenerConfig{Enabled: true, Port: 0, LatencyMS: -1},
		},
	}
	errs := validateGlobalConfig(cfg)
	have := map[string]bool{}
	for _, e := range errs {
		have[e.Path] = true
	}
	if !have["global_config.listeners.rtmp.port"] {
		t.Error("missing listeners.rtmp.port error")
	}
	if !have["global_config.listeners.srt.port"] {
		t.Error("missing listeners.srt.port error")
	}
	if !have["global_config.listeners.srt.latency_ms"] {
		t.Error("missing listeners.srt.latency_ms error")
	}
}

func TestValidateFullConfigNil(t *testing.T) {
	errs := validateFullConfig(nil)
	if len(errs) != 1 {
		t.Errorf("nil config should yield exactly one error, got %+v", errs)
	}
}

func TestValidateStreamsDuplicateCode(t *testing.T) {
	errs := validateStreams([]*domain.Stream{
		{Code: codeAlpha, Inputs: []domain.Input{{URL: "rtmp://a", Priority: 0}}},
		{Code: codeAlpha, Inputs: []domain.Input{{URL: "rtmp://b", Priority: 0}}},
	})
	found := false
	for _, e := range errs {
		if e.Path == "streams[1].code" && strings.Contains(e.Message, "duplicate") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected duplicate-code error at streams[1].code, got %+v", errs)
	}
}

func TestValidateStreamsNullEntry(t *testing.T) {
	errs := validateStreams([]*domain.Stream{nil, {Code: codeBeta}})
	found := false
	for _, e := range errs {
		if e.Path == "streams[0]" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected null-entry error at streams[0], got %+v", errs)
	}
}

func TestValidateHooksDuplicateID(t *testing.T) {
	errs := validateHooks([]*domain.Hook{
		{ID: "h1", Type: domain.HookTypeHTTP, Target: hookHTTPTarget},
		{ID: "h1", Type: domain.HookTypeFile, Target: hookFileTarget},
	})
	found := false
	for _, e := range errs {
		if e.Path == "hooks[1].id" && strings.Contains(e.Message, "duplicate") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected duplicate-id error at hooks[1].id, got %+v", errs)
	}
}

func TestValidateHooksTypeTarget(t *testing.T) {
	cases := []struct {
		name string
		hk   *domain.Hook
		path string
	}{
		{
			name: "http target without scheme",
			hk:   &domain.Hook{ID: "h", Type: domain.HookTypeHTTP, Target: "example.com"},
			path: "hooks[0].target",
		},
		{
			name: "unknown type",
			hk:   &domain.Hook{ID: "h", Type: "smtp", Target: "x"},
			path: "hooks[0].target",
		},
		{
			name: "empty target",
			hk:   &domain.Hook{ID: "h", Type: domain.HookTypeHTTP, Target: ""},
			path: "hooks[0].target",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errs := validateHooks([]*domain.Hook{tc.hk})
			found := false
			for _, e := range errs {
				if e.Path == tc.path {
					found = true
				}
			}
			if !found {
				t.Errorf("expected error at %q, got %+v", tc.path, errs)
			}
		})
	}
}

func TestValidateHooksMutuallyExclusiveStreamCodes(t *testing.T) {
	errs := validateHooks([]*domain.Hook{{
		ID: "h", Type: domain.HookTypeHTTP, Target: hookHTTPTarget,
		StreamCodes: &domain.StreamCodeFilter{
			Only:   []domain.StreamCode{codeAlpha},
			Except: []domain.StreamCode{codeBeta},
		},
	}})
	found := false
	for _, e := range errs {
		if e.Path == "hooks[0].stream_codes" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected stream_codes error, got %+v", errs)
	}
}

func TestValidateHooksNegativeRetriesAndTimeout(t *testing.T) {
	errs := validateHooks([]*domain.Hook{{
		ID: "h", Type: domain.HookTypeHTTP, Target: hookHTTPTarget,
		MaxRetries: -1, TimeoutSec: -1,
	}})
	have := map[string]bool{}
	for _, e := range errs {
		have[e.Path] = true
	}
	if !have["hooks[0].max_retries"] || !have["hooks[0].timeout_sec"] {
		t.Errorf("missing negative-int errors, got %+v", errs)
	}
}

// --- policies (config-yaml round-trip) ----------------------------------------

// fakePolicyRepo is an in-memory store.PolicyRepository for tests.
type fakePolicyRepo struct {
	items   map[domain.PolicyCode]*domain.Policy
	listErr error
}

func newFakePolicyRepo(seed ...*domain.Policy) *fakePolicyRepo {
	r := &fakePolicyRepo{items: make(map[domain.PolicyCode]*domain.Policy)}
	for _, p := range seed {
		r.items[p.Code] = p
	}
	return r
}

func (r *fakePolicyRepo) Save(_ context.Context, p *domain.Policy) error {
	r.items[p.Code] = p
	return nil
}

func (r *fakePolicyRepo) FindByCode(_ context.Context, code domain.PolicyCode) (*domain.Policy, error) {
	p, ok := r.items[code]
	if !ok {
		return nil, store.ErrNotFound
	}
	return p, nil
}

func (r *fakePolicyRepo) List(_ context.Context) ([]*domain.Policy, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	out := make([]*domain.Policy, 0, len(r.items))
	for _, p := range r.items {
		out = append(out, p)
	}
	return out, nil
}

func (r *fakePolicyRepo) Delete(_ context.Context, code domain.PolicyCode) error {
	delete(r.items, code)
	return nil
}

// fakeAuthz records SetPolicies calls so tests can assert the hot-reload fired.
type fakeAuthz struct{ lastSet []*domain.Policy }

func (a *fakeAuthz) SetPolicies(ps []*domain.Policy) { a.lastSet = ps }

func TestGetConfigYAMLBundlesPolicies(t *testing.T) {
	rtm := &fakeRuntimeManager{cfg: &domain.GlobalConfig{Server: &config.ServerConfig{HTTPAddr: ":8080"}}}
	h := newTestHandler(rtm, newFakeStreamRepo(), newFakeHookRepo(), newFakeCoord())
	h.policyRepo = newFakePolicyRepo(&domain.Policy{Code: "only_us", AllowCountries: []string{"US"}})

	w := httptest.NewRecorder()
	h.GetConfigYAML(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, yamlPath, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var back fullConfig
	if err := yaml.Unmarshal(w.Body.Bytes(), &back); err != nil {
		t.Fatalf("response not valid YAML: %v", err)
	}
	if len(back.Policies) != 1 || back.Policies[0].Code != "only_us" {
		t.Errorf("policies not bundled: %+v", back.Policies)
	}
}

func TestReplaceConfigYAMLAppliesPoliciesAndReloads(t *testing.T) {
	rtm := &fakeRuntimeManager{cfg: &domain.GlobalConfig{Server: &config.ServerConfig{HTTPAddr: ":8080"}}}
	h := newTestHandler(rtm, newFakeStreamRepo(), newFakeHookRepo(), newFakeCoord())
	pol := newFakePolicyRepo()
	authz := &fakeAuthz{}
	h.policyRepo = pol
	h.authz = authz

	body := "" +
		"global_config:\n  server:\n    http_addr: \":8080\"\n" +
		"policies:\n  - code: vip\n    require_token: true\n    token_secret: sk\n    allow_countries: [US, VN]\n"
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if _, ok := pol.items["vip"]; !ok {
		t.Error("policy 'vip' not persisted by config-yaml apply")
	}
	if len(authz.lastSet) != 1 || authz.lastSet[0].Code != "vip" {
		t.Errorf("authorizer not hot-reloaded with the new policy set: %+v", authz.lastSet)
	}
}

func TestReplaceConfigYAMLRejectsInvalidPolicy(t *testing.T) {
	rtm := &fakeRuntimeManager{cfg: &domain.GlobalConfig{Server: &config.ServerConfig{HTTPAddr: ":8080"}}}
	h := newTestHandler(rtm, newFakeStreamRepo(), newFakeHookRepo(), newFakeCoord())
	h.policyRepo = newFakePolicyRepo()

	// require_token with no secret must fail validation (422), nothing applied.
	body := "global_config:\n  server:\n    http_addr: \":8080\"\n" +
		"policies:\n  - code: bad\n    require_token: true\n"
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for invalid policy, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestReplaceConfigYAMLRefusesDeletingReferencedPolicy(t *testing.T) {
	rtm := &fakeRuntimeManager{cfg: &domain.GlobalConfig{Server: &config.ServerConfig{HTTPAddr: ":8080"}}}
	// A stream in the desired doc still references 'keep'.
	streams := newFakeStreamRepo()
	h := newTestHandler(rtm, streams, newFakeHookRepo(), newFakeCoord())
	h.policyRepo = newFakePolicyRepo(&domain.Policy{Code: "keep", AllowCountries: []string{"US"}})

	body := "global_config:\n  server:\n    http_addr: \":8080\"\n" +
		"policies: []\n" + // omit 'keep' → would delete it
		"streams:\n  - code: s1\n    playback_policy: keep\n    inputs:\n      - url: " + srcRTMPURL + "\n"
	w := httptest.NewRecorder()
	h.ReplaceConfigYAML(w, putYAML(t, body))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected refusal (500 APPLY_POLICIES_FAILED) when deleting a referenced policy, got %d body=%s", w.Code, w.Body.String())
	}
	if _, ok := h.policyRepo.(*fakePolicyRepo).items["keep"]; !ok {
		t.Error("referenced policy 'keep' must not be deleted")
	}
}
