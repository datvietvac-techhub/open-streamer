package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/datvietvac-techhub/open-streamer/config"
)

func mustHash(t *testing.T, pw string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.MinCost) // MinCost: fast tests
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	return string(h)
}

// okHandler returns 200 so the test can tell "passed the middleware" from a
// 401/403 the middleware produced.
func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
}

func doReq(t *testing.T, h http.Handler, method, user, pass string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), method, "/config", nil)
	if user != "" || pass != "" {
		req.SetBasicAuth(user, pass)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestRoleAllowsMethod(t *testing.T) {
	t.Parallel()
	cases := []struct {
		role, method string
		want         bool
	}{
		{roleRead, http.MethodGet, true},
		{roleRead, http.MethodHead, true},
		{roleRead, http.MethodOptions, true},
		{roleRead, http.MethodPost, false},
		{roleRead, http.MethodPut, false},
		{roleRead, http.MethodDelete, false},
		{roleRead, http.MethodPatch, false},
		{roleWrite, http.MethodGet, true},
		{roleWrite, http.MethodPost, true},
		{roleWrite, http.MethodDelete, true},
		{"bogus", http.MethodGet, false},
	}
	for _, c := range cases {
		if got := roleAllowsMethod(c.role, c.method); got != c.want {
			t.Errorf("roleAllowsMethod(%q,%q)=%v want %v", c.role, c.method, got, c.want)
		}
	}
}

func TestAPIAuth_DisabledPassesThrough(t *testing.T) {
	t.Parallel()
	a := newAuth(config.AuthConfig{API: config.APIAuthConfig{Enabled: false}})
	h := a.Middleware(okHandler())
	// No credentials, mutating method — must still pass (auth disabled).
	if rec := doReq(t, h, http.MethodPost, "", ""); rec.Code != http.StatusOK {
		t.Fatalf("disabled auth: code=%d want 200", rec.Code)
	}
}

func TestAPIAuth_RequiresAndValidatesCreds(t *testing.T) {
	t.Parallel()
	a := newAuth(config.AuthConfig{API: config.APIAuthConfig{
		Enabled: true,
		Users: []config.APIUser{
			{Username: "writer", PasswordHash: mustHash(t, "s3cret"), Role: roleWrite},
		},
	}})
	h := a.Middleware(okHandler())

	t.Run("no_creds_401_with_challenge", func(t *testing.T) {
		rec := doReq(t, h, http.MethodGet, "", "")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("code=%d want 401", rec.Code)
		}
		if rec.Header().Get("WWW-Authenticate") == "" {
			t.Error("missing WWW-Authenticate challenge")
		}
	})
	t.Run("wrong_password_401", func(t *testing.T) {
		if rec := doReq(t, h, http.MethodGet, "writer", "wrong"); rec.Code != http.StatusUnauthorized {
			t.Fatalf("code=%d want 401", rec.Code)
		}
	})
	t.Run("unknown_user_401", func(t *testing.T) {
		if rec := doReq(t, h, http.MethodGet, "ghost", "s3cret"); rec.Code != http.StatusUnauthorized {
			t.Fatalf("code=%d want 401", rec.Code)
		}
	})
	t.Run("valid_write_user_passes", func(t *testing.T) {
		if rec := doReq(t, h, http.MethodPost, "writer", "s3cret"); rec.Code != http.StatusOK {
			t.Fatalf("code=%d want 200", rec.Code)
		}
	})
}

func TestAPIAuth_ReadRoleCannotWrite(t *testing.T) {
	t.Parallel()
	a := newAuth(config.AuthConfig{API: config.APIAuthConfig{
		Enabled: true,
		Users: []config.APIUser{
			{Username: "ro", PasswordHash: mustHash(t, "pw"), Role: roleRead},
		},
	}})
	h := a.Middleware(okHandler())

	if rec := doReq(t, h, http.MethodGet, "ro", "pw"); rec.Code != http.StatusOK {
		t.Fatalf("read GET: code=%d want 200", rec.Code)
	}
	if rec := doReq(t, h, http.MethodPost, "ro", "pw"); rec.Code != http.StatusForbidden {
		t.Fatalf("read POST: code=%d want 403", rec.Code)
	}
	if rec := doReq(t, h, http.MethodDelete, "ro", "pw"); rec.Code != http.StatusForbidden {
		t.Fatalf("read DELETE: code=%d want 403", rec.Code)
	}
}

func TestAPIAuth_BootstrapFromEnv(t *testing.T) {
	t.Setenv(adminPasswordEnv, "bootpw")
	a := newAuth(config.AuthConfig{API: config.APIAuthConfig{Enabled: true}}) // no users
	h := a.Middleware(okHandler())

	if rec := doReq(t, h, http.MethodPost, "admin", "bootpw"); rec.Code != http.StatusOK {
		t.Fatalf("bootstrap admin write: code=%d want 200", rec.Code)
	}
	if rec := doReq(t, h, http.MethodGet, "admin", "nope"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("bootstrap admin wrong pw: code=%d want 401", rec.Code)
	}
}

func TestAPIAuth_EnabledNoUsersRejectsAll(t *testing.T) {
	// No env seed, enabled, no users → fail-closed (every request 401).
	a := newAuth(config.AuthConfig{API: config.APIAuthConfig{Enabled: true}})
	h := a.Middleware(okHandler())
	if rec := doReq(t, h, http.MethodGet, "anyone", "anything"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("enabled-no-users: code=%d want 401", rec.Code)
	}
}

func TestAPIAuth_SkipsInvalidUsers(t *testing.T) {
	t.Parallel()
	a := newAuth(config.AuthConfig{API: config.APIAuthConfig{
		Enabled: true,
		Users: []config.APIUser{
			{Username: "", PasswordHash: mustHash(t, "x"), Role: roleWrite},     // empty name → skip
			{Username: "norole", PasswordHash: mustHash(t, "x"), Role: "admin"}, // bad role → skip
			{Username: "nohash", PasswordHash: "", Role: roleWrite},             // empty hash → skip
			{Username: "good", PasswordHash: mustHash(t, "pw"), Role: roleWrite},
		},
	}})
	h := a.Middleware(okHandler())
	if rec := doReq(t, h, http.MethodPost, "norole", "x"); rec.Code != http.StatusUnauthorized {
		t.Errorf("invalid-role user must be rejected: code=%d", rec.Code)
	}
	if rec := doReq(t, h, http.MethodPost, "good", "pw"); rec.Code != http.StatusOK {
		t.Errorf("valid user must pass: code=%d", rec.Code)
	}
}

// TestAdminRouteUnmatchedMethodDoesNotBypassAuth locks the routing invariant
// from the adversarial review: an admin path registered via a subrouter must,
// for an UNREGISTERED method, stay inside the auth group (run the middleware,
// then 405) — it must NEVER fall through chi's trie to the public media
// catch-all, which would skip auth entirely.
func TestAdminRouteUnmatchedMethodDoesNotBypassAuth(t *testing.T) {
	t.Parallel()
	a := newAuth(config.AuthConfig{API: config.APIAuthConfig{
		Enabled: true,
		Users:   []config.APIUser{{Username: "w", PasswordHash: mustHash(t, "pw"), Role: roleWrite}},
	}})

	mediaHit := false
	r := chi.NewRouter()
	// Admin group (mirrors buildRouter): subrouter owns the /config subtree.
	r.Group(func(ar chi.Router) {
		ar.Use(a.Middleware)
		ar.Route("/config", func(cr chi.Router) {
			cr.Get("/", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
			cr.Post("/", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		})
	})
	// Public media catch-all (no auth).
	r.Group(func(mr chi.Router) {
		mr.HandleFunc("/*", func(w http.ResponseWriter, _ *http.Request) {
			mediaHit = true
			w.WriteHeader(http.StatusOK)
		})
	})

	// PUT /config — method not registered on the subrouter. Without creds the
	// auth middleware must still run (401), proving the request did NOT escape
	// to the unauthenticated media handler.
	if rec := doReq(t, r, http.MethodPut, "", ""); rec.Code != http.StatusUnauthorized || mediaHit {
		t.Fatalf("PUT /config no-creds: code=%d mediaHit=%v want 401 & no media", rec.Code, mediaHit)
	}
	// With valid write creds the subrouter returns 405 (still in-group), not media.
	if rec := doReq(t, r, http.MethodPut, "w", "pw"); rec.Code != http.StatusMethodNotAllowed || mediaHit {
		t.Fatalf("PUT /config write-creds: code=%d mediaHit=%v want 405 & no media", rec.Code, mediaHit)
	}
	// A genuine media path still reaches the public handler unauthenticated.
	mreq := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/somestream/index.m3u8", nil)
	mrec := httptest.NewRecorder()
	r.ServeHTTP(mrec, mreq)
	if !mediaHit || mrec.Code != http.StatusOK {
		t.Fatalf("media path: code=%d mediaHit=%v want 200 & media hit", mrec.Code, mediaHit)
	}
}

func TestAPIAuth_HotReload(t *testing.T) {
	t.Parallel()
	a := newAuth(config.AuthConfig{API: config.APIAuthConfig{
		Enabled: true,
		Users:   []config.APIUser{{Username: "old", PasswordHash: mustHash(t, "old"), Role: roleWrite}},
	}})
	h := a.Middleware(okHandler())
	if rec := doReq(t, h, http.MethodGet, "old", "old"); rec.Code != http.StatusOK {
		t.Fatalf("old user before reload: code=%d want 200", rec.Code)
	}
	// Swap config: remove old, add new.
	a.SetConfig(config.AuthConfig{API: config.APIAuthConfig{
		Enabled: true,
		Users:   []config.APIUser{{Username: "new", PasswordHash: mustHash(t, "new"), Role: roleWrite}},
	}})
	if rec := doReq(t, h, http.MethodGet, "old", "old"); rec.Code != http.StatusUnauthorized {
		t.Errorf("old user after reload: code=%d want 401", rec.Code)
	}
	if rec := doReq(t, h, http.MethodGet, "new", "new"); rec.Code != http.StatusOK {
		t.Errorf("new user after reload: code=%d want 200", rec.Code)
	}
}
