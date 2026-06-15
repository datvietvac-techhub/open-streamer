package api

// apiauth.go — HTTP Basic auth for the management (control-plane) API.
//
// Two roles only: "read" (GET/HEAD/OPTIONS) and "write" (all methods). Users
// live in the global config (config.AuthConfig.API) and are managed like any
// other config by a write user. Passwords are stored as bcrypt hashes — never
// plaintext. The whole feature is opt-in: when Enabled is false the middleware
// is a pass-through, preserving the prior open-API behaviour.
//
// Threading: the resolved user set is held in an atomic.Pointer so the config
// hot-reload path (runtime.Manager.diff) can swap it via SetConfig without
// rebuilding the router or restarting the server.

import (
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"golang.org/x/crypto/bcrypt"

	"github.com/datvietvac-techhub/open-streamer/config"
)

const (
	roleRead  = "read"
	roleWrite = "write"

	// adminPasswordEnv seeds a bootstrap `admin` write user when auth is
	// enabled but no users are configured yet — avoids locking the operator
	// out on first enable.
	adminPasswordEnv = "OPEN_STREAMER_API_ADMIN_PASSWORD"

	basicRealm = "open-streamer"
)

// dummyHash is a valid bcrypt hash of a random string. Comparing an unknown
// username's password against it keeps the auth-failure path's timing close to
// the success path, so response time can't be used to enumerate usernames.
var dummyHash = []byte("$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy")

// apiUser is a resolved (validated) account.
type apiUser struct {
	hash []byte
	role string
}

// authState is an immutable snapshot of the auth configuration. Swapped
// atomically on config reload.
type authState struct {
	enabled bool
	users   map[string]apiUser
}

// Auth enforces Basic auth + read/write roles on the management API.
type Auth struct {
	state atomic.Pointer[authState]
}

// newAuth builds the authenticator from config. Invalid users are skipped
// with a warning; an enabled-but-empty user set is left empty (every request is
// then rejected — fail-closed) unless the bootstrap env var seeds an admin.
func newAuth(cfg config.AuthConfig) *Auth {
	a := &Auth{}
	a.SetConfig(cfg)
	return a
}

// SetConfig swaps in a new auth configuration atomically (config hot-reload).
func (a *Auth) SetConfig(cfg config.AuthConfig) {
	st := buildAuthState(cfg.API)
	a.state.Store(st)
	if st.enabled && len(st.users) == 0 {
		slog.Warn("api: Basic auth is ENABLED but no users are configured — all management requests will be rejected; " +
			"define auth.api.users or set " + adminPasswordEnv)
	}
}

// buildAuthState validates users and applies the bootstrap env seed.
func buildAuthState(cfg config.APIAuthConfig) *authState {
	st := &authState{enabled: cfg.Enabled, users: make(map[string]apiUser)}
	if !cfg.Enabled {
		return st
	}
	for _, u := range cfg.Users {
		name := strings.TrimSpace(u.Username)
		role := strings.ToLower(strings.TrimSpace(u.Role))
		if name == "" || u.PasswordHash == "" {
			slog.Warn("api: skipping auth user with empty username/password_hash", "username", name)
			continue
		}
		if role != roleRead && role != roleWrite {
			slog.Warn("api: skipping auth user with invalid role (want read|write)", "username", name, "role", u.Role)
			continue
		}
		st.users[name] = apiUser{hash: []byte(u.PasswordHash), role: role}
	}
	// Bootstrap: seed an admin from the env var when no users are defined, so
	// enabling auth doesn't immediately lock the operator out.
	if len(st.users) == 0 {
		if pw := os.Getenv(adminPasswordEnv); pw != "" {
			if h, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost); err == nil {
				st.users["admin"] = apiUser{hash: h, role: roleWrite}
				slog.Warn("api: seeded bootstrap 'admin' (write) user from " + adminPasswordEnv + " — define auth.api.users to make it permanent")
			} else {
				slog.Error("api: failed to hash bootstrap admin password", "err", err)
			}
		}
	}
	return st
}

// Middleware enforces auth on the wrapped (management) handlers. It is a
// pass-through when auth is disabled.
func (a *Auth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := a.state.Load()
		if st == nil || !st.enabled {
			next.ServeHTTP(w, r)
			return
		}
		user, pass, ok := r.BasicAuth()
		if !ok {
			a.challenge(w)
			return
		}
		role, authed := st.verify(user, pass)
		if !authed {
			a.challenge(w)
			return
		}
		if !roleAllowsMethod(role, r.Method) {
			http.Error(w, "forbidden: read-only user cannot perform this action", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *Auth) challenge(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Basic realm="`+basicRealm+`", charset="UTF-8"`)
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}

// verify checks credentials and returns the user's role. An unknown username
// still runs a bcrypt comparison against a dummy hash so timing doesn't reveal
// whether the username exists.
func (st *authState) verify(user, pass string) (role string, ok bool) {
	u, found := st.users[user]
	if !found {
		_ = bcrypt.CompareHashAndPassword(dummyHash, []byte(pass))
		return "", false
	}
	if bcrypt.CompareHashAndPassword(u.hash, []byte(pass)) != nil {
		return "", false
	}
	return u.role, true
}

// roleAllowsMethod maps the two roles to HTTP methods: read may only perform
// safe (non-mutating) methods; write may perform any.
func roleAllowsMethod(role, method string) bool {
	if role == roleWrite {
		return true
	}
	if role == roleRead {
		switch method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			return true
		}
	}
	return false
}
