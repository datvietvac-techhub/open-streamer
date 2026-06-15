package mediaauth

import (
	"net"
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

const (
	testCode   = domain.StreamCode("ch1")
	testPolicy = domain.PolicyCode("p1")
)

func ip(s string) net.IP { return net.ParseIP(s) }

// authzAll binds EVERY stream code to policy p and loads p as the only policy.
// Use for single-policy chain tests where the request's stream code is
// irrelevant beyond selecting the policy.
func authzAll(geo GeoResolver, p *domain.Policy) *Authorizer {
	a := New(geo, func(domain.StreamCode) domain.PolicyCode { return p.Code })
	a.SetPolicies([]*domain.Policy{p})
	return a
}

// ── token sign/verify ──

func TestToken_SignVerifyRoundTrip(t *testing.T) {
	t.Parallel()
	secret := []byte("sekret")
	exp := time.Now().Add(time.Hour).Unix()
	tok := SignToken(secret, testCode, exp)
	if !verify(secret, testCode, tok) {
		t.Fatal("valid token failed to verify")
	}
}

func TestToken_Rejects(t *testing.T) {
	t.Parallel()
	secret := []byte("sekret")
	valid := SignToken(secret, testCode, time.Now().Add(time.Hour).Unix())

	t.Run("expired", func(t *testing.T) {
		old := SignToken(secret, testCode, time.Now().Add(-2*time.Hour).Unix())
		if verify(secret, testCode, old) {
			t.Error("expired token verified")
		}
	})
	t.Run("wrong_stream_code", func(t *testing.T) {
		if verify(secret, "other", valid) {
			t.Error("token for ch1 verified against 'other'")
		}
	})
	t.Run("tampered_sig", func(t *testing.T) {
		if verify(secret, testCode, valid+"x") {
			t.Error("tampered token verified")
		}
	})
	t.Run("different_secret", func(t *testing.T) {
		if verify([]byte("other"), testCode, valid) {
			t.Error("token verified under a different secret")
		}
	})
	t.Run("malformed", func(t *testing.T) {
		for _, bad := range []string{"", "noexp", "abc.def", ".sig", "999"} {
			if verify(secret, testCode, bad) {
				t.Errorf("malformed token %q verified", bad)
			}
		}
	})
}

// ── chain ──

func TestAuthorize_NoPolicyAllowsAll(t *testing.T) {
	t.Parallel()
	// nil resolver → no policy for any stream → public.
	a := New(nil, nil)
	if d := a.Authorize(AuthRequest{Code: testCode, ClientIP: ip("8.8.8.8")}); !d.Allow {
		t.Fatalf("no resolver must allow, got deny: %s", d.Reason)
	}
	// resolver returning "" → no policy → public.
	b := New(nil, func(domain.StreamCode) domain.PolicyCode { return "" })
	if d := b.Authorize(AuthRequest{Code: testCode}); !d.Allow {
		t.Fatalf("empty policy code must allow, got deny: %s", d.Reason)
	}
}

func TestAuthorize_UnknownPolicyFailsClosed(t *testing.T) {
	t.Parallel()
	// Stream references a policy that is not in the loaded set → deny.
	a := New(nil, func(domain.StreamCode) domain.PolicyCode { return "ghost" })
	a.SetPolicies([]*domain.Policy{{Code: "real"}})
	if d := a.Authorize(AuthRequest{Code: testCode}); d.Allow {
		t.Error("reference to an unknown policy must fail closed")
	}
}

func TestAuthorize_IPRules(t *testing.T) {
	t.Parallel()
	a := authzAll(nil, &domain.Policy{
		Code:     testPolicy,
		DenyIPs:  []string{"8.8.8.8", "10.0.0.0/8"},
		AllowIPs: []string{"1.2.3.4", "192.168.0.0/16"},
	})

	cases := []struct {
		ip   string
		want bool
	}{
		{"8.8.8.8", false},     // deny exact
		{"10.5.5.5", false},    // deny CIDR
		{"1.2.3.4", true},      // allow exact
		{"192.168.1.9", true},  // allow CIDR
		{"203.0.113.7", false}, // not on allow list → deny
	}
	for _, c := range cases {
		if d := a.Authorize(AuthRequest{Code: testCode, ClientIP: ip(c.ip)}); d.Allow != c.want {
			t.Errorf("ip %s: allow=%v want %v (%s)", c.ip, d.Allow, c.want, d.Reason)
		}
	}
}

func TestAuthorize_CountryRules(t *testing.T) {
	t.Parallel()
	geo := func(p net.IP) string {
		switch p.String() {
		case "1.1.1.1":
			return "VN"
		case "2.2.2.2":
			return "RU"
		}
		return "" // unknown
	}
	a := authzAll(geo, &domain.Policy{
		Code:           testPolicy,
		AllowCountries: []string{"VN", "US"},
		DenyCountries:  []string{"RU"},
	})

	if d := a.Authorize(AuthRequest{Code: testCode, ClientIP: ip("1.1.1.1")}); !d.Allow {
		t.Errorf("VN must be allowed: %s", d.Reason)
	}
	if d := a.Authorize(AuthRequest{Code: testCode, ClientIP: ip("2.2.2.2")}); d.Allow {
		t.Error("RU must be denied")
	}
	if d := a.Authorize(AuthRequest{Code: testCode, ClientIP: ip("9.9.9.9")}); d.Allow {
		t.Error("unknown country must fail the allow-list gate")
	}
}

func TestAuthorize_UserAgentRules(t *testing.T) {
	t.Parallel()
	a := authzAll(nil, &domain.Policy{Code: testPolicy, DenyUserAgents: []string{"badbot"}})
	if d := a.Authorize(AuthRequest{Code: testCode, UserAgent: "Mozilla BadBot/1.0"}); d.Allow {
		t.Error("denied UA substring must block")
	}
	if d := a.Authorize(AuthRequest{Code: testCode, UserAgent: "VLC/3.0"}); !d.Allow {
		t.Errorf("allowed UA must pass: %s", d.Reason)
	}

	b := authzAll(nil, &domain.Policy{Code: testPolicy, AllowUserAgents: []string{"exoplayer"}})
	if d := b.Authorize(AuthRequest{Code: testCode, UserAgent: "ExoPlayerLib/2.1"}); !d.Allow {
		t.Errorf("UA on allow list must pass: %s", d.Reason)
	}
	if d := b.Authorize(AuthRequest{Code: testCode, UserAgent: "curl/8"}); d.Allow {
		t.Error("UA off allow list must be denied")
	}
}

func TestAuthorize_AllowedDomains(t *testing.T) {
	t.Parallel()
	a := authzAll(nil, &domain.Policy{Code: testPolicy, AllowedDomains: []string{"example.com"}})
	cases := []struct {
		ref  string
		want bool
	}{
		{"https://example.com/player", true},
		{"https://play.example.com/embed", true}, // subdomain
		{"https://evil.com/", false},
		{"", false}, // no referer → fail the gate
	}
	for _, c := range cases {
		if d := a.Authorize(AuthRequest{Code: testCode, Referer: c.ref}); d.Allow != c.want {
			t.Errorf("referer %q: allow=%v want %v", c.ref, d.Allow, c.want)
		}
	}
}

func TestAuthorize_TokenPolicy(t *testing.T) {
	t.Parallel()
	a := authzAll(nil, &domain.Policy{Code: testPolicy, RequireToken: true, TokenSecret: "sk"})
	good := SignToken([]byte("sk"), testCode, time.Now().Add(time.Hour).Unix())
	if d := a.Authorize(AuthRequest{Code: testCode, Token: good}); !d.Allow {
		t.Errorf("valid token must pass: %s", d.Reason)
	}
	if d := a.Authorize(AuthRequest{Code: testCode, Token: ""}); d.Allow {
		t.Error("missing token under token policy must deny")
	}
	if d := a.Authorize(AuthRequest{Code: testCode, Token: "bogus.sig"}); d.Allow {
		t.Error("bogus token must deny")
	}
	// authzAll binds ch2 to the same token policy; a token minted for ch1 must
	// not authorize ch2 (the MAC is bound to the stream code).
	if d := a.Authorize(AuthRequest{Code: "ch2", Token: good}); d.Allow {
		t.Error("token bound to ch1 must not authorize ch2")
	}
}

func TestAuthorize_TokenPolicyNoSecretFailsClosed(t *testing.T) {
	t.Parallel()
	// Defensive: domain.Validate rejects this, but the authorizer must still
	// fail closed if a secret-less token policy ever reaches it.
	a := authzAll(nil, &domain.Policy{Code: testPolicy, RequireToken: true})
	if d := a.Authorize(AuthRequest{Code: testCode, Token: "x.y"}); d.Allow {
		t.Error("token policy without a secret must fail closed")
	}
}

func TestAuthorize_PerStreamPolicySelection(t *testing.T) {
	t.Parallel()
	open := &domain.Policy{Code: "open"} // no token, no lists → public
	tok := &domain.Policy{Code: "tok", RequireToken: true, TokenSecret: "sk"}
	resolve := func(c domain.StreamCode) domain.PolicyCode {
		switch c {
		case "public1":
			return "open"
		case "token1":
			return "tok"
		}
		return "" // no policy → public
	}
	a := New(nil, resolve)
	a.SetPolicies([]*domain.Policy{open, tok})

	if d := a.Authorize(AuthRequest{Code: "public1"}); !d.Allow {
		t.Errorf("stream bound to an open policy must allow without token: %s", d.Reason)
	}
	if d := a.Authorize(AuthRequest{Code: "token1"}); d.Allow {
		t.Error("stream bound to a token policy must require a token")
	}
	good := SignToken([]byte("sk"), "token1", time.Now().Add(time.Hour).Unix())
	if d := a.Authorize(AuthRequest{Code: "token1", Token: good}); !d.Allow {
		t.Errorf("valid token for token1 must allow: %s", d.Reason)
	}
	if d := a.Authorize(AuthRequest{Code: "nopolicy"}); !d.Allow {
		t.Errorf("stream with no policy must allow: %s", d.Reason)
	}
}

func TestAuthorize_DenyBeatsAllow(t *testing.T) {
	t.Parallel()
	// IP is on both lists — deny must win.
	a := authzAll(nil, &domain.Policy{
		Code:     testPolicy,
		AllowIPs: []string{"1.2.3.4"},
		DenyIPs:  []string{"1.2.3.4"},
	})
	if d := a.Authorize(AuthRequest{Code: testCode, ClientIP: ip("1.2.3.4")}); d.Allow {
		t.Error("deny must take precedence over allow")
	}
}

func TestAuthorize_HotReload(t *testing.T) {
	t.Parallel()
	a := New(nil, func(domain.StreamCode) domain.PolicyCode { return testPolicy })
	a.SetPolicies([]*domain.Policy{{Code: testPolicy}}) // empty policy → allow all
	if d := a.Authorize(AuthRequest{Code: testCode, ClientIP: ip("8.8.8.8")}); !d.Allow {
		t.Fatalf("empty policy should allow: %s", d.Reason)
	}
	a.SetPolicies([]*domain.Policy{{Code: testPolicy, DenyIPs: []string{"8.8.8.8"}}})
	if d := a.Authorize(AuthRequest{Code: testCode, ClientIP: ip("8.8.8.8")}); d.Allow {
		t.Error("after reload, denied IP must be blocked")
	}
}
