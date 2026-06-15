package netguard

import (
	"context"
	"net"
	"net/http"
	"testing"
)

func TestBlockedIP(t *testing.T) {
	t.Parallel()
	none := Policy{}
	priv := Policy{AllowPrivate: true}                          // webhooks: private OK, loopback NOT
	internal := Policy{AllowLoopback: true, AllowPrivate: true} // ingest opt-in
	cases := []struct {
		ip      string
		p       Policy
		blocked bool
	}{
		// Link-local / cloud-metadata / unspecified: blocked UNCONDITIONALLY.
		{"169.254.169.254", internal, true}, // AWS/GCP/Azure IMDS (link-local)
		{"169.254.1.1", internal, true},
		{"fe80::1", internal, true},
		{"0.0.0.0", internal, true},
		{"::", internal, true},
		// Loopback: blocked by default + under AllowPrivate-only (webhook policy),
		// allowed only under AllowLoopback.
		{"127.0.0.1", none, true},
		{"127.0.0.1", priv, true}, // webhook policy must NOT reach localhost
		{"::1", priv, true},
		{"127.0.0.1", internal, false},
		{"::1", internal, false},
		// Private: blocked by default, allowed under AllowPrivate.
		{"10.0.0.1", none, true},
		{"192.168.1.10", none, true},
		{"fc00::1", none, true},         // IPv6 ULA
		{"100.100.100.200", none, true}, // RFC6598 CGNAT (Alibaba metadata)
		{"10.0.0.1", priv, false},
		{"192.168.1.10", priv, false},
		{"fc00::1", priv, false},
		// Public: always allowed.
		{"8.8.8.8", none, false},
		{"1.1.1.1", none, false},
		// Multicast (UDP ingest) is not private → allowed.
		{"239.0.0.1", none, false},
	}
	for _, c := range cases {
		err := blockedIP(net.ParseIP(c.ip), c.p)
		if (err != nil) != c.blocked {
			t.Errorf("blockedIP(%s, %+v) err=%v, want blocked=%v", c.ip, c.p, err, c.blocked)
		}
	}
}

func TestValidateInputURL(t *testing.T) {
	t.Parallel()
	bad := []string{
		"http://169.254.169.254/latest/meta-data/",
		"http://0.0.0.0/x",
	}
	for _, u := range bad {
		if err := ValidateInputURL(u); err == nil {
			t.Errorf("ValidateInputURL(%q) = nil, want error", u)
		}
	}
	good := []string{
		"http://cdn.example.com/playlist.m3u8", // hostname → checked at dial time
		"https://8.8.8.8/x",
		"http://127.0.0.1:8080/x", // loopback NOT blocked at save time (dial-time gated)
		"http://10.0.0.5/x",       // private NOT blocked at save time (dial-time gated)
		"rtsp://192.168.1.9/s",
		"udp://239.0.0.1:1234",
		"file:///srv/media/a.ts",
		"publish://ingest",
	}
	for _, u := range good {
		if err := ValidateInputURL(u); err != nil {
			t.Errorf("ValidateInputURL(%q) = %v, want nil", u, err)
		}
	}
}

func TestCheckRedirect(t *testing.T) {
	t.Parallel()
	cr := CheckRedirect()

	ctx := context.Background()
	// Depth cap.
	via := make([]*http.Request, MaxRedirects)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://a.example/x", nil)
	if err := cr(req, via); err == nil {
		t.Error("expected redirect-depth error at the cap")
	}

	// Cross-host strips credential headers.
	prev, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://a.example/x", nil)
	next, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://b.example/y", nil)
	next.Header.Set("Authorization", "Bearer secret")
	next.Header.Set("Cookie", "sid=1")
	if err := cr(next, []*http.Request{prev}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next.Header.Get("Authorization") != "" || next.Header.Get("Cookie") != "" {
		t.Error("credential headers must be stripped on cross-host redirect")
	}

	// Same-host keeps headers.
	p2, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://a.example/x", nil)
	n2, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://a.example/y", nil)
	n2.Header.Set("Authorization", "Bearer keep")
	if err := cr(n2, []*http.Request{p2}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n2.Header.Get("Authorization") == "" {
		t.Error("same-host redirect must keep headers")
	}
}
