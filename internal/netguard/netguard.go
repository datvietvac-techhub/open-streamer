// Package netguard provides an SSRF egress guard for the webhook HTTP client,
// which makes outbound requests to operator-supplied URLs.
//
// The core is a dial-time IP filter installed via net.Dialer.Control. Control
// runs with the RESOLVED ip:port for each connection attempt, so it sees the
// real destination even when a hostname resolves to a forbidden address — this
// defeats DNS-rebinding and redirect-to-internal chains that a save-time string
// check cannot.
//
// Link-local (which covers cloud-metadata 169.254.169.254) and the unspecified
// address are rejected UNCONDITIONALLY. Loopback and private (RFC1918 / IPv6-ULA
// / RFC6598) are each gated by the caller's Policy:
//
//   - Webhooks: AllowPrivate (internal webhooks are legitimate) but NOT loopback
//     (a hook must never reach the local admin API).
//
// NOTE: ingest pull clients (HLS / HTTP-TS) historically applied this guard via
// IngestPolicy + ingestor.allow_private_targets, but ingest SSRF guarding was
// removed by operator decision — the server's primary role is pulling from
// internal/LAN sources, so blocking private targets was counterproductive (see
// the security audit, S-4). IngestPolicy / ValidateInputURL remain as helpers.
package netguard

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// MaxRedirects caps redirect chains (matches Go's default) and is enforced by
// CheckRedirect so a redirect loop can't be used to amplify or evade the guard.
const MaxRedirects = 10

// cgNAT is RFC 6598 shared address space (100.64.0.0/10) — used by some cloud
// metadata services (e.g. Alibaba 100.100.100.200) and never a valid public
// origin, so it is treated like RFC1918 under the AllowPrivate gate.
var cgNAT = mustCIDR("100.64.0.0/10")

func mustCIDR(s string) *net.IPNet {
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		panic(err)
	}
	return n
}

// ErrBlocked is the sentinel wrapped by every guard rejection.
var ErrBlocked = errors.New("netguard: destination blocked")

// Policy selects which otherwise-internal destinations a guard tolerates.
// Link-local / cloud-metadata / unspecified are always blocked regardless.
type Policy struct {
	// AllowLoopback permits 127.0.0.0/8 and ::1.
	AllowLoopback bool
	// AllowPrivate permits RFC1918, IPv6-ULA, and RFC6598 shared space.
	AllowPrivate bool
}

// IngestPolicy is the policy for ingest pull clients: a single
// allow_private_targets operator opt-in unlocks BOTH loopback and private
// (a trusted-network "allow internal sources" switch). Default false blocks
// all internal targets.
func IngestPolicy(allowPrivate bool) Policy {
	return Policy{AllowLoopback: allowPrivate, AllowPrivate: allowPrivate}
}

// blockedIP reports why ip must not be dialed under p, or nil when allowed.
func blockedIP(ip net.IP, p Policy) error {
	switch {
	case ip == nil:
		return nil // unparseable here — the dialer will fail on its own
	case ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast():
		return fmt.Errorf("%w: link-local / cloud-metadata (169.254.0.0/16, fe80::/10)", ErrBlocked)
	case ip.IsUnspecified():
		return fmt.Errorf("%w: unspecified address", ErrBlocked)
	case ip.IsLoopback():
		if p.AllowLoopback {
			return nil
		}
		return fmt.Errorf("%w: loopback", ErrBlocked)
	}
	if !p.AllowPrivate && (ip.IsPrivate() || cgNAT.Contains(ip)) {
		return fmt.Errorf("%w: private/shared address", ErrBlocked)
	}
	return nil
}

// dialControl returns a net.Dialer.Control hook that rejects forbidden resolved
// IPs. address is the post-DNS "ip:port".
func dialControl(p Policy) func(network, address string, c syscall.RawConn) error {
	return func(_ /*network*/, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			host = address
		}
		if e := blockedIP(net.ParseIP(host), p); e != nil {
			return fmt.Errorf("%s: %w", host, e)
		}
		return nil
	}
}

// ApplyToTransport installs the dial-time guard on t (replacing its DialContext)
// so connections to forbidden IPs fail at connect time.
func ApplyToTransport(t *http.Transport, p Policy) {
	if t == nil {
		return
	}
	d := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second, Control: dialControl(p)}
	t.DialContext = d.DialContext
}

// CheckRedirect returns an http.Client.CheckRedirect that caps the chain depth
// and strips credential headers when a redirect crosses to a different host (so
// configured upstream auth isn't leaked to an attacker-chosen target). The
// redirect's own dial is still re-validated by the transport's dial guard.
func CheckRedirect() func(req *http.Request, via []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= MaxRedirects {
			return fmt.Errorf("netguard: stopped after %d redirects", MaxRedirects)
		}
		if len(via) > 0 && !strings.EqualFold(req.URL.Host, via[len(via)-1].URL.Host) {
			for _, h := range []string{"Authorization", "Cookie", "Proxy-Authorization"} {
				req.Header.Del(h)
			}
		}
		return nil
	}
}

// NewClient builds an http.Client with the dial guard + redirect policy. A
// timeout of 0 means no client-level timeout (stream-body callers set 0 and
// rely on per-request context deadlines).
func NewClient(p Policy, timeout time.Duration) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	ApplyToTransport(t, p)
	return &http.Client{
		Timeout:       timeout,
		Transport:     t,
		CheckRedirect: CheckRedirect(),
	}
}

// ValidateInputURL is a fast, config-independent save-time check: it rejects a
// URL whose host is a literal always-blocked IP (link-local / cloud-metadata /
// unspecified). It deliberately does NOT block loopback or private IPs (those
// are gated at dial time by the Policy, so a LAN or local source still saves)
// and does NOT resolve DNS. The authoritative guard is ApplyToTransport — this
// is fast operator feedback.
func ValidateInputURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	host := u.Hostname()
	if host == "" {
		return nil // file:// / publish:// / schemes without a host — out of scope
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil // hostname — resolved + guarded at dial time
	}
	switch {
	case ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast():
		return fmt.Errorf("host %q is a link-local / cloud-metadata address", host)
	case ip.IsUnspecified():
		return fmt.Errorf("host %q is the unspecified address", host)
	}
	return nil
}
