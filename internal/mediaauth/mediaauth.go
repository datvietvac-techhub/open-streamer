// Package mediaauth authorizes playback (media-plane) requests across every
// delivery protocol (HLS, DASH, HTTP-MPEGTS, RTMP, SRT, RTSP). It is the
// counterpart to the control-plane (admin) auth in internal/api: that gates
// who can CONFIGURE the server; this gates who can WATCH a stream.
//
// Authorization is policy-based. A stream references at most one named Policy
// (domain.Policy) via Stream.PlaybackPolicy; a stream with NO policy is public
// (allow-all). Each policy is fully self-contained — it carries its OWN token
// secret and its OWN static allow/deny chain. There is no global media-auth
// config.
//
// One Authorizer is shared by all protocol handlers. Each handler builds an
// AuthRequest and calls Authorize before allocating any streaming state. The
// chain (deny wins, allow-lists restrict, then a token gate) runs against the
// stream's resolved policy:
//
//  1. stream has no policy                                  → ALLOW
//  2. stream references an unknown policy                   → DENY (fail-closed)
//  3. ClientIP / Country / User-Agent on a Deny* list       → DENY
//  4. any non-empty Allow* list the request value misses    → DENY
//  5. AllowedDomains set and the Referer host isn't covered → DENY
//  6. policy requires a token and it is missing/invalid     → DENY
//  7. otherwise                                             → ALLOW
//
// The compiled policy set is swapped atomically for hot-reload (SetPolicies).
package mediaauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// clockSkew tolerates small clock differences when checking token expiry.
const clockSkew = 60 * time.Second

// AuthRequest is the per-request context a protocol handler hands to Authorize.
type AuthRequest struct {
	Code      domain.StreamCode
	ClientIP  net.IP // may be nil if unparseable
	Token     string // playback token (?token= / streamid / Authorization)
	UserAgent string
	Referer   string
}

// Decision is the outcome. Allow=false carries a short, non-leaky Reason for
// logs (never returned to the client verbatim).
type Decision struct {
	Allow  bool
	Reason string
}

func allow() Decision             { return Decision{Allow: true} }
func deny(reason string) Decision { return Decision{Allow: false, Reason: reason} }

// GeoResolver maps a client IP to an ISO 3166-1 alpha-2 country code, or "" if
// unknown / unavailable. Backed by the sessions GeoIP database.
type GeoResolver func(net.IP) string

// PolicyResolver returns the code of the Policy a stream is bound to, or "" when
// the stream has no policy (→ public). Backed by the publisher's in-memory
// stream table for live streams (O(1), no store read per segment) with a
// store+template fallback for stopped streams whose DVR archive is still served.
type PolicyResolver func(domain.StreamCode) domain.PolicyCode

// Authorizer evaluates the policy chain. Safe for concurrent use.
type Authorizer struct {
	policies atomic.Pointer[map[domain.PolicyCode]*policy]
	geo      GeoResolver
	resolve  PolicyResolver
}

// policy is an immutable, pre-parsed snapshot of one domain.Policy.
type policy struct {
	requireToken bool
	secret       []byte

	allowNets, denyNets []*net.IPNet
	allowIPs, denyIPs   map[string]struct{} // exact IPs (canonical string)
	allowCountries      map[string]struct{}
	denyCountries       map[string]struct{}
	allowUAs, denyUAs   []string // lower-cased substrings
	allowedDomains      []string // lower-cased hosts
	hasIPAllow          bool
	hasCountryAllow     bool
	hasUAAllow          bool
	hasDomainAllow      bool
}

// New builds an Authorizer with an empty policy set. geo and resolve may be nil
// (country rules then never match an allow-list, and every stream resolves to no
// policy → allow-all). Call SetPolicies to load the compiled rule set.
func New(geo GeoResolver, resolve PolicyResolver) *Authorizer {
	a := &Authorizer{geo: geo, resolve: resolve}
	a.SetPolicies(nil)
	return a
}

// SetPolicies compiles and atomically swaps in a fresh policy set (hot-reload).
// Safe to call concurrently with Authorize.
func (a *Authorizer) SetPolicies(ps []*domain.Policy) {
	m := make(map[domain.PolicyCode]*policy, len(ps))
	for _, p := range ps {
		if p == nil {
			continue
		}
		m[p.Code] = compilePolicy(p)
	}
	a.policies.Store(&m)
}

func compilePolicy(p *domain.Policy) *policy {
	st := &policy{
		requireToken:   p.RequireToken,
		secret:         []byte(p.TokenSecret),
		allowCountries: upperSet(p.AllowCountries),
		denyCountries:  upperSet(p.DenyCountries),
		allowUAs:       lowerList(p.AllowUserAgents),
		denyUAs:        lowerList(p.DenyUserAgents),
		allowedDomains: lowerList(p.AllowedDomains),
	}
	st.allowNets, st.allowIPs = parseIPRules(p.AllowIPs)
	st.denyNets, st.denyIPs = parseIPRules(p.DenyIPs)
	st.hasIPAllow = len(st.allowNets) > 0 || len(st.allowIPs) > 0
	st.hasCountryAllow = len(st.allowCountries) > 0
	st.hasUAAllow = len(st.allowUAs) > 0
	st.hasDomainAllow = len(st.allowedDomains) > 0
	return st
}

// Authorize resolves the stream's policy and runs its chain. A stream with no
// policy is public; a reference to an unknown policy fails closed.
func (a *Authorizer) Authorize(req AuthRequest) Decision {
	var code domain.PolicyCode
	if a.resolve != nil {
		code = a.resolve(req.Code)
	}
	if code == "" {
		return allow() // no policy → public
	}
	m := a.policies.Load()
	var pol *policy
	if m != nil {
		pol = (*m)[code]
	}
	if pol == nil {
		// Stream references a policy that no longer exists — fail closed.
		// The policy delete handler refuses to remove a referenced policy, so
		// this only happens after a direct store edit; denying is the safe call.
		return deny("unknown policy")
	}
	return pol.evaluate(req, a.geo)
}

// evaluate runs the deny → allow → token chain for one policy.
func (p *policy) evaluate(req AuthRequest, geo GeoResolver) Decision {
	// Country is resolved at most once, only when a rule needs it.
	country := ""
	if (len(p.denyCountries) > 0 || p.hasCountryAllow) && geo != nil {
		country = strings.ToUpper(geo(req.ClientIP))
	}
	if d := p.checkDeny(req, country); !d.Allow {
		return d
	}
	if d := p.checkAllow(req, country); !d.Allow {
		return d
	}
	return p.checkToken(req)
}

// checkDeny applies the hard-block deny lists (evaluated first).
func (p *policy) checkDeny(req AuthRequest, country string) Decision {
	if ipInRules(req.ClientIP, p.denyNets, p.denyIPs) {
		return deny("ip on deny list")
	}
	if country != "" {
		if _, bad := p.denyCountries[country]; bad {
			return deny("country on deny list")
		}
	}
	if uaMatches(req.UserAgent, p.denyUAs) {
		return deny("user-agent on deny list")
	}
	return allow()
}

// checkAllow applies the allow gates: each configured list must be satisfied.
func (p *policy) checkAllow(req AuthRequest, country string) Decision {
	if p.hasIPAllow && !ipInRules(req.ClientIP, p.allowNets, p.allowIPs) {
		return deny("ip not on allow list")
	}
	if p.hasCountryAllow {
		if _, ok := p.allowCountries[country]; !ok || country == "" {
			return deny("country not on allow list")
		}
	}
	if p.hasUAAllow && !uaMatches(req.UserAgent, p.allowUAs) {
		return deny("user-agent not on allow list")
	}
	if p.hasDomainAllow && !domainAllowed(req.Referer, p.allowedDomains) {
		return deny("referer domain not allowed")
	}
	return allow()
}

// checkToken applies the token gate when the policy requires a signed token.
func (p *policy) checkToken(req AuthRequest) Decision {
	if !p.requireToken {
		return allow()
	}
	if len(p.secret) == 0 {
		return deny("token required but policy has no secret")
	}
	if !verify(p.secret, req.Code, req.Token) {
		return deny("missing or invalid token")
	}
	return allow()
}

// ── playback token ──

// SignToken is the Go reference implementation of the playback-token format.
// The server only VERIFIES tokens; clients mint them with the policy's secret —
// reproduce this in any language:
//
//	exp   = future expiry, unix seconds (decimal string)
//	msg   = "<stream_code>|<exp>"
//	sig   = HMAC-SHA256(secret, msg)               // raw 32 bytes
//	token = "<exp>." + base64url-nopad(sig)
//	URL   = .../<code>/index.m3u8?token=<token>     // (or SRT streamid / RTSP query)
func SignToken(secret []byte, code domain.StreamCode, exp int64) string {
	return strconv.FormatInt(exp, 10) + "." + base64.RawURLEncoding.EncodeToString(tokenMAC(secret, code, exp))
}

func verify(secret []byte, code domain.StreamCode, token string) bool {
	dot := strings.IndexByte(token, '.')
	if dot <= 0 {
		return false
	}
	exp, err := strconv.ParseInt(token[:dot], 10, 64)
	if err != nil {
		return false
	}
	if time.Now().Add(-clockSkew).Unix() > exp {
		return false // expired
	}
	got, err := base64.RawURLEncoding.DecodeString(token[dot+1:])
	if err != nil {
		return false
	}
	want := tokenMAC(secret, code, exp)
	return subtle.ConstantTimeCompare(got, want) == 1
}

func tokenMAC(secret []byte, code domain.StreamCode, exp int64) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(string(code)))
	m.Write([]byte{'|'})
	m.Write([]byte(strconv.FormatInt(exp, 10)))
	return m.Sum(nil)
}

// ── helpers ──

func parseIPRules(rules []string) (nets []*net.IPNet, exact map[string]struct{}) {
	exact = map[string]struct{}{}
	for _, raw := range rules {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if _, n, err := net.ParseCIDR(s); err == nil {
			nets = append(nets, n)
			continue
		}
		if ip := net.ParseIP(s); ip != nil {
			exact[ip.String()] = struct{}{}
		}
	}
	return nets, exact
}

func ipInRules(ip net.IP, nets []*net.IPNet, exact map[string]struct{}) bool {
	if ip == nil {
		return false
	}
	if _, ok := exact[ip.String()]; ok {
		return true
	}
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

func uaMatches(ua string, patterns []string) bool {
	if ua == "" || len(patterns) == 0 {
		return false
	}
	lc := strings.ToLower(ua)
	for _, p := range patterns {
		if p != "" && strings.Contains(lc, p) {
			return true
		}
	}
	return false
}

// domainAllowed parses the Referer and checks its host against the allow-list
// (exact match or a parent domain, e.g. "example.com" covers "play.example.com").
func domainAllowed(referer string, domains []string) bool {
	if referer == "" {
		return false
	}
	u, err := url.Parse(referer)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return false
	}
	for _, d := range domains {
		if host == d || strings.HasSuffix(host, "."+d) {
			return true
		}
	}
	return false
}

func upperSet(in []string) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for _, s := range in {
		if s = strings.ToUpper(strings.TrimSpace(s)); s != "" {
			out[s] = struct{}{}
		}
	}
	return out
}

func lowerList(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.ToLower(strings.TrimSpace(s)); s != "" {
			out = append(out, s)
		}
	}
	return out
}
