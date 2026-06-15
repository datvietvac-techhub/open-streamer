package domain

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
)

// MaxPolicyCodeLen is the maximum length of a playback-policy code.
const MaxPolicyCodeLen = 128

// policyCodePattern allows alphanumerics, dash, and underscore. Like template
// codes, policies live in a flat namespace — they are referenced by streams
// (Stream.PlaybackPolicy), not mounted on a media URL path.
var policyCodePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// PolicyCode is the user-assigned primary key for a playback policy.
// Allowed characters: a-z, A-Z, 0-9, underscore, dash.
type PolicyCode string

// ValidatePolicyCode reports whether s is a non-empty valid policy code.
func ValidatePolicyCode(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return errors.New("policy code is required")
	}
	if len(s) > MaxPolicyCodeLen {
		return fmt.Errorf("policy code exceeds max length %d", MaxPolicyCodeLen)
	}
	if !policyCodePattern.MatchString(s) {
		return errors.New("policy code must contain only A-Z, a-z, 0-9, '_', and '-'")
	}
	return nil
}

// Policy is a reusable, named bundle of media-plane (playback) authorization
// rules. A stream references at most one Policy via Stream.PlaybackPolicy; a
// stream with no policy is public (allow-all). Each policy is fully
// self-contained — it carries its OWN token secret and its OWN static
// allow/deny chain. There is no global media-auth config.
//
// Evaluation per playback request (a chain; deny wins, allow-lists restrict,
// then the token gate): a value on any Deny* list rejects; for any non-empty
// Allow* list the request's value MUST appear on it; finally if RequireToken is
// set a valid signed token (HMAC over the stream code, keyed by TokenSecret) is
// required. AllowedDomains gates the HTTP Referer host for browser embeds.
type Policy struct {
	// Code is the unique key chosen by the operator.
	Code PolicyCode `json:"code" yaml:"code"`

	// Name and Description are operator-facing metadata.
	Name        string `json:"name,omitempty" yaml:"name,omitempty"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`

	// RequireToken makes a valid signed playback token mandatory. The server
	// only VERIFIES tokens; clients mint them with TokenSecret — see
	// internal/mediaauth.SignToken for the canonical token format.
	RequireToken bool `json:"require_token,omitempty" yaml:"require_token,omitempty"`

	// TokenSecret is this policy's HMAC-SHA256 verification key. Required when
	// RequireToken is true. Each policy owns its own secret so revoking one
	// policy's key never affects another.
	TokenSecret string `json:"token_secret,omitempty" yaml:"token_secret,omitempty"`

	// Static allow/deny chain. IPs accept exact addresses or CIDR ranges;
	// Countries are ISO 3166-1 alpha-2 codes (need a GeoIP DB — see
	// SessionsConfig.GeoIPDBPath); UserAgents match case-insensitive substring;
	// AllowedDomains match the Referer host (exact or parent domain).
	AllowIPs        []string `json:"allow_ips,omitempty" yaml:"allow_ips,omitempty"`
	DenyIPs         []string `json:"deny_ips,omitempty" yaml:"deny_ips,omitempty"`
	AllowCountries  []string `json:"allow_countries,omitempty" yaml:"allow_countries,omitempty"`
	DenyCountries   []string `json:"deny_countries,omitempty" yaml:"deny_countries,omitempty"`
	AllowUserAgents []string `json:"allow_user_agents,omitempty" yaml:"allow_user_agents,omitempty"`
	DenyUserAgents  []string `json:"deny_user_agents,omitempty" yaml:"deny_user_agents,omitempty"`
	AllowedDomains  []string `json:"allowed_domains,omitempty" yaml:"allowed_domains,omitempty"`
}

// countryCodePattern matches a two-letter ISO 3166-1 alpha-2 code (any case).
var countryCodePattern = regexp.MustCompile(`^[A-Za-z]{2}$`)

// Validate reports whether the policy is internally consistent: a valid code,
// a secret present when a token is required, and every IP / country entry
// well-formed. It is called at save time so bad rules never reach the on-disk
// store (where a silently-dropped CIDR could weaken a deny list).
func (p *Policy) Validate() error {
	if err := ValidatePolicyCode(string(p.Code)); err != nil {
		return err
	}
	if p.RequireToken && strings.TrimSpace(p.TokenSecret) == "" {
		return errors.New("policy requires a token but token_secret is empty")
	}
	if err := validateIPRules("allow_ips", p.AllowIPs); err != nil {
		return err
	}
	if err := validateIPRules("deny_ips", p.DenyIPs); err != nil {
		return err
	}
	if err := validateCountryCodes("allow_countries", p.AllowCountries); err != nil {
		return err
	}
	if err := validateCountryCodes("deny_countries", p.DenyCountries); err != nil {
		return err
	}
	return nil
}

// validateIPRules checks each entry parses as a bare IP or a CIDR range.
func validateIPRules(field string, rules []string) error {
	for _, raw := range rules {
		s := strings.TrimSpace(raw)
		if s == "" {
			return fmt.Errorf("%s: empty entry", field)
		}
		if _, _, err := net.ParseCIDR(s); err == nil {
			continue
		}
		if net.ParseIP(s) != nil {
			continue
		}
		return fmt.Errorf("%s: %q is not a valid IP address or CIDR range", field, s)
	}
	return nil
}

// validateCountryCodes checks each entry is a two-letter ISO 3166-1 code.
func validateCountryCodes(field string, codes []string) error {
	for _, raw := range codes {
		s := strings.TrimSpace(raw)
		if !countryCodePattern.MatchString(s) {
			return fmt.Errorf("%s: %q is not a 2-letter ISO 3166-1 country code", field, raw)
		}
	}
	return nil
}
