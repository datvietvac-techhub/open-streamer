package domain

import "testing"

func TestValidatePolicyCode(t *testing.T) {
	t.Parallel()
	good := []string{"vip", "geo_block", "token-only", "P1"}
	for _, c := range good {
		if err := ValidatePolicyCode(c); err != nil {
			t.Errorf("ValidatePolicyCode(%q) = %v, want nil", c, err)
		}
	}
	bad := []string{"", "  ", "with/slash", "has space", "dot.dot"}
	for _, c := range bad {
		if err := ValidatePolicyCode(c); err == nil {
			t.Errorf("ValidatePolicyCode(%q) = nil, want error", c)
		}
	}
}

func TestPolicyValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		p       Policy
		wantErr bool
	}{
		{"minimal", Policy{Code: "p1"}, false},
		{"full ok", *NewFullPolicyForTest("p1"), false},
		{"bad code", Policy{Code: "a/b"}, true},
		{"token no secret", Policy{Code: "p1", RequireToken: true}, true},
		{"token with secret", Policy{Code: "p1", RequireToken: true, TokenSecret: "sk"}, false},
		{"bad allow ip", Policy{Code: "p1", AllowIPs: []string{"not-an-ip"}}, true},
		{"bad deny cidr", Policy{Code: "p1", DenyIPs: []string{"10.0.0.0/99"}}, true},
		{"good cidr + ip", Policy{Code: "p1", AllowIPs: []string{"10.0.0.0/8", "1.2.3.4"}}, false},
		{"bad country", Policy{Code: "p1", AllowCountries: []string{"VNM"}}, true},
		{"good country", Policy{Code: "p1", DenyCountries: []string{"vn", "RU"}}, false},
		{"empty ip entry", Policy{Code: "p1", AllowIPs: []string{""}}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.p.Validate()
			if (err != nil) != c.wantErr {
				t.Errorf("Validate() err = %v, wantErr = %v", err, c.wantErr)
			}
		})
	}
}

// NewFullPolicyForTest mirrors storetest.NewFullPolicy but lives in-package so
// the domain test avoids importing storetest (which imports domain).
func NewFullPolicyForTest(code PolicyCode) *Policy {
	return &Policy{
		Code:            code,
		Name:            "Full",
		RequireToken:    true,
		TokenSecret:     "sk",
		AllowIPs:        []string{"203.0.113.10", "10.0.0.0/8"},
		DenyIPs:         []string{"198.51.100.7"},
		AllowCountries:  []string{"VN", "SG"},
		DenyCountries:   []string{"CN"},
		AllowUserAgents: []string{"exoplayer"},
		AllowedDomains:  []string{"example.com"},
	}
}
