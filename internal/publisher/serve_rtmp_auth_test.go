package publisher

// serve_rtmp_auth_test.go — media-plane authorization for the RTMP play path.
// The cases mirror exactly what HandleRTMPPlay does: take the play-URL raw
// query, extract the token via sessions.TokenFromQuery, and run playAllowed.

import (
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/mediaauth"
	"github.com/datvietvac-techhub/open-streamer/internal/sessions"
)

func TestRTMPPlay_TokenPolicy(t *testing.T) {
	const (
		code   = domain.StreamCode("mychannel")
		polCfg = domain.PolicyCode("tok")
		secret = "s3cr3t"
	)

	authz := mediaauth.New(nil, func(c domain.StreamCode) domain.PolicyCode {
		if c == code {
			return polCfg
		}
		return ""
	})
	authz.SetPolicies([]*domain.Policy{{
		Code:         polCfg,
		RequireToken: true,
		TokenSecret:  secret,
	}})

	svc := &Service{}
	svc.SetMediaAuthorizer(authz)

	now := time.Now()
	valid := mediaauth.SignToken([]byte(secret), code, now.Add(time.Hour).Unix())
	expired := mediaauth.SignToken([]byte(secret), code, now.Add(-time.Hour).Unix())
	otherStream := mediaauth.SignToken([]byte(secret), domain.StreamCode("other"), now.Add(time.Hour).Unix())
	wrongSecret := mediaauth.SignToken([]byte("nope"), code, now.Add(time.Hour).Unix())

	// play mirrors HandleRTMPPlay's auth step: rawQuery -> token -> playAllowed.
	play := func(rawQuery string) bool {
		tok := sessions.TokenFromQuery(rawQuery)
		return svc.playAllowed(code, "rtmp", "203.0.113.5:5555", tok, "")
	}

	cases := []struct {
		name     string
		rawQuery string
		want     bool
	}{
		{"valid token", "token=" + valid, true},
		{"valid token with extra params", "token=" + valid + "&foo=bar", true},
		{"missing query", "", false},
		{"empty token param", "token=", false},
		{"garbage token", "token=not-a-token", false},
		{"expired token", "token=" + expired, false},
		{"token bound to another stream", "token=" + otherStream, false},
		{"token signed with wrong secret", "token=" + wrongSecret, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := play(tc.rawQuery); got != tc.want {
				t.Fatalf("play(%q) = %v, want %v", tc.rawQuery, got, tc.want)
			}
		})
	}
}

func TestRTMPPlay_NoPolicyIsPublic(t *testing.T) {
	const code = domain.StreamCode("public")
	authz := mediaauth.New(nil, func(domain.StreamCode) domain.PolicyCode { return "" })
	svc := &Service{}
	svc.SetMediaAuthorizer(authz)

	if !svc.playAllowed(code, "rtmp", "203.0.113.9:1", "", "") {
		t.Fatal("stream with no policy must be public over RTMP")
	}
}

func TestRTMPPlay_NilAuthorizerAllows(t *testing.T) {
	svc := &Service{} // no authorizer wired → media auth disabled
	if !svc.playAllowed("any", "rtmp", "203.0.113.9:1", "", "") {
		t.Fatal("nil authorizer must allow playback")
	}
}
