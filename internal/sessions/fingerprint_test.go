package sessions

import (
	"testing"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

const (
	tIP     = "10.0.0.1"
	tUA     = "Chrome/123"
	tCode   = domain.StreamCode("foo")
	tCode2  = domain.StreamCode("bar")
	tProto  = domain.SessionProtoHLS
	tProto2 = domain.SessionProtoDASH
)

func TestFingerprintIDStable(t *testing.T) {
	a := fingerprintID(tCode, tProto, false, tIP, tUA, "")
	b := fingerprintID(tCode, tProto, false, tIP, tUA, "")
	if a != b {
		t.Fatalf("fingerprint not stable: %s vs %s", a, b)
	}
	if len(a) != fingerprintIDLen {
		t.Fatalf("unexpected fingerprint length: got %d want %d", len(a), fingerprintIDLen)
	}
}

func TestFingerprintIDDifferentiates(t *testing.T) {
	base := fingerprintID(tCode, tProto, false, tIP, tUA, "")

	for name, fp := range map[string]string{
		"diff stream": fingerprintID(tCode2, tProto, false, tIP, tUA, ""),
		"diff proto":  fingerprintID(tCode, tProto2, false, tIP, tUA, ""),
		"diff dvr":    fingerprintID(tCode, tProto, true, tIP, tUA, ""),
		"diff ip":     fingerprintID(tCode, tProto, false, "10.0.0.2", tUA, ""),
		"diff ua":     fingerprintID(tCode, tProto, false, tIP, "ua-other", ""),
		"diff token":  fingerprintID(tCode, tProto, false, tIP, tUA, "tok"),
	} {
		if fp == base {
			t.Errorf("%s: fingerprint did not change", name)
		}
	}
}

func TestFingerprintIDIPCaseInsensitive(t *testing.T) {
	// IPv6 mixed case should hash to the same bucket — operators expect
	// 'fe80::1' and 'FE80::1' to count as one viewer.
	a := fingerprintID(tCode, tProto, false, "fe80::1", tUA, "")
	b := fingerprintID(tCode, tProto, false, "FE80::1", tUA, "")
	if a != b {
		t.Fatalf("ipv6 case-sensitivity leaked: %s vs %s", a, b)
	}
}

func TestShortFingerprintLabel(t *testing.T) {
	if got := shortFingerprintLabel("0123456789abcdef"); got != "01234567" {
		t.Errorf("short label = %q, want %q", got, "01234567")
	}
	if got := shortFingerprintLabel("abc"); got != "abc" {
		t.Errorf("short label = %q, want %q", got, "abc")
	}
}
