package sessions

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// fingerprintIDLen is how many hex chars of the SHA-256 we keep for the
// session ID. 16 hex = 64 bits — enough collision resistance for in-memory
// session sets sized in the low millions.
const fingerprintIDLen = 16

// fingerprintID derives a deterministic session ID for HTTP-based protocols
// (HLS / DASH) from the (stream + proto + dvr + ip + ua + token) tuple. Stable
// across requests so consecutive segment GETs from the same viewer collapse
// onto one session record.
//
// Protocol and DVR participate in the key so a viewer playing both HLS and
// DASH, or watching both live and timeshift, for the same stream from the
// same client gets distinct session records — otherwise the second variant's
// hits silently merge into the first session and its Protocol / DVR label
// is wrong.
//
// We exclude port and any cache-buster query params on purpose — the goal
// is "same human across reconnects within idle window". Token participates
// to disambiguate viewers behind shared NAT (same IP+UA but different
// signed URLs).
func fingerprintID(streamCode domain.StreamCode, proto domain.SessionProto, dvr bool, ip, ua, token string) string {
	h := sha256.New()
	h.Write([]byte(streamCode))
	h.Write([]byte{0})
	h.Write([]byte(proto))
	h.Write([]byte{0})
	if dvr {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	h.Write([]byte{0})
	h.Write([]byte(strings.ToLower(strings.TrimSpace(ip))))
	h.Write([]byte{0})
	h.Write([]byte(strings.TrimSpace(ua)))
	h.Write([]byte{0})
	h.Write([]byte(strings.TrimSpace(token)))
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:fingerprintIDLen/2])
}

// shortFingerprintLabel returns the first 8 hex chars of the fingerprint —
// short enough to display in logs and the UserName fallback while still
// being useful for grep/search across the dashboard.
func shortFingerprintLabel(fp string) string {
	if len(fp) > 8 {
		return fp[:8]
	}
	return fp
}
