package sessions

import (
	"context"
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

func newTestService(t *testing.T) *service {
	t.Helper()
	return newService(config.SessionsConfig{Enabled: true, IdleTimeoutSec: 5}, nil, NullGeoIP{})
}

func TestTrackHTTPOpensThenAccumulates(t *testing.T) {
	s := newTestService(t)
	hit := HTTPHit{
		StreamCode: tCode,
		Protocol:   domain.SessionProtoHLS,
		IP:         tIP,
		UserAgent:  tUA,
		BytesDelta: 1024,
	}

	first := s.TrackHTTP(context.Background(), hit)
	if first == nil {
		t.Fatal("TrackHTTP returned nil on first hit")
		return // unreachable; satisfies staticcheck SA5011
	}
	if first.Bytes != 1024 {
		t.Errorf("first.Bytes = %d, want 1024", first.Bytes)
	}
	if first.NamedBy != domain.SessionNamedByFingerprint {
		t.Errorf("expected fingerprint named_by, got %s", first.NamedBy)
	}

	hit.BytesDelta = 512
	second := s.TrackHTTP(context.Background(), hit)
	if second.ID != first.ID {
		t.Errorf("second hit got new ID; expected merge: %s vs %s", first.ID, second.ID)
	}
	if second.Bytes != 1536 {
		t.Errorf("Bytes = %d, want 1536", second.Bytes)
	}

	if got := s.openedTotal.Load(); got != 1 {
		t.Errorf("openedTotal = %d, want 1 (only one open)", got)
	}
}

// Same client (stream + ip + ua + token) playing both HLS and DASH must yield
// two independent sessions — protocol participates in the fingerprint key.
// Regression guard: previously the second protocol's hits silently merged
// into the first session and inherited the wrong Protocol label.
func TestTrackHTTPProtocolIsolation(t *testing.T) {
	s := newTestService(t)
	base := HTTPHit{StreamCode: tCode, IP: tIP, UserAgent: tUA, BytesDelta: 100}

	hls := base
	hls.Protocol = domain.SessionProtoHLS
	dash := base
	dash.Protocol = domain.SessionProtoDASH

	a := s.TrackHTTP(context.Background(), hls)
	b := s.TrackHTTP(context.Background(), dash)

	if a.ID == b.ID {
		t.Fatalf("HLS and DASH collapsed onto one session id %s", a.ID)
	}
	if a.Protocol != domain.SessionProtoHLS || b.Protocol != domain.SessionProtoDASH {
		t.Fatalf("protocols leaked: a=%s b=%s", a.Protocol, b.Protocol)
	}
	if got := s.openedTotal.Load(); got != 2 {
		t.Errorf("openedTotal = %d, want 2", got)
	}
}

func TestTrackHTTPTokenChangesNamedBy(t *testing.T) {
	s := newTestService(t)
	got := s.TrackHTTP(context.Background(), HTTPHit{
		StreamCode: tCode,
		Protocol:   domain.SessionProtoHLS,
		IP:         tIP,
		UserAgent:  tUA,
		Token:      "abc-123",
	})
	if got.NamedBy != domain.SessionNamedByToken {
		t.Errorf("named_by = %s, want token", got.NamedBy)
	}
	if got.UserName != "abc-123" {
		t.Errorf("user_name = %s, want token value", got.UserName)
	}
}

func TestTrackHTTPDisabledNoop(t *testing.T) {
	s := newService(config.SessionsConfig{Enabled: false}, nil, NullGeoIP{})
	if got := s.TrackHTTP(context.Background(), HTTPHit{StreamCode: tCode, IP: tIP}); got != nil {
		t.Errorf("expected nil when disabled, got %+v", got)
	}
	if got := s.Stats().Active; got != 0 {
		t.Errorf("Active=%d, want 0 when disabled", got)
	}
}

func TestOpenConnReturnsCloserAndCounts(t *testing.T) {
	s := newTestService(t)
	sess, closer := s.OpenConn(context.Background(), ConnHit{
		StreamCode: tCode,
		Protocol:   domain.SessionProtoSRT,
		RemoteAddr: "1.2.3.4:9000",
	})
	if sess == nil {
		t.Fatal("nil session from OpenConn")
		return // unreachable; satisfies staticcheck SA5011
	}
	if sess.IP != "1.2.3.4" {
		t.Errorf("IP = %s, want 1.2.3.4 (port stripped)", sess.IP)
	}

	closer.Close(domain.SessionCloseClient, 4096)
	// Idempotency.
	closer.Close(domain.SessionCloseClient, 1)

	if _, ok := s.Get(sess.ID); ok {
		t.Errorf("session %s still active after Close", sess.ID)
	}
	if got := s.closedTotal.Load(); got != 1 {
		t.Errorf("closedTotal = %d, want 1 (close idempotent)", got)
	}
}

func TestKickSetsKickedReason(t *testing.T) {
	s := newTestService(t)
	sess := s.TrackHTTP(context.Background(), HTTPHit{
		StreamCode: tCode,
		Protocol:   domain.SessionProtoHLS,
		IP:         tIP,
	})
	if !s.Kick(sess.ID) {
		t.Fatal("Kick returned false on active session")
	}
	if s.Kick(sess.ID) {
		t.Error("Kick of already-kicked session should return false")
	}
	if got := s.kickedTotal.Load(); got != 1 {
		t.Errorf("kickedTotal = %d, want 1", got)
	}
}

func TestListAppliesFilter(t *testing.T) {
	s := newTestService(t)
	for _, p := range []domain.SessionProto{
		domain.SessionProtoHLS, domain.SessionProtoDASH, domain.SessionProtoHLS,
	} {
		s.TrackHTTP(context.Background(), HTTPHit{
			StreamCode: tCode,
			Protocol:   p,
			IP:         "10.0.0." + string(rune('1'+len(s.sessions))),
		})
	}

	all := s.List(Filter{})
	if len(all) != 3 {
		t.Errorf("List() = %d sessions, want 3", len(all))
	}

	hls := s.List(Filter{Protocol: domain.SessionProtoHLS})
	if len(hls) != 2 {
		t.Errorf("HLS-filtered = %d, want 2", len(hls))
	}

	limited := s.List(Filter{Limit: 1})
	if len(limited) != 1 {
		t.Errorf("Limit=1 returned %d", len(limited))
	}
}

func TestReaperIdleClose(t *testing.T) {
	s := newService(config.SessionsConfig{Enabled: true, IdleTimeoutSec: 1}, nil, NullGeoIP{})
	now := time.Now()
	s.now = func() time.Time { return now }

	s.TrackHTTP(context.Background(), HTTPHit{StreamCode: tCode, IP: tIP})

	// Advance the clock past the idle window and reap.
	s.now = func() time.Time { return now.Add(2 * time.Second) }
	s.reapOnce(context.Background())

	if got := s.Stats().Active; got != 0 {
		t.Errorf("Active=%d after idle reap, want 0", got)
	}
	if got := s.idleClosedTotal.Load(); got != 1 {
		t.Errorf("idleClosedTotal=%d, want 1", got)
	}
}

func TestShutdownClosesEveryone(t *testing.T) {
	s := newTestService(t)
	for i := 0; i < 3; i++ {
		s.TrackHTTP(context.Background(), HTTPHit{
			StreamCode: tCode,
			IP:         tIP,
			UserAgent:  string(rune('a' + i)), // unique fingerprints
		})
	}
	if s.Stats().Active != 3 {
		t.Fatalf("setup: expected 3 active, got %d", s.Stats().Active)
	}
	s.shutdownActiveSessions(context.Background())
	if s.Stats().Active != 0 {
		t.Errorf("after shutdown: active=%d, want 0", s.Stats().Active)
	}
	if got := s.closedTotal.Load(); got != 3 {
		t.Errorf("closedTotal=%d, want 3", got)
	}
}
