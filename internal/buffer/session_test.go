package buffer

import (
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// TestSetSessionStampsNextWrite verifies that after SetSession the very next
// packet observed by a subscriber carries the new SessionID and
// SessionStart=true, and that subsequent packets carry the same SessionID
// with SessionStart=false.
func TestSetSessionStampsNextWrite(t *testing.T) {
	s := NewServiceForTesting(8)
	s.Create(testStream)
	sub, err := s.Subscribe(testStream)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer s.Unsubscribe(testStream, sub)

	sess := s.SetSession(testStream, domain.SessionStartFresh, nil, nil)
	if sess == nil {
		t.Fatal("SetSession returned nil")
	}
	if sess.ID == 0 {
		t.Fatalf("SetSession ID = 0, want >0")
	}
	if sess.Reason != domain.SessionStartFresh {
		t.Fatalf("reason = %v, want Fresh", sess.Reason)
	}

	if err := s.Write(testStream, TSPacket([]byte{0x47})); err != nil {
		t.Fatalf("write: %v", err)
	}

	first := recvOne(t, sub)
	if first.SessionID != sess.ID {
		t.Fatalf("first packet SessionID = %d, want %d", first.SessionID, sess.ID)
	}
	if !first.SessionStart {
		t.Fatal("first packet SessionStart = false, want true")
	}

	if err := s.Write(testStream, TSPacket([]byte{0x47})); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	second := recvOne(t, sub)
	if second.SessionID != sess.ID {
		t.Fatalf("second packet SessionID = %d, want %d", second.SessionID, sess.ID)
	}
	if second.SessionStart {
		t.Fatal("second packet SessionStart = true, want false")
	}
}

// TestSetSessionMonotonicIDs verifies that two SetSession calls on the same
// buffer produce strictly increasing session IDs.
func TestSetSessionMonotonicIDs(t *testing.T) {
	s := NewServiceForTesting(8)
	s.Create(testStream)

	a := s.SetSession(testStream, domain.SessionStartFresh, nil, nil)
	b := s.SetSession(testStream, domain.SessionStartReconnect, nil, nil)
	if a == nil || b == nil {
		t.Fatal("SetSession returned nil")
	}
	if b.ID <= a.ID {
		t.Fatalf("session B ID %d not greater than A ID %d", b.ID, a.ID)
	}
}

// TestSetSessionUnknownBufferIsNoop verifies that SetSession returns nil
// without panicking when the buffer has not been Created.
func TestSetSessionUnknownBufferIsNoop(t *testing.T) {
	s := NewServiceForTesting(8)
	if got := s.SetSession("not-created", domain.SessionStartFresh, nil, nil); got != nil {
		t.Fatalf("SetSession on unknown buffer returned %+v, want nil", got)
	}
}

// TestSessionReturnsLatest verifies that Session() returns the most recently
// declared session, including its reason and configs.
func TestSessionReturnsLatest(t *testing.T) {
	s := NewServiceForTesting(8)
	s.Create(testStream)

	if got := s.Session(testStream); got != nil {
		t.Fatalf("Session before SetSession = %+v, want nil", got)
	}

	video := &domain.SessionVideoConfig{Codec: domain.AVCodecH264, Width: 1920, Height: 1080}
	audio := &domain.SessionAudioConfig{Codec: domain.AVCodecAAC, SampleRate: 48000, Channels: 2}
	want := s.SetSession(testStream, domain.SessionStartFailover, video, audio)

	got := s.Session(testStream)
	if got != want {
		t.Fatalf("Session() = %p, want %p", got, want)
	}
	if got.Video == nil || got.Video.Width != 1920 {
		t.Fatalf("Video config not retained: %+v", got.Video)
	}
	if got.Audio == nil || got.Audio.SampleRate != 48000 {
		t.Fatalf("Audio config not retained: %+v", got.Audio)
	}
}

// TestWritePreservesExplicitSessionFields verifies that when the caller
// supplies a non-zero SessionID directly on the Packet (e.g. a mixer
// fan-out that owns its own session table), Write does NOT overwrite it
// with the buffer's auto-stamped session.
func TestWritePreservesExplicitSessionFields(t *testing.T) {
	s := NewServiceForTesting(8)
	s.Create(testStream)
	sub, err := s.Subscribe(testStream)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer s.Unsubscribe(testStream, sub)

	_ = s.SetSession(testStream, domain.SessionStartFresh, nil, nil)

	explicit := Packet{
		TS:           []byte{0x47},
		SessionID:    99999,
		SessionStart: true,
	}
	if err := s.Write(testStream, explicit); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := recvOne(t, sub)
	if got.SessionID != 99999 {
		t.Fatalf("explicit SessionID overwritten: got %d, want 99999", got.SessionID)
	}
	if !got.SessionStart {
		t.Fatal("explicit SessionStart cleared")
	}
}

// TestSessionStartLatchClearedAfterFirstWrite verifies that the SessionStart
// flag is a one-shot: only the very first packet after SetSession carries
// it; later packets within the same session do not, until the next
// SetSession.
func TestSessionStartLatchClearedAfterFirstWrite(t *testing.T) {
	s := NewServiceForTesting(8)
	s.Create(testStream)
	sub, err := s.Subscribe(testStream)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer s.Unsubscribe(testStream, sub)

	_ = s.SetSession(testStream, domain.SessionStartFresh, nil, nil)

	for i := 0; i < 3; i++ {
		if err := s.Write(testStream, TSPacket([]byte{byte(i)})); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	pkts := []Packet{
		recvOne(t, sub),
		recvOne(t, sub),
		recvOne(t, sub),
	}
	if !pkts[0].SessionStart {
		t.Fatal("first packet missing SessionStart")
	}
	if pkts[1].SessionStart || pkts[2].SessionStart {
		t.Fatal("non-first packets carrying SessionStart")
	}

	// New SetSession → next write should carry SessionStart again.
	next := s.SetSession(testStream, domain.SessionStartReconnect, nil, nil)
	if err := s.Write(testStream, TSPacket([]byte{0xAA})); err != nil {
		t.Fatalf("write after re-session: %v", err)
	}
	got := recvOne(t, sub)
	if got.SessionID != next.ID {
		t.Fatalf("after re-session SessionID = %d, want %d", got.SessionID, next.ID)
	}
	if !got.SessionStart {
		t.Fatal("packet after re-session missing SessionStart")
	}
}

// TestClonePacketPropagatesSessionFields guards against future regressions
// in clonePacket — every subscriber must receive a clone with the session
// metadata preserved.
func TestClonePacketPropagatesSessionFields(t *testing.T) {
	in := Packet{
		TS:           []byte{1, 2, 3},
		SessionID:    42,
		SessionStart: true,
	}
	out := clonePacket(in)
	if out.SessionID != 42 || !out.SessionStart {
		t.Fatalf("clonePacket dropped session fields: %+v", out)
	}
}

// TestSessionReasonString covers the stable string form used in log fields.
func TestSessionReasonString(t *testing.T) {
	cases := []struct {
		in   domain.SessionStartReason
		want string
	}{
		{domain.SessionStartUnknown, "unknown"},
		{domain.SessionStartFresh, "fresh"},
		{domain.SessionStartReconnect, "reconnect"},
		{domain.SessionStartFailover, "failover"},
		{domain.SessionStartMixerCycle, "mixer_cycle"},
		{domain.SessionStartConfigChange, "config_change"},
	}
	for _, tc := range cases {
		if got := tc.in.String(); got != tc.want {
			t.Errorf("%d.String() = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// recvOne reads a single packet from sub, failing the test on timeout.
func recvOne(t *testing.T, sub *Subscriber) Packet {
	t.Helper()
	select {
	case pkt := <-sub.Recv():
		return pkt
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for packet")
	}
	return Packet{}
}
