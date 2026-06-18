package ingestor

import (
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

func TestSustainedProbeOK(t *testing.T) {
	cases := []struct {
		name           string
		bytes, packets int
		span           time.Duration
		want           bool
	}{
		{"silent source", 0, 0, 0, false},
		{"single blip", 3000, 2, 0, false},
		{"two sparse blips far apart", 4000, 2, 2 * time.Second, false}, // span ok, but too few packets/bytes
		{"burst then quiet (no span)", 2_000_000, 500, 0, false},        // lots of data, all at one instant
		{"sustained healthy stream", 2_000_000, 500, 1500 * time.Millisecond, true},
		{"boundary exact", minProbeBytes, minProbePackets, minProbeSpan, true},
		{"bytes one below", minProbeBytes - 1, minProbePackets, minProbeSpan, false},
		{"packets one below", minProbeBytes, minProbePackets - 1, minProbeSpan, false},
		{"span one below", minProbeBytes, minProbePackets, minProbeSpan - time.Millisecond, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sustainedProbeOK(tc.bytes, tc.packets, tc.span); got != tc.want {
				t.Fatalf("sustainedProbeOK(%d,%d,%s) = %v, want %v", tc.bytes, tc.packets, tc.span, got, tc.want)
			}
		})
	}
}

func TestProbeStats_SputteringNotSustained(t *testing.T) {
	// A sputtering feed: one small burst, then long silence, then one more —
	// never enough packets/bytes within a continuous span. Must stay un-sustained.
	var s probeStats
	t0 := time.Unix(1_700_000_000, 0)
	s.add([]domain.AVPacket{{Data: make([]byte, 1316)}, {Data: make([]byte, 1316)}}, t0)
	if s.sustained() {
		t.Fatal("a 2-packet blip must not count as sustained")
	}
	s.add([]domain.AVPacket{{Data: make([]byte, 1316)}}, t0.Add(2*time.Second))
	if s.sustained() {
		t.Fatal("two sparse blips 2s apart must not count as sustained")
	}
}

func TestProbeStats_SustainedStream(t *testing.T) {
	// Continuous payload across > minProbeSpan with plenty of packets/bytes.
	var s probeStats
	t0 := time.Unix(1_700_000_000, 0)
	for i := 0; i < 20; i++ {
		batch := make([]domain.AVPacket, 8)
		for j := range batch {
			batch[j] = domain.AVPacket{Data: make([]byte, 1316)}
		}
		s.add(batch, t0.Add(time.Duration(i)*100*time.Millisecond))
	}
	if !s.sustained() {
		t.Fatalf("continuous stream should be sustained (bytes=%d packets=%d span=%s)",
			s.bytes, s.packets, s.lastAt.Sub(s.firstAt))
	}
}
