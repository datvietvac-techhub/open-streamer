package publisher

import "testing"

// TestTSBuffer_DefaultCapHoldsBurstyHLSChunk — the default cap must
// accommodate a single bursty HLS-pull chunk without overflow. Customer
// streams in the 5–10 Mbps × 4–8 s range deliver chunks of up to ~10 MB
// at once; a too-small cap forces a backlog drop on every chunk and
// truncates the in-flight NALU, surfacing as CHUNK_DEMUXER_ERROR_APPEND_FAILED
// on browser players. Regression test for the 2 MiB-cap bug observed
// in production on copy-mode HLS-pull streams.
func TestTSBuffer_DefaultCapHoldsBurstyHLSChunk(t *testing.T) {
	tb := newTSBuffer("regression")

	// Single 5 MiB chunk — represents a 10 Mbps × 4 s HLS segment
	// arriving at once. Must fit in the default cap without dropping.
	chunk := make([]byte, 5<<20)
	if _, err := tb.Write(chunk); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if tb.dropCount != 0 {
		t.Errorf("dropCount = %d, want 0 (default cap must hold a 5 MiB chunk)", tb.dropCount)
	}
}

// TestTSBuffer_OverflowStillFiresPastCap — overflow protection still
// works when the backlog + incoming actually exceed the cap. Uses a
// tiny test-only cap so the test stays fast.
func TestTSBuffer_OverflowStillFiresPastCap(t *testing.T) {
	const cap = 1024
	tb := newTSBufferWithCap("test", cap)

	// First write fits.
	if _, err := tb.Write(make([]byte, 800)); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if tb.dropCount != 0 {
		t.Errorf("after first write: dropCount = %d, want 0", tb.dropCount)
	}

	// Second write would overflow → existing backlog dropped.
	if _, err := tb.Write(make([]byte, 800)); err != nil {
		t.Fatalf("second Write: %v", err)
	}
	if tb.dropCount != 1 {
		t.Errorf("after overflow write: dropCount = %d, want 1", tb.dropCount)
	}
}
