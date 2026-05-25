package native

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// TestLooksLikeTS_DetectsSyncByte locks in the byte-pattern probe the
// pipeline relies on to route raw-TS chunks through the demuxer vs.
// passing Annex-B straight to the decoder. Misclassification either
// way blocks transcoding for the affected source type, so the cases
// here cover every shape we observe in production: full TS chunks,
// single-packet chunks below the 188-byte probe, Annex-B start codes,
// and empty data.
func TestLooksLikeTS_DetectsSyncByte(t *testing.T) {
	t.Parallel()

	type want bool
	tcs := []struct {
		name string
		data []byte
		want want
	}{
		{"empty rejects", nil, false},
		{"annex-b 3-byte start code rejects", []byte{0x00, 0x00, 0x01, 0x67}, false},
		{"annex-b 4-byte start code rejects", []byte{0x00, 0x00, 0x00, 0x01, 0x67}, false},
		{"single TS packet accepts", buildShortTSChunk(1), true},
		{"two TS packets accepts", buildShortTSChunk(2), true},
		{"sync at 0 but garbage at 188 rejects", append(buildShortTSChunk(1), 0xAB, 0xCD), false},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := looksLikeTS(tc.data)
			if got != bool(tc.want) {
				t.Fatalf("looksLikeTS(%x) = %v, want %v", tc.data, got, tc.want)
			}
		})
	}
}

// TestTSInput_FeedReturnsErrorAfterClose ensures the supervisor's
// gRPC pump gets a hard error and can return cleanly instead of
// silently blocking on a closed pipeline.
func TestTSInput_FeedReturnsErrorAfterClose(t *testing.T) {
	t.Parallel()

	in := newTSInput(context.Background())
	in.Close()

	err := in.Feed([]byte{0x47, 0x00, 0x00})
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Feed after Close = %v, want io.ErrClosedPipe", err)
	}
}

// TestTSInput_CloseIsIdempotent — Close on the pipeline path runs in
// Flush AND in the shutdown defer; both must succeed without panic.
func TestTSInput_CloseIsIdempotent(t *testing.T) {
	t.Parallel()

	in := newTSInput(context.Background())
	in.Close()
	in.Close()
}

// TestTSInput_FeedDoesNotBlockUnderChurn feeds far more chunks than
// the chunks buffer holds, of data that makes astits restart
// repeatedly (sync bytes but no valid PAT/PMT — the mid-switch
// shape). The demuxer-restart loop must keep draining chunks so Feed
// never blocks permanently; a regression here reintroduces the hard
// stall under rapid input switching. A watchdog goroutine fails the
// test if any Feed call hangs.
func TestTSInput_FeedDoesNotBlockUnderChurn(t *testing.T) {
	t.Parallel()
	in := newTSInput(context.Background())
	defer in.Close()

	chunk := buildShortTSChunk(1) // sync byte, PID 0x1FFF, no PSI
	done := make(chan struct{})
	go func() {
		defer close(done)
		// 10x the buffer size — if the consumer dies, this blocks well
		// before finishing.
		for i := 0; i < tsChunksBufferSize*10; i++ {
			if err := in.Feed(append([]byte(nil), chunk...)); err != nil {
				return // ErrClosedPipe is acceptable (consumer gone)
			}
			in.DrainReady()
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Feed blocked under demuxer churn — rapid-switch stall regression")
	}
}

// TestTSInput_DrainReadyEmptyReturnsNil — the pipeline's lazy-drain
// model calls DrainReady eagerly; an empty queue must yield nil, not
// block.
func TestTSInput_DrainReadyEmptyReturnsNil(t *testing.T) {
	t.Parallel()

	in := newTSInput(context.Background())
	defer in.Close()

	out := in.DrainReady()
	if len(out) != 0 {
		t.Fatalf("DrainReady on empty queue = %d frames, want 0", len(out))
	}
}

// buildShortTSChunk fabricates n consecutive 188-byte TS packets with
// the bare minimum to pass the sync-byte probe — PID 0x1FFF (null
// padding), no PSI / PES, no adaptation field. Sufficient for
// looksLikeTS testing; NOT a valid stream for demuxing.
func buildShortTSChunk(n int) []byte {
	out := make([]byte, 0, 188*n)
	for i := 0; i < n; i++ {
		pkt := make([]byte, 188)
		pkt[0] = 0x47
		pkt[1] = 0x1F
		pkt[2] = 0xFF
		pkt[3] = 0x10
		out = append(out, pkt...)
	}
	return out
}
