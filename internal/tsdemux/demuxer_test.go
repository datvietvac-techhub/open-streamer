package tsdemux_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/tsdemux"
	"github.com/datvietvac-techhub/open-streamer/internal/tsmux"
)

// Public bitstream fixtures (same constants the blob/dash tests use): a real
// H.264 SPS/PPS pair + minimal IDR slice and one ADTS AAC frame, enough for
// tsmux to emit decodable-shaped PES.
var (
	tdSPS  = []byte{0x67, 0x4d, 0x40, 0x28, 0xeb, 0x05, 0x07, 0x80, 0x44, 0x00, 0x00, 0x03, 0x00, 0x04, 0x00, 0x00, 0x03, 0x00, 0xf0, 0x3c, 0x60, 0xc6, 0x58}
	tdPPS  = []byte{0x68, 0xee, 0x3c, 0x80}
	tdIDR  = []byte{0x65, 0x88, 0x80, 0x40, 0x00, 0x00}
	tdADTS = []byte{0xFF, 0xF1, 0x4C, 0x80, 0x01, 0x1F, 0xFC, 0xAA}
)

func tdAnnexB(nalus ...[]byte) []byte {
	var out []byte
	for _, n := range nalus {
		out = append(out, 0x00, 0x00, 0x00, 0x01)
		out = append(out, n...)
	}
	return out
}

// muxWire muxes (codec, wirePTSms) pairs into a TS byte stream. The PTS values
// are WIRE-domain milliseconds — i.e. what a 33-bit-wrapped stream carries —
// so tests can simulate a source crossing the wrap boundary.
func muxWire(t *testing.T, pkts []domain.AVPacket) []byte {
	t.Helper()
	mux := tsmux.NewFromAV(context.Background())
	var buf []byte
	for i := range pkts {
		mux.Write(&pkts[i], func(b []byte) { buf = append(buf, b...) })
	}
	require.NotEmpty(t, buf)
	return buf
}

func vPkt(ptsMs uint64) domain.AVPacket {
	return domain.AVPacket{Codec: domain.AVCodecH264, Data: tdAnnexB(tdSPS, tdPPS, tdIDR), PTSms: ptsMs, DTSms: ptsMs, KeyFrame: true}
}

func aPkt(ptsMs uint64) domain.AVPacket {
	return domain.AVPacket{Codec: domain.AVCodecAAC, Data: tdADTS, PTSms: ptsMs}
}

type gotFrame struct {
	cid tsdemux.StreamType
	pts uint64
	dts uint64
}

func demuxAll(t *testing.T, ts []byte) []gotFrame {
	t.Helper()
	var got []gotFrame
	d := tsdemux.New()
	d.OnFrame = func(cid tsdemux.StreamType, _ []byte, pts, dts uint64) {
		got = append(got, gotFrame{cid, pts, dts})
	}
	require.NoError(t, d.Input(context.Background(), bytes.NewReader(ts)))
	return got
}

// wire→unwrapped expectation: a wire ms value observed in epoch k unwraps to
// (ms·90 + k·2³³)/90 ms (integer division mirrors the demuxer's tick→ms step).
func unwrapped(wireMs uint64, epoch int64) uint64 {
	return uint64(int64(wireMs)*90+epoch*(int64(1)<<33)) / 90
}

// TestUnwrap_Identity — no wrap in range: timestamps pass through unchanged.
func TestUnwrap_Identity(t *testing.T) {
	t.Parallel()
	ts := muxWire(t, []domain.AVPacket{vPkt(1000), aPkt(1010), vPkt(1040), aPkt(1031), vPkt(1080)})
	for _, f := range demuxAll(t, ts) {
		assert.Less(t, f.pts, uint64(2000))
		assert.Equal(t, f.pts, f.dts, "OnlyPTS PES substitutes DTS=PTS")
	}
}

// TestUnwrap_CrossesWrapBoundary — video and audio cross the 33-bit wrap
// mid-stream; the demuxer must emit one continuous timeline (the post-wrap
// values continue ~26.5 h up, not collapse back to ~0), with a single shared
// epoch for both tracks.
func TestUnwrap_CrossesWrapBoundary(t *testing.T) {
	t.Parallel()
	// Wire ms just below the wrap (2³³ ticks = 95443717 ms), then wrapped.
	// A trailing pair flushes astits's per-PID PES assembly so every
	// asserted PES is emitted; cross-track interleave order is the muxer's
	// business, so assertions are per track.
	const preA, preB = 95_443_000, 95_443_400
	const postA, postB = 111, 511 // ≡ pre-wrap + ~700/1100 ms on the real timeline
	ts := muxWire(t, []domain.AVPacket{
		vPkt(preA - 500), aPkt(preA - 490), // primer (first PES per PID may be swallowed by assembly)
		vPkt(preA), aPkt(preA + 10), vPkt(preB), aPkt(preB + 10),
		vPkt(postA), aPkt(postA + 10), vPkt(postB), aPkt(postB + 10),
		vPkt(postB + 400), aPkt(postB + 410), // flusher tail (not asserted)
	})
	var vp, ap []uint64
	for _, f := range demuxAll(t, ts) {
		if f.cid == tsdemux.StreamTypeH264 {
			vp = append(vp, f.pts)
		} else {
			ap = append(ap, f.pts)
		}
	}
	assert.Equal(t, []uint64{preA, preB, unwrapped(postA, 1), unwrapped(postB, 1)}, firstWindow(vp, preA))
	assert.Equal(t, []uint64{preA + 10, preB + 10, unwrapped(postA+10, 1), unwrapped(postB+10, 1)}, firstWindow(ap, preA+10))
	// Continuity: the first post-wrap PTS lands under a second after the
	// last pre-wrap one — not 26.5 h back.
	assert.Less(t, unwrapped(postA, 1)-preB, uint64(1000))
}

// firstWindow returns the 4-element window of s starting at the first
// occurrence of v, or nil when absent — assertion helper that tolerates
// the muxer/demuxer swallowing unasserted primer/flusher PES.
func firstWindow(s []uint64, v uint64) []uint64 {
	for i, x := range s {
		if x == v && i+4 <= len(s) {
			return s[i : i+4]
		}
	}
	return nil
}

// TestUnwrap_StragglerAcrossBoundary — a PES stamped just before the wrap
// arriving AFTER the epoch advanced (B-frame-style reorder across the
// boundary) is pulled back into the previous epoch instead of being pushed a
// full 26.5 h ahead; the epoch stays advanced for subsequent frames.
func TestUnwrap_StragglerAcrossBoundary(t *testing.T) {
	t.Parallel()
	const pre, post, straggler, post2 = 95_443_700, 40, 95_443_710, 440
	ts := muxWire(t, []domain.AVPacket{vPkt(pre), vPkt(post), vPkt(straggler), vPkt(post2)})
	got := demuxAll(t, ts)
	require.Len(t, got, 4)

	assert.Equal(t, uint64(pre), got[0].pts)
	assert.Equal(t, unwrapped(post, 1), got[1].pts)
	assert.Equal(t, uint64(straggler), got[2].pts, "straggler stays in the old epoch")
	assert.Equal(t, unwrapped(post2, 1), got[3].pts, "epoch survives the straggler")
}

// TestUnwrap_MultipleWraps — two wrap cycles accumulate. The wire timeline
// walks forward in sub-half-span steps (as any real stream does) through
// epoch 1 before wrapping again.
func TestUnwrap_MultipleWraps(t *testing.T) {
	t.Parallel()
	const nearEnd = 95_443_600
	ts := muxWire(t, []domain.AVPacket{
		vPkt(nearEnd),                                 // epoch 0, just below the wrap
		vPkt(100), vPkt(40_000_000), vPkt(80_000_000), // wrap 1 → walk epoch 1 forward
		vPkt(nearEnd), // epoch 1, just below the next wrap
		vPkt(200),     // wrap 2
	})
	got := demuxAll(t, ts)
	require.Len(t, got, 6)
	assert.Equal(t, uint64(nearEnd), got[0].pts)
	assert.Equal(t, unwrapped(100, 1), got[1].pts)
	assert.Equal(t, unwrapped(40_000_000, 1), got[2].pts)
	assert.Equal(t, unwrapped(80_000_000, 1), got[3].pts)
	assert.Equal(t, unwrapped(nearEnd, 1), got[4].pts)
	assert.Equal(t, unwrapped(200, 2), got[5].pts)
}
