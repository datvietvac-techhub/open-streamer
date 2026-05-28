package publisher

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// stripPESHeader is the PES-header skip used inside the scanner — verify the
// happy path so callers can rely on it. PES_packet_length = 0 (unbounded)
// matches what live encoders emit; we don't validate it.
func TestStripPESHeader_ReturnsESBytes(t *testing.T) {
	t.Parallel()
	// PES with PES_header_data_length = 5 (PTS 5 bytes), then ES bytes.
	pes := []byte{
		0x00, 0x00, 0x01, // start_code_prefix
		0xE0,       // stream_id (video)
		0x00, 0x00, // PES_packet_length = 0 (unbounded)
		0x80, 0x80, // marker / PTS_DTS_flags = '10' (PTS only)
		0x05,                         // PES_header_data_length
		0x21, 0x00, 0x01, 0x00, 0x01, // 5-byte PTS payload
		0xAA, 0xBB, 0xCC, // ES bytes
	}
	es := stripPESHeader(pes)
	require.Equal(t, []byte{0xAA, 0xBB, 0xCC}, es)
}

// scanForH264IDR is what the segmenter ultimately uses to decide a flush
// boundary — verify against an Annex-B stream with a known IDR NAL.
func TestScanForH264IDR_DetectsIDR(t *testing.T) {
	t.Parallel()
	// 4-byte start code + NAL type byte 0x65 (= forbidden_zero_bit=0,
	// nal_ref_idc=3, nal_unit_type=5 → IDR slice) + payload.
	es := []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0xDE, 0xAD, 0xBE, 0xEF}
	require.True(t, scanForH264IDR(es))
}

// Negative case: a non-IDR slice (nal_unit_type=1) must not trip the IDR check.
func TestScanForH264IDR_RejectsNonIDR(t *testing.T) {
	t.Parallel()
	es := []byte{0x00, 0x00, 0x00, 0x01, 0x41, 0xAA} // type=1 (P/B slice)
	require.False(t, scanForH264IDR(es))
}

// 3-byte start code variant must also be recognised — some encoders use it
// after the first NAL of an access unit.
func TestScanForH264IDR_ThreeByteStartCode(t *testing.T) {
	t.Parallel()
	es := []byte{0x00, 0x00, 0x01, 0x65, 0xCC}
	require.True(t, scanForH264IDR(es))
}

// HEVC IRAP types 16..23 are all valid keyframes for player resync; sample
// type 19 (IDR_W_RADL) which is the most common IRAP from x265.
func TestScanForH265IRAP_DetectsIDR(t *testing.T) {
	t.Parallel()
	// Type 19 → high 6 bits of byte = 19<<1 = 0x26.
	es := []byte{0x00, 0x00, 0x00, 0x01, 0x26, 0x01, 0xAA}
	require.True(t, scanForH265IRAP(es))
}

// Type 1 (TRAIL_N) is a non-IRAP slice — must not trip.
func TestScanForH265IRAP_RejectsNonIRAP(t *testing.T) {
	t.Parallel()
	es := []byte{0x00, 0x00, 0x00, 0x01, 0x02, 0x01, 0xAA} // type=1
	require.False(t, scanForH265IRAP(es))
}

// End-to-end: feed the scanner a synthetic TS stream containing PAT, PMT,
// then a video PES whose payload is an IDR. After feeding, LastIDROffset
// should point at the PAT preceding the IDR — that is what the segmenter
// splits at, so the next segment opens with PAT → PMT → IDR (the canonical
// clean-start sequence a player needs to begin decoding from byte 0).
func TestTSKeyframeScanner_RecordsIDROffsetAtPAT(t *testing.T) {
	t.Parallel()
	s := newTSKeyframeScanner()

	patPkt := buildPATPacket(0x1000)            // packet 0  (PAT)
	pmtPkt := buildPMTPacket(0x1000, 0x101, 27) // packet 1  (PMT)
	idrPesPkt := buildVideoPESWithIDR(0x101)    // packet 2  (IDR PES start)
	nextPkt := buildVideoPESWithIDR(0x101)      // packet 3  (finalises packet 2)

	s.Feed(patPkt)
	s.Feed(pmtPkt)
	s.Feed(idrPesPkt)
	s.Feed(nextPkt)

	require.Equal(t, domain.AVCodecH264, s.videoCodec)
	require.Equal(t, uint16(0x101), s.videoPID)
	// Boundary anchored at PAT (offset 0), not at IDR PES start (376) —
	// next segment will replay PAT → PMT → IDR cleanly.
	require.Equal(t, int64(0), s.LastIDROffset)
}

// When PAT is far in the past (> patIDRMaxGap from the IDR), the scanner
// falls back to using the IDR PES start as the split point instead of
// reaching back to a stale PAT. Better to start a segment slightly without
// PSI than to include a huge prefix of stale-PAT-then-stream bytes.
func TestTSKeyframeScanner_FallsBackToIDROffsetWhenPATTooFar(t *testing.T) {
	t.Parallel()
	s := newTSKeyframeScanner()

	s.Feed(buildPATPacket(0x1000))
	s.Feed(buildPMTPacket(0x1000, 0x101, 27))
	// Pad with 2000 video packets (~376 KB > patIDRMaxGap=256 KB) so the
	// PAT becomes "stale" before the IDR arrives.
	pad := buildVideoNonIDR(0x101)
	for range 2000 {
		s.Feed(pad)
	}
	s.Feed(buildVideoPESWithIDR(0x101))
	s.Feed(buildVideoPESWithIDR(0x101))

	// Expected: boundary == IDR PES start (NOT the stale PAT at offset 0).
	// IDR PES is at packet index 2002 (PAT + PMT + 2000 pad).
	require.Equal(t, int64(2002*188), s.LastIDROffset)
}

// buildVideoNonIDR mirrors buildVideoPESWithIDR but uses NAL type 1 (P/B
// slice) so the scanner does NOT treat the PES as a keyframe.
func buildVideoNonIDR(videoPID uint16) []byte {
	pkt := make([]byte, 188)
	pkt[0] = 0x47
	pkt[1] = 0x40 | byte(videoPID>>8)
	pkt[2] = byte(videoPID & 0xFF)
	pkt[3] = 0x10
	off := 4
	pesHdr := []byte{
		0x00, 0x00, 0x01, 0xE0, 0x00, 0x00,
		0x80, 0x80, 0x05, 0x21, 0x00, 0x01, 0x00, 0x01,
	}
	copy(pkt[off:], pesHdr)
	off += len(pesHdr)
	nonIDR := []byte{0x00, 0x00, 0x00, 0x01, 0x41, 0xAA} // type 1 = P/B slice
	copy(pkt[off:], nonIDR)
	for i := off + len(nonIDR); i < 188; i++ {
		pkt[i] = 0xFF
	}
	return pkt
}

// ─── Helpers — build minimal MPEG-TS packets for the test above ──────────

func buildPATPacket(pmtPID uint16) []byte {
	pkt := make([]byte, 188)
	pkt[0] = 0x47
	pkt[1] = 0x40 // PUSI=1, PID hi=0
	pkt[2] = 0x00 // PID lo=0 (PAT)
	pkt[3] = 0x10 // payload only, continuity 0
	pkt[4] = 0x00 // pointer field = 0
	// PAT section starts at byte 5.
	section := pkt[5:]
	section[0] = 0x00 // table_id
	section[1] = 0xB0 // section_syntax_indicator + reserved
	section[2] = 0x0D // section_length = 13
	section[3] = 0x00 // ts_id hi
	section[4] = 0x01 // ts_id lo
	section[5] = 0xC1 // version + current_next_indicator
	section[6] = 0x00 // section_number
	section[7] = 0x00 // last_section_number
	// Program loop entry: program_number = 1, PMT_PID
	section[8] = 0x00
	section[9] = 0x01
	section[10] = byte(0xE0 | byte(pmtPID>>8))
	section[11] = byte(pmtPID & 0xFF)
	// CRC32 stub (4 bytes, zero) lives in section[12..15] (= pkt[17..20]);
	// padding starts AFTER the section so we don't overwrite the program
	// loop entry's PID byte.
	for i := 21; i < 188; i++ {
		pkt[i] = 0xFF
	}
	return pkt
}

func buildPMTPacket(pmtPID, videoPID uint16, streamType byte) []byte {
	pkt := make([]byte, 188)
	pkt[0] = 0x47
	pkt[1] = 0x40 | byte(pmtPID>>8) // PUSI=1
	pkt[2] = byte(pmtPID & 0xFF)
	pkt[3] = 0x10
	pkt[4] = 0x00 // pointer field
	section := pkt[5:]
	section[0] = 0x02 // table_id (PMT)
	section[1] = 0xB0
	section[2] = 0x12 // section_length = 18
	section[3] = 0x00 // program_number hi
	section[4] = 0x01 // program_number lo
	section[5] = 0xC1
	section[6] = 0x00
	section[7] = 0x00
	section[8] = 0xE0 // PCR_PID hi (reserved bits + 0)
	section[9] = byte(videoPID & 0xFF)
	section[10] = 0xF0 // program_info_length = 0
	section[11] = 0x00
	// Stream info entry
	section[12] = streamType
	section[13] = byte(0xE0 | byte(videoPID>>8))
	section[14] = byte(videoPID & 0xFF)
	section[15] = 0xF0 // ES_info_length = 0
	section[16] = 0x00
	for i := 22; i < 188; i++ {
		pkt[i] = 0xFF
	}
	return pkt
}

//nolint:unparam // tests pass 0x101 today; keep arg for symmetry with buildPMTPacket / buildVideoNonIDR
func buildVideoPESWithIDR(videoPID uint16) []byte {
	pkt := make([]byte, 188)
	pkt[0] = 0x47
	pkt[1] = 0x40 | byte(videoPID>>8) // PUSI=1
	pkt[2] = byte(videoPID & 0xFF)
	pkt[3] = 0x10
	off := 4
	// PES header (9 bytes header + 5-byte PTS payload).
	pesHdr := []byte{
		0x00, 0x00, 0x01,
		0xE0,
		0x00, 0x00,
		0x80, 0x80, 0x05,
		0x21, 0x00, 0x01, 0x00, 0x01,
	}
	copy(pkt[off:], pesHdr)
	off += len(pesHdr)
	// Annex-B start code + IDR NAL header (type 5).
	idr := []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0xAA, 0xBB}
	copy(pkt[off:], idr)
	for i := off + len(idr); i < 188; i++ {
		pkt[i] = 0xFF
	}
	return pkt
}
