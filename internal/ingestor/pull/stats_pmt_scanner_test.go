package pull

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// codecFromDescriptors recognises DVB descriptors that carry a codec
// identifier under stream_type 0x06 (PES private data). Verified against
// ETSI EN 300 468 (AC-3 = 0x6A, E-AC-3 = 0x7A) and DVB TS 101 154 (AV1
// registration descriptor with format ID "AV01").
func TestCodecFromDescriptors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		descs []byte
		want  domain.AVCodec
	}{
		{
			name:  "DVB AC-3 descriptor (tag 0x6A)",
			descs: []byte{0x6A, 0x01, 0x80}, // tag + len=1 + body
			want:  domain.AVCodecAC3,
		},
		{
			name:  "DVB Enhanced AC-3 descriptor (tag 0x7A)",
			descs: []byte{0x7A, 0x02, 0x80, 0x00},
			want:  domain.AVCodecEAC3,
		},
		{
			name:  "AV1 registration descriptor (format AV01)",
			descs: []byte{0x05, 0x04, 'A', 'V', '0', '1'},
			want:  domain.AVCodecAV1,
		},
		{
			name:  "registration descriptor with unknown format → unknown",
			descs: []byte{0x05, 0x04, 'X', 'X', 'X', 'X'},
			want:  domain.AVCodecUnknown,
		},
		{
			name:  "no descriptors → unknown",
			descs: nil,
			want:  domain.AVCodecUnknown,
		},
		{
			name:  "AC-3 wins when listed first; later descriptor ignored",
			descs: []byte{0x6A, 0x01, 0x80, 0x05, 0x04, 'A', 'V', '0', '1'},
			want:  domain.AVCodecAC3,
		},
		{
			name:  "truncated descriptor body returns unknown safely",
			descs: []byte{0x6A, 0x05, 0x80}, // claims len=5 but only 1 byte
			want:  domain.AVCodecUnknown,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, codecFromDescriptors(tc.descs))
		})
	}
}

// peekMPEGAudioLayer scans the first MPEG-audio sync pattern (0xFF F?) in
// a PES payload to distinguish Layer III (MP3) from Layer I/II (MP2).
// Verified against the MPEG-1 Audio frame header spec (ISO/IEC 11172-3).
func TestPeekMPEGAudioLayer(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		payload []byte
		want    domain.AVCodec
	}{
		{
			name:    "Layer III at offset 0 (sync 0xFFFB)",
			payload: []byte{0xFF, 0xFB, 0x90, 0x00},
			want:    domain.AVCodecMP3,
		},
		{
			name:    "Layer II at offset 0 (sync 0xFFFD)",
			payload: []byte{0xFF, 0xFD, 0x40, 0x04},
			want:    domain.AVCodecMP2,
		},
		{
			name:    "Layer III after PES header bytes",
			payload: append([]byte{0x00, 0x00, 0x01, 0xC0, 0x00, 0x00, 0x80, 0x80, 0x05, 0x21, 0, 1, 0, 1}, 0xFF, 0xFB, 0x90, 0x00),
			want:    domain.AVCodecMP3,
		},
		{
			name:    "no sync pattern → unknown",
			payload: []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05},
			want:    domain.AVCodecUnknown,
		},
		{
			name:    "false sync byte (0xFF) followed by non-sync second byte",
			payload: []byte{0xFF, 0x00, 0xFF, 0x01},
			want:    domain.AVCodecUnknown,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, peekMPEGAudioLayer(tc.payload))
		})
	}
}

// End-to-end: feed PAT + PMT advertising H.264 video + AC-3 audio (DVB
// stream_type 0x06 + descriptor 0x6A) and verify the scanner emits
// stats AVPackets for both PIDs with the correct codec labels.
func TestStatsPMTScanner_RecognisesACThreeViaDVBDescriptor(t *testing.T) {
	t.Parallel()
	const (
		pmtPID   = 0x100
		videoPID = 0x101
		audioPID = 0x102
	)

	pat := buildPATForPMTPID(pmtPID)
	pmt := buildPMTWithACThree(pmtPID, videoPID, audioPID)
	video := buildESPacket(videoPID, 0xAA)
	audio := buildESPacket(audioPID, 0xBB)

	chunk := concat(pat, pmt, video, audio)

	var seen []*domain.AVPacket
	scanner := newStatsPMTScanner(func(p *domain.AVPacket) {
		seen = append(seen, p)
	})
	require.NotNil(t, scanner)
	scanner.scan(chunk)

	require.Len(t, seen, 2, "expected one stats packet per ES PID")
	assert.Equal(t, domain.AVCodecH264, seen[0].Codec)
	assert.Equal(t, domain.AVCodecAC3, seen[1].Codec)

	scanner.Close()
}

// Same shape as the AC-3 test but using ATSC stream_type 0x81 directly
// instead of stream_type 0x06 + DVB descriptor. Both encoding schemes
// must resolve to AVCodecAC3.
func TestStatsPMTScanner_RecognisesACThreeViaATSCStreamType(t *testing.T) {
	t.Parallel()
	const (
		pmtPID   = 0x100
		audioPID = 0x102
	)
	pat := buildPATForPMTPID(pmtPID)
	pmt := buildSimplePMT(pmtPID, audioPID, 0x81) // stream_type 0x81 = ATSC AC-3
	audio := buildESPacket(audioPID, 0x99)

	chunk := concat(pat, pmt, audio)

	var seen []*domain.AVPacket
	scanner := newStatsPMTScanner(func(p *domain.AVPacket) {
		seen = append(seen, p)
	})
	require.NotNil(t, scanner)
	scanner.scan(chunk)
	scanner.Close()

	require.Len(t, seen, 1)
	assert.Equal(t, domain.AVCodecAC3, seen[0].Codec)
}

// MPEG-2 Video (stream_type 0x02) is the SD-DVB workhorse — must surface
// as AVCodecMPEG2Video, not generic Unknown.
func TestStatsPMTScanner_RecognisesMPEG2Video(t *testing.T) {
	t.Parallel()
	const (
		pmtPID   = 0x100
		videoPID = 0x101
	)
	pat := buildPATForPMTPID(pmtPID)
	pmt := buildSimplePMT(pmtPID, videoPID, 0x02)
	video := buildESPacket(videoPID, 0x77)

	chunk := concat(pat, pmt, video)

	var seen []*domain.AVPacket
	scanner := newStatsPMTScanner(func(p *domain.AVPacket) {
		seen = append(seen, p)
	})
	require.NotNil(t, scanner)
	scanner.scan(chunk)
	scanner.Close()

	require.Len(t, seen, 1)
	assert.Equal(t, domain.AVCodecMPEG2Video, seen[0].Codec)
}

// AV1 in TS uses stream_type 0x06 + registration descriptor with format
// identifier "AV01" (DVB TS 101 154 v2.7.1). Must be recognised via the
// descriptor path.
func TestStatsPMTScanner_RecognisesAV1ViaRegistration(t *testing.T) {
	t.Parallel()
	const (
		pmtPID   = 0x100
		videoPID = 0x101
	)
	pat := buildPATForPMTPID(pmtPID)
	pmt := buildPMTWithDescriptor(pmtPID, videoPID, 0x06,
		[]byte{0x05, 0x04, 'A', 'V', '0', '1'})
	video := buildESPacket(videoPID, 0x55)

	chunk := concat(pat, pmt, video)

	var seen []*domain.AVPacket
	scanner := newStatsPMTScanner(func(p *domain.AVPacket) {
		seen = append(seen, p)
	})
	require.NotNil(t, scanner)
	scanner.scan(chunk)
	scanner.Close()

	require.Len(t, seen, 1)
	assert.Equal(t, domain.AVCodecAV1, seen[0].Codec)
}

// nil callback → constructor returns nil, no goroutine spawned.
func TestNewStatsPMTScanner_NilCallback(t *testing.T) {
	t.Parallel()
	scanner := newStatsPMTScanner(nil)
	assert.Nil(t, scanner)
}

// ─── Test helpers — build minimal PAT/PMT TS packets ────────────────────────

// buildPATForPMTPID returns one 188-byte TS packet carrying a PAT with a
// single program → pmtPID mapping. Reuses the buildMultiProgramPAT helper
// from mpts_filter_test.go but provides a more focused name for stats tests.
//
//nolint:unparam // pmtPID kept as a parameter for clarity / symmetry with the real PAT structure even though every test currently passes 0x100.
func buildPATForPMTPID(pmtPID uint16) []byte {
	return buildMultiProgramPAT(0x1234, []patEntry{
		{programNumber: 1, pmtPID: pmtPID},
	})
}

// buildSimplePMT returns one TS packet carrying a PMT with exactly one ES
// of the given streamType on esPID. No descriptors. Used for tests that
// only care about stream_type → codec resolution.
func buildSimplePMT(pmtPID, esPID uint16, streamType byte) []byte {
	const sectionLength = 18 // see buildMinimalPMT for derivation
	pmt := make([]byte, 0, sectionLength+3)
	pmt = append(pmt, 0x02) // table_id PMT
	pmt = append(pmt,
		0xB0|byte((sectionLength>>8)&0x0F),
		byte(sectionLength&0xFF),
	)
	pn := make([]byte, 2)
	binary.BigEndian.PutUint16(pn, 1)
	pmt = append(pmt, pn...)
	pmt = append(pmt, 0xC1)       // version=0, current_next=1
	pmt = append(pmt, 0x00, 0x00) // section_number, last_section_number
	pmt = append(pmt,
		0xE0|byte((esPID>>8)&0x1F),
		byte(esPID&0xFF),
	) // PCR_PID = esPID
	pmt = append(pmt, 0xF0, 0x00) // program_info_length = 0
	pmt = append(pmt,
		streamType,
		0xE0|byte((esPID>>8)&0x1F),
		byte(esPID&0xFF),
		0xF0, 0x00, // ES_info_length = 0
	)
	crc := mpegCRC32(pmt)
	crcB := make([]byte, 4)
	binary.BigEndian.PutUint32(crcB, crc)
	pmt = append(pmt, crcB...)
	return wrapInTSPacket(pmt, pmtPID, 0xD)
}

// buildPMTWithACThree returns a PMT packet describing two ES streams:
// one H.264 video on videoPID (stream_type 0x1B) and one AC-3 audio on
// audioPID via DVB's stream_type 0x06 + AC-3 descriptor (tag 0x6A).
func buildPMTWithACThree(pmtPID, videoPID, audioPID uint16) []byte {
	// section_length = TSID(2)+ver(1)+sec#(1)+last#(1) + PCR_PID(2) +
	// program_info_length(2) + ES1(5) + AC3 descriptor(3) + ES2(5+3) + CRC(4)
	// We compute by building first then back-filling the length.
	pmt := make([]byte, 0, 64)
	pmt = append(pmt, 0x02)             // table_id PMT
	pmt = append(pmt, 0x00, 0x00)       // section_length placeholder
	pmt = append(pmt, 0x00, 0x01)       // program_number
	pmt = append(pmt, 0xC1, 0x00, 0x00) // version, section_number, last
	pmt = append(pmt,
		0xE0|byte((videoPID>>8)&0x1F),
		byte(videoPID&0xFF),
	)
	pmt = append(pmt, 0xF0, 0x00) // program_info_length = 0

	// ES 1: video (H.264, no descriptors)
	pmt = append(pmt,
		0x1B,
		0xE0|byte((videoPID>>8)&0x1F), byte(videoPID&0xFF),
		0xF0, 0x00,
	)
	// ES 2: AC-3 audio via stream_type 0x06 + 3-byte AC-3 descriptor
	ac3Desc := []byte{0x6A, 0x01, 0x80}
	esInfoLen := len(ac3Desc)
	pmt = append(pmt,
		0x06,
		0xE0|byte((audioPID>>8)&0x1F), byte(audioPID&0xFF),
		0xF0|byte((esInfoLen>>8)&0x0F), byte(esInfoLen&0xFF),
	)
	pmt = append(pmt, ac3Desc...)

	// Reserve CRC32 trailing bytes; section_length spans tsid through CRC.
	crcStart := len(pmt)
	pmt = append(pmt, 0, 0, 0, 0)
	sectionLen := len(pmt) - 3 // bytes after section_length field through CRC
	pmt[1] = 0xB0 | byte((sectionLen>>8)&0x0F)
	pmt[2] = byte(sectionLen & 0xFF)
	crc := mpegCRC32(pmt[:crcStart])
	binary.BigEndian.PutUint32(pmt[crcStart:], crc)
	return wrapInTSPacket(pmt, pmtPID, 0xD)
}

// buildPMTWithDescriptor returns a single-ES PMT carrying the given
// descriptor block under the ES_info_length field. Used for AV1 tests.
func buildPMTWithDescriptor(pmtPID, esPID uint16, streamType byte, desc []byte) []byte {
	pmt := make([]byte, 0, 32)
	pmt = append(pmt, 0x02)
	pmt = append(pmt, 0x00, 0x00)
	pmt = append(pmt, 0x00, 0x01)
	pmt = append(pmt, 0xC1, 0x00, 0x00)
	pmt = append(pmt,
		0xE0|byte((esPID>>8)&0x1F), byte(esPID&0xFF),
	)
	pmt = append(pmt, 0xF0, 0x00)

	esInfoLen := len(desc)
	pmt = append(pmt,
		streamType,
		0xE0|byte((esPID>>8)&0x1F), byte(esPID&0xFF),
		0xF0|byte((esInfoLen>>8)&0x0F), byte(esInfoLen&0xFF),
	)
	pmt = append(pmt, desc...)

	crcStart := len(pmt)
	pmt = append(pmt, 0, 0, 0, 0)
	sectionLen := len(pmt) - 3
	pmt[1] = 0xB0 | byte((sectionLen>>8)&0x0F)
	pmt[2] = byte(sectionLen & 0xFF)
	crc := mpegCRC32(pmt[:crcStart])
	binary.BigEndian.PutUint32(pmt[crcStart:], crc)
	return wrapInTSPacket(pmt, pmtPID, 0xD)
}
