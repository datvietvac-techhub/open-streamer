package publisher

// ts_keyframe_scanner.go — lightweight MPEG-TS scanner that finds the byte
// offset where the next H.264/H.265 IDR PES begins. Used by the HLS segmenter's
// raw-TS path so segments split at IDR boundaries instead of mid-PES.
//
// Why we don't use gompeg2's TSDemuxer here: the demuxer's OnFrame fires
// AFTER a complete PES has been assembled — by then several TS packets have
// passed and we don't know which one contained the PES start. To split a
// segment cleanly we need the byte offset of the TS packet whose PUSI=1 began
// the IDR PES. So we parse the surface structure of TS packets ourselves
// (PAT → PMT → video PID, then per-packet PUSI tracking) without decoding
// frames, and only peek into the PES payload to look for the AVC/HEVC NAL
// type byte that identifies an IDR / IRAP.
//
// Scanner is single-goroutine (called inline from the segmenter loop). All
// state is per-stream; the segmenter creates one per source.
//
// Limitations (acceptable for v1):
//   - Single-program TS only. parsePAT picks the first PMT and ignores the
//     rest; parsePMT picks the first video ES.
//   - PMT longer than ~180 bytes (split across multiple TS packets) is not
//     reassembled — first packet's section is parsed and the rest discarded.
//     Real-world PMTs are well under this; not seen in production yet.
//   - PES header crossing a TS packet boundary is handled because we
//     accumulate the full PES payload in a small buffer before scanning for
//     start codes.

import (
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// tsPacketSize is the fixed MPEG-TS packet length.
const tsPacketSize = 188

// pesScanCap caps the per-PES accumulation buffer. We only need to scan
// the first few KiB to find the SPS/PPS/IDR start codes — further bytes
// don't change the IDR decision.
const pesScanCap = 64 * 1024

// tsKeyframeScanner consumes raw MPEG-TS bytes and reports the absolute byte
// offset where each video IDR PES *began*. The offset is "absolute" within
// the scanner's lifetime — caller subtracts its own stream-start offset to
// translate into a segBuf-relative position.
type tsKeyframeScanner struct {
	// PSI state: zero values mean "not yet learned".
	pmtPID     uint16
	videoPID   uint16
	videoCodec domain.AVCodec

	// Total bytes fed to feed()/process188(). Used as the time-base for
	// offset reports.
	feedOffset int64

	// lastPATOffset is the offset of the most recent PAT packet seen. Used
	// as the segment boundary (split point) when an IDR follows shortly
	// after — that way the next segment starts with PAT → PMT → IDR, the
	// canonical clean-start sequence a player needs to begin decoding.
	// -1 = no PAT seen yet.
	lastPATOffset int64

	// pesBuf accumulates the active video PES payload (between two PUSI=1
	// packets on videoPID). On the *next* PUSI=1 we scan it for IDR.
	pesBuf         []byte
	pesStartOffset int64 // byte offset of the TS packet that started the PES

	// LastIDROffset is the segment boundary offset for the most recent IDR
	// — i.e. the offset of the PAT *preceding* the IDR PES, not the PES
	// itself. The segmenter splits there so the new segment opens with the
	// natural PAT → PMT → IDR sequence the source emits. -1 = none yet.
	LastIDROffset int64
}

func newTSKeyframeScanner() *tsKeyframeScanner {
	return &tsKeyframeScanner{LastIDROffset: -1, lastPATOffset: -1}
}

// patIDRMaxGap caps how far back from the IDR PES we look for the most recent
// PAT. Most encoders interleave PAT/PMT every 100 ms — well within 100 KB at
// even 8 Mbps. If lastPATOffset is older than this we fall back to using the
// IDR offset itself as the split point (still better than mid-PES, just no
// PSI at the very start until the next PAT cycle).
const patIDRMaxGap int64 = 256 * 1024

// Feed processes a chunk of MPEG-TS bytes. The chunk MUST be 188-aligned
// starting from byte 0 (the segmenter's alignedFeed already enforces this).
// Mis-aligned input is silently skipped.
func (s *tsKeyframeScanner) Feed(chunk []byte) {
	for i := 0; i+tsPacketSize <= len(chunk); i += tsPacketSize {
		s.process188(chunk[i : i+tsPacketSize])
	}
}

func (s *tsKeyframeScanner) process188(pkt []byte) {
	defer func() { s.feedOffset += tsPacketSize }()
	if pkt[0] != 0x47 {
		return
	}
	pusi := pkt[1]&0x40 != 0
	pid := uint16(pkt[1]&0x1F)<<8 | uint16(pkt[2])

	switch {
	case pid == 0 && pusi:
		// Track only PAT packets that start a new PAT section (PUSI=1) —
		// continuation packets (PUSI=0) carry section-overflow bytes, not a
		// natural segment boundary. Splitting on a continuation packet
		// would put half of the previous PAT section into the next segment.
		s.lastPATOffset = s.feedOffset
		s.parsePAT(pkt)
	case s.pmtPID != 0 && pid == s.pmtPID && pusi:
		s.parsePMT(pkt)
	case s.videoPID != 0 && pid == s.videoPID:
		s.processVideoPacket(pkt, pusi)
	}
}

// processVideoPacket handles a TS packet on the video PID.
//
// On PUSI=1 we *finalize* the previously-accumulated PES: scan it for IDR,
// and if found mark its start offset as a candidate split point. Then start
// fresh accumulation for the new PES.
//
// On PUSI=0 we append payload to the active PES buffer (capped — see
// pesScanCap).
func (s *tsKeyframeScanner) processVideoPacket(pkt []byte, pusi bool) {
	if pusi {
		if len(s.pesBuf) > 0 && s.isIDR(s.pesBuf) {
			// Prefer splitting at the PAT immediately preceding the IDR, so
			// the next segment starts with the natural PAT → PMT → IDR
			// sequence the source emits. This is what lets a player decode
			// from byte 0 of the segment instead of waiting for the next
			// PSI cycle (which can be 10+ MB into a high-bitrate segment).
			boundary := s.pesStartOffset
			if s.lastPATOffset >= 0 &&
				s.lastPATOffset <= s.pesStartOffset &&
				s.pesStartOffset-s.lastPATOffset <= patIDRMaxGap {
				boundary = s.lastPATOffset
			}
			s.LastIDROffset = boundary
		}
		s.pesBuf = s.pesBuf[:0]
		s.pesStartOffset = s.feedOffset
	}
	if payload := tsPayload(pkt); len(payload) > 0 && len(s.pesBuf) < pesScanCap {
		room := pesScanCap - len(s.pesBuf)
		if len(payload) > room {
			payload = payload[:room]
		}
		s.pesBuf = append(s.pesBuf, payload...)
	}
}

// tsPayload extracts the payload bytes of a TS packet, skipping the 4-byte
// header and any adaptation field. Returns nil if the packet has no payload
// or the adaptation field is malformed.
func tsPayload(pkt []byte) []byte {
	adaptCtrl := (pkt[3] >> 4) & 0x03
	hasPayload := adaptCtrl&0x01 != 0
	if !hasPayload {
		return nil
	}
	off := 4
	if adaptCtrl&0x02 != 0 {
		// adaptation_field_length is at byte 4; payload begins after it.
		if off >= len(pkt) {
			return nil
		}
		afLen := int(pkt[off])
		off += 1 + afLen
		if off >= len(pkt) {
			return nil
		}
	}
	return pkt[off:]
}

// parsePAT handles the first PAT packet — extracts the PMT PID for the
// first program.
func (s *tsKeyframeScanner) parsePAT(pkt []byte) {
	payload := tsPayload(pkt)
	if len(payload) < 1 {
		return
	}
	// Pointer field tells where the PAT section starts.
	ptr := int(payload[0])
	off := 1 + ptr
	if off+12 > len(payload) {
		return
	}
	section := payload[off:]
	// table_id (1) + section_syntax_indicator/reserved/length (2) +
	// transport_stream_id (2) + reserved/version/current_next (1) +
	// section_number (1) + last_section_number (1) = 8 bytes header.
	// Then loop of (program_number, reserved | network_or_pmt_PID) — 4 bytes
	// each. We pick the first program whose PID is non-zero (skip NIT entries
	// where program_number = 0).
	for i := 8; i+4 <= len(section); i += 4 {
		programNumber := uint16(section[i])<<8 | uint16(section[i+1])
		if programNumber == 0 {
			continue // network PID, not a PMT
		}
		s.pmtPID = uint16(section[i+2]&0x1F)<<8 | uint16(section[i+3])
		return
	}
}

// parsePMT handles the first PMT packet — finds the first video elementary
// stream (H.264 stream_type=0x1B or H.265 stream_type=0x24) and records its
// PID + codec.
func (s *tsKeyframeScanner) parsePMT(pkt []byte) {
	payload := tsPayload(pkt)
	if len(payload) < 1 {
		return
	}
	ptr := int(payload[0])
	off := 1 + ptr
	if off+12 > len(payload) {
		return
	}
	section := payload[off:]
	// Header: table_id(1) + section_syntax/reserved/length(2) +
	// program_number(2) + reserved/version(1) + section_number(1) +
	// last_section_number(1) + reserved/PCR_PID(2) +
	// reserved/program_info_length(2) = 12 bytes.
	if len(section) < 12 {
		return
	}
	progInfoLen := int(uint16(section[10]&0x0F)<<8 | uint16(section[11]))
	pos := 12 + progInfoLen
	for pos+5 <= len(section) {
		streamType := section[pos]
		esPID := uint16(section[pos+1]&0x1F)<<8 | uint16(section[pos+2])
		esInfoLen := int(uint16(section[pos+3]&0x0F)<<8 | uint16(section[pos+4]))
		switch streamType {
		case 0x1B: // H.264
			s.videoPID = esPID
			s.videoCodec = domain.AVCodecH264
			return
		case 0x24: // H.265 / HEVC
			s.videoPID = esPID
			s.videoCodec = domain.AVCodecH265
			return
		}
		pos += 5 + esInfoLen
	}
}

// isIDR scans a complete video PES payload for an IDR (H.264) / IRAP
// (H.265) NAL unit. Strips the PES header first, then walks Annex-B start
// codes and tests the NAL type byte. Returns false on any parse failure
// (bad PES header, no NALs found) — the caller treats that as "no keyframe
// here" and continues looking at the next PES.
func (s *tsKeyframeScanner) isIDR(pes []byte) bool {
	es := stripPESHeader(pes)
	if es == nil {
		return false
	}
	switch s.videoCodec {
	case domain.AVCodecH264:
		return scanForH264IDR(es)
	case domain.AVCodecH265:
		return scanForH265IRAP(es)
	case domain.AVCodecUnknown,
		domain.AVCodecAAC, domain.AVCodecMP2, domain.AVCodecMP3,
		domain.AVCodecAC3, domain.AVCodecEAC3,
		domain.AVCodecRawTSChunk:
		return false // audio or non-frame marker — not an IDR carrier
	case domain.AVCodecAV1, domain.AVCodecMPEG2Video:
		// IDR detection for AV1 / MPEG-2 Video would require dedicated
		// parsers (OBU walking for AV1, GOP-header scan for MPEG-2). The
		// scanner is currently used by the HLS raw-TS segmenter only for
		// H.264 / H.265 IDR alignment; AV1 / MPEG-2 sources fall back to
		// the wall-clock force-flush path which is acceptable until a
		// specific use case justifies the extra parsers.
		return false
	default:
		return false
	}
}

// stripPESHeader removes the PES packet header from pes, returning the raw
// elementary-stream payload. nil on malformed input.
//
// PES layout (PES_packet_start_code_prefix = 0x000001):
//
//	[3]   start_code_prefix 00 00 01
//	[1]   stream_id
//	[2]   PES_packet_length
//	[1]   marker/scrambling/priority flags
//	[1]   PTS_DTS_flags + ...
//	[1]   PES_header_data_length = N
//	[N]   PTS/DTS + extensions
//	...   ES bytes
func stripPESHeader(pes []byte) []byte {
	if len(pes) < 9 {
		return nil
	}
	if pes[0] != 0x00 || pes[1] != 0x00 || pes[2] != 0x01 {
		return nil
	}
	hdrLen := int(pes[8])
	off := 9 + hdrLen
	if off > len(pes) {
		return nil
	}
	return pes[off:]
}

// scanForH264IDR walks Annex-B NALs and returns true on a NAL with type=5
// (Coded slice of an IDR picture).
func scanForH264IDR(es []byte) bool {
	for _, nalType := range walkAnnexBNALTypes(es) {
		if nalType&0x1F == 5 {
			return true
		}
	}
	return false
}

// scanForH265IRAP returns true on any HEVC IRAP NAL (BLA / IDR / CRA, types
// 16..23) — these are the access points a player can resync on.
func scanForH265IRAP(es []byte) bool {
	for _, nalType := range walkAnnexBNALTypes(es) {
		t := (nalType >> 1) & 0x3F
		if t >= 16 && t <= 23 {
			return true
		}
	}
	return false
}

// walkAnnexBNALTypes yields the first byte of each NAL unit found in es.
// Recognises both 3-byte (00 00 01) and 4-byte (00 00 00 01) start codes.
// Returned slice is for iteration only — a small alloc per call but the
// caller only uses scanner during segment-flush decisions, not the hot
// per-packet loop.
func walkAnnexBNALTypes(es []byte) []byte {
	out := make([]byte, 0, 8)
	i := 0
	for i+3 < len(es) {
		switch {
		case es[i] == 0 && es[i+1] == 0 && es[i+2] == 0 && es[i+3] == 1:
			if i+4 < len(es) {
				out = append(out, es[i+4])
			}
			i += 4
		case es[i] == 0 && es[i+1] == 0 && es[i+2] == 1:
			if i+3 < len(es) {
				out = append(out, es[i+3])
			}
			i += 3
		default:
			i++
		}
	}
	return out
}
