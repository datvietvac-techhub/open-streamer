package pull

// stats_pmt_scanner.go — PSI-based stats scanner that recognises codecs
// gompeg2's TSDemuxer drops on the floor.
//
// Why not use gompeg2: it only emits OnFrame for stream_type 0x03/0x04/0x0F/
// 0x1B/0x24 (MPEG audio, AAC, H.264, H.265). DVB AC-3 (0x06+desc 0x6A or
// 0x81), E-AC-3 (0x06+desc 0x7A or 0x87), MPEG-2 Video (0x02), and AV1
// (0x06 + registration descriptor "AV01") all silently disappear, leaving
// the manager's input "tracks" panel empty for sources whose audio/video
// is anything other than the gompeg2 short-list.
//
// This scanner is purely PSI-driven and codec-agnostic at frame level:
//
//  1. Parse PAT (PID 0) → learn PMT PID
//  2. Parse PMT → for each ES PID, determine its codec from
//     stream_type plus DVB descriptors (AC-3, Enhanced AC-3, registration)
//  3. For every TS packet on a learned ES PID, count payload bytes and
//     synthesise a stats AVPacket{Codec, Data: payload} so the existing
//     inputTrackStats observer (which folds len(Data) into a per-codec
//     bitrate EWMA) updates correctly
//
// What this scanner does NOT do:
//   - PES reassembly (no need for stats-only)
//   - Frame extraction (mixer:// for these codecs is a separate concern)
//   - SPS / OBU parsing (resolution stays unknown — acceptable for audio
//     and for video shown alongside the kernel-detected resolution)
//
// MP3 vs MP2 disambiguation: stream_types 0x03 / 0x04 land as AVCodecMP2
// initially. The first PES payload byte run is peeked for the MPEG-audio
// sync pattern (0xFF F?) and the Layer field is parsed once per PID; the
// codec is upgraded to AVCodecMP3 if Layer III is detected. Reuses the
// same heuristic as mpegAudioCodecFromFrame in tsdemux_packet_reader.go.

import (
	"sync"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// streamTypeCodec maps a TS PMT stream_type value to the matching AVCodec.
// Stream types not in this table are either codec-agnostic private streams
// (handled via descriptors below) or explicitly unsupported.
//
// Reference: ISO/IEC 13818-1 Table 2-34 + ATSC A/52 + DVB TS 101 154.
var streamTypeCodec = map[byte]domain.AVCodec{
	0x01: domain.AVCodecMPEG2Video, // MPEG-1 video — lumped with MPEG-2 (same downstream path)
	0x02: domain.AVCodecMPEG2Video, // MPEG-2 Part 2 (H.262)
	0x03: domain.AVCodecMP2,        // MPEG-1 audio (may upgrade to MP3 via frame inspection)
	0x04: domain.AVCodecMP2,        // MPEG-2 audio (may upgrade to MP3)
	0x0F: domain.AVCodecAAC,        // MPEG-2 AAC ADTS
	0x11: domain.AVCodecAAC,        // MPEG-4 AAC LATM (same codec, different syntax)
	0x1B: domain.AVCodecH264,
	0x24: domain.AVCodecH265,
	0x81: domain.AVCodecAC3,  // ATSC AC-3
	0x87: domain.AVCodecEAC3, // ATSC Enhanced AC-3
}

// DVB descriptor tags appearing in the PMT ES descriptor loop. Stream type
// 0x06 (PES private data) is a generic carrier — the actual codec is
// indicated by one of these descriptors.
const (
	descRegistration = 0x05 // 4-byte format identifier (AV01, Opus, …)
	descAC3          = 0x6A // DVB AC-3 descriptor (ETSI EN 300 468)
	descEnhancedAC3  = 0x7A // DVB E-AC-3 descriptor
)

// statsPIDState holds per-ES-PID detection state for the scanner.
type statsPIDState struct {
	codec domain.AVCodec
	// mpegAudioLockedToMP3 records whether we have peeked at the first
	// MPEG-audio PES payload and confirmed Layer III. Subsequent packets
	// on the same PID skip the peek.
	mpegAudioLocked bool
}

// statsPMTScanner is the side-channel codec/bitrate observer for raw-TS
// sources. Construct via newStatsPMTScanner; feed via Feed; tear down via
// Close. Safe for one writer (Feed) + one closer (Close); never call Feed
// from multiple goroutines.
type statsPMTScanner struct {
	onAV func(p *domain.AVPacket)

	// PSI state.
	pmtPID    uint16
	pidStates map[uint16]*statsPIDState

	// Channel between Feed and the worker goroutine.
	ch chan []byte
	wg sync.WaitGroup

	closeMu sync.Mutex
	closed  bool
}

// newStatsPMTScanner starts a goroutine that consumes raw TS chunks fed
// via Feed and invokes onAV for every learned-PID packet. Returns nil for
// nil callback so callers don't need to guard the construction site.
func newStatsPMTScanner(onAV func(p *domain.AVPacket)) *statsPMTScanner {
	if onAV == nil {
		return nil
	}
	s := &statsPMTScanner{
		onAV:      onAV,
		pidStates: make(map[uint16]*statsPIDState, 4),
		ch:        make(chan []byte, statsChunkBuffer),
	}
	s.wg.Add(1)
	go s.run()
	return s
}

// Feed enqueues chunk for asynchronous scanning. Drops on full channel
// (best-effort) or post-Close. The slice is copied internally.
func (s *statsPMTScanner) Feed(chunk []byte) {
	if s == nil || len(chunk) == 0 {
		return
	}
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return
	}
	cp := append([]byte(nil), chunk...)
	select {
	case s.ch <- cp:
	default:
	}
}

// Close stops the scanner goroutine and waits for it to exit. Safe to call
// multiple times and from any goroutine.
func (s *statsPMTScanner) Close() {
	if s == nil {
		return
	}
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return
	}
	s.closed = true
	close(s.ch)
	s.closeMu.Unlock()
	s.wg.Wait()
}

// run is the scanner goroutine.
func (s *statsPMTScanner) run() {
	defer s.wg.Done()
	for chunk := range s.ch {
		s.scan(chunk)
	}
}

// scan walks chunk packet-by-packet (188 bytes), updating PSI state and
// emitting stats AVPackets for ES PIDs.
func (s *statsPMTScanner) scan(chunk []byte) {
	for i := 0; i+tsPacketSize <= len(chunk); i += tsPacketSize {
		s.processPacket(chunk[i : i+tsPacketSize])
	}
}

// processPacket dispatches one TS packet to PAT / PMT / ES handlers.
func (s *statsPMTScanner) processPacket(pkt []byte) {
	if pkt[0] != 0x47 {
		return
	}
	pid := uint16(pkt[1]&0x1F)<<8 | uint16(pkt[2])
	pusi := pkt[1]&0x40 != 0

	switch {
	case pid == 0 && pusi:
		s.parsePAT(pkt)
	case s.pmtPID != 0 && pid == s.pmtPID && pusi:
		s.parsePMT(pkt)
	default:
		st, ok := s.pidStates[pid]
		if !ok {
			return
		}
		s.emitForPacket(pkt, pusi, st)
	}
}

// parsePAT learns the first program's PMT PID. Mirrors tsKeyframeScanner.
func (s *statsPMTScanner) parsePAT(pkt []byte) {
	payload := tsPayloadBytes(pkt)
	if len(payload) < 1 {
		return
	}
	ptr := int(payload[0])
	off := 1 + ptr
	if off+12 > len(payload) {
		return
	}
	section := payload[off:]
	for i := 8; i+4 <= len(section); i += 4 {
		programNumber := uint16(section[i])<<8 | uint16(section[i+1])
		if programNumber == 0 {
			continue // network PID, not a PMT
		}
		s.pmtPID = uint16(section[i+2]&0x1F)<<8 | uint16(section[i+3])
		return
	}
}

// parsePMT walks the ES loop and registers each PID's codec.
func (s *statsPMTScanner) parsePMT(pkt []byte) {
	payload := tsPayloadBytes(pkt)
	if len(payload) < 1 {
		return
	}
	ptr := int(payload[0])
	off := 1 + ptr
	if off+12 > len(payload) {
		return
	}
	section := payload[off:]
	if len(section) < 12 {
		return
	}
	sectionLen := int(section[1]&0x0F)<<8 | int(section[2])
	if 3+sectionLen > len(section) {
		return
	}
	progInfoLen := int(section[10]&0x0F)<<8 | int(section[11])
	pos := 12 + progInfoLen
	end := 3 + sectionLen - 4 // exclude trailing CRC32
	if end > len(section) || pos > end {
		return
	}

	for pos+5 <= end {
		streamType := section[pos]
		esPID := uint16(section[pos+1]&0x1F)<<8 | uint16(section[pos+2])
		esInfoLen := int(section[pos+3]&0x0F)<<8 | int(section[pos+4])
		descStart := pos + 5
		descEnd := descStart + esInfoLen
		if descEnd > end {
			descEnd = end
		}

		codec := s.codecFromStreamType(streamType, section[descStart:descEnd])
		if codec != domain.AVCodecUnknown {
			if existing, ok := s.pidStates[esPID]; ok {
				existing.codec = codec
			} else {
				s.pidStates[esPID] = &statsPIDState{codec: codec}
			}
		}
		pos += 5 + esInfoLen
	}
}

// codecFromStreamType resolves a PMT entry's codec from stream_type plus
// DVB descriptors. Returns AVCodecUnknown when the entry is not a codec
// we want to track (e.g. PCR-only PIDs, unsupported private streams).
func (s *statsPMTScanner) codecFromStreamType(streamType byte, descriptors []byte) domain.AVCodec {
	if c, ok := streamTypeCodec[streamType]; ok {
		return c
	}
	// stream_type 0x06 = PES private data; the codec is in the descriptors.
	if streamType == 0x06 {
		return codecFromDescriptors(descriptors)
	}
	return domain.AVCodecUnknown
}

// codecFromDescriptors walks the descriptor block following an ES_info_length
// field and returns the codec implied by the first recognised descriptor.
//
// Recognised tags:
//   - 0x6A (DVB AC-3 descriptor)              → AC3
//   - 0x7A (DVB Enhanced AC-3 descriptor)     → EAC3
//   - 0x05 (registration descriptor) with
//     format_identifier "AV01"                → AV1
func codecFromDescriptors(descs []byte) domain.AVCodec {
	for i := 0; i+2 <= len(descs); {
		tag := descs[i]
		length := int(descs[i+1])
		body := i + 2
		if body+length > len(descs) {
			return domain.AVCodecUnknown
		}
		switch tag {
		case descAC3:
			return domain.AVCodecAC3
		case descEnhancedAC3:
			return domain.AVCodecEAC3
		case descRegistration:
			if length >= 4 {
				id := string(descs[body : body+4])
				switch id {
				case "AV01":
					return domain.AVCodecAV1
				}
			}
		}
		i = body + length
	}
	return domain.AVCodecUnknown
}

// emitForPacket synthesises a stats AVPacket for one TS packet on a known
// ES PID. The Data field is the TS-level payload bytes — len(Data) feeds
// the inputTrackStats EWMA bitrate calculator. PIDs carrying MPEG audio
// are inspected once for Layer ID to upgrade MP2 → MP3 when applicable.
func (s *statsPMTScanner) emitForPacket(pkt []byte, pusi bool, st *statsPIDState) {
	payload := tsPayloadBytes(pkt)
	if len(payload) == 0 {
		return
	}

	// MP2 / MP3 layer detection: peek the first PES payload byte run on
	// PUSI=1 to read the MPEG audio frame header. After PES header there's
	// a sync 0xFF F? followed by VVLLB (version/layer/protection).
	if !st.mpegAudioLocked && (st.codec == domain.AVCodecMP2 || st.codec == domain.AVCodecMP3) {
		if pusi {
			if c := peekMPEGAudioLayer(payload); c != domain.AVCodecUnknown {
				st.codec = c
			}
			st.mpegAudioLocked = true
		}
	}

	s.onAV(&domain.AVPacket{
		Codec: st.codec,
		Data:  payload,
	})
}

// peekMPEGAudioLayer scans a PES-payload-bearing TS packet for the MPEG
// audio sync pattern (0xFF F?) and returns AVCodecMP3 when Layer III is
// detected. Returns AVCodecMP2 otherwise (Layer I / II / unknown).
// Returns AVCodecUnknown when no sync is found in the visible bytes.
func peekMPEGAudioLayer(payload []byte) domain.AVCodec {
	// Skip the PES header: 6-byte fixed prefix + variable extension. Find
	// the first MPEG audio sync (0xFF followed by a byte with top 3 bits
	// set). Limit the scan to the first 64 bytes to avoid burning cycles
	// on malformed PES.
	scanLen := len(payload)
	if scanLen > 64 {
		scanLen = 64
	}
	for i := 0; i+1 < scanLen; i++ {
		if payload[i] != 0xFF {
			continue
		}
		if payload[i+1]&0xE0 != 0xE0 {
			continue
		}
		const layerIII = 0b01
		if (payload[i+1]>>1)&0x03 == layerIII {
			return domain.AVCodecMP3
		}
		return domain.AVCodecMP2
	}
	return domain.AVCodecUnknown
}
