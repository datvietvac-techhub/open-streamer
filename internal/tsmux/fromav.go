// Package tsmux converts AVPackets into 188-byte MPEG-TS packets.
//
// The Muxer is built on github.com/asticode/go-astits. Unlike gomedia's
// fixed 400 ms PAT/PMT cadence, astits emits PSI either every N PES
// packets (MuxerOptTablesRetransmitPeriod) OR immediately before any
// PES whose AdaptationField has RandomAccessIndicator=true on the
// PCR PID. We set RAI on every H.264/H.265 keyframe so each IDR is
// preceded by fresh PAT → PMT → IDR. This guarantees HLS / DASH
// segments cut at an IDR start with the canonical PSI sequence —
// without it, players hit "non-existing PPS 0 referenced" decoder
// errors on segments where source-side PSI happened to land in the
// middle of a previous GOP.
package tsmux

import (
	"bytes"
	"context"

	"github.com/Eyevinn/mp4ff/avc"
	"github.com/asticode/go-astits"
	gocodec "github.com/yapingcat/gomedia/go-codec"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// PID convention. astits requires explicit elementary stream PIDs
// (gomedia auto-assigned starting at 0x100). Mirror that convention so
// the wire format stays familiar for diagnostic tools (Wireshark,
// TSDuck) and so any downstream consumer that hard-coded a PID still
// works after the migration.
const (
	videoPID = 0x100 // 256
	audioPID = 0x101 // 257
)

// tablesRetransmitPeriod is how often astits emits PAT/PMT in steady
// state (counted in PES packets, not wallclock). The IDR-triggered
// force-emit is the primary mechanism; this is the fallback for the
// gap between IDRs. At 30 fps video, 40 PES packets ≈ 1.3 s — short
// enough that a player joining mid-GOP catches PSI within typical
// HLS segment latency.
const tablesRetransmitPeriod = 40

// FromAV is a per-pipeline TS muxer. One instance per output channel
// (HLS segmenter, RTMP publisher pump, transcoder stdin feeder, …).
type FromAV struct {
	mux *astits.Muxer
	buf bytes.Buffer // astits writes 188-byte packets into here

	hasV bool
	hasA bool

	// pendingDisc tracks PIDs whose NEXT emitted packet needs
	// adaptation_field.discontinuity_indicator=1. Each PID is added on
	// AddElementaryStream (CC starts at 0 from a fresh muxer) and
	// cleared after the first emit so the flag fires exactly once per
	// PID per FromAV lifetime. Strict analyzers (TSDuck tsanalyze,
	// DVB / ATSC compliance suites) flag a fresh CC=0 without the
	// indicator as a stream-corruption event; modern players ignore.
	pendingDisc map[uint16]bool
}

// NewFromAV creates an empty muxer; streams are added on first packet per codec.
// ctx is forwarded to astits.NewMuxer — pass the caller's lifecycle context
// (typically the publisher / transcoder / ingestor goroutine ctx) so the
// muxer's internal state is tied to that lifecycle.
func NewFromAV(ctx context.Context) *FromAV {
	f := &FromAV{pendingDisc: make(map[uint16]bool)}
	f.mux = astits.NewMuxer(
		ctx,
		bufWriter{buf: &f.buf},
		astits.MuxerOptTablesRetransmitPeriod(tablesRetransmitPeriod),
	)
	return f
}

// bufWriter is the io.Writer astits emits TS bytes into. Reset before
// each WriteData call so we can collect the produced 188-byte packets
// and forward them to the caller's onPacket without retaining state.
type bufWriter struct{ buf *bytes.Buffer }

func (w bufWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

// Write converts one AVPacket to TS transport packets via onPacket.
// onPacket may be called multiple times when a single PES spans more
// than one 188-byte packet (the common case for video keyframes).
func (f *FromAV) Write(p *domain.AVPacket, onPacket func([]byte)) {
	if p == nil || len(p.Data) == 0 || onPacket == nil {
		return
	}
	pid, streamType, isVideo, ok := f.codecRouting(p.Codec)
	if !ok {
		return
	}
	if isVideo && !f.hasV {
		if err := f.mux.AddElementaryStream(astits.PMTElementaryStream{
			ElementaryPID: pid,
			StreamType:    streamType,
		}); err != nil {
			return
		}
		// PCR rides on the video PID (canonical for AV streams). This is
		// what gives RandomAccessIndicator on a keyframe its force-emit
		// effect: astits checks `d.PID == m.pmt.PCRPID` before honouring
		// RAI as a "force tables" signal.
		f.mux.SetPCRPID(pid)
		f.hasV = true
		f.pendingDisc[pid] = true
		// Force PSI emit NOW (rather than waiting for the next
		// retransmit period or for an RAI=true on this WriteData) so
		// downstream demuxers learn the freshly-added video PID before
		// the PES bytes hit the wire. Without this, audio that already
		// got a PMT emit earlier would carry on; video PES would land
		// on a PID downstream hasn't announced. Same reasoning the
		// audio branch below — adding a stream MID-stream is the
		// trigger that needs an explicit WriteTables.
		_, _ = f.mux.WriteTables()
	} else if !isVideo && !f.hasA {
		if err := f.mux.AddElementaryStream(astits.PMTElementaryStream{
			ElementaryPID: pid,
			StreamType:    streamType,
		}); err != nil {
			return
		}
		// Fallback PCR PID for audio-only pipelines. Force-emit on
		// keyframes won't fire for audio (no IDR concept on AAC), so
		// audio-only pipelines rely on the retransmit period.
		if !f.hasV {
			f.mux.SetPCRPID(pid)
		}
		f.hasA = true
		f.pendingDisc[pid] = true
		// Force PSI emit so the new audio PID is announced before the
		// first audio PES — see the video branch above for the full
		// reasoning. This fix is the reason test2 (file:// MP4 source,
		// video emitted before audio) now produces an AAC track in the
		// downstream DASH MPD instead of a silent stream.
		_, _ = f.mux.WriteTables()
	}

	// PTS / DTS in 90 kHz ticks (MPEG-TS native).
	ptsTicks := int64(p.PTSms) * 90 //nolint:gosec
	dtsTicks := int64(p.DTSms) * 90 //nolint:gosec
	if p.DTSms == 0 && p.PTSms != 0 {
		dtsTicks = ptsTicks
	}
	pts := &astits.ClockReference{Base: ptsTicks}
	dts := &astits.ClockReference{Base: dtsTicks}

	// AdaptationField carries two HLS-critical signals:
	//
	// 1. RandomAccessIndicator on video keyframes triggers astits to
	//    emit PAT + PMT immediately before this PES (see package
	//    doc-comment for why this fixes HLS IDR alignment).
	//
	// 2. PCR (Program Clock Reference) on every video PES gives
	//    players a wall-clock anchor for A/V sync. astits does NOT
	//    emit PCR automatically; the muxer's SetPCRPID only marks
	//    which PID to LOOK for PCR in. Without PCR every < 100 ms
	//    (ITU-T H.222.0 § 2.4.2.2) strict HLS players (hls.js most
	//    notably) refuse to play. Emitting PCR on every video PES
	//    keeps the cadence at ~33 ms for 30 fps sources, well under
	//    the spec ceiling.
	//
	// Audio frames carry neither RAI (no IDR concept) nor PCR
	// (per-spec PCR rides on the elementary stream that owns the
	// PCR PID, which we set to video).
	// Pull the discontinuity_indicator flag for THIS PID's next emit.
	// pendingDisc was seeded when the stream was lazy-added; cleared
	// on the first emit so the flag fires exactly once per PID per
	// FromAV lifetime.
	needDisc := f.pendingDisc[pid]
	if needDisc {
		delete(f.pendingDisc, pid)
	}

	var af *astits.PacketAdaptationField
	if isVideo {
		af = &astits.PacketAdaptationField{
			HasPCR:                 true,
			PCR:                    &astits.ClockReference{Base: dtsTicks},
			RandomAccessIndicator:  p.KeyFrame,
			DiscontinuityIndicator: needDisc,
		}
	} else if needDisc {
		// Audio has no PCR / RAI but still needs the discontinuity
		// flag on its first packet so strict analyzers don't flag the
		// CC=0 start.
		af = &astits.PacketAdaptationField{
			DiscontinuityIndicator: true,
		}
	}

	// Per-frame indicator: BothPresent only when DTS truly differs
	// from PTS (B-frame reorder). For I/P-frames where DTS == PTS,
	// OnlyPTS saves 5 bytes and players treat the absent DTS as
	// "DTS = PTS" implicitly.
	//
	// We tried "always BothPresent for video" to fix test2's hls.js
	// playback (mixed indicator was suspected to confuse the player),
	// but that EXPOSED non-monotonic DTS values previously masked
	// by OnlyPTS in test_puhser / test5 / test_mixer — sources that
	// emit duplicate or near-duplicate DTS get tolerated when DTS
	// is implicit but rejected when explicit. Net effect was: test2
	// improved marginally, other streams regressed. Reverted.
	indicator := uint8(astits.PTSDTSIndicatorOnlyPTS)
	if p.DTSms != 0 && p.DTSms != p.PTSms {
		indicator = astits.PTSDTSIndicatorBothPresent
	}

	f.buf.Reset()
	_, _ = f.mux.WriteData(&astits.MuxerData{
		PID:             pid,
		AdaptationField: af,
		PES: &astits.PESData{
			Data: p.Data,
			Header: &astits.PESHeader{
				OptionalHeader: &astits.PESOptionalHeader{
					PTS:             pts,
					DTS:             dts,
					PTSDTSIndicator: indicator,
				},
			},
		},
	})

	emitPerPacket(f.buf.Bytes(), onPacket)
}

// emitPerPacket splits a multi-packet buffer into individual 188-byte
// callbacks, matching gomedia.TSMuxer's per-packet OnPacket cadence.
// Tail bytes that don't form a full 188-byte packet (shouldn't happen
// since astits pads with stuffing) are skipped.
func emitPerPacket(buf []byte, onPacket func([]byte)) {
	const tsPacketSize = 188
	for i := 0; i+tsPacketSize <= len(buf); i += tsPacketSize {
		// Copy out — caller may retain (e.g. HLS segBuf append) and
		// astits reuses its internal buffer across WriteData calls.
		pkt := make([]byte, tsPacketSize)
		copy(pkt, buf[i:i+tsPacketSize])
		onPacket(pkt)
	}
}

// codecRouting maps a domain codec to its TS PID + StreamType. Returns
// ok=false for codecs we don't currently re-mux (raw-TS marker,
// unsupported audio variants, codecs without an AVPacket extractor).
func (f *FromAV) codecRouting(c domain.AVCodec) (pid uint16, st astits.StreamType, isVideo, ok bool) {
	switch c { //nolint:exhaustive // unsupported codecs intentionally drop — RawTSChunk is forwarded as Packet.TS upstream, never reaches the muxer; AC-3 / E-AC-3 / AV1 / MPEG-2 video have no AVPacket producer today
	case domain.AVCodecH264:
		return videoPID, astits.StreamTypeH264Video, true, true
	case domain.AVCodecH265:
		return videoPID, astits.StreamTypeH265Video, true, true
	case domain.AVCodecAAC:
		return audioPID, astits.StreamTypeAACAudio, false, true
	case domain.AVCodecMP2, domain.AVCodecMP3:
		// MPEG-1/2 Audio Layer I/II/III. astits's MPEG2Audio (0x04) is
		// the broader-compatibility choice; the Layer is encoded in the
		// frame header itself so a player picks Layer III (MP3) or
		// Layer II (MP2) automatically.
		return audioPID, astits.StreamTypeMPEG2Audio, false, true
	}
	return 0, 0, false, false
}

// KeyFrameH264 reports whether Annex-B H.264 payload contains an IDR slice.
func KeyFrameH264(annexB []byte) bool {
	return gocodec.IsH264IDRFrame(annexB)
}

// KeyFrameH265 reports whether Annex-B H.265 payload contains an IRAP slice.
func KeyFrameH265(annexB []byte) bool {
	return gocodec.IsH265IDRFrame(annexB)
}

// FeedWirePacket forwards raw TS chunks or muxes one AVPacket to TS via
// mux (lazily allocated). ctx is forwarded to NewFromAV on lazy
// allocation — pass the caller's lifecycle context so the underlying
// astits.Muxer ties cleanly to that lifecycle.
func FeedWirePacket(ctx context.Context, ts []byte, av *domain.AVPacket, mux **FromAV, onTS func([]byte)) {
	if len(ts) > 0 {
		onTS(ts)
		return
	}
	if av == nil || len(av.Data) == 0 || onTS == nil {
		return
	}
	if *mux == nil {
		*mux = NewFromAV(ctx)
	}
	(*mux).Write(av, onTS)
}

// FindH264ParameterSets scans raw bytes for the first NAL units of type 7
// (SPS) and type 8 (PPS) and returns their bodies (without start codes).
// Detects both 4-byte (00 00 00 01) and 3-byte (00 00 01) Annex-B start
// codes. Returns nil for any NAL not present.
//
// Use case: the publisher's serve_rtmp pipeline needs SPS/PPS to build
// the AVCDecoderConfigurationRecord seq header tag, but receives raw TS
// from the buffer hub (no AV packets). The TS demuxer should preserve
// SPS/PPS in OnFrame access-unit bytes per its source, but some upstream
// code paths or stream variations have produced frames without
// parameter sets. Scanning raw TS bytes is a defensive fallback that
// does NOT depend on the demuxer's frame-splitting behaviour.
//
// SPS validation: every candidate (byte after a start code with low-5
// bits == 7) is parsed via mp4ff's avc.ParseSPSNALUnit. False
// positives — random payload bytes that look like an Annex-B start
// code followed by a byte whose nal_unit_type bits equal 7 — fail
// parsing, so we keep scanning forward. Without this validation we
// returned ~1KB blobs of payload as "SPS" and pushed garbage AVC
// sequence headers to RTMP clients (observed: width=16, height=16,
// ProfileIdc=1 — impossible for real video).
//
// This is a best-effort scan: NAL units split across TS-packet
// boundaries (with adaptation-field stuffing) may be missed. For SPS
// (~30 bytes for typical 1080p H264) this is unlikely since the whole
// NAL fits within a single 184-byte TS payload. PPS is even smaller.
//
// Caller should retain the returned slices (do not reference into the
// input — we copy out so subsequent buffer reuse cannot corrupt the
// cached parameter sets).
func FindH264ParameterSets(raw []byte) (sps, pps []byte) {
	sps = findValidSPS(raw)
	pps = findNALWithType(raw, 8, 0)
	if pps != nil && !looksLikePPS(pps) {
		pps = nil
	}
	return sps, pps
}

// findValidSPS scans raw repeatedly for nal_unit_type 7, parses each
// candidate via mp4ff, and returns the first one that parses to a
// plausible SPS (width >= 32 and height >= 32 — anything smaller is
// almost certainly a false positive). Returns nil if no candidate
// validates.
func findValidSPS(raw []byte) []byte {
	from := 0
	for {
		cand, next := findNALWithTypeAt(raw, 7, from)
		if cand == nil {
			return nil
		}
		if sps, err := avc.ParseSPSNALUnit(cand, false); err == nil && sps != nil {
			if sps.Width >= 32 && sps.Height >= 32 {
				return cand
			}
		}
		from = next
	}
}

// looksLikePPS rejects oversized blobs that are almost certainly
// payload data masquerading as a PPS. Real PPS units are typically
// <100 bytes; we allow 256 as a conservative ceiling.
func looksLikePPS(pps []byte) bool {
	return len(pps) > 0 && len(pps) <= 256
}

// findNALWithType returns the first NAL with the given nal_unit_type
// in `raw`, starting the scan from `start`. Matches both 4-byte and
// 3-byte Annex-B start codes.
func findNALWithType(raw []byte, nalType byte, start int) []byte {
	body, _ := findNALWithTypeAt(raw, nalType, start)
	return body
}

// findNALWithTypeAt is like findNALWithType but also returns the byte
// offset of the matched NAL header so callers can resume scanning past
// a failed-validation candidate.
func findNALWithTypeAt(raw []byte, nalType byte, start int) (body []byte, headerIdx int) {
	if start < 0 {
		start = 0
	}
	for i := start; i+4 < len(raw); i++ {
		if raw[i] != 0x00 {
			continue
		}
		// 4-byte start code: 00 00 00 01
		if raw[i+1] == 0x00 && raw[i+2] == 0x00 && raw[i+3] == 0x01 {
			if raw[i+4]&0x1F == nalType {
				return extractNALBody(raw, i+4), i + 4
			}
			continue
		}
		// 3-byte start code: 00 00 01
		if raw[i+1] == 0x00 && raw[i+2] == 0x01 {
			if raw[i+3]&0x1F == nalType {
				return extractNALBody(raw, i+3), i + 3
			}
		}
	}
	return nil, len(raw)
}

// extractNALBody returns the bytes of a single NAL starting at startIdx
// (which points to the NAL header byte itself) up to the next start code
// or end of raw. Caller must ensure startIdx is within bounds.
func extractNALBody(raw []byte, startIdx int) []byte {
	if startIdx >= len(raw) {
		return nil
	}
	// Search forward for the next start code; the NAL ends just before it.
	end := len(raw)
	for i := startIdx + 1; i+2 < len(raw); i++ {
		if raw[i] == 0x00 && raw[i+1] == 0x00 && (raw[i+2] == 0x01 ||
			(i+3 < len(raw) && raw[i+2] == 0x00 && raw[i+3] == 0x01)) {
			end = i
			break
		}
	}
	out := make([]byte, end-startIdx)
	copy(out, raw[startIdx:end])
	return out
}
