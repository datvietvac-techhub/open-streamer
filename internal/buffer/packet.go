package buffer

import "github.com/datvietvac-techhub/open-streamer/internal/domain"

// Packet is the Buffer Hub wire format. Exactly one of TS or AV should be set per write:
//   - TS: raw MPEG-TS chunk (transcoder output, push ingest, or legacy passthrough).
//   - AV: one elementary-stream access unit (ingest pull after demux or native ES readers).
//
// SessionID and SessionStart are stamped by Service.Write from the
// per-stream session table maintained via SetSession. Consumers read
// them to detect session boundaries explicitly — this is the SOLE
// boundary cue (the legacy `AVPacket.Discontinuity` flag was deleted
// in Phase 5 of the refactor). SessionID==0 means "no session metadata
// yet" (synthetic packet from a code path that bypasses SetSession).
type Packet struct {
	TS []byte
	AV *domain.AVPacket

	SessionID    uint64
	SessionStart bool
}

// TSPacket wraps a raw MPEG-TS chunk.
func TSPacket(b []byte) Packet { return Packet{TS: b} }

func clonePacket(p Packet) Packet {
	out := Packet{
		SessionID:    p.SessionID,
		SessionStart: p.SessionStart,
	}
	if len(p.TS) > 0 {
		out.TS = append([]byte(nil), p.TS...)
	}
	if p.AV != nil {
		out.AV = p.AV.Clone()
	}
	return out
}

func (p Packet) empty() bool {
	return len(p.TS) == 0 && (p.AV == nil || len(p.AV.Data) == 0)
}
