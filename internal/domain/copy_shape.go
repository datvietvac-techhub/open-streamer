package domain

import (
	"errors"
	"fmt"

	"github.com/datvietvac-techhub/open-streamer/pkg/protocol"
)

// IsCopyInput reports whether the given Input is a `copy://` reference.
// Convenience helper for callers that want to short-circuit per-input logic
// (e.g. skip network reconnect logic for copy inputs).
func IsCopyInput(in Input) bool {
	return protocol.Detect(in.URL) == protocol.KindCopy
}

// CopyInputTarget extracts the upstream code from a copy:// input. Returns
// ("", error) for non-copy or malformed inputs. The error wraps
// protocol.CopyTarget's error so callers can preserve the user-facing message.
func CopyInputTarget(in Input) (StreamCode, error) {
	if protocol.Detect(in.URL) != protocol.KindCopy {
		return "", fmt.Errorf("input %q is not a copy:// URL", in.URL)
	}
	code, err := protocol.CopyTarget(in.URL)
	if err != nil {
		return "", err
	}
	return StreamCode(code), nil
}

// CopyShapeError reports a copy:// configuration that violates the v1
// constraints (e.g. local transcoder set when upstream has ABR, mixed
// shapes in the input list). The Reason string is API-surface text — the
// handler returns it verbatim in the 400 response.
type CopyShapeError struct {
	StreamCode StreamCode
	Reason     string
}

func (e *CopyShapeError) Error() string {
	return fmt.Sprintf("stream %q: copy:// shape: %s", e.StreamCode, e.Reason)
}

// IsCopyShapeError reports whether err is a *CopyShapeError.
func IsCopyShapeError(err error) bool {
	var t *CopyShapeError
	return errors.As(err, &t)
}

// StreamLookup returns the upstream stream by code. Used by ValidateCopyShape
// to inspect upstream transcoder shape without coupling domain to the
// repository layer. The bool reflects "found"; missing upstream is not an
// error here (handled at runtime by the coordinator).
type StreamLookup func(StreamCode) (*Stream, bool)

// ValidateCopyShape enforces copy:// constraints on a single stream:
//
//  1. Self-copy (`copy://A` inside stream A) is rejected — a stream cannot
//     re-stream itself; that's an infinite fan-out loop.
//  2. When upstream X has an ABR ladder, downstream MUST satisfy:
//     a) `copy://X` is the SOLE input (no fallback in v1)
//     b) downstream's own `transcoder` field is nil (ladder is inherited
//     from upstream; configuring local re-transcode is ambiguous)
//  3. When mixing copy:// inputs with regular network inputs in the same
//     priority list, all referenced upstreams must be single-stream
//     (no ABR), since failover between rendition shapes is not supported
//     in v1.
//
// `lookup` resolves upstream streams. Missing upstream is treated as
// "shape unknown" — the rule it enables is skipped, never failed. The
// coordinator validates upstream presence at start time as a hard error.
func ValidateCopyShape(s *Stream, lookup StreamLookup) error {
	if s == nil {
		return nil
	}

	// Pre-pass: classify each input.
	type classified struct {
		raw         Input
		isCopy      bool
		target      StreamCode
		upstream    *Stream // nil if not copy or upstream missing
		upstreamABR bool
	}

	classes := make([]classified, 0, len(s.Inputs))
	for _, in := range s.Inputs {
		c := classified{raw: in}
		if !IsCopyInput(in) {
			classes = append(classes, c)
			continue
		}
		c.isCopy = true
		target, err := CopyInputTarget(in)
		if err != nil {
			// Per-input parse error is reported by upstream URL validation;
			// don't double-report here. Skip the rule that needs target.
			classes = append(classes, c)
			continue
		}
		if target == s.Code {
			return &CopyShapeError{
				StreamCode: s.Code,
				Reason:     "self-copy not allowed (a stream cannot re-stream itself)",
			}
		}
		c.target = target
		if up, ok := lookup(target); ok {
			c.upstream = up
			c.upstreamABR = streamHasRenditions(up)
		}
		classes = append(classes, c)
	}

	// Find any ABR-copy reference.
	abrIdx := -1
	for i, c := range classes {
		if c.isCopy && c.upstreamABR {
			abrIdx = i
			break
		}
	}

	// Rule 2(a): ABR-copy must be the sole input.
	if abrIdx >= 0 && len(classes) > 1 {
		return &CopyShapeError{
			StreamCode: s.Code,
			Reason: fmt.Sprintf(
				"copy://%s targets an ABR upstream (%d rungs) — must be the only input; fallback inputs are not supported in v1",
				classes[abrIdx].target, len(classes[abrIdx].upstream.Transcoder.Video.Profiles),
			),
		}
	}

	// Rule 2(b) (formerly "ABR-copy + own transcoder is ambiguous") was
	// removed: downstream MAY configure its own transcoder when copying from
	// an ABR upstream. In that case the runtime taps only the BEST rendition
	// of the upstream (single source) and feeds the downstream transcoder,
	// which builds its own ladder. Other upstream rungs are unused — mirroring
	// is reserved for the no-transcoder case.

	// Rule 3: mixed-shape inputs (copy from single + copy from ABR mix
	// already caught by rule 2(a)). The remaining concern is mixing
	// copy:// + network inputs where the resolvable copy target is ABR —
	// also caught by rule 2(a). Single-stream copy + network fallback is
	// allowed and needs no further check (publishers see a single TS source
	// regardless of which input is active).

	return nil
}

// streamHasRenditions returns true when the stream's transcoder produces an
// ABR ladder (= one or more rendition buffers, distinct from the main raw
// buffer). Mirrors the buffer.RenditionsForTranscoder logic without
// importing the buffer package.
func streamHasRenditions(s *Stream) bool {
	if s == nil || s.Transcoder == nil {
		return false
	}
	if s.Transcoder.Video.Copy {
		return false
	}
	return len(s.Transcoder.Video.Profiles) > 0
}

// StreamMainBufferIsTS reports whether the stream's main playback buffer
// contains raw MPEG-TS bytes (Packet.TS) rather than decoded AVPackets
// (Packet.AV). Three sources of TS in the main buffer:
//
//   - any transcoder is active → buffer holds the transcoder's stdout TS;
//   - source is a raw-TS protocol (UDP / HLS / SRT / File) → ingestor uses
//     TSPassthroughPacketReader and writes Packet.TS directly;
//   - rendition buffers always hold TS (transcoder per-profile output) — but
//     this helper reports on the MAIN buffer; callers that need to know about
//     rendition shape should use streamHasRenditions instead.
//
// Used by mixer / copy paths to decide whether to subscribe-and-read AV
// packets vs run a TS demuxer over the buffer's chunks. Misclassifying the
// shape silently drops audio (rare AV packets but a mode-mismatched consumer
// filters them all out).
func StreamMainBufferIsTS(s *Stream) bool {
	if s == nil {
		return false
	}
	if s.Transcoder != nil {
		return true
	}
	if len(s.Inputs) == 0 {
		return false
	}
	return inputSourceIsRawTS(s.Inputs[0].URL)
}

// inputSourceIsRawTS reports whether the URL indicates a raw MPEG-TS source
// (UDP / HLS / HTTP-TS / SRT / File) — the protocols whose ingest reader uses
// TSPassthroughPacketReader, so their main buffer carries TS chunks.
//
// Delegates to protocol.Detect (already imported above) instead of an inlined
// scheme/suffix scan: the old hand-rolled version missed HTTP-TS (`.ts`,
// `/mpegts`), `.m3u`, and uppercase variants, misclassifying those upstreams
// as direct-AV and blacking out copy:// / mixer:// of them (B-7).
func inputSourceIsRawTS(rawURL string) bool {
	switch protocol.Detect(rawURL) { //nolint:exhaustive // default: every other Kind is not a TS-passthrough source
	case protocol.KindUDP, protocol.KindHLS, protocol.KindHTTPTS, protocol.KindSRT, protocol.KindFile:
		return true
	default:
		return false
	}
}
