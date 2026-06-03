// Package ingestor handles raw stream ingestion.
//
// The ingestor accepts a plain URL from the user and automatically derives
// the protocol from the URL scheme — no protocol configuration is needed.
//
// Pull mode (server connects to remote source). All pull paths yield
// [domain.AVPacket] via [PacketReader] / [NewPacketReader]:
//   - UDP MPEG-TS  → TS demux → AVPacket
//   - HLS playlist → HTTP + M3U8 → TS demux → AVPacket
//   - Local file   → resolved through the VOD registry → TS demux → AVPacket
//   - SRT pull     → gosrt → TS demux → AVPacket
//   - RTMP pull    → native RTMP → AVPacket
//   - RTSP pull    → gortsplib → AVPacket
//
// Push mode (external encoder connects to our server):
//   - RTMP → RTMPServer (gomedia RTMP server handle, native FLV→MPEG-TS)
//   - SRT  → SRTServer  (gosrt native, MPEG-TS passthrough)
//
// Push mode is auto-detected: if the URL host is 0.0.0.0 / :: and the scheme
// is rtmp or srt, the ingestor starts/uses the shared push server instead of
// a pull worker.
//
// Local file inputs MUST take the form file://<vod_mount>/<relative/path>;
// bare paths and absolute file:/// URLs are rejected so the VOD mount layer
// is the single point of policy.
package ingestor

import (
	"context"
	"fmt"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor/pull"
	"github.com/datvietvac-techhub/open-streamer/pkg/protocol"
)

// VODResolver resolves a file:// URL referencing a registered VOD mount to an
// absolute filesystem path. It is the contract between the ingestor and the
// internal/vod registry — kept as an interface so tests can stub it.
type VODResolver interface {
	Resolve(rawURL string) (path string, loop bool, err error)
}

// PacketReader is a pull source that yields elementary-stream access units
// ([domain.AVPacket]).
type PacketReader interface {
	Open(ctx context.Context) error
	ReadPackets(ctx context.Context) ([]domain.AVPacket, error)
	Close() error
}

// NewPacketReader constructs the appropriate PacketReader for the given input URL.
// RTSP and RTMP pull emit native AVPackets; MPEG-TS transports are demuxed to AVPackets.
//
// For file:// URLs the VODResolver translates the URL to an absolute host path;
// a nil resolver or an unknown mount both produce an error. Any other scheme
// ignores the resolver.
//
// For copy:// URLs the buffer service + stream lookup are required:
//   - buf: provides the upstream subscriber
//   - lookup: resolves upstream stream config to find the buffer ID and
//     verify the upstream is single-stream (ABR upstreams use a different
//     coordinator path, never this factory)
//
// Returns an error when:
//   - The URL scheme is unrecognised
//   - The URL describes a push-listen address (handled by the push servers)
//   - The URL is a file:// reference that cannot be resolved against a VOD mount
//   - The URL is a copy:// reference and buf or lookup is nil, the upstream
//     is missing, or the upstream has an ABR ladder
func NewPacketReader(
	input domain.Input,
	cfg config.IngestorConfig,
	vods VODResolver,
	buf *buffer.Service,
	lookup pull.StreamLookup,
) (PacketReader, error) {
	if protocol.IsPushListen(input.URL) {
		return nil, fmt.Errorf(
			"ingestor: %q is a push-listen address — handled by the push server, not a pull reader",
			input.URL,
		)
	}

	switch protocol.Detect(input.URL) {
	case protocol.KindRTSP:
		return pull.NewRTSPReader(input), nil
	case protocol.KindRTMP:
		return pull.NewRTMPReader(input), nil
	case protocol.KindUDP:
		// Raw-TS passthrough: avoid the demux/remux round-trip that scrambles
		// PCR/PTS and re-assigns PIDs. See pull/ts_passthrough.go.
		// MPTS program filter and PID allowlist apply only to UDP — that is
		// where multi-program / non-standard PSI realistically appears (DVB
		// headend multicast feeds). HLS / SRT / File sources are SPTS by
		// convention. Filter chain: program rewrites PAT and learns ES PIDs
		// first, then pids further narrows the output. Both are optional.
		var udp pull.TSChunkReader = pull.NewUDPReader(input)
		udp = maybeWrapMPTS(udp, input)
		udp = maybeWrapPIDs(udp, input)
		return pull.NewTSPassthroughPacketReader(udp), nil
	case protocol.KindHLS:
		// HLS pull also delivers MPEG-TS; same passthrough rationale as UDP.
		// Note: realtime pacing was previously enabled to throttle the demux
		// emission rate. With raw-TS passthrough each segment is forwarded
		// as a chunk-shaped AVPacket; the segmenter/transcoder downstream
		// already paces by wall-clock, so explicit pacing is not needed.
		return pull.NewTSPassthroughPacketReader(pull.NewHLSReader(input, cfg)), nil
	case protocol.KindHTTPTS:
		// Raw MPEG-TS over chunked HTTP — the low-latency relay path.
		// Counterpart to publisher/serve_mpegts.go on the source side.
		// Same passthrough handling as UDP / HLS so PCR / PIDs survive intact.
		return pull.NewTSPassthroughPacketReader(pull.NewHTTPTSReader(input)), nil
	case protocol.KindFile:
		if vods == nil {
			return nil, fmt.Errorf("ingestor: cannot resolve %q — no VOD resolver configured", input.URL)
		}
		path, loop, err := vods.Resolve(input.URL)
		if err != nil {
			return nil, fmt.Errorf("ingestor: resolve file url: %w", err)
		}
		return pull.NewTSPassthroughPacketReader(pull.NewFileReader(path, loop)), nil
	case protocol.KindSRT:
		return pull.NewTSPassthroughPacketReader(pull.NewSRTReader(input)), nil
	case protocol.KindCopy:
		if buf == nil || lookup == nil {
			return nil, fmt.Errorf(
				"ingestor: copy:// requires buffer service and stream lookup (got %q with missing deps)",
				input.URL,
			)
		}
		return pull.NewCopyReader(input, buf, lookup)
	case protocol.KindMixer:
		if buf == nil || lookup == nil {
			return nil, fmt.Errorf(
				"ingestor: mixer:// requires buffer service and stream lookup (got %q with missing deps)",
				input.URL,
			)
		}
		return pull.NewMixerReader(input, buf, lookup)
	case protocol.KindPublish, protocol.KindUnknown:
		// KindPublish is handled before this switch; KindUnknown falls through to error.
		fallthrough
	default:
		return nil, fmt.Errorf(
			"ingestor: cannot infer protocol from URL %q — unsupported scheme",
			input.URL,
		)
	}
}

// maybeWrapMPTS wraps a raw-TS chunk source with an MPTS program filter when
// the input config selects a specific program (input.Program > 0). When
// Program is zero (default) the source is returned unchanged so single-
// program TS feeds pay no per-packet PID filter cost.
//
// Used for UDP / HLS / SRT / File — the four protocols whose ingest pipeline
// is raw-TS passthrough. RTSP / RTMP are single-program by protocol design
// and never reach this helper.
func maybeWrapMPTS(r pull.TSChunkReader, input domain.Input) pull.TSChunkReader {
	if input.Program <= 0 {
		return r
	}
	return pull.NewMPTSProgramFilter(r, input.Program)
}

// maybeWrapPIDs wraps a raw-TS chunk source with a PID allowlist filter when
// the input config specifies an explicit pid list (len(input.Pids) > 0).
// Empty list (default) returns the source unchanged. Designed to chain after
// maybeWrapMPTS so program-scoped output can be further narrowed.
func maybeWrapPIDs(r pull.TSChunkReader, input domain.Input) pull.TSChunkReader {
	if len(input.Pids) == 0 {
		return r
	}
	return pull.NewPIDFilter(r, input.Pids)
}
