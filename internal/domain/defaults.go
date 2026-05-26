package domain

// Default* constants are the runtime-applied values when the matching
// configuration field is left zero / empty by the user. They are the SINGLE
// SOURCE OF TRUTH for implicit defaults — every consumer (handler, packager,
// transcoder, validator) must reference these instead of inlining its own
// literal so behaviour stays consistent across services.
//
// When changing a default here, audit references with:
//
//	grep -RIn 'Default[A-Z][A-Za-z]*' internal/ config/
//
// to make sure no stale literal still exists in a service module.
const (
	// DefaultBufferCapacity is the per-subscriber channel buffer size in
	// MPEG-TS packets. 1024 packets ≈ 1 MiB ≈ ~1.5s of 1080p60 video at
	// 5Mbps — enough headroom for HLS pull bursts (one segment per Read)
	// without growing memory unbounded across many subscribers.
	DefaultBufferCapacity = 1024

	// DefaultInputPacketTimeoutSec is the manager's failover threshold: the
	// maximum gap (in seconds) without a successful read on the active
	// input before it is marked failed and the next-priority input takes
	// over.
	DefaultInputPacketTimeoutSec = 30

	// DefaultLiveSegmentSec is the live HLS / DASH segment target length
	// in seconds when the operator leaves the field unset.
	DefaultLiveSegmentSec = 2
	// DefaultLiveWindow is the segment count in the sliding playlist
	// window (HLS / DASH live).
	DefaultLiveWindow = 12
	// DefaultLiveHistory is extra segments retained on disk after they
	// leave the live window — 0 = drop immediately after sliding out.
	DefaultLiveHistory = 0

	// DefaultDVRSegmentDuration is the DVR segment length in seconds when
	// StreamDVRConfig.SegmentDuration is zero.
	DefaultDVRSegmentDuration = 4
	// DefaultDVRRoot is the on-disk root directory used as the parent of
	// "{DefaultDVRRoot}/{streamCode}" when StreamDVRConfig.StoragePath is
	// empty.
	DefaultDVRRoot = "./out/dvr"

	// DefaultPushTimeoutSec is the publish handshake budget for an outbound
	// push destination when PushDestination.TimeoutSec is zero.
	DefaultPushTimeoutSec = 10
	// DefaultPushRetryTimeoutSec is the delay between retry attempts.
	DefaultPushRetryTimeoutSec = 5

	// DefaultHookMaxRetries is the per-hook retry budget when the user
	// leaves Hook.MaxRetries=0. For HTTP hooks this caps retries WITHIN a
	// single batch flush; events still re-queue across flushes regardless.
	DefaultHookMaxRetries = 3
	// DefaultHookTimeoutSec is the per-attempt delivery timeout when
	// Hook.TimeoutSec=0.
	DefaultHookTimeoutSec = 10
	// DefaultHookBatchMaxItems is the cap on events per HTTP POST body
	// when neither Hook.BatchMaxItems nor HooksConfig.BatchMaxItems is set.
	// Picked to keep payloads under typical 1 MiB receiver caps (each event
	// ~1-3 KiB after JSON encoding). File hooks ignore this — they always
	// write one line per event.
	DefaultHookBatchMaxItems = 100
	// DefaultHookBatchFlushIntervalSec is the maximum time a batch waits
	// before flushing even when below BatchMaxItems. Trade-off: lower =
	// fresher delivery, more requests; higher = bigger batches, more lag
	// for low-volume hooks.
	DefaultHookBatchFlushIntervalSec = 5
	// DefaultHookBatchMaxQueueItems caps the per-hook in-memory queue.
	// Events that fail delivery are re-queued for the next flush; if the
	// downstream stays unreachable, the queue eventually overflows and the
	// OLDEST events are dropped (with a warn log) to bound memory.
	DefaultHookBatchMaxQueueItems = 10000

	// DefaultVideoBitrateK is the fallback target video bitrate (kbps)
	// when a profile leaves Bitrate=0.
	DefaultVideoBitrateK = 2500

	// DefaultAudioBitrateK is the audio bitrate (kbps) when
	// AudioTranscodeConfig.Bitrate is zero.
	DefaultAudioBitrateK = 128

	// DefaultHLSPlaylistTimeoutSec is the upstream HLS pull playlist GET
	// timeout (seconds) when InputNetConfig.TimeoutSec is zero.
	DefaultHLSPlaylistTimeoutSec = 15
	// DefaultHLSSegmentTimeoutSec is the upstream HLS segment GET timeout
	// floor — segments can be many MB so this is held above the playlist
	// timeout.
	DefaultHLSSegmentTimeoutSec = 60
	// DefaultHLSMaxSegmentBuffer caps pre-fetched HLS segments held in
	// memory per stream when IngestorConfig.HLSMaxSegmentBuffer is zero.
	DefaultHLSMaxSegmentBuffer = 8

	// DefaultPTSJumpThresholdMs is the Normaliser's jump cap (ms). When
	// the gap between an output PTS and local wallclock exceeds this,
	// the Normaliser re-anchors. The re-anchor event is recorded in
	// LastDiagnostic for telemetry but is NOT propagated to consumers
	// (session boundaries reach consumers via buffer.Packet.SessionStart
	// instead). 2 s tolerates frame-cadence jitter and minor encoder
	// drift while catching the multi-second forward jumps (CDN playlist
	// resync, transcoder restart) that otherwise bake permanent offsets
	// into segment timelines. Server-level invariant — not operator-
	// tunable.
	DefaultPTSJumpThresholdMs int64 = 2000

	// DefaultPTSMaxAheadMs is intentionally 0 (disabled). When non-zero
	// the AV-path PTS rebaser drops incoming packets whose proposed
	// output PTS sits more than this far ahead of (now − wallOrigin).
	// The drop has a stuck-state pathology: once drift exceeds the cap
	// it never decreases for real-time input (input rate == wallclock
	// rate keeps drift constant), so every subsequent packet drops
	// indefinitely — observed empirically on consumers fed from a
	// drifted upstream main buffer (mixer://, republish RTSP/RTMP)
	// where it zeroed out the video track. The DASH packager keeps a
	// per-track drift cap that re-anchors at IDR boundary instead of
	// dropping; that's the durable defense against MPDs racing ahead
	// of publishTime. This knob stays in the codebase for tests and
	// for operators willing to accept the trade-off explicitly.
	DefaultPTSMaxAheadMs int64 = 0

	// DefaultPTSMaxBehindMs caps how far behind (now − wallOrigin) the
	// proposed output PTS may sit before the rebaser hard-re-anchors the
	// track to wallclock. Symmetric counterpart of MaxAheadMs but safe
	// to enable by default — re-anchoring on backward drift jumps the
	// track FORWARD onto wallclock, closing the gap rather than racing
	// further behind (the stuck-state pathology only applies to the
	// ahead-side drop). Catches the long-runtime A/V split observed in
	// the field on RTSP relays and HLS-pull passthroughs where one
	// track seeds late or pauses while the other keeps flowing — left
	// unbounded, the lagging track stays permanently offset (test3:
	// audio anchored 229 s behind video and never recovered). 3 s
	// leaves room for CrossTrackSnapMs (1 s), GOP-cadence jitter, and
	// minor RTP jitter without firing on normal live latency.
	DefaultPTSMaxBehindMs int64 = 3000

	// DefaultSRTLatencyMS is the SRT ARQ latency window (milliseconds) when
	// SRTListenerConfig.LatencyMS is zero. 120ms matches Haivision's
	// reference for low-latency contribution links.
	DefaultSRTLatencyMS = 120

	// DefaultListenHost is the bind address used for RTMP / RTSP / SRT
	// listeners when ListenHost is empty. "0.0.0.0" listens on all
	// interfaces — the standard server behaviour.
	DefaultListenHost = "0.0.0.0"

	// DefaultRTMPTimeoutSec is the dial timeout (seconds) for RTMP pull
	// when InputNetConfig.TimeoutSec is zero.
	DefaultRTMPTimeoutSec = 10
	// DefaultRTSPTimeoutSec is the dial / read timeout (seconds) for RTSP
	// pull when InputNetConfig.TimeoutSec is zero.
	DefaultRTSPTimeoutSec = 10
)
