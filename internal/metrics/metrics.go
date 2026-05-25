// Package metrics registers all Prometheus collectors for Open Streamer.
// Naming convention: open_streamer_<module>_<metric>_<unit>
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/samber/do/v2"
)

// Metrics holds all registered Prometheus collectors.
type Metrics struct {
	// ── Ingestor ─────────────────────────────────────────────────────────────
	// IngestorBytesTotal counts raw bytes received from each input source.
	IngestorBytesTotal *prometheus.CounterVec
	// IngestorPacketsTotal counts MPEG-TS packets written to the buffer.
	IngestorPacketsTotal *prometheus.CounterVec
	// IngestorErrorsTotal counts transient read/reconnect errors per stream.
	IngestorErrorsTotal *prometheus.CounterVec

	// ── Stream Manager ────────────────────────────────────────────────────────
	// ManagerFailoversTotal counts input source switches (primary → backup).
	ManagerFailoversTotal *prometheus.CounterVec
	// ManagerInputHealth is 1 when an input is delivering packets, 0 when degraded.
	ManagerInputHealth *prometheus.GaugeVec

	// ── Transcoder ────────────────────────────────────────────────────────────
	// TranscoderWorkersActive is the number of active transcoder subprocesses per stream.
	TranscoderWorkersActive *prometheus.GaugeVec
	// TranscoderRestartsTotal counts transcoder subprocess crash-restarts per stream.
	TranscoderRestartsTotal *prometheus.CounterVec
	// TranscoderQualitiesActive is the number of active ABR renditions per stream.
	TranscoderQualitiesActive *prometheus.GaugeVec
	// TranscoderFramesTotal counts encoded frames emitted by the
	// transcoder, observed downstream at the publisher's per-frame
	// ingress (DASH packager handleH264 / handleAAC). Use
	// `rate(transcoder_frames_total[1m])` to chart per-stream FPS;
	// dips below the source frame rate signal the transcoder is
	// dropping or queueing frames. Mirrors the operator-tooling
	// "Output frames count" panel shape.
	//
	// Labels:
	//   - stream_code: the parent stream
	//   - profile: rendition slug ("track_1", "track_2") for ABR
	//     shards; "v0" for single-rendition streams. Matches the
	//     Representation id emitted in the MPD.
	//   - kind: "video" | "audio"
	TranscoderFramesTotal *prometheus.CounterVec

	// ── DVR ───────────────────────────────────────────────────────────────────
	// DVRSegmentsWrittenTotal counts TS segments flushed to disk per stream.
	DVRSegmentsWrittenTotal *prometheus.CounterVec
	// DVRBytesWrittenTotal counts bytes written to disk per stream.
	DVRBytesWrittenTotal *prometheus.CounterVec

	// ── Stream lifecycle ──────────────────────────────────────────────────────
	// StreamStartTimeSeconds is the Unix timestamp when a stream pipeline was
	// last started. 0 / absent means the stream is not currently running.
	// Uptime in Grafana: time() - open_streamer_stream_start_time_seconds
	StreamStartTimeSeconds *prometheus.GaugeVec

	// ── Buffer Hub ────────────────────────────────────────────────────────────
	// BufferDropsTotal counts MPEG-TS packets dropped at the Buffer Hub
	// fan-out because a subscriber's channel was full. This is the canonical
	// signal that the "write never blocks" invariant is shielding the writer
	// from a slow consumer — alert on `rate(...) > 0` to catch quality
	// regressions before users complain.
	BufferDropsTotal *prometheus.CounterVec

	// ── Publisher ─────────────────────────────────────────────────────────────
	// PublisherSegmentsTotal counts HLS / DASH media segments successfully
	// packaged. format=hls|dash; profile is the rendition slug (track_1…) or
	// "main" for single-rendition streams. Rate sanity-checks output bitrate.
	PublisherSegmentsTotal *prometheus.CounterVec
	// PublisherPushState reports the state of each RTMP/RTMPS push destination
	// as a small enum cast to int: 0=failed, 1=reconnecting, 2=starting, 3=active.
	// Per-destination labels let alerts target individual sinks.
	PublisherPushState *prometheus.GaugeVec

	// ── Sessions ──────────────────────────────────────────────────────────────
	// SessionsActive is the number of currently-tracked play sessions per
	// stream and protocol. Reset to 0 on restart (in-memory tracker), but
	// converges back as viewers reconnect — Prometheus gauge semantics
	// match this exactly.
	SessionsActive *prometheus.GaugeVec
	// SessionsOpenedTotal counts every session creation per stream and proto.
	// Pair with rate() / increase() over a window for "viewer churn".
	SessionsOpenedTotal *prometheus.CounterVec
	// SessionsClosedTotal counts session terminations per stream, proto and
	// reason (idle, client_gone, kicked, shutdown). Pair with opened_total
	// to compute average session lifetime; spike of `kicked` could indicate
	// abuse mitigation kicking in.
	SessionsClosedTotal *prometheus.CounterVec

	// ── Hooks ─────────────────────────────────────────────────────────────────
	// HooksDeliveryTotal counts hook deliveries per hook, type and outcome
	// (success / failure). For HTTP hooks each batch counts as ONE delivery
	// regardless of batch size — divide events_dropped_total by this to
	// derive lost-event rate.
	HooksDeliveryTotal *prometheus.CounterVec
	// HooksEventsDroppedTotal counts events evicted from the per-hook batch
	// queue when it overflowed `BatchMaxQueueItems`. Non-zero rate means a
	// downstream HTTP target is stalled long enough to threaten data loss —
	// alert immediately.
	HooksEventsDroppedTotal *prometheus.CounterVec
	// HooksBatchQueueDepth is the current per-hook in-memory queue depth.
	// Together with HooksEventsDroppedTotal this gives the early-warning
	// signal: queue rising → target back-pressuring; queue at cap → drops imminent.
	HooksBatchQueueDepth *prometheus.GaugeVec

	// ── DVR ───────────────────────────────────────────────────────────────────
	// DVRRecordingActive is 1 while a DVR recording is being written for a
	// stream, 0 otherwise. Combine with rate(DVRSegmentsWrittenTotal) to
	// alert on "DVR enabled but no segments flowing".
	DVRRecordingActive *prometheus.GaugeVec
	// DVRRetentionPrunedBytes counts bytes deleted by the retention loop
	// per stream. Useful for capacity planning (how much churn the
	// retention policy is creating) and as the canonical signal that
	// retention is actually running. Pair with the rate of incoming
	// DVRBytesWrittenTotal to verify steady-state behaviour.
	DVRRetentionPrunedBytes *prometheus.CounterVec

	// ── Buffer Hub (capacity + fan-out visibility) ────────────────────────────
	// BufferCapacityUsed reports the MAX subscriber-channel depth on a
	// stream's ring buffer, as a fraction of capacity (0.0 — 1.0). We
	// emit max-across-subscribers (not per-consumer) because the buffer
	// API doesn't know which consumer attached — operators get the
	// leading-indicator signal (something is back-pressuring) without
	// the label-cardinality blowup of one series per anonymous consumer.
	// Refreshed by a sampler goroutine every BufferSampleInterval.
	BufferCapacityUsed *prometheus.GaugeVec
	// BufferSubscribers reports the number of active subscribers (consumer
	// channels) on a stream's buffer. Visibility into fan-out density —
	// confirms that publisher / transcoder / DVR have all attached, and
	// catches "stream registered but no consumers" misconfigurations.
	BufferSubscribers *prometheus.GaugeVec

	// ── HTTP API (request observability) ──────────────────────────────────────
	// HTTPRequestDuration is the standard request-latency histogram per
	// route + method + status. Use route TEMPLATES (e.g. "/streams/{code}")
	// not full paths to keep label cardinality bounded. Buckets cover the
	// fast hot-path APIs (List, Get) and slow long-poll / sweep endpoints.
	HTTPRequestDuration *prometheus.HistogramVec

	// ── Publisher (segment write timing + push bytes) ─────────────────────────
	// PublisherSegmentWriteDuration is the wall-clock time to write one
	// HLS / DASH segment to disk. Sustained tail-latency growth signals
	// disk I/O backpressure before BufferDropsTotal reflects it. Buckets
	// cover the fast (single-rendition local SSD) and slow (network FS)
	// regimes.
	PublisherSegmentWriteDuration *prometheus.HistogramVec
	// PublisherPushBytes counts bytes shipped per push destination. Useful
	// for egress-cost dashboards (CDN bandwidth attribution per dest_url)
	// and as the canonical "is push actually delivering data" signal.
	PublisherPushBytes *prometheus.CounterVec

	// ── Stream rollup (top-level operator dashboard) ──────────────────────────
	// StreamsTotal reports the count of streams in each top-level status
	// bucket. Reconciler refreshes this once per tick so a Grafana panel
	// can show the system-wide health summary without scraping per-stream
	// gauges and reducing client-side.
	StreamsTotal *prometheus.GaugeVec

	// ── Hooks (delivery error categorisation + batch sizing) ──────────────────
	// HooksBatchSize is the per-delivery batch size histogram. Tunes
	// `BatchMaxItems` — small batches = wasted HTTP overhead, large
	// batches = stale data. Pair with HooksDeliveryTotal to see whether
	// flushes are size-driven or interval-driven.
	HooksBatchSize *prometheus.HistogramVec

	// ── Manager (probe loop visibility) ───────────────────────────────────────
	// ManagerProbeAttemptsTotal counts failback probe runs against
	// degraded inputs, labelled by outcome (`success`/`failure`). Alert
	// on absence of probes (`absent_over_time(...[5m]) > 0`) — silent
	// probe loop means the manager never re-checks degraded inputs.
	ManagerProbeAttemptsTotal *prometheus.CounterVec

	// ── Reconciler (safety-net liveness) ──────────────────────────────────────
	// ReconcilerLastRunSeconds is the Unix timestamp of the last
	// successful reconciler tick. Should advance every 10s; an unchanged
	// value across multiple scrapes means the reconciler goroutine has
	// died — the safety net is gone.
	ReconcilerLastRunSeconds prometheus.Gauge
}

// New registers all metrics and returns a Metrics instance.
func New(i do.Injector) (*Metrics, error) {
	_ = i
	m := &Metrics{
		IngestorBytesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_ingestor_bytes_total",
			Help: "Total bytes ingested from pull/push sources per stream and protocol.",
		}, []string{"stream_code", "protocol"}),

		IngestorPacketsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_ingestor_packets_total",
			Help: "Total MPEG-TS packets written to the buffer per stream and protocol.",
		}, []string{"stream_code", "protocol"}),

		IngestorErrorsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_ingestor_errors_total",
			Help: "Total ingestion errors per stream, categorised by reason (reconnect, failover).",
		}, []string{"stream_code", "reason"}),

		ManagerFailoversTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_manager_failovers_total",
			Help: "Total input source switches (primary degraded → backup activated) per stream.",
		}, []string{"stream_code"}),

		ManagerInputHealth: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "open_streamer_manager_input_health",
			Help: "Input health per stream and priority: 1 = delivering packets, 0 = degraded.",
		}, []string{"stream_code", "input_priority"}),

		TranscoderWorkersActive: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "open_streamer_transcoder_workers_active",
			Help: "Number of FFmpeg encoder processes currently running per stream.",
		}, []string{"stream_code"}),

		TranscoderRestartsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_transcoder_restarts_total",
			Help: "Total FFmpeg process crash-restarts per stream.",
		}, []string{"stream_code"}),

		TranscoderFramesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_transcoder_frames_total",
			Help: "Total encoded frames observed at the publisher's per-frame ingress. Diff vs source FPS = transcoder dropping or queueing.",
		}, []string{"stream_code", "profile", "kind"}),
		TranscoderQualitiesActive: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "open_streamer_transcoder_qualities_active",
			Help: "Number of active ABR rendition profiles per stream.",
		}, []string{"stream_code"}),

		DVRSegmentsWrittenTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_dvr_segments_written_total",
			Help: "Total TS segments successfully written to disk per stream.",
		}, []string{"stream_code"}),

		DVRBytesWrittenTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_dvr_bytes_written_total",
			Help: "Total bytes written to disk by the DVR per stream.",
		}, []string{"stream_code"}),

		StreamStartTimeSeconds: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "open_streamer_stream_start_time_seconds",
			Help: "Unix timestamp when the stream pipeline was last started. Absent when stopped. Uptime = time() - this value.",
		}, []string{"stream_code"}),

		BufferDropsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_buffer_drops_total",
			Help: "Total MPEG-TS packets dropped by the Buffer Hub fan-out because a subscriber channel was full.",
		}, []string{"stream_code"}),

		PublisherSegmentsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_publisher_segments_total",
			Help: "Total HLS/DASH segments packaged per stream, format and rendition profile.",
		}, []string{"stream_code", "format", "profile"}),

		PublisherPushState: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "open_streamer_publisher_push_state",
			Help: "RTMP/RTMPS push destination state: 0=failed, 1=reconnecting, 2=starting, 3=active.",
		}, []string{"stream_code", "dest_url"}),

		SessionsActive: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "open_streamer_sessions_active",
			Help: "Currently-tracked play sessions per stream and protocol.",
		}, []string{"stream_code", "proto"}),

		SessionsOpenedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_sessions_opened_total",
			Help: "Total play sessions opened per stream and protocol.",
		}, []string{"stream_code", "proto"}),

		SessionsClosedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_sessions_closed_total",
			Help: "Total play sessions closed per stream, protocol and reason (idle|client_gone|kicked|shutdown).",
		}, []string{"stream_code", "proto", "reason"}),

		HooksDeliveryTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_hooks_delivery_total",
			// reason categorises failures so dashboards can split alert thresholds:
			//   "ok"        — success
			//   "timeout"   — context deadline / TCP timeout
			//   "http_4xx"  — target returned 4xx (likely auth / payload bug, not transient)
			//   "http_5xx"  — target returned 5xx (transient, expected to retry)
			//   "transport" — connection refused / DNS / TLS handshake
			//   "encode"    — body marshal / signing failure
			// Successful deliveries record reason="ok" so a single PromQL
			// expression `rate(...{reason!="ok"}[5m])` gives the failure rate
			// regardless of category.
			Help: "Total hook delivery attempts per hook, type, outcome (success|failure) and reason (ok|timeout|http_4xx|http_5xx|transport|encode). For HTTP hooks each batch is one delivery.",
		}, []string{"hook_id", "hook_type", "outcome", "reason"}),

		HooksEventsDroppedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_hooks_events_dropped_total",
			Help: "Total events evicted from the per-hook batch queue on overflow (target unreachable too long).",
		}, []string{"hook_id"}),

		HooksBatchQueueDepth: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "open_streamer_hooks_batch_queue_depth",
			Help: "Current per-hook in-memory batch queue depth.",
		}, []string{"hook_id"}),

		DVRRecordingActive: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "open_streamer_dvr_recording_active",
			Help: "1 while a DVR recording is being written for the stream, 0 otherwise.",
		}, []string{"stream_code"}),

		DVRRetentionPrunedBytes: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_dvr_retention_pruned_bytes_total",
			Help: "Total bytes deleted by the DVR retention loop per stream. reason label classifies the trigger (age vs size).",
		}, []string{"stream_code"}),

		BufferCapacityUsed: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "open_streamer_buffer_capacity_used",
			Help: "Max subscriber-channel depth on a stream's ring buffer as a fraction of capacity (0.0-1.0). Leading indicator before BufferDropsTotal increments.",
		}, []string{"stream_code"}),

		BufferSubscribers: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "open_streamer_buffer_subscribers",
			Help: "Currently active subscribers (consumer channels) on the stream's buffer.",
		}, []string{"stream_code"}),

		HTTPRequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name: "open_streamer_http_request_duration_seconds",
			Help: "HTTP request latency per route template, method and status code. Buckets cover fast hot-path APIs and slow sweep endpoints.",
			// 1ms..30s — covers JSON CRUD, swagger.json, slow YAML round-trips.
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"route", "method", "status"}),

		PublisherSegmentWriteDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name: "open_streamer_publisher_segment_write_duration_seconds",
			Help: "Wall-clock time to write one HLS/DASH segment to disk per stream and format. Sustained tail-latency rise signals disk I/O backpressure.",
			// 1ms..2s — local SSD writes typically <50ms; network FS / busy
			// disk can spike past 1s. Above 2s downstream players miss the
			// segment-target window anyway.
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2},
		}, []string{"stream_code", "format"}),

		PublisherPushBytes: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_publisher_push_bytes_total",
			Help: "Total bytes shipped per push destination (counts payload bytes only, excluding RTMP chunk headers).",
		}, []string{"stream_code", "dest_url"}),

		StreamsTotal: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "open_streamer_streams_total",
			Help: "Number of streams currently in each top-level status (idle|active|degraded|stopped|exhausted). Refreshed by the coordinator reconciler.",
		}, []string{"status"}),

		HooksBatchSize: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name: "open_streamer_hooks_batch_size",
			Help: "Number of events per HTTP hook delivery batch. Tunes BatchMaxItems — small batches = wasted overhead, large = stale data.",
			// Powers of 2 from 1 to 1024 — covers tiny single-event flushes
			// up to the typical max queue cap.
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
		}, []string{"hook_id"}),

		ManagerProbeAttemptsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "open_streamer_manager_probe_attempts_total",
			Help: "Failback probe runs against degraded inputs per stream and outcome (success|failure).",
		}, []string{"stream_code", "outcome"}),

		ReconcilerLastRunSeconds: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "open_streamer_reconciler_last_run_seconds",
			Help: "Unix timestamp of the most recent reconciler tick. Stale value across scrapes means the safety-net loop has stalled.",
		}),
	}

	return m, nil
}
