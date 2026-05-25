# Open Streamer — Metrics Reference

All Prometheus collectors registered by Open Streamer, what they label,
when they update, and the recommended alert/dashboard expressions.

Scraped at `GET /metrics`. Naming convention: `open_streamer_<module>_<metric>_<unit>`.

---

## 1. Reading this doc

Each metric block lists:

- **Metric name** — Prometheus name on `/metrics`.
- **Type** — counter / gauge / histogram (Prometheus semantics).
- **Labels** — values that split a single metric into series.
- **Updated by** — the code path that writes to it.
- **Why it matters** — the operational signal it carries.
- **Alert / panel** — PromQL expressions that turn it into something
  actionable.

For the high-level metrics philosophy ("write never blocks", drop counters
as the early-warning rule) see [ARCHITECTURE.md § Buffer Hub](./ARCHITECTURE.md#buffer-hub-internalbuffer).

---

## 2. Ingestor

### `open_streamer_ingestor_bytes_total`

- **Type**: counter
- **Labels**: `stream_code`, `protocol`
- **Updated by**: each pull/push reader on every successful `Read`
- **Why**: top-level "is data flowing" sanity check + per-protocol traffic
- **Alert**: `rate(open_streamer_ingestor_bytes_total[2m]) == 0` for an active stream → source has gone silent

### `open_streamer_ingestor_packets_total`

- **Type**: counter
- **Labels**: `stream_code`, `protocol`
- **Updated by**: ingestor on each MPEG-TS packet written to the buffer
- **Why**: bitrate sanity at packet level (multiplied by ~188 B/pkt → bytes/s)

### `open_streamer_ingestor_errors_total`

- **Type**: counter
- **Labels**: `stream_code`, `reason`
- **Updated by**: ingestor on each transient read / reconnect error
- **`reason`** values: `reconnect`, `failover`, source-protocol-specific values

---

## 3. Stream Manager (failover)

### `open_streamer_manager_failovers_total`

- **Type**: counter
- **Labels**: `stream_code`
- **Updated by**: manager on every successful input switch
- **Why**: rate sanity. Sustained `> 0/min` → flapping source set
- **Alert**: `rate(open_streamer_manager_failovers_total[10m]) > 0.1` (more than 1/min) → unstable inputs

### `open_streamer_manager_input_health`

- **Type**: gauge (0 / 1)
- **Labels**: `stream_code`, `input_priority`
- **Updated by**: manager on health state transitions
- **Why**: "is THIS specific source delivering"

### `open_streamer_manager_probe_attempts_total`

- **Type**: counter
- **Labels**: `stream_code`, `outcome`
- **Updated by**: manager `runProbe` on every failback probe attempt
- **`outcome`** values: `success`, `failure`
- **Why**: confirms the failback probe loop is alive AND classifies whether
  degraded sources are recovering. `absent_over_time(...[5m]) == 1` with
  degraded inputs present → probe loop has stalled
- **Alert**: `rate(open_streamer_manager_probe_attempts_total[5m]) == 0 AND open_streamer_manager_input_health == 0` → inputs degraded and we're not even trying to recover

---

## 4. Coordinator (lifecycle + reconciler)

### `open_streamer_streams_total`

- **Type**: gauge
- **Labels**: `status`
- **`status`** values: `idle`, `active`, `degraded`, `stopped` (and `exhausted` if exposed by the coordinator)
- **Updated by**: coordinator reconciler, every tick (default 10s) refreshes the snapshot
- **Why**: top-level dashboard tile. Replaces the per-stream sum reduction operators previously had to write client-side
- **Panel**: `sum by (status) (open_streamer_streams_total)` → stacked bar of stream health

### `open_streamer_stream_start_time_seconds`

- **Type**: gauge (Unix timestamp)
- **Labels**: `stream_code`
- **Updated by**: coordinator on Start (set) / Stop (delete)
- **Uptime**: `time() - open_streamer_stream_start_time_seconds`

### `open_streamer_reconciler_last_run_seconds`

- **Type**: gauge (Unix timestamp)
- **Labels**: _none_
- **Updated by**: coordinator reconciler at the end of every successful tick
- **Why**: liveness check for the safety-net loop that picks up streams
  the create handler missed (rare bootstrap race)
- **Alert**: `time() - open_streamer_reconciler_last_run_seconds > 60` → reconciler goroutine is stuck or dead

---

## 5. Buffer Hub

### `open_streamer_buffer_drops_total`

- **Type**: counter
- **Labels**: `stream_code`
- **Updated by**: ring-buffer fan-out on every dropped packet (slow consumer)
- **Why**: the **canonical** "write never blocks" signal. Any non-zero rate
  means ONE consumer is back-pressuring AND the writer protected itself
  by dropping rather than blocking
- **Alert**: `rate(open_streamer_buffer_drops_total[2m]) > 0` → a consumer has fallen behind

### `open_streamer_buffer_capacity_used`

- **Type**: gauge (0.0 — 1.0)
- **Labels**: `stream_code`
- **Updated by**: buffer service `sampleLoop` (every 2s); reports the MAX
  channel depth across subscribers, normalised against capacity
- **Why**: leading indicator. A subscriber sitting at 0.8 saturation gives
  you minutes of warning before it tips over to 1.0 + drops; the drops
  counter itself is the lagging signal
- **Alert**: `open_streamer_buffer_capacity_used > 0.7 for 30s` → consumer near saturation

### `open_streamer_buffer_subscribers`

- **Type**: gauge (count)
- **Labels**: `stream_code`
- **Updated by**: buffer sampler (every 2s)
- **Why**: confirms fan-out density. A "stream registered but no consumers"
  config bug shows up as a stream with 0 subscribers but non-zero ingestor packets
- **Alert**: `open_streamer_buffer_subscribers == 0 AND rate(open_streamer_ingestor_packets_total[1m]) > 0` → orphan stream

---

## 6. Transcoder

### `open_streamer_transcoder_workers_active`

- **Type**: gauge
- **Labels**: `stream_code`
- **Updated by**: transcoder on subprocess spawn / exit
- **Use**: dashboard "transcoder subprocesses per stream" tile

### `open_streamer_transcoder_qualities_active`

- **Type**: gauge
- **Labels**: `stream_code`
- **Updated by**: transcoder on profile add/remove + UpdateProfiles
- **Use**: ABR rendition count visibility

### `open_streamer_transcoder_restarts_total`

- **Type**: counter
- **Labels**: `stream_code`
- **Updated by**: the supervisor's subprocess respawn loop on every retry
- **Alert**: `rate(open_streamer_transcoder_restarts_total[10m]) > 0.1` → profile crashing repeatedly

---

## 7. Publisher

### `open_streamer_publisher_segments_total`

- **Type**: counter
- **Labels**: `stream_code`, `format` (hls|dash), `profile`
- **Updated by**: HLS / DASH segmenters on each successful disk write
- **Why**: positive control for "segments are flowing". Pair with `live_segment_sec` to verify the rate matches expected cadence
- **Alert**: `rate(open_streamer_publisher_segments_total[1m]) == 0` for an active stream → segmenter stalled

### `open_streamer_publisher_segment_write_duration_seconds`

- **Type**: histogram
- **Labels**: `stream_code`, `format`
- **Buckets**: `[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2]` (seconds)
- **Updated by**: HLS / DASH segmenters wrap `os.WriteFile` / `writeFileAtomic` in `time.Since`
- **Why**: disk-I/O backpressure surfaces here BEFORE it shows up as buffer drops
- **Alert**: `histogram_quantile(0.99, rate(open_streamer_publisher_segment_write_duration_seconds_bucket[5m])) > 0.5` → slow disk path

### `open_streamer_publisher_push_state`

- **Type**: gauge (0 / 1 / 2 / 3 enum)
- **Labels**: `stream_code`, `dest_url`
- **Values**: `0=failed`, `1=reconnecting`, `2=starting`, `3=active`
- **Updated by**: publisher `setPushStatus` on every push state transition
- **Alert**: `open_streamer_publisher_push_state < 3 for 1m` → push delivery degraded

### `open_streamer_publisher_push_bytes_total`

- **Type**: counter
- **Labels**: `stream_code`, `dest_url`
- **Updated by**: RTMP push writer per `WriteMsg` payload (chunk header bytes excluded)
- **Why**: egress-cost attribution per CDN destination. Pair with rate of
  ingest bytes to verify push isn't lossy
- **Panel**: `sum by (dest_url) (rate(open_streamer_publisher_push_bytes_total[5m])) * 8` → bits/s sent per destination

---

## 8. DVR

### `open_streamer_dvr_segments_written_total`

- **Type**: counter
- **Labels**: `stream_code`
- **Updated by**: DVR on every successful segment flush
- **Alert**: `rate(open_streamer_dvr_segments_written_total[1m]) == 0 AND open_streamer_dvr_recording_active == 1` → DVR enabled but stuck

### `open_streamer_dvr_bytes_written_total`

- **Type**: counter
- **Labels**: `stream_code`
- **Updated by**: DVR on each segment write
- **Why**: storage growth rate; pair with retention metric below for net change

### `open_streamer_dvr_retention_pruned_bytes_total`

- **Type**: counter
- **Labels**: `stream_code`
- **Updated by**: DVR `applyRetention` per pruned segment
- **Why**: confirms the retention loop is actually freeing bytes. The
  derived metric `bytes_written - bytes_pruned` is the steady-state
  storage consumption rate
- **Panel**: `rate(open_streamer_dvr_bytes_written_total[5m]) - rate(open_streamer_dvr_retention_pruned_bytes_total[5m])` → net storage growth per stream

### `open_streamer_dvr_recording_active`

- **Type**: gauge (0 / 1)
- **Labels**: `stream_code`

---

## 9. Sessions

### `open_streamer_sessions_active`

- **Type**: gauge
- **Labels**: `stream_code`, `proto` (hls|dash|rtmp|srt|rtsp|mpegts)
- **Updated by**: sessions tracker on open/close + reaper sweep

### `open_streamer_sessions_opened_total`

- **Type**: counter
- **Labels**: `stream_code`, `proto`

### `open_streamer_sessions_closed_total`

- **Type**: counter
- **Labels**: `stream_code`, `proto`, `reason`
- **`reason`** values: `idle`, `client_gone`, `kicked`, `shutdown`
- **Use**: average session lifetime via `rate(closed) / rate(opened)` × bucket window

---

## 10. Hooks

### `open_streamer_hooks_delivery_total`

- **Type**: counter
- **Labels**: `hook_id`, `hook_type` (http|file), `outcome` (success|failure), `reason`
- **`reason`** values:
  - `ok` — pair with `outcome=success`
  - `timeout` — context deadline / TCP timeout
  - `http_4xx` — target rejected (auth / payload schema bug; not transient)
  - `http_5xx` — target overloaded / down (transient, retried)
  - `transport` — connection refused / DNS / TLS handshake failure
  - `encode` — body marshal / signing error (caller bug)
  - `unknown` — classifier fell through; escalate as "missing case in classifier"
- **Updated by**: hooks batcher per HTTP delivery + dispatch path per file write
- **Failure rate (any reason)**: `rate(open_streamer_hooks_delivery_total{reason!="ok"}[5m])`
- **Specific class alert**: `rate(open_streamer_hooks_delivery_total{reason="http_5xx"}[5m]) > 0.5/m` → target service is unhealthy

### `open_streamer_hooks_batch_size`

- **Type**: histogram
- **Labels**: `hook_id`
- **Buckets**: powers of 2 from 1 to 1024
- **Updated by**: HTTP batcher per flush attempt (success or failure)
- **Why**: tunes `BatchMaxItems`. P50 ≪ `BatchMaxItems` means flushes are interval-driven (small batches, more HTTP overhead). P95 == `BatchMaxItems` means size-driven (the limit is the bottleneck → consider raising)

### `open_streamer_hooks_events_dropped_total`

- **Type**: counter
- **Labels**: `hook_id`
- **Updated by**: HTTP batcher when the bounded queue overflows
- **Alert**: `rate(open_streamer_hooks_events_dropped_total[5m]) > 0` → DATA LOSS, target unreachable too long

### `open_streamer_hooks_batch_queue_depth`

- **Type**: gauge
- **Labels**: `hook_id`
- **Updated by**: batcher on every enqueue / takeBatch
- **Why**: leading indicator before drops. A queue rising toward `BatchMaxQueueItems` over minutes warns that the target is back-pressuring

---

## 11. HTTP API

### `open_streamer_http_request_duration_seconds`

- **Type**: histogram
- **Labels**: `route` (chi pattern, e.g. `/streams/{code}`), `method`, `status`
- **Buckets**: `[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30]` (seconds)
- **Excluded**: `/metrics`, `/healthz`, `/readyz` (self-scrape noise + LB probes)
- **Updated by**: API middleware ([server.go § httpDurationMiddleware](../internal/api/server.go))
- **Why**: standard SLO histogram. Use route TEMPLATES not full paths so the cardinality stays bounded by handler count
- **SLO panel**: `histogram_quantile(0.95, sum by (route, le) (rate(open_streamer_http_request_duration_seconds_bucket[5m])))` → P95 per route
- **Error rate**: `sum by (route) (rate(open_streamer_http_request_duration_seconds_count{status=~"5.."}[5m])) / sum by (route) (rate(open_streamer_http_request_duration_seconds_count[5m]))`

---

## 12. Common dashboards

### Top-line health

```promql
# Streams by status — stacked
sum by (status) (open_streamer_streams_total)

# Active stream count
sum(open_streamer_streams_total{status="active"})

# Reconciler liveness
time() - open_streamer_reconciler_last_run_seconds
```

### Per-stream health

```promql
# Ingest bitrate (Mbps)
rate(open_streamer_ingestor_bytes_total{stream_code="$stream"}[1m]) * 8 / 1e6

# Buffer pressure (closer to 1.0 = bad)
open_streamer_buffer_capacity_used{stream_code="$stream"}

# Drops (should be flat-zero)
rate(open_streamer_buffer_drops_total{stream_code="$stream"}[2m])

# Active viewers
sum by (proto) (open_streamer_sessions_active{stream_code="$stream"})
```

### Push delivery (CDN egress)

```promql
# Bytes/s per destination
sum by (dest_url) (rate(open_streamer_publisher_push_bytes_total[5m]))

# Failure rate
1 - avg by (dest_url) (open_streamer_publisher_push_state == bool 3)
```

### DVR storage trend

```promql
# Net growth per stream
rate(open_streamer_dvr_bytes_written_total[5m])
  - rate(open_streamer_dvr_retention_pruned_bytes_total[5m])
```

### Hook health

```promql
# Delivery error rate by class
sum by (hook_id, reason) (rate(open_streamer_hooks_delivery_total{reason!="ok"}[5m]))

# Queue saturation (warning before drops)
open_streamer_hooks_batch_queue_depth
  / on (hook_id) group_left() <BatchMaxQueueItems-from-config>
```

---

## 13. Reference files

| Concern | File |
|---|---|
| Metric collectors registration | [internal/metrics/metrics.go](../internal/metrics/metrics.go) |
| Buffer-hub sampler | [internal/buffer/service.go § sampleLoop](../internal/buffer/service.go) |
| API request-duration middleware | [internal/api/server.go § httpDurationMiddleware](../internal/api/server.go) |
| Coordinator reconciler refresh | [internal/coordinator/coordinator.go § refreshStatusGauge](../internal/coordinator/coordinator.go) |
| Hook delivery error classifier | [internal/hooks/batcher.go § classifyHookDeliveryErr](../internal/hooks/batcher.go) |
