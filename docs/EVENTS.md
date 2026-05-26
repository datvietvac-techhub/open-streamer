# Open Streamer — Event Reference

Domain events emitted on the in-process event bus and delivered to
registered hooks. Every state change worth alerting on or auditing fires
an event; consumers subscribe via [hook configuration](./CONFIG.md#hooks)
or — for in-process integrations — via `events.Bus.Subscribe`.

For the data-flow / hooks delivery contract see
[ARCHITECTURE.md § Event Bus & Hooks](./ARCHITECTURE.md#event-bus--hooks-internalevents-internalhooks).

---

## 1. Envelope

Every event carries the same shape:

```go
type Event struct {
    ID         string         // UUID — idempotent delivery key
    Type       EventType      // see catalogue below
    StreamCode StreamCode     // empty for system-level events (config / hooks meta)
    OccurredAt time.Time      // server-side wall clock at publish
    Payload    map[string]any // event-specific fields, see "Payload" column
}
```

When delivered over HTTP the bus serialises the event to JSON inside an
array (HTTP hooks ship batches; file hooks ship one JSON object per
line). Hooks layer adds an optional `metadata` map merged into `Payload`
from the operator-configured per-hook fields — useful for routing keys.

---

## 2. Catalogue

### 2.1 Stream lifecycle

| Type | Emitter | Triggers when | Payload |
|---|---|---|---|
| `stream.created` | API handler ([stream.go](../internal/api/handler/stream.go)) | `POST /streams/{code}` succeeds with a fresh code | _empty_ |
| `stream.updated` | API handler ([stream.go](../internal/api/handler/stream.go)) | `POST /streams/{code}` succeeds on an existing code | `was_running: bool`, `now_enabled: bool`, `disabled: bool` |
| `stream.started` | Coordinator ([coordinator.go](../internal/coordinator/coordinator.go)) | Pipeline goes from stopped → running (Start succeeds) | _empty_ |
| `stream.stopped` | Coordinator ([coordinator.go](../internal/coordinator/coordinator.go)) | Pipeline goes from running → stopped (Stop / shutdown) | _empty_ |
| `stream.deleted` | API handler ([stream.go](../internal/api/handler/stream.go)) | `DELETE /streams/{code}` succeeds | _empty_ |
| `stream.runtime_created` | Autopublish ([service.go](../internal/autopublish/service.go)) | A template-prefix match materialises a runtime stream (encoder pushed to a path matching a template prefix; the matched template carries a `publish://` input). Runtime streams are NEVER in the on-disk repo | `template_code: string` |
| `stream.runtime_expired` | Autopublish ([service.go](../internal/autopublish/service.go)) | Idle reaper stops a runtime stream after 30 s without a packet on the buffer hub | `template_code: string` |

### 2.2 Input health

| Type | Emitter | Triggers when | Payload |
|---|---|---|---|
| `input.connected` | Ingestor ([service.go:220, 402](../internal/ingestor/service.go)) | `PacketReader.Open` succeeds (initial or after reconnect) | `input_priority: int` |
| `input.reconnecting` | Ingestor ([service.go:409](../internal/ingestor/service.go)) | Transient read error; pull worker is retrying | `input_priority: int`, `err: string` |
| `input.degraded` | Manager ([service.go:656, 708](../internal/manager/service.go)) + Coordinator | Health timeout OR `ReportInputError` triggered | `input_priority: int`, `reason: string` |
| `input.failed` | Ingestor ([service.go:254, 440](../internal/ingestor/service.go)) | Pull worker exited non-retriable | `input_priority: int`, `err: string` |
| `input.failover` | Manager ([service.go:611, 831](../internal/manager/service.go)) | Active input switched (degraded primary → backup OR failback OR manual) | `from: int`, `to: int`, `reason: string` |
| `input.recovered` | Manager ([service.go:`runProbe`](../internal/manager/service.go)) | Failback probe succeeded — previously-degraded input is healthy again. Distinct from `input.failover` reason="recovery"/"failback" because consumers monitoring "is the source back" want one signal, not a payload-string scrape | `input_priority: int`, `was_exhausted: bool` |

`reason` enum on `input.failover`: `initial`, `error`, `timeout`,
`manual`, `failback`, `recovery`, `input_added`, `input_removed`.

### 2.3 Transcoder

| Type | Emitter | Triggers when | Payload |
|---|---|---|---|
| `transcoder.started` | Transcoder ([service.go:408](../internal/transcoder/service.go)) | Transcoder subprocess spawned for the stream | _empty_ |
| `transcoder.stopped` | Transcoder ([service.go:494](../internal/transcoder/service.go)) | The stream's transcoder subprocess has exited cleanly | _empty_ |
| `transcoder.error` | Transcoder ([supervisor.go](../internal/transcoder/supervisor.go)) + Coordinator ([coordinator.go](../internal/coordinator/coordinator.go)) | Transcoder subprocess crash / non-zero exit | `profile: string`, `err: string` |

### 2.4 DVR / Recordings

| Type | Emitter | Triggers when | Payload |
|---|---|---|---|
| `recording.started` | DVR ([service.go:197](../internal/dvr/service.go)) | A recording subscription begins (operator enables DVR or pipeline starts with `dvr.enabled=true`) | `recording_id: string` |
| `recording.stopped` | DVR ([service.go:236](../internal/dvr/service.go)) | Recording subscription ends gracefully | `recording_id: string` |
| `recording.failed` | DVR ([service.go:443](../internal/dvr/service.go)) | Segment write or rotation hits a non-recoverable error | `recording_id: string`, `err: string` |
| `segment.written` | DVR ([service.go:483](../internal/dvr/service.go)) | One TS segment flushed to disk + manifest updated | `recording_id: string`, `index: int`, `size_bytes: int64`, `duration_sec: float`, `discontinuity: bool` |
| `dvr.segment_pruned` | DVR ([service.go:`applyRetention`](../internal/dvr/service.go)) | Retention loop deleted an aged-out segment | `recording_id: string`, `segment_index: int`, `size_bytes: int64`, `reason: "age" \| "size"` |

### 2.5 Push out (RTMP / RTMPS)

Per-destination state transitions. Each event fires only on STATE CHANGE
— no spam on noisy retry loops because `setPushStatus` short-circuits
when the new status equals the previous.

| Type | Emitter | Triggers when | Payload |
|---|---|---|---|
| `push.started` | Publisher ([runtime.go:`publishPushEvent`](../internal/publisher/runtime.go)) | Push goroutine spawns — about to dial target | `url: string` |
| `push.active` | Publisher | `lal.PushSession.Push` handshake succeeded; media flowing | `url: string` |
| `push.reconnecting` | Publisher | Write fail or input discontinuity; retrying | `url: string` |
| `push.failed` | Publisher | `dest.Limit` retry attempts exhausted, or terminal error | `url: string` |

### 2.6 Play sessions

| Type | Emitter | Triggers when | Payload |
|---|---|---|---|
| `session.opened` | Sessions tracker ([tracker.go:614](../internal/sessions/tracker.go)) | New `PlaySession` created — first segment GET (HLS/DASH) or TCP handshake (RTMP/SRT/RTSP) | `session_id`, `proto`, `ip`, `user_agent`, `country`, `user_name` |
| `session.closed` | Sessions tracker ([tracker.go:638](../internal/sessions/tracker.go)) | TCP disconnect / idle timeout / kicked / shutdown sweep | `session_id`, `proto`, `ip`, `bytes`, `duration_sec`, `reason` |

`reason` enum: `idle`, `client_gone`, `kicked`, `shutdown`.

### 2.7 Server config

| Type | Emitter | Triggers when | Payload |
|---|---|---|---|
| `config.changed` | API handler ([config.go:`UpdateConfig`](../internal/api/handler/config.go), [config_yaml.go:`ReplaceConfigYAML`](../internal/api/handler/config_yaml.go)) | `POST /config` or `PUT /config/yaml` succeeds | `source: "post_config" \| "put_config_yaml"` |

Note: the bulk `PUT /config/yaml` replace fans out into individual
`stream.updated` / `hook.updated` events as well — `config.changed` is
the umbrella audit signal for the whole transaction.

### 2.8 Watermark assets

| Type | Emitter | Triggers when | Payload |
|---|---|---|---|
| `watermark.asset_created` | API handler ([watermark.go:`Upload`](../internal/api/handler/watermark.go)) | `POST /watermarks` upload succeeds | `asset_id: string`, `name: string` |
| `watermark.asset_deleted` | API handler ([watermark.go:`Delete`](../internal/api/handler/watermark.go)) | `DELETE /watermarks/{id}` succeeds | `asset_id: string` |

### 2.9 Hook lifecycle (meta-events)

Audit events for the hook system itself. Useful for inventory sync /
infrastructure-as-code drift detection.

| Type | Emitter | Triggers when | Payload |
|---|---|---|---|
| `hook.created` | API handler ([hook.go:`Create`](../internal/api/handler/hook.go)) | `POST /hooks` succeeds | `hook_id: string`, `hook_type: "http" \| "file"` |
| `hook.updated` | API handler ([hook.go:`Update`](../internal/api/handler/hook.go)) | `PUT /hooks/{hid}` succeeds | `hook_id: string`, `hook_type: string` |
| `hook.deleted` | API handler ([hook.go:`Delete`](../internal/api/handler/hook.go)) | `DELETE /hooks/{hid}` succeeds | `hook_id: string`, `hook_type: string` (from the pre-delete record) |

Beware the recursion risk: a `hook.*` event subscriber that creates /
updates / deletes hooks will trigger more `hook.*` events.

### 2.10 Template lifecycle (meta-events)

Audit events for the template system. Updates fire AFTER every
dependent stream's pipeline has been hot-reloaded so downstream
consumers can assume the new resolved config is live by the time the
event is delivered.

| Type | Emitter | Triggers when | Payload |
|---|---|---|---|
| `template.created` | API handler ([template.go](../internal/api/handler/template.go)) | `POST /templates/{code}` succeeds with a fresh code | `template_code: string` |
| `template.updated` | API handler ([template.go](../internal/api/handler/template.go)) | `POST /templates/{code}` succeeds on an existing code; dependent running streams have been hot-reloaded via `coordinator.Update` | `template_code: string` |
| `template.deleted` | API handler ([template.go](../internal/api/handler/template.go)) | `DELETE /templates/{code}` succeeds (no streams reference the template) | `template_code: string` |

---

## 3. Filtering (per-hook)

Each hook has two filter fields:

```yaml
event_types:    ["stream.started", "input.failover"]   # whitelist
stream_codes:   ["news", "sports"]                     # whitelist
event_types_except: ["session.opened"]                  # blacklist
stream_codes_except: ["test*"]                          # blacklist (glob)
```

Empty whitelist = match all. Blacklists override whitelists when both
match. Stream-code filters use `path.Match` glob syntax (`*` and `?`,
no `**`).

---

## 4. Delivery shapes

### HTTP hooks

Events accumulate in a per-hook batcher; flushed when:

- `BatchMaxItems` reached (default 100), OR
- `BatchFlushIntervalSec` elapsed (default 1)

POST body is a JSON ARRAY of event envelopes. HMAC signs the entire
body when `secret` is set:

```
X-OpenStreamer-Signature: sha256=<hex>
X-OpenStreamer-Batch-Size: <int>
Content-Type: application/json
```

Failed batches re-queue at the FRONT of the buffer for the next flush
— chronological order preserved across retries. The buffer is bounded
by `BatchMaxQueueItems` (default 1000); overflow drops the OLDEST events
and increments
[`open_streamer_hooks_events_dropped_total`](./METRICS.md#hook-delivery).

### File hooks

Append one JSON-encoded event per line to an absolute path. Concurrent
deliveries to the same path serialise via a per-target mutex. Drop-in
for `Filebeat` / `Vector` / `Promtail` tail-and-ship pipelines.

---

## 5. Event-driven recipes

### Slack alert on push failure

```yaml
hooks:
  - id: slack-push-alert
    type: http
    target: https://hooks.slack.com/services/T.../B.../...
    event_types: ["push.failed"]
    metadata:
      channel: "#streaming-ops"
```

### Audit log of every config change to S3 (via Vector sidecar)

```yaml
hooks:
  - id: config-audit
    type: file
    target: /var/log/open-streamer/config-audit.jsonl
    event_types: ["stream.updated", "config.changed", "hook.created", "hook.updated", "hook.deleted"]
```

### Auto-resolve PagerDuty on input recovery

```yaml
hooks:
  - id: pd-resolve
    type: http
    target: https://events.pagerduty.com/v2/enqueue
    event_types: ["input.recovered", "stream.started"]
    secret: "${PD_ROUTING_KEY}"
```

---

## 6. Reference files

| Concern | File |
|---|---|
| EventType constants | [internal/domain/event.go](../internal/domain/event.go) |
| Bus implementation | [internal/events/bus.go](../internal/events/bus.go) |
| Hook delivery (HTTP batcher + file sink) | [internal/hooks/](../internal/hooks/) |
| Per-hook subscription wiring | [internal/hooks/service.go § Start](../internal/hooks/service.go) |
| Metrics for hook delivery | [METRICS.md § Hooks](./METRICS.md#hooks) |
