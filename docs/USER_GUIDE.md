# Open Streamer — User Guide

How to install, configure, and operate Open Streamer end-to-end.

> Companion docs: [CONFIG.md](./CONFIG.md) (every config field + examples) ·
> [ARCHITECTURE.md](./ARCHITECTURE.md) (how it works) ·
> [APP_FLOW.md](./APP_FLOW.md) (pipeline + events) ·
> [FEATURES_CHECKLIST.md](./FEATURES_CHECKLIST.md) (what's implemented).

---

## 1. Install

### Binary release (recommended)

Latest release ships pre-built archives + Linux installer:

```bash
# Linux/systemd installer — downloads + verifies + installs as a service.
# Idempotent: re-running with a new tag uninstalls cleanly first.
sudo bash <(curl -sL https://raw.githubusercontent.com/datvietvac-techhub/open-streamer/main/build/reinstall.sh) v1.0.0
```

This installs the binary to `/usr/local/bin/open-streamer`, a systemd unit
to `/etc/systemd/system/open-streamer.service`, and creates the data dir
at `/var/lib/open-streamer` (preserved across version upgrades).

### From source

```bash
git clone https://github.com/datvietvac-techhub/open-streamer.git
cd open-streamer
make build          # → bin/open-streamer
make run            # run without persisting binary
```

Requires Go 1.25.x+. Transcoding is built on libavcodec (see § 2).

### Docker

```bash
make docker-build   # → open-streamer:local
make compose-up     # docker compose up
```

---

## 2. Transcoding

Open Streamer transcodes in-process via libavcodec — a per-stream
`open-streamer-transcoder` subprocess driven over gRPC; ingest is pure Go.
There is no FFmpeg CLI in the media path.

The encoders the transcoder can use are fixed when the
`open-streamer-transcoder` binary is built against libav. The release
bundles and the builder image (`Dockerfile.builder`) ship:

- **Always**: `libx264` (H.264 CPU), `aac`, MPEG-TS
- **GPU / extra**: `h264_nvenc` / `hevc_nvenc` (NVENC), `libx265`,
  `libsvtav1`, `libopus`, `libmp3lame`, `ac3`, HLS / DASH muxers

The `POST /api/v1/config/transcoder/probe` endpoint (UI "Test" button) is a
no-op that always reports ok.

---

## 3. First boot

Default storage is JSON flat-file under `./test_data/`. To override
location or backend, set env vars BEFORE first boot:

```bash
export OPEN_STREAMER_STORAGE_DRIVER=yaml
export OPEN_STREAMER_STORAGE_YAML_DIR=/var/lib/open-streamer
./bin/open-streamer
```

The server starts unconfigured (no listeners, no streams) on first boot.
Configure via the REST API or UI. To enable HTTP API, POST a server
config:

```bash
curl -XPOST http://localhost:8080/api/v1/config -d '{
  "server":    { "http_addr": ":8080" },
  "buffer":    {},
  "manager":   {},
  "publisher": { "hls":  { "dir": "/var/hls"  },
                 "dash": { "dir": "/var/dash" } },
  "listeners": { "rtmp": { "enabled": true, "port": 1935 },
                 "rtsp": { "enabled": true, "port": 554  },
                 "srt":  { "enabled": true, "port": 9999 } }
}'
```

If the server isn't running on `:8080` yet, initialize via the YAML
endpoint or pre-seed `open_streamer.yaml` in your data dir.

---

## 4. Create a stream

Streams are the central entity — each binds N inputs (with priority
failover), an optional transcoder ladder, output protocols, push
destinations, and DVR settings. Stream code accepts
`[A-Za-z0-9_/-]+` — slashes namespace streams as
`region/north/news`. The route layer keeps the whole prefix as one
opaque key; `..` and consecutive `/` are rejected.

### URL conventions for push / play

Single-segment codes (`news`) use the `live/` prefix; multi-segment
codes (`region/north/news`) use the raw path. The server strips a
leading `live/` when present, so multi-segment codes also work via
`live/region/north/news` for non-canonical clients. Bare
single-segment URLs (e.g. `rtmp://host/news`) are rejected so a
half-typed URL can't accidentally hit a stream.

| Form | RTMP | RTSP | SRT |
|---|---|---|---|
| Single-segment `news` | `rtmp://host/live/news` | `rtsp://host/live/news` | `srt://host?streamid=live/news` |
| Multi-segment `region/north/news` | `rtmp://host/region/north/news` | `rtsp://host/region/north/news` | `srt://host?streamid=region/north/news` |

HLS / DASH delivery URLs (`/{code}/index.m3u8`, `/{code}/index.mpd`)
always use the raw code without `live/`.

### Minimum viable: pull HLS, publish HLS

```bash
curl -XPOST http://localhost:8080/api/v1/streams/news -d '{
  "name": "News Channel",
  "inputs": [
    { "url": "https://upstream.example.com/news/playlist.m3u8", "priority": 0 }
  ],
  "protocols": { "hls": true }
}'
```

Stream is up at `http://localhost:8080/news/index.m3u8`. No transcoding
— packets pass through untouched.

### Multi-input failover

Add backup sources sorted by priority (lower = preferred):

```bash
curl -XPOST http://localhost:8080/api/v1/streams/news -d '{
  "inputs": [
    { "url": "rtmp://primary.example.com/live/news",   "priority": 0 },
    { "url": "rtmp://backup.example.com/live/news",    "priority": 1 },
    { "url": "https://cdn.example.com/news/index.m3u8","priority": 2 }
  ],
  "protocols": { "hls": true }
}'
```

Stream Manager monitors the active input. On failure (configurable via
`manager.input_packet_timeout_sec`, default 30s) it switches to the
next-priority input within ~150ms — **without restarting the transcoder**. The
switch is recorded in `runtime.switches[]`.

When the higher-priority input recovers (background probe succeeds), the
manager fails back automatically. Override manually:

```bash
curl -XPOST http://localhost:8080/api/v1/streams/news/inputs/switch \
     -d '{ "priority": 0 }'
```

### Add ABR transcoding

```bash
curl -XPOST http://localhost:8080/api/v1/streams/news -d '{
  "transcoder": {
    "global": { "hw": "nvenc" },
    "audio":  { "codec": "aac", "bitrate": 128 },
    "video": {
      "profiles": [
        { "width": 1920, "height": 1080, "bitrate": 5000, "codec": "h264" },
        { "width": 1280, "height": 720,  "bitrate": 2500, "codec": "h264" },
        { "width": 854,  "height": 480,  "bitrate": 1200, "codec": "h264" }
      ]
    }
  },
  "protocols": { "hls": true, "dash": true }
}'
```

The transcoder subprocess decodes the source once and fans out to every
profile (one encoder per rendition), writing each into its own buffer. The
HLS master playlist auto-aggregates them at `/news/index.m3u8`. DASH MPD at
`/news/index.mpd`.

### Push to platforms

```bash
curl -XPOST http://localhost:8080/api/v1/streams/news -d '{
  "push": [
    {
      "url": "rtmp://rtmp.example.com/live2/STREAM_KEY",
      "enabled": true
    },
    {
      "url": "rtmps://rtmps.example.com:443/rtmp/STREAM_KEY",
      "enabled": true,
      "limit": 10,
      "retry_timeout_sec": 30
    }
  ]
}'
```

Per-destination state visible at `runtime.publisher.pushes[]` —
status (`starting` / `active` / `reconnecting` / `failed`), attempt
counter, last 5 errors, connected_at timestamp.

### DVR

Per-stream opt-in:

```bash
curl -XPOST http://localhost:8080/api/v1/streams/news -d '{
  "dvr": {
    "enabled":           true,
    "segment_duration":  4,
    "retention_sec":     604800,
    "max_size_gb":       100
  }
}'
```

Storage path defaults to `./out/dvr/{streamCode}`. Recording resumes
across restarts using the on-disk `playlist.m3u8` + `index.json`.
`#EXT-X-DISCONTINUITY` markers on every gap (signal loss + server
downtime).

VOD playback — full recording:
```
GET /recordings/news/playlist.m3u8
```

Timeshift — last 30 minutes:
```
GET /recordings/news/timeshift.m3u8?offset_sec=-1800&duration=1800
```

Timeshift — absolute window:
```
GET /recordings/news/timeshift.m3u8?from=2026-04-26T10:00:00Z&duration=3600
```

---

## 5. Templates

A **Template** is a reusable bundle of config-like stream fields —
Transcoder, Protocols, Push, DVR, Watermark, Thumbnail, Inputs, Tags,
StreamKey, plus the auto-publish `Prefixes` list. Streams reference at
most one template via the `template` field; every field the stream
leaves at its zero value inherits from the template. The
non-inheritable fields are `code` (per-stream identity) and `disabled`
(per-stream runtime toggle).

### 5.1 Create a template

Template code accepts `[A-Za-z0-9_-]+` — no `/` because templates live
in a flat namespace.

```bash
curl -XPOST http://localhost:8080/api/v1/templates/news_profile -d '{
  "name": "News profile",
  "description": "1080p + 720p ABR ladder, HLS + DASH, DVR enabled",
  "transcoder": {
    "global": { "hw": "nvenc" },
    "audio":  { "codec": "aac", "bitrate": 128 },
    "video": {
      "profiles": [
        { "width": 1920, "height": 1080, "bitrate": 5000, "codec": "h264" },
        { "width": 1280, "height": 720,  "bitrate": 2500, "codec": "h264" }
      ]
    }
  },
  "protocols": { "hls": true, "dash": true },
  "dvr": { "enabled": true, "retention_sec": 86400 }
}'
```

### 5.2 Reference the template from a stream

```bash
curl -XPOST http://localhost:8080/api/v1/streams/news -d '{
  "name": "News Channel",
  "template": "news_profile",
  "inputs": [
    { "url": "https://upstream.example.com/news/playlist.m3u8", "priority": 0 }
  ]
}'
```

The stream now inherits Transcoder, Protocols, and DVR from
`news_profile`. The on-disk record still carries only `code`, `name`,
`template`, and `inputs` — clean and minimal. The operator can see
the inherited fields by fetching the template separately:

```bash
curl http://localhost:8080/api/v1/templates/news_profile
```

### 5.3 Override individual fields

Any non-zero value on the stream wins. Example: same template but a
stream that needs an extra 480p rung and disables DASH.

```bash
curl -XPOST http://localhost:8080/api/v1/streams/sports -d '{
  "template": "news_profile",
  "inputs": [
    { "url": "rtmp://primary.example.com/live/sports", "priority": 0 }
  ],
  "transcoder": {
    "global": { "hw": "nvenc" },
    "audio":  { "codec": "aac", "bitrate": 128 },
    "video": {
      "profiles": [
        { "width": 1920, "height": 1080, "bitrate": 5000 },
        { "width": 1280, "height": 720,  "bitrate": 2500 },
        { "width": 854,  "height": 480,  "bitrate": 1200 }
      ]
    }
  },
  "protocols": { "hls": true }
}'
```

Note: the override REPLACES the inherited value completely — the
stream's `transcoder` carries the full 3-rung ladder, the template's
2-rung ladder is not concatenated.

### 5.4 Hot reload via template update

`POST /templates/{code}` on an existing template walks every running
stream that inherits from it and dispatches the same diff-based
`coordinator.Update` used by `POST /streams/{code}`. Stopped streams
skip the reload — the next start picks up the new template.

```bash
# Add 480p to the template — every running stream that doesn't
# already override `transcoder` picks up the new rung.
curl -XPOST http://localhost:8080/api/v1/templates/news_profile -d '{
  "transcoder": {
    "video": {
      "profiles": [
        { "width": 1920, "height": 1080, "bitrate": 5000 },
        { "width": 1280, "height": 720,  "bitrate": 2500 },
        { "width": 854,  "height": 480,  "bitrate": 1200 }
      ]
    }
  },
  "protocols": { "hls": true, "dash": true },
  "dvr": { "enabled": true, "retention_sec": 86400 }
}'
```

### 5.5 Auto-publish via prefix matching

Templates can declare a `prefixes` list. When an encoder pushes (RTMP)
to a path matching one of the prefixes on a segment boundary AND the
template has a `publish://` input, the server materialises a **runtime
stream** on the fly — no `POST /streams/{code}` needed beforehand.

```bash
curl -XPOST http://localhost:8080/api/v1/templates/auto_news -d '{
  "name": "Auto-news template",
  "prefixes": ["live"],
  "inputs": [{ "url": "publish://" }],
  "protocols": { "hls": true }
}'
```

Now an encoder pushing to `rtmp://host:1935/live/foo` triggers a new
runtime stream with code `live/foo` (the FULL incoming path, not just
the suffix). The stream appears in `GET /streams` with
`source: "runtime"`. Runtime streams are RAM-only — they are NOT
persisted and disappear 30 s after the last packet reaches the buffer
hub. Stop the encoder → the reaper tears down the pipeline.

```bash
curl http://localhost:8080/api/v1/streams
# {
#   "data": [
#     { "code": "live/foo", "template": "auto_news",
#       "source": "runtime", "runtime": { ... } },
#     ...
#   ]
# }
```

**Prefix rules**:

- Prefix `live` matches `live/foo/bar` and `live` itself but NOT
  `livestream/foo` (segment boundary).
- Prefixes are unique across templates — `POST /templates/{code}` returns
  409 `PREFIX_OVERLAP` when any prefix is a path-prefix of another
  template's prefix (including equality).
- Without at least one `publish://` input on the template, prefix match
  rejects the push (debug-logged "matching template has no publish://
  input").

### 5.6 Deleting a template

`DELETE /templates/{code}` refuses with 409 `TEMPLATE_IN_USE` while
any stream still references it. The response lists the dependent stream
codes so the operator knows which to detach:

```bash
curl -XDELETE http://localhost:8080/api/v1/templates/news_profile
# 409 Conflict
# { "error": "TEMPLATE_IN_USE", "streams": ["news", "sports"], ... }

# Detach a stream by setting template: null, or pick another template
curl -XPOST http://localhost:8080/api/v1/streams/news -d '{ "template": null, "transcoder": {...}, "protocols": {...} }'
```

### 5.7 Response shape note

`GET /streams/{code}` and `GET /streams` return the **raw** stored
record — overrides only, never merged with the template. Clients that
need the effective config fetch the template separately. Mixing the two
shapes would obscure which fields the operator actually set vs. which
come from the template.

---

## 6. Hot-reload

`PUT /streams/{code}` (or repeat `POST`) merges only the changed fields:

```bash
# Add 360p rung — restarts the stream's transcoder subprocess (renditions share one decode).
curl -XPOST http://localhost:8080/api/v1/streams/news -d '{
  "transcoder": {
    "video": {
      "profiles": [
        { "width": 1920, "height": 1080, "bitrate": 5000 },
        { "width": 1280, "height": 720,  "bitrate": 2500 },
        { "width": 854,  "height": 480,  "bitrate": 1200 },
        { "width": 640,  "height": 360,  "bitrate": 800  }
      ]
    }
  }
}'
```

The diff engine handles 5 categories independently — see
[ARCHITECTURE.md § Coordinator](./ARCHITECTURE.md#coordinator).

To force a full pipeline restart (e.g. switch stream key):

```bash
curl -XPOST http://localhost:8080/api/v1/streams/news/restart
```

To remove a stream entirely:

```bash
curl -XDELETE http://localhost:8080/api/v1/streams/news
```

---

## 7. Hooks (webhooks + file sink)

Subscribe to lifecycle events with one of two delivery backends:

```bash
# HTTP webhook — events ship as a JSON ARRAY (batched). HMAC signing
# covers the entire array body when `secret` is set.
curl -XPOST http://localhost:8080/api/v1/hooks -d '{
  "id":        "log-everything",
  "type":      "http",
  "target":    "https://ops.example.com/streams/events",
  "secret":    "shared-secret-for-hmac",
  "enabled":   true,
  "max_retries": 5,
  "timeout_sec": 10,

  // HTTP batching knobs — leave 0 to use server-wide defaults.
  "batch_max_items":          50,    // ship after every 50 events
  "batch_flush_interval_sec": 2,     // ... or every 2 seconds, whichever first
  "batch_max_queue_items":    20000  // memory cap if target goes down
}'

# File sink — appends one JSON event per line. NOT batched (one event per
# write) so log shippers (Filebeat / Vector / Promtail) tail-and-ship one
# line at a time.
curl -XPOST http://localhost:8080/api/v1/hooks -d '{
  "id":      "audit-log",
  "type":    "file",
  "target":  "/var/log/open-streamer/events.log",
  "enabled": true,
  "event_types": ["stream.started", "stream.stopped", "input.failover"]
}'
```

### HTTP batching semantics

- Each event is enqueued to a per-hook in-memory buffer (~µs latency).
- A flusher goroutine ships the buffer when EITHER `batch_max_items` is
  reached OR `batch_flush_interval_sec` elapses since the last flush.
- POST body is a JSON array (`[{event1},{event2},…]`). The
  `X-OpenStreamer-Batch-Size` header reports the array length so receivers
  can tune ingestion.
- `max_retries` retries within a single flush attempt (with 1s/5s/30s
  backoff). If all retries fail, events re-queue at the FRONT of the
  buffer for the next flush — chronological order preserved.
- `batch_max_queue_items` caps the buffer when the downstream is
  unreachable. Overflow drops the OLDEST events with a warning log.
- On graceful server shutdown (SIGTERM), every batcher gets one last
  best-effort flush.

### File backend behaviour

The file backend creates the target on first delivery (mode 0644). The
parent directory must already exist and be writable by the open-streamer
process. Concurrent deliveries on the same path serialise via a
per-target mutex; different paths run in parallel. Each line is one
complete JSON event followed by `\n` — the same envelope the HTTP
backend wraps in an array.

Filter by event type or stream code:

```json
{
  "event_types": ["input.failover", "transcoder.error"],
  "stream_codes": { "only": ["news", "sports"] }
}
```

Test delivery:

```bash
curl -XPOST http://localhost:8080/api/v1/hooks/log-everything/test
```

Hook server-wide settings (worker pool size) are at
`global_config.hooks` — see [CONFIG.md](./CONFIG.md). Per-hook
defaults: 3 retries, 10s timeout (overridden via Hook fields above).

For the full event catalogue see [APP_FLOW.md § Events
reference](./APP_FLOW.md#events-reference).

---

## 8. copy:// and mixer:// — in-process re-stream

Re-stream another in-process stream as input. No network round-trip; the
downstream stream subscribes directly to the upstream's published
buffer.

### copy:// — straight relay

```json
{
  "code": "news_backup",
  "inputs": [
    { "url": "copy://news", "priority": 0 }
  ],
  "protocols": { "hls": true }
}
```

If `news` has an ABR ladder, `news_backup` mirrors every rendition into
a parallel ABR ladder (no transcoding, just buffer subscriptions). This
is **ABR-copy** mode — the coordinator detects it and bypasses ingest +
transcoder entirely.

If `news` has no ladder, `copy://news` subscribes to its main playback
buffer — single-rendition copy.

Cycle detection prevents `copy://A` ↔ `copy://B` infinite loops via the
copy-graph validator at save time.

### mixer:// — combine video + audio from two streams

```json
{
  "code": "tv_with_radio",
  "inputs": [
    { "url": "mixer://tv_silent?audio=radio_fm", "priority": 0 }
  ],
  "protocols": { "hls": true }
}
```

Video tracks come from `tv_silent`, audio tracks from `radio_fm`.
Useful for muting a TV broadcast and overlaying a radio commentary.

Both streams must already exist; mixer:// validates at save time
(`audio=` param required, no self-mix, no nested mixer chains).

---

## 9. Watermarks

Apply text or image overlays to the encoded video. Two-step workflow
for image watermarks: upload to the asset library, then reference by
asset ID from a stream.

### 8.1 Upload an image asset

```bash
curl -XPOST http://localhost:8080/api/v1/watermarks?name=ChannelLogo \
     -F "file=@channel-logo.png"
# 201 Created
# {
#   "data": {
#     "id":          "8a3f1c0e2b9d",
#     "name":        "ChannelLogo",
#     "file_name":   "channel-logo.png",
#     "content_type":"image/png",
#     "size_bytes":  4521,
#     "uploaded_at": "2026-04-28T15:00:00Z"
#   }
# }
```

PNG / JPG / GIF supported. Cap: 8 MiB per asset. Library stays under
`watermarks.dir` (default `./watermarks`).

```bash
curl http://localhost:8080/api/v1/watermarks | jq .         # list
curl http://localhost:8080/api/v1/watermarks/8a3f1c0e2b9d/raw -o /tmp/preview.png
curl -XDELETE http://localhost:8080/api/v1/watermarks/8a3f1c0e2b9d
```

### 8.2 Apply to a stream

**Image watermark** referencing the uploaded asset:

```bash
curl -XPOST http://localhost:8080/api/v1/streams/news -d '{
  "watermark": {
    "enabled":   true,
    "type":      "image",
    "asset_id":  "8a3f1c0e2b9d",
    "position":  "top_right",
    "offset_x":  30, "offset_y": 30,
    "opacity":   0.85
  }
}'
```

**Text watermark** with a live clock:

```bash
curl -XPOST http://localhost:8080/api/v1/streams/news -d '{
  "watermark": {
    "enabled":    true,
    "type":       "text",
    "text":       "LIVE %{localtime\\:%H\\:%M}",
    "font_size":  28,
    "font_color": "white",
    "opacity":    0.9,
    "position":   "bottom_right",
    "offset_x":   20, "offset_y": 20
  }
}'
```

**Custom position** (raw libavfilter expression — full power):

```bash
curl -XPOST http://localhost:8080/api/v1/streams/news -d '{
  "watermark": {
    "enabled":  true,
    "type":     "image",
    "asset_id": "8a3f1c0e2b9d",
    "position": "custom",
    "x":        "main_w-overlay_w-50",
    "y":        "if(gt(t,5),10,-100)"
  }
}'
```

The `y` example slides the watermark in from above the frame after 5
seconds — useful for animated brand intros without an external editor.

### 8.3 Position presets

| Position | Where | offset_x / offset_y mean |
|---|---|---|
| `top_left` | upper-left corner | inward padding from edge |
| `top_right` | upper-right corner | inward padding |
| `bottom_left` | lower-left corner | inward padding |
| `bottom_right` | lower-right corner (default) | inward padding |
| `center` | exact frame centre | offsets ignored |
| `custom` | wherever your `x` / `y` expression evaluates | offsets ignored |

GPU pipelines (NVENC) automatically round-trip via CPU for the
watermark filter — costs ~5% CPU per rendition; no operator action
needed. The filter graph is built per rendition so every variant draws
the watermark independently.

### 8.4 Updating watermark on a running stream

Changing any watermark field on a stream that is currently transcoding
**restarts the transcoder pipeline** (~2-3s downtime per stream):

- Buffer hub keeps running; rendition buffers are recreated on the new
  start path
- HLS / DASH / RTMP / SRT / RTSP viewers see one
  `#EXT-X-DISCONTINUITY` (HLS) or equivalent gap, then resume
- DVR pauses for the gap and adds a `DVRGap` entry

This is necessary because the libavfilter graph is fixed when the
encoder is built — there's no way to live-swap it without restarting the
subprocess. The diff engine routes any watermark-related field change
through the topology-reload path so the new filter graph applies.

**Watermark on a passthrough stream** (no transcoder) is silently
ignored — no transcoder is running so the filter graph never exists.
Enable transcoding to get a server-side watermark; otherwise overlay
client-side in your player.

---

## 10. Play sessions — who's watching?

Open Streamer tracks every active player across all delivery
protocols (HLS / DASH / RTMP / SRT / RTSP). State is in-memory only —
restart loses records, viewers reconnect into fresh sessions.

### 9.1 Enable

Sessions tracking is **opt-in** — POST a `sessions` config section:

```bash
curl -XPOST http://localhost:8080/api/v1/config -d '{
  "sessions": { "enabled": true, "idle_timeout_sec": 30 }
}'
```

Hot-reloadable: toggling `enabled` or changing `idle_timeout_sec`
takes effect on the next reaper tick (≤ 5s) without restart.

### 9.2 Inspect

```bash
# All active sessions
curl http://localhost:8080/api/v1/sessions | jq .

# Per-stream
curl http://localhost:8080/api/v1/streams/news/sessions | jq .data

# Filter by protocol
curl 'http://localhost:8080/api/v1/sessions?proto=hls' | jq .data
```

Each session record includes:

```json
{
  "id":          "abc123…",                       // fingerprint or UUID
  "stream_code": "news",
  "proto":       "hls",                           // hls|dash|rtmp|srt|rtsp
  "ip":          "203.0.113.42",
  "user_agent":  "Mozilla/5.0 …",
  "country":     "VN",                            // when GeoIP wired
  "bytes":       1481726,
  "opened_at":   "2026-04-28T15:01:00Z",
  "updated_at":  "2026-04-28T15:04:13Z",
  "duration_sec": 193
}
```

The list response also includes aggregate `stats` (active /
opened_total / closed_total / idle_closed_total / kicked_total).

### 9.3 Kick a viewer

```bash
curl -XDELETE http://localhost:8080/api/v1/sessions/abc123…
# 204 No Content
```

Idempotent — calling delete on an already-closed session returns 404.
Emits `EventSessionClosed` with `reason=kicked` on the event bus —
hooks subscribed to the event see the kick in the same channel as
organic disconnects.

### 9.4 Subscribe to session events

```bash
curl -XPOST http://localhost:8080/api/v1/hooks -d '{
  "id":     "viewer-analytics",
  "type":   "file",
  "target": "/var/log/open-streamer/sessions.log",
  "event_types": ["session.opened", "session.closed"]
}'
```

The bus carries an event for every open/close — feed Loki, ClickHouse,
or your analytics pipeline. The sessions package stays in-memory; long-
term persistence is the hook's responsibility.

### 9.5 Notes & caveats

- **Fingerprint sessions** (HLS / DASH) collapse repeated GETs from one
  viewer onto one record while the idle window is open. Two viewers
  behind shared NAT without a `?token=…` will merge into one session —
  add a token query param to disambiguate (the tracker stores it as
  the user_name and `named_by="token"`).
- **RTMP / SRT / RTSP** are connection-bound — one TCP/UDP session per
  record, closed exactly when the transport ends.
- **RTSP bytes** are always 0: gortsplib's mux is internal and there's
  no per-subscriber hook today. Other counters are accurate.
- **GeoIP** field is empty unless an operator wires a custom resolver —
  the default `NullGeoIP` always returns `""`. The `geoip_db_path`
  config field is reserved for the future MaxMind integration.

---

## 11. Operations

### Health checks

- `GET /healthz` — liveness (always 200 if process is up)
- `GET /readyz` — readiness (200 once services initialised)

### Metrics

Prometheus scrape endpoint at `GET /metrics`:

- `manager_failovers_total{stream_code}` — rate of input switches
- `transcoder_restarts_total{stream_code}` — transcoder subprocess crash count
- `transcoder_workers_active{stream_code}` — running profile count
- `manager_input_health{stream_code, input_priority}` — 1 healthy, 0 degraded
- Buffer depth, bytes/packets per stream

### Per-stream runtime status

```bash
curl http://localhost:8080/api/v1/streams/news | jq .data.runtime
```

Returns:
- `status` — `active` / `degraded` / `stopped` / `idle`
- `pipeline_active` — bool
- `active_input_priority` — current source
- `exhausted` — true when all inputs degraded
- `inputs[]` — per-input health + last 5 errors
- `switches[]` — last 20 active-input switches with reason
- `transcoder.profiles[]` — per-rung restart count + errors
- `publisher.pushes[]` — per-destination state

A stream is `degraded` when EITHER:
- All inputs exhausted (manager: no failover candidate), OR
- Transcoder is in a crash loop (3 consecutive subprocess crashes < 30s
  apart). Recovers automatically when it runs > 30s sustained.

### YAML editor (entire system state)

```bash
curl http://localhost:8080/api/v1/config/yaml > backup.yaml
# edit backup.yaml...
curl -XPUT http://localhost:8080/api/v1/config/yaml \
     -H "Content-Type: application/yaml" \
     --data-binary @backup.yaml
```

Round-trips GlobalConfig + all streams + all hooks. Useful for
ops-as-code workflows.

### Logs

`slog` structured output to stderr. Configurable via `log.level` (debug
/ info / warn / error) and `log.format` (text / json). Common ops grep:

```bash
journalctl -u open-streamer | grep -E '(failover|crashed|degraded)'
```

---

## 12. Troubleshooting

| Symptom | Likely cause | Where to look |
|---|---|---|
| Status `degraded` while inputs healthy | Transcoder is in a crash loop on at least one rung | `runtime.transcoder.profiles[].errors[]` (UI or `/streams/{code}`) |
| Encoder rejects an option | Codec/preset typo, or option unsupported by the resolved encoder | Stream config + `runtime.transcoder.profiles[].errors[]` |
| Stream up but HLS 404 | `publisher.hls.dir` is empty, or transcoder not producing output | Server logs (`publisher: HLS disabled — …` etc.) |
| Push stuck in `reconnecting` | Destination reject, auth fail, or network loss | `runtime.publisher.pushes[].errors[]` |
| GPU encoder near 100% saturation | NVENC chip overloaded — reduce profile count / framerate, or set `bframes=0` | `nvidia-smi` or Grafana GPU dashboard |
| `copy://X` save rejected | `X` doesn't exist, OR cycle detected, OR shape constraint violated | API error message details which check failed |
| Encoder unavailable at transcode start | The `open-streamer-transcoder` binary lacks that encoder (fixed at build time) | Server logs / `runtime.transcoder.profiles[].errors[]`; rebuild against a libav with the encoder (see `Dockerfile.builder`) |
| `/sessions` returns empty after enabling | Tracker config not persisted (UI didn't save) OR sub-section absent before restart | `curl /api/v1/config \| jq .sessions` to verify; restart once after first enabling |
| Watermark POST returns `INVALID_WATERMARK` | `image_path` and `asset_id` both set, OR custom position with empty x/y, OR opacity outside [0,1] | API error message names the rule; fix the config JSON |
| Watermark image not appearing on output | Asset deleted while stream running, OR `image_path` not absolute / unreadable by the open-streamer user | Server logs (`coordinator: watermark asset resolve failed`); chown the asset / re-upload |

For deeper troubleshooting see [APP_FLOW.md](./APP_FLOW.md) — covers
exact event sequences, status reconciliation logic, and what each error
in the runtime snapshot means.

---

## 13. Production checklist

- [ ] `open-streamer-transcoder` binary built with the required encoders (see `Dockerfile.builder`)
- [ ] HLS + DASH dirs are different (when both enabled)
- [ ] HTTP server bind address chosen — reverse proxy in front for TLS
- [ ] Storage backend chosen (`json` flat-file or `yaml` single-doc)
- [ ] Hooks configured for at least `stream.stopped` + `transcoder.error` so ops gets paged on crashes
- [ ] DVR retention sized against disk capacity
- [ ] `manager.input_packet_timeout_sec` tuned per-protocol (HLS pull bursts may need ≥ segment duration × 2)
- [ ] Prometheus scrape configured against `/metrics`
- [ ] Pre-commit hook installed for contributors (`make hooks-install`)

---

## 14. Updating

```bash
# Use the provided installer for atomic upgrades on systemd hosts:
sudo bash build/reinstall.sh v1.0.1
```

Stops the service, swaps the binary + systemd unit, restarts. Data
directory (streams, recordings, hooks) is preserved across upgrades.

For source builds: `git pull && make build && systemctl restart
open-streamer`.

---

## See also

- [CONFIG.md](./CONFIG.md) — every config field explained, with examples
- [ARCHITECTURE.md](./ARCHITECTURE.md) — design rationale, data flow,
  invariants
- [APP_FLOW.md](./APP_FLOW.md) — pipeline lifecycle, event sequences,
  status reconciliation
- [FEATURES_CHECKLIST.md](./FEATURES_CHECKLIST.md) — what's
  implemented vs planned
