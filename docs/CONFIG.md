# Open Streamer — Configuration Reference

Every config field, what it does, what happens when you leave it unset.
Companion to [USER_GUIDE.md](./USER_GUIDE.md) (workflows) and
[FEATURES_CHECKLIST.md](./FEATURES_CHECKLIST.md) (capabilities).

> **Two layers of config**
> - **`config.yaml` + env (StorageConfig only)** — bootstrap-only: where
>   to find the persistent store. Read once at startup.
> - **GlobalConfig + Stream config (the rest)** — persisted in the store,
>   editable at runtime via REST API. Hot-applied without restart.
>
> Defaults are the SINGLE SOURCE OF TRUTH at
> [internal/domain/defaults.go](../internal/domain/defaults.go) and
> reachable live via `GET /api/v1/config/defaults`.

---

## 1. Storage (bootstrap-only)

Loaded by `viper` in this order: defaults → `config.yaml` → env (`OPEN_STREAMER_*`, dots → underscores).

```yaml
# config.yaml
storage:
  driver:   json          # json | yaml
  json_dir: ./test_data   # for driver=json
  yaml_dir: ./test_data   # for driver=yaml
```

Equivalent env override:

```bash
export OPEN_STREAMER_STORAGE_DRIVER=yaml
export OPEN_STREAMER_STORAGE_YAML_DIR=/var/lib/open-streamer
```

| Driver | Backend | Notes |
|---|---|---|
| `json` (default) | Flat JSON files under `json_dir/` | Fastest setup; one file per resource |
| `yaml` | Single `open_streamer.yaml` under `yaml_dir/` | Human-editable; good for ops-as-code workflows |

---

## 2. GlobalConfig

Edit at runtime via `POST /api/v1/config { ... }` (partial merge — only present fields update). Or wholesale via `PUT /api/v1/config/yaml`.

A section being **`null` / absent** means the corresponding subsystem is **disabled**. Setting a section enables it — the runtime manager hot-starts services to match.

```yaml
global_config:
  server: ...        # HTTP API listener
  buffer: ...        # Per-stream ring buffer capacity
  manager: ...       # Failover settings
  publisher: ...     # HLS / DASH directories + segment params
  listeners: ...     # Network listeners (RTMP / RTSP / SRT)
  ingestor: ...      # Server-wide ingest settings
  transcoder: ...    # ffmpeg binary path (auxiliary tasks)
  hooks: ...         # Worker pool
  sessions: ...      # Play-sessions tracker (HLS/DASH/RTMP/SRT/RTSP viewers)
  watermarks: ...    # Watermark asset library directory
  log: ...           # Level + format
```

### 2.1 server

```yaml
server:
  http_addr: ":8080"        # Required. Binds REST API + static delivery + metrics.
  cors:
    enabled:           true
    allowed_origins:   ["https://console.example.com"]
    allowed_methods:   ["GET", "POST", "PUT", "DELETE"]
    allowed_headers:   ["Content-Type", "Authorization"]
    allow_credentials: true       # MUST be false when allowed_origins=["*"]
    max_age:           600        # Preflight cache, seconds
```

`server.http_addr` is **required**. Unset → server cannot start.

### 2.2 buffer

```yaml
buffer:
  capacity: 1024    # MPEG-TS packets per subscriber. Default: 1024 (~1MB / 1.5s of 1080p).
```

Higher = more headroom for HLS pull bursts, more RAM per subscriber. Lower = more drops under load.

### 2.3 manager

```yaml
manager:
  input_packet_timeout_sec: 30   # Default. Active input declared dead after this gap.
```

Tune higher (60+) for HLS pull where one Read = one full segment (segment duration × 2 minimum). Tune lower (5-10) for low-latency RTMP / SRT.

### 2.4 publisher

```yaml
publisher:
  hls:
    dir:                /var/hls    # REQUIRED. Empty disables HLS for all streams.
    live_segment_sec:   2           # Default. Segment target duration.
    live_window:        12          # Default. Segments in sliding playlist.
    live_history:       0           # Default. Extra segments kept on disk after sliding out.
    live_ephemeral:     true        # If true: bounded retention; segments deleted as they leave window.
  dash:
    dir:                /var/dash   # REQUIRED if dash enabled. MUST differ from hls.dir.
    live_segment_sec:   2
    live_window:        12
    live_history:       0
    live_ephemeral:     true
```

**`dir` is required** when the corresponding format is enabled — empty path disables that format silently and logs an error per stream.

**Sizing `live_segment_sec`**: pick a value that comfortably exceeds **¼ of your source's keyframe interval**. Open Streamer ends every HLS segment at a video keyframe; if no keyframe arrives for `4 × live_segment_sec` (the safety deadline) the segmenter cuts mid-GOP and a player may briefly glitch at that boundary. For example, a source with a 10 s GOP needs `live_segment_sec ≥ 3` (default 2 will trip the safety net every segment). When in doubt, raise this value rather than lower — segments end at keyframes regardless, so a higher target just delays the safety deadline without lengthening typical segments.

### 2.5 listeners

Each protocol shares ONE port serving both ingest and playback:

```yaml
listeners:
  rtmp:
    enabled:     true
    listen_host: "0.0.0.0"   # Default. All interfaces.
    port:        1935        # Default.
  rtsp:
    enabled:     true
    listen_host: "0.0.0.0"
    port:        554         # Default.
    transport:   "tcp"       # Default. tcp | udp.
  srt:
    enabled:     true
    listen_host: "0.0.0.0"
    port:        9999        # Default.
    latency_ms:  120         # Default. Haivision low-latency reference.
```

`listen_host` empty → bind all interfaces. Use specific IP for multi-NIC isolation.

### 2.6 ingestor

```yaml
ingestor:
  hls_max_segment_buffer: 8     # Default. Pre-fetched HLS segments held per stream.
```

Server-wide. Per-input HLS playlist / segment timeouts derive from `inputs[].net.timeout_sec` (defaults 15s playlist / 60s segment).

### 2.7 transcoder

```yaml
transcoder:
  ffmpeg_path:  ""           # ffmpeg binary for auxiliary tasks (e.g. thumbnails). "" = lookup via $PATH.
```

The transcoder runs in-process via libavcodec in a per-stream
`open-streamer-transcoder` subprocess — one decode fans out to N renditions,
so there is no per-process topology to choose. `ffmpeg_path` points at an
ffmpeg binary the server uses only for auxiliary tasks (e.g. thumbnail
capture); the transcode pipeline itself links libavcodec and never shells
out. Changing it hot-restarts running streams.

### 2.8 hooks

```yaml
hooks:
  worker_count:               4     # Default 4. events.Bus worker pool size.
  batch_max_items:            100   # Default. HTTP hook batch size cap.
  batch_flush_interval_sec:   5     # Default. HTTP hook flush timer.
  batch_max_queue_items:      10000 # Default. Per-hook in-memory queue cap.
```

**`worker_count`** sizes the events.Bus worker pool that fans events out
to subscribers. With batched HTTP hook delivery (see below), the hook
handler just enqueues into a per-hook batcher (~µs) so this number rarely
needs tuning — 1-4 covers nearly every workload. Keep at default unless
you have a specific bottleneck. Operators may set 0 to fall back to 4.

**`batch_max_items` / `batch_flush_interval_sec` / `batch_max_queue_items`**
are GLOBAL DEFAULTS for HTTP hooks; each Hook record can override via its
own fields with the same names. Resolution order:
hook value → global → code default.

Per-hook `type` / `target` / `secret` / `max_retries` / `timeout_sec` live
on each Hook record. Two delivery backends are supported:

- **HTTP** webhook (`type: http`, `target: https://…`) — events accumulate
  in a per-hook batcher and ship as a JSON array on `BatchMaxItems` or
  `BatchFlushIntervalSec`, whichever comes first. Optional HMAC signing.
  Failed batches re-queue to the front for the next flush; the queue is
  bounded by `BatchMaxQueueItems` (oldest events dropped on overflow).
- **File** sink (`type: file`, `target: /var/log/open-streamer/events.log`)
  appends one JSON event per line — drop-in for Filebeat / Vector / Promtail
  tail-and-ship pipelines. **Never batched** to keep the JSON-lines
  contract intact.

### 2.9 sessions

```yaml
sessions:
  enabled:           true     # Default false. Disabled = tracker is no-op, /sessions returns empty.
  idle_timeout_sec:  30       # Default. HLS/DASH have no TCP "left" signal; reaper closes after this gap.
  max_lifetime_sec:  0        # 0 = no cap. Hard-close any session older than this even if still active.
  geoip_db_path:     ""       # Reserved for future MaxMind/IP2Location integration. Default no-op resolver returns Country="".
```

Hot-reloadable: `enabled` toggle and timeouts apply on the next reaper
tick (≤ 5s) without restart. In-flight session state is preserved.

When the section is **null / absent** the tracker doesn't run at all
and `/sessions` endpoints return empty payloads — handy for hosts where
operators don't care about per-viewer attribution.

### 2.10 watermarks

```yaml
watermarks:
  dir: /var/lib/open-streamer/watermarks   # Default "./watermarks". Must be writable.
```

Backing store for the `/watermarks` REST API. Each upload writes two
files into this directory:

```text
<dir>/<id>.<ext>      # image bytes (PNG / JPG / GIF)
<dir>/<id>.json       # domain.WatermarkAsset metadata sidecar
```

`os.ReadDir` rebuilds the in-memory registry after restart so the
library is restart-safe with no external database. A corrupt sidecar
skips just that asset (others keep loading) — see logs for `watermarks: rebuild cache`.

Per-asset cap: 8 MiB image / 16 MiB request body (multipart
overhead). Operators wanting a different cap should fork — values are
constants in `domain` to keep the upload path simple.

### 2.11 log

```yaml
log:
  level:  "info"     # trace | debug | info | warn | error  (trace = debug + per-segment / per-frame hot-path logs)
  format: "text"     # text | json
```

Hot-applied — change without restart.

---

## 3. Stream config

Stored per-stream. Edit via `POST /api/v1/streams/{code}` (partial merge).

```yaml
code:        news                # Required. [A-Za-z0-9_/-]+, max 128 chars.
                                 # Slashes namespace streams (e.g. "region/north/news").
                                 # `..` and consecutive `/` are rejected.
template:    null                # Optional. References a Template by its code.
                                 # null = no inheritance. Set to e.g. "news_profile"
                                 # to fill in every config-like field this stream
                                 # leaves at its zero value. See § 4 (Template config).
name:        "News Channel"
description: ""
tags:        ["news", "production"]
stream_key:  ""                  # Auth token for RTMP/SRT push. "" = no auth.
disabled:    false               # true = exclude from boot + reject Start.

inputs:                          # Ordered by priority (must be 0..N-1 contiguous).
  - url:      "rtmp://primary/live"
    priority: 0
    headers:  { "Authorization": "Bearer X" }       # HTTP/HLS only.
    params:   { "passphrase": "..." }               # SRT/S3.
    net:
      timeout_sec:  15    # Per-protocol op budget (HLS playlist GET, RTMP/RTSP/SRT dial). 0 = default.
      insecure_tls: false # HTTPS — allow self-signed (use with care).
  - url:      "rtmp://backup/live"
    priority: 1

transcoder:                      # null = no transcoding (passthrough).
  global:
    hw:        "nvenc"           # none | nvenc | vaapi | qsv | videotoolbox
    fps:       0                 # 0 = match source. Override applies to all profiles.
    gop:       0                 # 0 = derive from KeyframeInterval × fps. Else explicit GOP frames.
    deviceid:  0                 # CUDA device index when HW=nvenc on multi-GPU host.
  decoder:
    name:      ""                # libavcodec decoder name. "" = auto-pick.
  audio:
    copy:        false           # true = passthrough audio (no re-encode).
    codec:       "aac"           # aac | mp3 | opus | ac3 | copy
    bitrate:     128             # kbps
    sample_rate: 0               # Hz. 0 = match source.
    channels:    0               # 1=mono, 2=stereo, 6=5.1. 0 = match source.
    language:    ""              # ISO 639-1 (en, vi, ...).
    normalize:   false           # EBU R128 -23 LUFS.
  video:
    copy:      false             # true = passthrough video.
    interlace: ""                # "" | auto | tff | bff | progressive
    profiles:                    # Ordered ladder. track_1, track_2, …
      - width:             1920
        height:            1080
        bitrate:           5000  # kbps. 0 = encoder default (~2500).
        max_bitrate:       7500  # kbps peak. 0 = no -maxrate emitted (CBR-ish).
        framerate:         30    # 0 = match source.
        keyframe_interval: 2     # GOP target in seconds.
        codec:             ""    # "" / h264 / h265 / vp9 / av1 — auto-routed by hw.
                                 # OR explicit: h264_nvenc / h264_qsv / h264_vaapi / etc.
        preset:            ""    # libx264: ultrafast..placebo. NVENC: p1..p7 + legacy aliases.
                                 # Auto-translated between families; "veryfast" works on NVENC too.
        profile:           ""    # baseline | main | high (H.264). main | main10 (H.265).
        level:             ""    # "3.1", "4.0", "4.1", ...
        bframes:           null  # null = encoder default. 0 = disable. 2-3 = typical VOD.
        refs:              null  # null = encoder default. Higher = better compression, more CPU.
        sar:               ""    # "" = inherit. "N:M" = explicit Sample Aspect Ratio.
        resize_mode:       "pad" # pad | crop | stretch | fit. Default: pad.

protocols:                       # Each true → publisher starts that output.
  hls:  true
  dash: false
  rtsp: false
  rtmp: false
  srt:  false

push:                            # Outbound destinations.
  - url:               "rtmp://rtmp.example.com/live2/{key}"
    enabled:           true
    timeout_sec:       0         # 0 = default (10).
    retry_timeout_sec: 0         # 0 = default (5).
    limit:             0         # 0 = unlimited retries.
    comment: "Live stream"

dvr:                             # null = no DVR.
  enabled:          true
  segment_duration: 0            # 0 = default (4 sec).
  retention_sec:    0            # 0 = forever.
  storage_path:     ""           # "" = default ./out/dvr/{streamCode}.
  max_size_gb:      0            # 0 = no limit.

watermark:                       # Optional text or image overlay applied before encoding.
  enabled: true
  type:    "text"                # text | image
  # --- text ---
  text:       "LIVE %{localtime\\:%H\\:%M}"   # strftime supported (escape libavfilter delimiters)
  font_file:  ""                              # absolute path to .ttf/.otf, or "" for the libavfilter default
  font_size:  24                              # px. 0 → 24
  font_color: "white"                         # libavfilter color syntax
  # --- image ---
  asset_id:   ""                              # references /watermarks library (resolved by coordinator)
  image_path: ""                              # absolute path; mutually exclusive with asset_id
  # --- common ---
  opacity:    1.0                             # 0..1
  position:   "bottom_right"                  # top_left|top_right|bottom_left|bottom_right|center|custom
  offset_x:   10                              # padding from anchor edge (presets only)
  offset_y:   10
  x:          ""                              # raw libavfilter expression — only used when position=custom
  y:          ""                              # e.g. "main_w-overlay_w-50", "if(gt(t,5),10,-100)"

thumbnail: null                  # Schema only — not applied yet.
```

### 3.1 Input URL formats

```
# Pull (server connects out)
rtmp://server.com/live/key
rtsp://camera.local:554/stream
http://cdn.com/live.ts
https://cdn.com/playlist.m3u8
udp://239.1.1.1:5000
srt://relay.com:9999
file:///recordings/loop.ts                # ?loop=true
s3://bucket/path/file.ts                  # ?endpoint=https://minio.local

# Push (encoder connects in)
rtmp://0.0.0.0:1935/live/key              # detected by 0.0.0.0 host
srt://0.0.0.0:9999?streamid=key

# In-process re-stream
copy://upstream_code                       # subscribe to another stream
mixer://video_code?audio=audio_code        # mix video + audio from 2 streams
```

### 3.2 Codec / encoder routing

The user-facing `codec` field is encoder-agnostic — server routes to the right FFmpeg encoder based on `global.hw`:

| codec input | hw=none | hw=nvenc | hw=vaapi | hw=qsv | hw=videotoolbox |
|---|---|---|---|---|---|
| `""` / `h264` / `avc` | libx264 | h264_nvenc | libx264 | libx264 | libx264 |
| `h265` / `hevc` | libx265 | hevc_nvenc | libx265 | libx265 | libx265 |
| `vp9` | libvpx-vp9 | libvpx-vp9 | libvpx-vp9 | libvpx-vp9 | libvpx-vp9 |
| `av1` | libsvtav1 | libsvtav1 | libsvtav1 | libsvtav1 | libsvtav1 |

For VAAPI/QSV/VT operators must spell the full encoder name explicitly (`h264_vaapi`, `h264_qsv`, `h264_videotoolbox`) — empty codec defaults to libx264 on those backends.

The full routing table is in `GET /api/v1/config/defaults` for frontend lookup.

### 3.3 Watermark — text vs image, asset_id vs image_path

Two ways to reference an image watermark:

| Field | Source | Lifecycle |
|---|---|---|
| `asset_id` | uploaded via `POST /watermarks` and stored under `watermarks.dir` | managed via REST; resolves to absolute path at `tc.Start` |
| `image_path` | absolute path to a file the open-streamer process can read | operator stages on host out-of-band |

The two are **mutually exclusive** — set one, leave the other empty.
The API rejects configs that set both with `INVALID_WATERMARK`.

**Position presets** treat `offset_x` / `offset_y` as inward edge
padding. Center ignores offsets:

```text
top_left     → x=offset_x,            y=offset_y
top_right    → x=W-w-offset_x,        y=offset_y
bottom_left  → x=offset_x,            y=H-h-offset_y
bottom_right → x=W-w-offset_x,        y=H-h-offset_y
center       → x=(W-w)/2,             y=(H-h)/2
```

**Custom position** (`position=custom`) hands raw libavfilter expressions
to drawtext / overlay — full power, no preset clipping:

```yaml
position: "custom"
x: "main_w-overlay_w-50"      # 50 px in from right
y: "if(gt(t,5),10,-100)"      # off-screen for first 5s, then 10px from top
```

Variables exposed (per libavfilter docs):

- drawtext (text): `w`/`h` = frame size, `tw`/`th` = rendered text size, `t` = clock time
- overlay (image): `W`/`H` = main video size, `w`/`h` = overlay image size, `t` = clock time

**Opacity** is folded into the filter:

- text → `fontcolor=<color>@<opacity>` (libavfilter native)
- image → `colorchannelmixer=aa=<opacity>` after the `movie=` source

**GPU pipeline** (NVENC) automatically wraps the watermark with
`hwdownload,format=nv12` ↔ `hwupload_cuda` because `drawtext` is
CPU-only and `overlay_cuda` requires `--enable-cuda-nvcc` (Ubuntu apt
ffmpeg skips it). Cost: ~5% of one CPU core per rendition at 1080p25.

**Hot-reload behaviour**: editing any watermark field on a running
stream forces a transcoder topology reload (~2-3s downtime per stream)
because the libavfilter graph is fixed when the encoder is built. The diff
engine routes any `Stream.Watermark` change through `reloadTranscoderFull`
so the new filter graph applies cleanly. Watermark on a passthrough stream
(no transcoder) is silently inert — no transcoder runs.

### 3.4 Preset translation

Cross-family preset names are auto-translated so the value works on whichever encoder the codec/HW selection resolves to:

| User input | NVENC encoder | libx264/libx265/QSV |
|---|---|---|
| `ultrafast` / `superfast` | `p1` | passthrough |
| `veryfast` | `p2` | passthrough |
| `faster` / `fast` | `p3` / passthrough (legacy alias) | passthrough |
| `medium` | passthrough (legacy alias) | passthrough |
| `slow` | passthrough (legacy alias) | passthrough |
| `slower` | `p6` | passthrough |
| `veryslow` / `placebo` | `p7` | passthrough |
| `p1` — `p7` | passthrough | translated to nearest libx264 name |

VAAPI / VideoToolbox don't have `-preset` — value dropped silently (encoder uses its own defaults).

---

## 4. Template config

Stored separately from streams (`/api/v1/templates` endpoint, top-level
`templates` key in JSON / YAML store). A template bundles config-like
stream fields; streams reference one via `Stream.template` and inherit
every field they leave at the zero value. Template code is
`[A-Za-z0-9_-]+` (no `/` — flat namespace), max 128 chars.

```yaml
code:        news_profile        # Required. [A-Za-z0-9_-]+, max 128 chars.
name:        "News profile"      # Operator-facing label.
description: ""
tags:        ["news"]            # Inherited by streams that leave tags empty.
stream_key:  ""                  # Inherited push auth token; "" = no inheritance.

prefixes:    ["live"]            # Optional. Each URL-path prefix that triggers
                                 # auto-publish. Match honours segment boundaries —
                                 # "live" matches "live/foo/bar" but NOT
                                 # "livestream/foo". Prefixes are globally unique
                                 # across templates (POST returns 409 PREFIX_OVERLAP
                                 # when any prefix is a path-prefix of another's).
                                 # Requires at least one publish:// in `inputs`
                                 # for auto-publish to actually materialise streams.

inputs:                          # Inherited by streams that leave inputs empty.
  - url:      "publish://"       # Required for prefix-driven auto-publish to fire.
    priority: 0
                                 # Same Input shape as a Stream — see § 3.

transcoder:  { ... }             # All same shapes as Stream config — see § 3.
protocols:   { hls: true, dash: true }
push:        [ ... ]
dvr:         { enabled: true, retention_sec: 86400 }
watermark:   { ... }
thumbnail:   null
```

**Inheritance rule** (`domain.ResolveStream`): zero value on the stream
= inherit. Pointer fields (`transcoder`, `dvr`, `watermark`,
`thumbnail`) inherit when null. Slice fields (`inputs`, `tags`,
`push`) inherit when empty. String fields (`name`, `description`,
`stream_key`) inherit when empty. The `protocols` struct inherits when
ALL its bool flags are false. Non-inheritable: `code` (per-stream
identity) and `disabled` (per-stream runtime toggle).

**Storage**: persisted in the same backend as streams (JSON / YAML)
under the top-level `templates` map. Survives restarts. Runtime
streams materialised via prefix auto-publish are NOT persisted — they
live in memory only and the idle reaper stops them 30 s after the
last packet.

**Endpoints**: `GET /templates` · `GET /templates/{code}` ·
`POST /templates/{code}` (upsert) · `DELETE /templates/{code}`.
`DELETE` refuses with 409 `TEMPLATE_IN_USE` when any stream still
references the template — detach the streams first.

---

## 5. Hook config

```yaml
id:           "ops-pager"           # Required. Unique hook ID.
name:         "Pager for ops"
type:         "http"                # http | file
target:       "https://ops/events"  # http(s)://… URL OR absolute file path
                                    # (e.g. /var/log/open-streamer/events.log)
secret:       "shared-secret"       # HMAC-SHA256 signing for HTTP. Empty = no signing.
event_types:  ["stream.stopped", "transcoder.error", "input.failover"]
                                    # [] = all events
stream_codes:                       # null = all streams
  only:   ["news", "sports"]
  except: ["test_*"]                # only / except mutually exclusive — only wins
metadata:                           # merged into every payload as `metadata.*`
  environment: "prod"
  region:      "ap-southeast-1"
enabled:      true
max_retries:  3                     # Per-attempt retry budget. 0 = use default (3).
timeout_sec:  10                    # Per-attempt deadline. 0 = use default (10).

# HTTP-only batching knobs (override global hooks.batch_*).
batch_max_items:           100      # 0 = use global / code default.
batch_flush_interval_sec:  5        # 0 = use global / code default.
batch_max_queue_items:     10000    # 0 = use global / code default.
```

For event payload schemas see [APP_FLOW.md § Events reference](./APP_FLOW.md#events-reference).

---

## 6. Defaults reference

Single source of truth: [internal/domain/defaults.go](../internal/domain/defaults.go).

| Field | Default | Sentinel meaning of 0 / "" |
|---|---|---|
| `buffer.capacity` | 1024 | — |
| `manager.input_packet_timeout_sec` | 30 | — |
| `publisher.hls.live_segment_sec` | 2 | — |
| `publisher.hls.live_window` | 12 | — |
| `publisher.hls.live_history` | 0 | drop after sliding out |
| `publisher.hls.live_ephemeral` | false | — |
| (DASH same defaults as HLS) |  | |
| `listeners.rtmp.port` | 1935 | — |
| `listeners.rtmp.listen_host` | "0.0.0.0" | all interfaces |
| `listeners.rtsp.port` | 554 | — |
| `listeners.rtsp.transport` | "tcp" | — |
| `listeners.srt.port` | 9999 | — |
| `listeners.srt.latency_ms` | 120 | — |
| `ingestor.hls_max_segment_buffer` | 8 | — |
| `ingestor.rtmp_timeout_sec` | 10 | — |
| `ingestor.rtsp_timeout_sec` | 10 | — |
| `ingestor.hls_playlist_timeout_sec` | 15 | — |
| `ingestor.hls_segment_timeout_sec` | 60 | — |
| `timeline.Normaliser.JumpThresholdMs` (server-internal) | 2000 | server-level invariant; not exposed in YAML/API. Drift gap (output PTS vs `max(actualNow, lastOutputDts)`) above which the Normaliser hard re-anchors a track |
| `timeline.Normaliser.MaxAheadMs` (server-internal) | 0 | **0 = drop semantics disabled.** Setting >0 makes the Normaliser drop incoming AV packets whose proposed output PTS sits more than `N` ms ahead of `now − wallOrigin`. Disabled by default because the drop has a stuck-state pathology when sustained drift exceeds the cap (drift never decreases for real-time input → every subsequent packet drops). DASH packager's `behindPrevSegEnd` pacing gate compensates downstream instead |
| `timeline.Normaliser.MaxBehindMs` (server-internal) | 3000 | Hard-re-anchor a track when the proposed output sits more than `N` ms behind wallclock. Enabled by default at 3 s — re-anchoring jumps the track FORWARD onto wallclock so there is no stuck-state pathology (unlike MaxAheadMs). Catches the long-runtime A/V split when one track seeds late or pauses while the other keeps flowing (e.g. RTSP relays whose audio appeared minutes after stream start). Set to 0 to disable |
| `timeline.Normaliser.CrossTrackSnapMs` (server-internal) | 1000 | When a track seeds and the OTHER track has already moved more than this many ms of output PTS, snap this track's `outputAnchor` onto the other's `lastOutputDts` so V and A start in lockstep |
| `transcoder.ffmpeg_path` | "ffmpeg" | ffmpeg binary for auxiliary tasks (e.g. thumbnails) |
| `transcoder.video.bitrate_k` | 2500 | — |
| `transcoder.video.resize_mode` | "pad" | — |
| `transcoder.audio.codec` | "aac" | — |
| `transcoder.audio.bitrate_k` | 128 | — |
| `transcoder.global.hw` | "none" | — |
| `transcoder.global.deviceid` | 0 | CUDA device 0 |
| `dvr.segment_duration` | 4 | — |
| `dvr.storage_path_template` | `./out/dvr/{streamCode}` | — |
| `dvr.retention_sec` | — | **0 = forever** |
| `dvr.max_size_gb` | — | **0 = unlimited** |
| `push.timeout_sec` | 10 | — |
| `push.retry_timeout_sec` | 5 | — |
| `push.limit` | — | **0 = unlimited retries** |
| `hook.max_retries` | 3 | 0 = use default |
| `hook.timeout_sec` | 10 | 0 = use default |
| `hooks.worker_count` | 4 | 0 = 4 |
| `hooks.batch_max_items` (HTTP) | 100 | 0 = 100 |
| `hooks.batch_flush_interval_sec` (HTTP) | 5 | 0 = 5 |
| `hooks.batch_max_queue_items` (HTTP) | 10000 | 0 = 10000 |
| `sessions.idle_timeout_sec` | 30 | 0 = use default |
| `sessions.max_lifetime_sec` | — | **0 = no cap** |
| `watermarks.dir` | `./watermarks` | empty = `./watermarks` |
| `watermark.font_size` | 24 | 0 = 24 |
| `watermark.font_color` | `white` | empty = white |
| `watermark.opacity` | 1.0 | 0 = 1.0 |
| `watermark.position` | `bottom_right` | empty = bottom_right |
| `watermark.offset_x/_y` (presets) | 10 | 0 = 10 (Center keeps 0) |

---

## 7. Worked examples

### Pull HLS, transcode 3 rungs NVENC, publish HLS + DASH + RTSP, push to multiple live destinations, record DVR

```yaml
code: tv1
inputs:
  - url: "https://upstream.example.com/tv1.m3u8"
    priority: 0
transcoder:
  global: { hw: "nvenc" }
  audio:  { codec: "aac", bitrate: 128 }
  video:
    profiles:
      - { width: 1920, height: 1080, bitrate: 5000, framerate: 30, keyframe_interval: 2, preset: "p4", profile: "high", level: "4.1" }
      - { width: 1280, height:  720, bitrate: 2500, framerate: 30, keyframe_interval: 2, preset: "p4", profile: "main" }
      - { width:  854, height:  480, bitrate: 1200, framerate: 30, keyframe_interval: 2, preset: "p4", profile: "main" }
protocols:
  hls:  true
  dash: true
  rtsp: true
push:
  - { url: "rtmp://rtmp.example.com/live2/STREAM_KEY",     enabled: true, comment: "Live stream" }
  - { url: "rtmp://rtmp.example.com/app/STREAM_KEY",       enabled: true, comment: "Live stream"  }
dvr:
  enabled:          true
  segment_duration: 4
  retention_sec:    604800     # 7 days
  max_size_gb:      200
```

### RTMP push ingest from an encoder, copy passthrough to HLS

```yaml
code: obs_feed
inputs:
  - url: "rtmp://0.0.0.0:1935/live/secret_key"   # 0.0.0.0 host = push listen
    priority: 0
protocols:
  hls: true
# no transcoder — copy passthrough
```

Encoder configuration: server `rtmp://your-server:1935/live`, key `secret_key`.

### Multi-input failover with manual override

```yaml
code: redundant_feed
inputs:
  - { url: "rtmp://primary/live",   priority: 0 }
  - { url: "rtmp://backup/live",    priority: 1 }
  - { url: "https://cdn/live.m3u8", priority: 2 }
manager:                  # Per-stream manager override is NOT supported — uses global config.
                          # See global_config.manager.input_packet_timeout_sec instead.
protocols: { hls: true }
```

Force priority 1 manually:
```bash
curl -XPOST /api/v1/streams/redundant_feed/inputs/switch -d '{"priority": 1}'
```

The manager records this as `runtime.switches[0]={ reason: "manual", to: 1 }`.

### copy:// re-stream

```yaml
# Stream A is the source (encoded once)
code: news_master
inputs:
  - { url: "rtmp://upstream/news", priority: 0 }
transcoder:
  global: { hw: "nvenc" }
  video:  { profiles: [ {width:1280,height:720,bitrate:2500}, {width:854,height:480,bitrate:1200} ] }
protocols: { hls: true }

# Stream B re-streams A as DASH (no re-encode, subscribes to A's published rendition buffers)
code: news_dash
inputs:
  - { url: "copy://news_master", priority: 0 }
protocols: { dash: true }
```

`news_dash` shares `news_master`'s ABR ladder via in-process buffer
subscription — zero extra encode cost.

### Add a profile to the ladder

```bash
# Original config: 1080p + 720p
# Add 480p to the ladder.

curl -XPOST /api/v1/streams/news -d '{
  "transcoder": {
    "video": {
      "profiles": [
        { "width": 1920, "height": 1080, "bitrate": 5000 },
        { "width": 1280, "height": 720,  "bitrate": 2500 },
        { "width": 854,  "height": 480,  "bitrate": 1200 }
      ]
    }
  }
}'
```

The diff engine identifies the ladder change → restarts the stream's
transcoder subprocess (the renditions share one decode), leaving other
streams and protocols untouched. HLS master playlist
updates with the new variant within ~50ms. DASH ABR MPD includes the
new track on next segment flush.

---

## See also

- [USER_GUIDE.md](./USER_GUIDE.md) — install, common workflows
- [ARCHITECTURE.md](./ARCHITECTURE.md) — what each subsystem does
  internally
- [APP_FLOW.md](./APP_FLOW.md) — request lifecycles + event sequences
- [FEATURES_CHECKLIST.md](./FEATURES_CHECKLIST.md) — completion status
