# Open Streamer

[![Go Reference](https://pkg.go.dev/badge/github.com/datvietvac-techhub/open-streamer.svg)](https://pkg.go.dev/github.com/datvietvac-techhub/open-streamer)
[![CI](https://github.com/datvietvac-techhub/open-streamer/actions/workflows/ci.yml/badge.svg)](https://github.com/datvietvac-techhub/open-streamer/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/datvietvac-techhub/open-streamer)](https://goreportcard.com/report/github.com/datvietvac-techhub/open-streamer)
[![Coverage](https://codecov.io/gh/datvietvac-techhub/open-streamer/branch/main/graph/badge.svg)](https://codecov.io/gh/datvietvac-techhub/open-streamer)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A high-availability live media server in Go. Ingests from any
common protocol, normalises through an internal MPEG-TS pipeline,
optionally transcodes in-process via libavcodec, and publishes over HLS, DASH, RTMP,
RTSP, and SRT — all from one binary, one process per host.

```mermaid
flowchart LR
    subgraph Ingest
        S1["RTMP / RTSP / SRT"]
        S2["HLS / HTTP / UDP"]
        S3["File / S3"]
        S4["copy:// / mixer://"]
    end

    subgraph Pipeline
        Hub(("Buffer Hub")):::data
        Tx["Transcoder<br/>libavcodec subprocess"]
        DVR["DVR + events"]
        Hub --> Tx
        Tx --> Hub
        Hub --> DVR
    end

    subgraph Deliver
        D1["HLS / DASH"]
        D2["RTMP / RTSP / SRT"]
        D3["Push out — RTMP / RTMPS"]
        D4["Webhooks (HMAC)<br/>or file sink"]
    end

    S1 --> Hub
    S2 --> Hub
    S3 --> Hub
    S4 --> Hub

    Hub --> D1
    Hub --> D2
    Hub --> D3
    DVR -.->|"events"| D4

    Mgr["Stream Manager<br/>failover — no transcoder restart"]
    Mgr -.->|"health"| Ingest

    classDef data fill:#5a3a1f,stroke:#e0a060,color:#fff
```

---

## Why Open Streamer

- **No process-per-stream.** One Go process handles N streams across N
  goroutines. Transcoding runs in-process (libavcodec) in a per-stream
  subprocess; ingest is pure Go.
- **Failover at the Go level.** When an input degrades, the Stream
  Manager swaps to the next-priority source in ~150ms without
  restarting the transcoder. Buffer continuity → players see one
  `#EXT-X-DISCONTINUITY` and resume.
- **Hot-reload by diff.** `PUT /streams/{code}` restarts only what
  changed — adding a push destination doesn't disturb HLS viewers,
  toggling DASH doesn't drop RTMP push sessions.
- **Write never blocks.** The Buffer Hub fan-out is non-blocking —
  slow consumers drop packets, the writer is shielded. One stuck DVR
  cannot freeze every viewer.
- **Self-healing.** Transcoder-subprocess crashes restart with exponential
  backoff forever; the pipeline never tears down. Health detection flips
  status to `degraded` after sustained failure so ops sees it.

---

## Quick start

```bash
# 1. Install via the systemd installer (Linux)
sudo bash <(curl -sL https://raw.githubusercontent.com/datvietvac-techhub/open-streamer/main/build/reinstall.sh) v1.0.0

# 2. Or build from source
git clone https://github.com/datvietvac-techhub/open-streamer.git
cd open-streamer
make build && ./bin/open-streamer

# 3. Or Docker
make compose-up
```

Then create a stream:

```bash
curl -XPOST http://localhost:8080/api/v1/streams/news -d '{
  "inputs":   [{ "url": "https://upstream/playlist.m3u8", "priority": 0 }],
  "protocols": { "hls": true }
}'
```

Stream is live at `http://localhost:8080/news/index.m3u8`. Full setup
walkthrough in [USER_GUIDE.md](./docs/USER_GUIDE.md).

---

## Documentation

| Doc | Audience | What's in it |
|---|---|---|
| [USER_GUIDE.md](./docs/USER_GUIDE.md) | Operator | Install, create streams, hot-reload, hooks, troubleshooting |
| [CONFIG.md](./docs/CONFIG.md) | Operator | Every config field with examples + defaults reference |
| [ARCHITECTURE.md](./docs/ARCHITECTURE.md) | Contributor | Subsystem design, invariants, data flow |
| [APP_FLOW.md](./docs/APP_FLOW.md) | Contributor / Ops | Step-by-step traces (boot, failover, transcoder crash, hot-reload) + full events reference |
| [EVENTS.md](./docs/EVENTS.md) | Operator / Integrator | Catalogue of every domain event, payload shape, hook recipes |
| [METRICS.md](./docs/METRICS.md) | Operator / SRE | Prometheus metrics reference + dashboard / alert PromQL examples |
| [FEATURES_CHECKLIST.md](./docs/FEATURES_CHECKLIST.md) | Everyone | What's implemented today, what's planned, what's locked-out |

API spec auto-generated at `/swagger/` (run `make swagger` to
regenerate from annotations).

---

## Highlights

- **URL-driven ingest** — protocol + push/pull mode detected from
  scheme/host. Supports RTMP, RTSP, SRT, UDP/multicast, HLS, raw
  HTTP-TS, file (with loop), S3, plus `copy://` (in-process re-stream)
  and `mixer://` (combine video + audio from two streams).
- **Multi-input failover** — N inputs per stream, prioritised. Manager
  monitors health, switches transparently. Last 20 switches recorded
  with reason (`error` / `timeout` / `manual` / `failback` / `recovery`
  / `input_added` / `input_removed`).
- **ABR transcoding** — in-process libavcodec with NVENC / VAAPI / QSV /
  VideoToolbox. Per-rung profiles (resolution, bitrate, codec, preset,
  GOP, B-frames, refs, SAR, resize mode). Full-GPU pipeline
  (NVDEC → scale_cuda → NVENC, no hwdownload round-trip) on NVENC hosts.
- **Single decode, N renditions** — one `open-streamer-transcoder`
  subprocess per stream decodes once and fans out to every rung, so an
  N-rung ladder costs 1×decode + N×encode.
- **Seamless input switch** — on failover the subprocess swaps only its
  decoder; the encoders stay alive, so players don't re-initialise.
- **HLS + DASH ABR** — master playlist + per-track variants;
  `#EXT-X-DISCONTINUITY` per failover.
- **RTSP / RTMP / SRT play** — shared listeners (one port per
  protocol). Single-segment codes use the `live/` prefix:
  `rtmp://host/live/news`. Multi-segment codes are addressed at their
  raw path: `rtmp://host/region/north/news`. The server strips a
  leading `live/` when present, so either form reaches multi-segment
  streams.
- **Templates** — reusable bundle of stream config (transcoder,
  protocols, push, DVR, watermark, thumbnail, inputs, tags,
  stream-key). Reference one from a stream via the `template` field
  and the stream inherits every config-like field it leaves at the
  zero value. Updating the template hot-reloads every running stream
  that inherits from it.
- **Auto-publish** — a template can declare URL-path prefixes. When
  an encoder pushes to a path matching one of the prefixes and the
  template carries a `publish://` input, the server materialises a
  **runtime stream** on the fly. Runtime streams are RAM-only, appear
  in `GET /streams` with `source: "runtime"`, and disappear 30 s
  after the last packet.
- **Push out** — RTMP/RTMPS to platforms (popular live platforms,
  CDN). Per-destination state visible at
  `runtime.publisher.pushes[]`.
- **DVR + Timeshift** — persistent recording per stream; resume across
  restarts; absolute / relative timeshift VOD endpoints; size + time
  retention.
- **Watermarks** — text (drawtext + strftime) or image (overlay) per
  stream. Position presets + raw libavfilter expressions for full
  flexibility (animated, time-aware). Image asset library with REST
  upload (`POST /watermarks`). Pure-GPU pipeline auto-bridges via
  hwdownload/hwupload_cuda for portability.
- **Play sessions** — track every viewer across HLS / DASH / RTMP /
  SRT / RTSP. Fingerprint dedup for pull protocols, UUID for
  connection-bound. Idle reaper, kick API, hot-reload config,
  `session.opened`/`closed` events on the bus.
- **Webhooks + file sink** — domain events delivered via HTTP (HMAC
  signed) or appended as JSON-lines to a local log file (drop-in for
  Filebeat / Vector / Promtail). Per-hook retries, event/stream filters,
  metadata injection.
- **Transcoder capability probe** — boot + on-demand check for
  required/optional encoders; UI sees a checklist before saving.
- **Pluggable storage** — JSON flat-file (default) or YAML single-document.
- **Prometheus metrics** + structured slog logging.

Full feature matrix in [FEATURES_CHECKLIST.md](./docs/FEATURES_CHECKLIST.md).

---

## Development

```bash
make build          # → bin/open-streamer
make run            # run without persisting binary
make test           # go test -race -shuffle=on -count=1 -timeout=5m ./...
make lint           # golangci-lint run ./...
make check          # tidy + vet + lint + test (full local CI)
make swagger        # regenerate api/docs from swag annotations
make hooks-install  # install pre-commit hook (auto-regen swagger)
```

Single test: `go test -run TestName ./internal/<pkg>/...`

Requires Go 1.25.x+. Transcoding links libavcodec (FFmpeg's libraries) into
the `open-streamer-transcoder` binary — the builder image and release
bundles provide them; the boot probe catches missing required encoders.

Repository layout:

```
cmd/server/           # main entrypoint
internal/
  api/                # chi router + handlers
  api/handler/        # HTTP handlers (incl. /templates CRUD)
  autopublish/        # template-prefix matcher + runtime stream registry + idle reaper
  buffer/             # ring buffer + fan-out
  coordinator/        # pipeline lifecycle + diff engine
  ingestor/           # pull workers (RTMP/RTSP/SRT/HLS/...) + push servers
  manager/            # input failover state machine
  transcoder/         # libavcodec subprocess supervisor + native pipeline + watermarks
  publisher/          # HLS/DASH segmenters + serve listeners + push out
  dvr/                # recording + retention + timeshift
  events/             # in-process event bus
  hooks/              # webhook (HTTP) + file sink delivery
  sessions/           # play-session tracker (HLS/DASH/RTMP/SRT/RTSP viewers)
  watermarks/         # asset library backing /watermarks REST API
  domain/             # types + defaults + resolvers (incl. Template / ResolveStream)
  store/              # repository pattern (json / yaml backends; Stream + Template repos)
  runtime/            # service lifecycle wrapper
config/               # bootstrap config (storage backend selection)
build/                # systemd unit + installer
bench/                # capacity sweep tooling (sample.sh / run-all.sh / aggregate.sh)
docs/                 # → see Documentation table above
```

---

## Contributing

PRs welcome. Before submitting:

1. `make hooks-install` — installs the pre-commit hook that
   auto-regenerates swagger when Go files change
2. `make check` — runs full local CI (tidy + vet + lint + tests)
3. Match the project's design invariants documented in
   [ARCHITECTURE.md § Design mindset](./docs/ARCHITECTURE.md#1-design-mindset)

Tests are required for new features. The native transcoder's libavcodec
stages (decoder / encoder / scaler / pipeline) have table tests that link
against libav — exercised in the builder image (`Dockerfile.builder`).

---

## License

MIT — see [LICENSE](./LICENSE).
