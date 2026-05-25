# Open-Streamer — Feature Checklist

Snapshot of what's implemented today, organised by subsystem. For
end-to-end pipeline flow see [APP_FLOW.md](./APP_FLOW.md); for design
rationale see [ARCHITECTURE.md](./ARCHITECTURE.md); for operator-facing
config see [CONFIG.md](./CONFIG.md).

Legend:

| Level | Meaning |
|---|---|
| **Complete** | Implemented and usable for the described scope |
| **Partial** | Works with known limitations or narrow codec/path support |
| **Schema only** | Domain / API / persistence fields exist; not wired into live pipeline |
| **Planned** | Documented intent only |

---

## Core Platform

| Feature | Status | Notes |
|---|---|---|
| Layered configuration (file + env) | Complete | `config.yaml` + `OPEN_STREAMER_*` env vars; only StorageConfig from file, rest from store |
| Dependency injection | Complete | `samber/do/v2`; all services in `cmd/server/main.go` |
| Structured logging | Complete | `slog` with text / json format; level configurable |
| Graceful shutdown | Complete | SIGINT/SIGTERM with 10s timeout; reverse-order teardown |
| Prometheus metrics | Complete | Per-stream uptime, bytes/packets, failovers, restarts, active workers, buffer depth |
| Hardware detection | Complete | `internal/hwdetect` probes /dev for NVIDIA / DRI / Intel — listed in `/config.hw_accels` |
| Transcoder capability probe | Stub | `POST /config/transcoder/probe` is a no-op (always reports ok); encoder availability is fixed when the `open-streamer-transcoder` binary is built |
| Build version stamping | Complete | `pkg/version` injected at compile via Makefile ldflags / Release workflow |

---

## Storage & API

| Feature | Status | Notes |
|---|---|---|
| Stream repository — JSON | Complete | Default; flat-file under `storage.json_dir` |
| Stream repository — YAML | Complete | Single `open_streamer.yaml` per data dir |
| Template repository | Complete | Both JSON + YAML backends; persisted under top-level `templates` key |
| Recording / Hook / VOD repositories | Complete | Both backends |
| REST API — streams CRUD + start/stop/restart | Complete | `chi/v5` router under `/streams`. Stream codes may contain `/` (namespacing) — the dispatcher uses chi's catch-all and splits the trailing `/restart` / `/switch` action off the code |
| REST API — templates CRUD | Complete | `/templates` list + `/templates/{code}` get/post(upsert)/delete. `Put` validates prefix uniqueness across templates (409 `PREFIX_OVERLAP`) and hot-reloads every running stream referencing the template via `coordinator.Update`. `Delete` refuses (409 `TEMPLATE_IN_USE`) when any stream still references it |
| REST API — `PUT /streams/{code}` hot-reload | Complete | Diff-based; only changed components restart. Validates `Stream.Template` references exist (400 `TEMPLATE_NOT_FOUND`) so orphan references can't be persisted |
| REST API — input switch | Complete | `POST /streams/{code}/inputs/switch` forces active priority |
| REST API — recordings | Complete | CRUD + unified `/{file}` dispatch (playlist.m3u8 vs timeshift via `?from=` / `?offset_sec=` query) + segment serve + info |
| REST API — hooks CRUD + test (HTTP & File) | Complete | `DeliverTestEvent` routes per hook type |
| REST API — config GET/POST | Complete | `/config` static enums + GlobalConfig; POST hot-applies |
| REST API — config defaults | Complete | `GET /config/defaults` returns implicit values for UI placeholders (incl. encoder routing table per HW) |
| REST API — config YAML editor | Complete | `GET/PUT /config/yaml` round-trips entire system state |
| REST API — VOD mounts | Complete | Browse on-disk recordings outside DVR scope |
| REST API — Watermark assets | Complete | `/watermarks` library: list / upload / get / raw / delete (mirrors VOD UX) |
| REST API — Play sessions | Complete | `/sessions`, `/streams/{code}/sessions`, `/sessions/{id}` (kick), filters by proto / status / limit |
| OpenAPI / Swagger | Complete | Spec served at `/swagger/`; `make swagger` regenerates |
| Static delivery — HLS / DASH | Complete | `/{code}/index.m3u8`, `/{code}/index.mpd`, `/{code}/*` (wrapped with sessions middleware when tracker enabled) |
| Health probes | Complete | `/healthz`, `/readyz` |
| CORS | Complete | Configurable origins/methods/headers/credentials |

---

## Buffer Hub

| Feature | Status | Notes |
|---|---|---|
| In-memory ring buffer per stream | Complete | Fan-out via independent `Subscriber`; write never blocks |
| Raw ingest buffer (`$raw$<code>`) | Complete | Created when transcoder is active |
| Rendition buffers (`$r$<code>$track_N`) | Complete | One per ABR ladder rung |
| Slow-consumer packet drop | Complete | `default:` in fan-out — ingestor never blocked |
| `PlaybackBufferID` resolver | Complete | Picks best rendition for ABR, else logical stream code |
| Capacity tunable | Complete | `buffer.capacity` (default 1024) |
| `Delete` closes subscriber channels | Complete | Subscribers see `ok=false` on `<-Recv()` so mixer/copy taps observe upstream tear-down and reconnect — fix for the "downstream silently dies after upstream restart" class of bugs |

---

## Templates & Auto-Publish (`internal/domain/template.go` + `internal/autopublish`)

Templates are reusable bundles of config-like stream fields. A stream
references at most one template via the `template` field; every field
the stream leaves at its zero value inherits from the template. The
non-inheritable fields are `Code` (per-stream identity) and `Disabled`
(per-stream runtime toggle). For the merge semantics and resolution
sites see [ARCHITECTURE.md § Templates](./ARCHITECTURE.md#templates-internaldomaintemplatego--internalapihandlertemplatego);
for the on-the-wire URL conventions see [URL routing](#stream-codes--url-routing) below.

| Feature | Status | Notes |
|---|---|---|
| Template domain model | Complete | Bundles `Inputs`, `Tags`, `StreamKey`, `Transcoder`, `Protocols`, `Push`, `DVR`, `Watermark`, `Thumbnail`, `Prefixes`. Code regex `[A-Za-z0-9_-]+` (no `/` — flat namespace) |
| `domain.ResolveStream(stream, tpl)` merge | Complete | Zero-value = inherit: nil pointer / empty slice / empty string / all-zero `OutputProtocols`. Stream's non-zero value always wins. Returns a copy when merge happens; same pointer when no template / no merge needed |
| Template CRUD endpoints | Complete | `GET /templates`, `GET /templates/{code}`, `POST /templates/{code}` (upsert), `DELETE /templates/{code}` |
| Cross-template prefix uniqueness | Complete | `Put` scans every other template via `templateRepo.List` and returns 409 `PREFIX_OVERLAP` with `conflicting_with` + `overlaps` payload when any prefix is a path-prefix of another's prefix |
| Template hot reload on update | Complete | `Put` walks every stream referencing the template, computes resolved view under OLD and NEW template, and dispatches `coordinator.Update(old, new)` for every RUNNING dependent. Coordinator's diff engine then routes the change minimally; stopped streams skip the reload |
| Template `Delete` reference guard | Complete | Returns 409 `TEMPLATE_IN_USE` with `streams[]` payload when any stream still references the template. Operators must detach (`POST /streams/{code}` with `template: null`) before retrying |
| Stream `Template` reference validation | Complete | `StreamHandler.Put` returns 400 `TEMPLATE_NOT_FOUND` when `body.Template` points at a missing template. Silently persisting orphan references would leave the runtime resolver unable to fill inherited fields |
| Resolution at bootstrap + reconciler | Complete | `coordinator.BootstrapPersistedStreams` and `Coordinator.reconcileOnce` both call `c.resolveTemplate(ctx, s)` before `Start` so pipelines see the inherited config across restarts |
| API responses are raw | Complete | `GET /streams` / `GET /streams/{code}` return the on-disk record unchanged — overrides only, never merged with the template. Clients that want the effective config fetch the template separately |
| Stream response `source` tag | Complete | Every entry carries `source: "config" \| "runtime"` so clients can filter / label correctly |
| `GET /streams/{code}` runtime-stream fallback | Complete | When the on-disk repo misses, the handler calls `autopublish.Lookup` and returns a stub `{code, template, source: "runtime"}` so a runtime stream resolves to 200 instead of 404 |

### Auto-publish (runtime streams via template prefix)

| Feature | Status | Notes |
|---|---|---|
| Prefix list on template (`Prefixes []string`) | Complete | URL-path prefixes that trigger auto-publish. Match honours segment boundaries — `live` matches `live/foo/bar` but NOT `livestream/foo` (`domain.PrefixMatches`) |
| Prefix format validation | Complete | `[A-Za-z0-9_/-]+`, no leading `/`, no `//`, no `..`, max 128 chars; per-template duplicates rejected at `ValidatePrefixes` |
| Matcher snapshot (`internal/autopublish/matcher.go`) | Complete | `prefix → templateCode` map built from `templateRepo.List`; held in `atomic.Pointer[matcher]` so push-server-goroutine lookups are lock-free. Entries sorted by descending prefix length (longest match wins) |
| Matcher hot reload | Complete | `Service.RefreshTemplates` swaps the snapshot atomically after every template `Put` / `Delete` from the handler |
| Push-server fallback | Complete | RTMP push server (`acquireOrAutoPublish`): on `registry.Acquire` miss, calls `AutoPublishResolver.ResolveOrCreate(ctx, path)`. Pass nil to disable auto-publish entirely (legacy "stream not registered" rejection). SRT push is not wired (no SRT push server exists); RTSP push is not a feature |
| Runtime stream materialisation | Complete | `ResolveOrCreate`: validate stream code → verify matched template has a `publish://` input (`TemplateAcceptsPush`) → resolve stub `{Code, Template}` against template → `coordinator.Start(resolved)` → record entry + spawn liveness observer. Write lock held across Start + insert so concurrent pushes to the same path collapse to a single Start |
| `ErrNoMatch` / `ErrTemplateNoPush` sentinels | Complete | `ResolveOrCreate` returns these for missing prefix and template-without-publish cases; caller surfaces as a publish rejection |
| Runtime stream identity | Complete | Stream code = full incoming push path (no prefix stripping). E.g. push `rtmp://host/live/foo/bar` with prefix `live/` → runtime stream code `live/foo/bar` |
| Idle reaper (30 s) | Complete | `Service.RunReaper` sweeps every 5 s and stops any entry whose `lastPacketAt` < `now − 30 s` via `coordinator.Stop` + entry removal + observer cancel. `IdleTimeout` + `reapInterval` exported as constants for tests |
| Liveness observer | Complete | Per-entry goroutine subscribes to the buffer hub for the runtime stream's code; updates `entry.lastPacketAt` (atomic.Int64) on every packet. Detached from the request context so it outlives the push callback |
| Runtime stream events | Complete | `stream.runtime_created` on materialisation, `stream.runtime_expired` on idle eviction. Payload carries `template_code` |
| API visibility in `GET /streams` | Complete | `listRuntimeStreams()` emits a `{Code, Template}` stub per entry; `withStatus` tags it `source: "runtime"`. Symmetric with config streams which the on-disk repo also serves raw |
| Per-stream lookup (`Lookup`) | Complete | `autopublish.Service.Lookup(code)` returns `(RuntimeEntry, true)` so the stream handler's `Get` path resolves runtime codes |

---

## Stream codes & URL routing

Stream codes accept `[A-Za-z0-9_/-]+` (no `..`, no leading / trailing
`/`, no `//`). The slash enables namespacing as `region/north/live`;
the route layer treats the whole prefix as one opaque key.

| Form | URL convention | Notes |
|---|---|---|
| Single-segment (`foo`) | `rtmp://host/live/foo`, `rtsp://host/live/foo`, `srt://host?streamid=live/foo` | The `live/` prefix is mandatory for single-segment codes; bare `rtmp://host/foo` etc. is rejected so a half-typed URL can't accidentally hit a stream |
| Multi-segment (`region/north/news`) | `rtmp://host/region/north/news` | No prefix. A leading `live/` is also accepted and stripped, so multi-segment codes are reachable via either form |
| HLS / DASH delivery | `/{code}/index.m3u8`, `/{code}/index.mpd` | Raw stream code, no `live/` prefix. Chi catch-all dispatcher routes multi-segment codes through the same handler |

Enforced in: `rtmpRouteKey` ([internal/ingestor/push/rtmp_server.go](../internal/ingestor/push/rtmp_server.go)), `rtspPathStreamCode` + `lookupStream` ([internal/publisher/serve_rtsp.go](../internal/publisher/serve_rtsp.go)), `srtStreamCode` ([internal/publisher/serve_srt.go](../internal/publisher/serve_srt.go)).

---

## Ingest

| Feature | Status | Notes |
|---|---|---|
| Pull — HLS / HLS-LL | Complete | `grafov/m3u8`; max-segment-buffer guard; per-input headers/auth |
| Pull — HTTP raw MPEG-TS | Complete | |
| Pull — RTSP | Complete | `gortsplib/v5`; H.264/H.265/AAC; RTCP A/V sync. **IDR codec-config guarantee** mirrors RTMP: H.264 / H.265 callbacks maintain a per-stream parameter-set cache seeded from SDP (`sprop-parameter-sets` / `sprop-vps,sprop-sps,sprop-pps`) and refreshed on every IDR with inline params (scanned via `findH264SPSPPSInNALUs` / `findH265VPSSPSPPSInNALUs`). Slice-only IDRs get the cached prefix prepended; IDRs with neither inline nor cached params are dropped. Required because some RTSP servers omit `sprop-parameter-sets` from SDP AND some encoders omit in-band SPS/PPS — without dual-source cache + drop fallback the empty-intersection case poisoned downstream muxers with un-init-able keyframes |
| Pull — RTMP | Complete | `q191201771/lal` PullSession; AVCC→Annex-B; ADTS wrap. **IDR codec-config guarantee**: every IDR access unit emitted into the buffer hub starts with an Annex-B SPS/PPS prefix (or VPS/SPS/PPS for HEVC). `RTMPMsgConverter.ensureKeyFrameHasParamSets` resolves params per IDR by (1) inline-scanning the frame's NALUs, (2) using the cached prefix populated by `captureVideoSeqHeader` from a sequence-header tag, or (3) dropping the IDR if neither yielded params — never emits an un-init-able keyframe downstream. Required because Open-Streamer's own RTMP-republish path strips SPS/PPS from per-frame NALU tags and ships the AVCDecoderConfigurationRecord lazily; without the inline-scan + drop fallback, clients connecting before the preload received IDRs forever-stuck without init params (test5 incident: HLS unplayable, DASH `videoPSGiveUp` latched audio-only) |
| Pull — SRT (caller) | Complete | `datarhei/gosrt` |
| Pull — UDP / MPEG-TS | Complete | Unicast + multicast; auto-strip RTP header |
| Pull — File (`.ts`, `.mp4`, `.flv`) | Complete | Loop mode; paced playback |
| Pull — S3 | Complete | GetObject stream; S3-compatible via `?endpoint=` |
| Pull — `copy://<code>` | Complete | In-process subscribe to another stream's published output (raw or per-rendition) |
| Pull — `mixer://<videoCode>,<audioCode>` | Complete | In-process video+audio mix from two upstream streams (comma-separated codes; optional `?audio_failure=continue` keeps video-only when audio source dies). MixerReader emits AVPackets with each track's PTS preserved relative to its upstream timebase; the unified [Timeline Normaliser](#timeline-normaliser-internaltimeline) below performs cross-track wallclock anchoring and per-track origin / snap math. **Known limitation**: combining clock-independent sources (live HLS video + file-paced audio) accumulates A/V drift mid-stream that the seed-time cross-track snap can't fix — HLS players load slowly waiting for V/A buffer alignment but eventually play. Production usage should pair clock-coherent sources |
| Push — RTMP listen | Complete | Shared `:1935` (default); RTMP relay → loopback pull |
| Push — SRT listen | Complete | Shared `:9999`; streamid `live/<code>` dispatch |
| Multi-input registration with priority | Complete | Lower value = higher priority |
| Per-input `Net` config | Complete | `timeout_sec` (per-protocol op budget) and `insecure_tls`. Reconnect/silence-detection knobs were dropped — pull workers use a hardcoded backoff and stream-level liveness lives in `manager.input_packet_timeout_sec`. |
| HLS pull tuning | Complete | Per-stream `net.timeout_sec` (sets playlist GET budget; segment timeout auto-derives ×4 floored at server default); server-wide `hls_max_segment_buffer` |

---

## Timeline Normaliser (`internal/timeline`)

Single unification of the three legacy PTS rebasers (`ingestor/ptsrebaser`, `ingestor/pull/mixer` V/A bases, `coordinator/abr_mixer`'s per-cycle rebaser) — Phase-2 refactor. Sits between every AV-path `PacketReader` and the buffer-hub write step (invoked from `ingestor/worker.writeOnePacket`). Re-anchors `PTSms` / `DTSms` to local wallclock so upstream encoder clock skew, sudden PTS jumps (CDN HLS playlist resync, NVENC stall recovery, transcoder restart), and arrival skew between V and A on a multi-source mixer don't poison the segment timelines emitted by HLS / DASH. See [ARCHITECTURE.md § Timeline Normaliser](./ARCHITECTURE.md#timeline-normaliser-internaltimeline) for the full design.

| Feature | Status | Notes |
|---|---|---|
| Per-track origins | Complete | `inputOrigin` + `outputAnchor` + `lastOutputDts` tracked independently for video / audio so the small intrinsic A/V offset (RTSP audio leading video by ~100 ms, RTMP codec-config pre-roll) survives |
| Cross-track snap (progression-based) | Complete | Newly-seeded track snaps its anchor onto the other track's `lastOutputDts` when the other has already moved more than `CrossTrackSnapMs` (1 s) of output PTS. Catches `mixer://` cases where one source bursts in milliseconds while the other delivers steadily; the typical sub-second RTSP/RTMP intrinsic A/V offset still survives |
| Jump-threshold re-anchor | Complete | Drift `expected − max(actualNow, lastOutputDts)` exceeding `JumpThresholdMs` (2 s default) re-anchors with `target = max(actualNow, lastOutputDts+1)`. The `max(actualNow, lastOutputDts)` floor absorbs bursty delivery (RTMP/SRT batched GOP-worth of frames in a single ms of wallclock) without manufacturing fake drift |
| Monotonic output | Complete | Re-anchor target is always `≥ lastOutputDts + 1` so downstream uint64 dur math (DASH packager, MSE source buffer) can never underflow. Backward-jumping inputs (source restart, PTS wrap, mid-burst regression) emit a forward-only output |
| MaxAheadMs drop semantics | Complete (opt-in) | `MaxAheadMs > 0` enables drop-when-output-races-past-wallclock — `Apply` returns `false` on drop so the caller skips the buffer write. Disabled by default (`DefaultConfig().MaxAheadMs = 0`) because the drop has a stuck-state pathology when sustained drift exceeds the cap. Default downstream defense is the DASH packager's `behindPrevSegEnd` pacing gate which holds emits without dropping content |
| MaxBehindMs re-anchor | Complete | Symmetric counterpart to MaxAheadMs — hard re-anchor when output lags wallclock by more than `MaxBehindMs`. Catches "track paused while wallclock kept moving" cases the JumpThresholdMs branch misses |
| Session-boundary signalling | Complete (Phase-3) | Moved off per-packet `Discontinuity` flag onto `buffer.Packet.SessionStart` marker (auto-stamped by `buffer.Service` after `SetSession`). `Normaliser.OnSession(reason, t)` resets per-track state; consumers dispatch on `pkt.SessionStart` instead of multi-source `Discontinuity` |
| Per-stream lifecycle | Complete | New `Normaliser` per `readLoop` invocation; reconnect builds a fresh anchor against the new wallclock |
| Wired in pull worker + RTMP push server | Complete | `worker.writeOnePacket` and `push/rtmp_server.OnReadRtmpAvMsg` both call `Apply` and skip the buffer write on `Apply == false` |

**Scope**: AV-path codecs (RTSP / RTMP pull, RTMP push, `copy://`, `mixer://`) write through `timeline.Normaliser` directly; raw-TS sources (UDP / HLS-pull / HTTP-TS / SRT / file) are demuxed → run through the Normaliser per-PES → remuxed via the [internal/ingestor/tsnorm](../internal/ingestor/tsnorm/tsnorm.go) wrapper. Residual quality-of-service items tracked in [docs/DASH_OUTSTANDING_BUGS.md](./DASH_OUTSTANDING_BUGS.md).

---

## Stream Manager (Failover)

| Feature | Status | Notes |
|---|---|---|
| Multi-input failover (Go-level, no transcoder restart) | Complete | Old ingestor stops, new one starts; buffer continuity preserved |
| Packet timeout detection | Complete | `manager.input_packet_timeout_sec` (default 30); hot-reload via `SetConfig` (atomic.Int64) — change applies on next health-check tick without pipeline restart |
| Background failback probe | Complete | Cooldown 8s probe / 12s switch |
| Bypass-probe recovery | Complete | When ingestor reader auto-reconnects faster than probe cycle, `RecordPacket` clears exhausted state + records recovery switch |
| Switch history (last 20) | Complete | `runtime.switches[]` per stream with reason: `initial`, `error`, `timeout`, `manual`, `failback`, `recovery`, `input_added`, `input_removed`; from/to/at/detail |
| Per-input error history (last 5) | Complete | `runtime.inputs[].errors[]` — degradation reasons + timestamps |
| Live input update (`UpdateInputs`) | Complete | Add/remove/update without pipeline stop; active removal triggers failover with `input_removed` reason |
| Live buffer write-target update | Complete | `UpdateBufferWriteID` — restart active ingestor with new target |
| Manual switch API | Complete | `POST /streams/{code}/inputs/switch { priority }` records `manual` reason |
| Exhausted callback → coordinator | Complete | `setStatus(degraded)`; auto-recover via probe success |

---

## Transcoder

| Feature | Status | Notes |
|---|---|---|
| Transcoder subprocess | Complete | `open-streamer-transcoder` per stream; in-process libavcodec, gRPC over a Unix socket; killed via context cancel |
| Single decode → N renditions | Complete | One subprocess decodes once and fans out to every rendition (scaler + encoder); an N-rung ladder costs 1×decode + N×encode |
| Seamless input switch | Complete | On failover the subprocess swaps only its decoder; encoders stay alive (stable SPS/PPS, continuous rebased PTS) so players don't re-initialise |
| Per-rendition runtime status | Complete | All N rungs appear in `RuntimeStatus.Profiles[]`; restart_count + last-5 errors per rung (index 0 carries live subprocess state) |
| ABR profile config | Complete | Resolution, bitrate, codec, preset, profile, level, framerate, GOP, B-frames, refs, SAR, resize_mode |
| Encoder codec routing | Complete | `domain.ResolveVideoEncoder` maps user alias (`""`/`h264`/`h265`/`vp9`/`av1`) + HW backend → FFmpeg encoder name; explicit names (`h264_nvenc`, `h264_qsv`) preserved |
| Preset normalization | Complete | Translates between encoder families (`veryfast` ↔ `p2`, `medium` ↔ `p4`); drops invalid values for backends without `-preset` (VAAPI, VideoToolbox) so cross-family preset choices remain valid |
| Audio encoding | Complete | AAC / MP3 / Opus / AC3 / copy |
| Copy video / copy audio modes | Complete | `video.copy=true` + `audio.copy=true` skips the transcoder entirely (passthrough) |
| Hardware acceleration | Complete | NVENC, VAAPI, VideoToolbox, QSV; full-GPU pipeline (decode→scale_cuda→encode) when HW matches encoder family |
| Resize modes (pure GPU) | Complete | `pad`, `crop`, `stretch`, `fit` — all stay on GPU (no CPU round-trip via hwdownload) for NVENC; `pad`/`crop` degrade to aspect-preserving fit on GPU |
| Deinterlace | Complete | yadif (CPU) / yadif_cuda (GPU); auto-detect parity or operator-specified tff/bff |
| Watermark — text overlay | Complete | drawtext-based; per-position presets + custom (raw libavfilter expressions for X/Y); strftime fields supported in text |
| Watermark — image overlay | Complete | `movie=`-source overlay (no second `-i` needed → uniform image/text graph); PNG / JPG / GIF; opacity; CPU + GPU pipelines (GPU round-trip via hwdownload/hwupload_cuda) |
| Watermark asset library | Complete | `/watermarks` REST API + on-disk store under `watermarks.dir`; ID-keyed files + JSON sidecar; resolved by coordinator before transcoder.Start |
| Thumbnail | Schema only | Domain fields exist; not yet generated |
| Subprocess crash auto-restart | Complete | Supervisor respawns with exponential backoff: 2s → 30s cap; retries forever |
| Crash log spam suppression | Complete | After 3 consecutive identical errors, warn drops to debug; events fire only on power-of-2 attempts |
| Per-rendition error history (last 5) | Complete | `runtime.transcoder.profiles[].errors[]` — subprocess error context embedded |
| Subprocess log forwarding | Complete | Subprocess stdout/stderr captured into the parent's structured log |
| Health detection → coordinator | Complete | After 3 consecutive crashes (sub-30s) fires `onUnhealthy` → status Degraded; sustained run (>30s) fires `onHealthy` → status Active. Hot-restart (Update path) clears flag via `dropHealthState` callback |
| Hot-swap config (`SetConfig`) | Complete | runtime updates the cached config (e.g. `FFmpegPath`); a transcoder config change restarts running streams' subprocesses |
| `StartProfile` / `StopProfile` | N/A | Not supported — the subprocess owns all renditions; a ladder change restarts the whole subprocess |

---

## Coordinator & Lifecycle

| Feature | Status | Notes |
|---|---|---|
| Start pipeline | Complete | Buffers → manager → publisher → transcoder; raw + rendition buffers per topology |
| Stop pipeline | Complete | Reverse-order teardown; buffer cleanup |
| Bootstrap persisted streams on boot | Complete | Skips disabled / zero-input streams |
| Stream reconciler (self-healing) | Complete | Background goroutine started by `runtime.Manager`; every 10s lists persisted streams and `Start`s any non-disabled stream with at least one input that is not currently running. Handles transient bootstrap failures (HLS source down at boot, recovers later), restart errors, and the create-handler edge case where a brand-new stream was saved but never dispatched. Idempotent — `Coordinator.Start` short-circuits when already running |
| Hot-reload (`Update`) | Complete | Diff engine: 5 categories — inputs, transcoder topology, profiles, protocols/push, DVR |
| Ladder add/remove/update | Complete | Restarts the stream's transcoder subprocess (renditions share one decode); other streams + protocols untouched |
| ABR ladder add/remove → `RestartHLSDASH` | Complete | Only HLS+DASH goroutines restart; RTSP/RTMP/SRT viewers preserved |
| ABR profile metadata update | Complete | `UpdateABRMasterMeta` rewrites HLS master playlist in-place (no transcoder restart) |
| Topology change → `reloadTranscoderFull` | Complete | Full pipeline rebuild when transcoder nil↔non-nil |
| ABR-copy pipeline (`copy://` upstream with ladder) | Complete | N tap goroutines re-publish each upstream rendition; bypasses ingest worker + transcoder; reconnects on upstream restart (relies on `buffer.Delete` channel-close signal). Note: bypassing manager means `runtime.media` is empty — see Operational Notes |
| ABR-mixer pipeline | Complete | Mirror video ladder + audio fan-out from two upstream streams; reconnects on upstream restart; **PTS/DTS rebased per-source against shared wall-clock anchor** so video (upstream A) and audio (upstream B) collapse onto a common timeline — without this, divergent PCR bases between unrelated sources caused players to render black + silent. Note: bypassing manager means `runtime.media` is empty — see Operational Notes |
| Stream-level health reconciliation | Complete | `streamDegradation` flags (`inputsExhausted`, `transcoderUnhealthy`) — Degraded if either set, Active when all clear |
| DVR hot-reload | Complete | Toggle on/off; restarts with new mediaBuf when best rendition shifts |
| Narrow service interfaces (`deps.go`) | Complete | `mgrDep`, `tcDep`, `pubDep`, `dvrDep` — spy-based testing |

---

## Publisher — Delivery

| Feature | Status | Notes |
|---|---|---|
| HLS — single rendition | Complete | Native TS segmenter + media playlist |
| HLS — ABR (master + per-track sub-playlists) | Complete | Auto-active when transcoder ladder present |
| HLS — master playlist `CODECS` includes audio | Complete | Master `EXT-X-STREAM-INF` declares `avc1.<profile>,mp4a.40.2` whenever the rendition has audio (transcoder `audio.copy=true` OR `audio.codec` set); without the audio entry, hls.js + MSE addSourceBuffer with a video-only codec list and silently drop audio. Detected from stream config at setup time, not from packet inspection |
| HLS — `#EXT-X-DISCONTINUITY` on failover | Complete | Per-variant generation counter |
| HLS — keyframe-aligned segments + audio-continuous safety net | Complete | AV-path segments end at the first IDR after `live_segment_sec` elapses (`handleAVPacket`). Wallclock safety deadline is `4 × live_segment_sec` (was 1.5× — bumped because long-GOP sources tripped it on every segment). When the safety net does fire on a pathological source, `discardUntilIDR` drops subsequent **video** packets only until the next keyframe so the next segment starts cleanly; **audio packets bypass the discard window** (`Codec.IsVideo() == false`) so the elementary audio stream stays continuous — without that exemption, listeners hear 3–4 s gaps every time the safety net trips on a long-GOP source |
| DASH — single representation (fMP4 + dynamic MPD) | Complete | H.264 / H.265 / AAC; MP3 skipped |
| DASH — ABR (root MPD + per-track dirs) | Complete | Audio packaged on best track only |
| DASH — cross-track tfdt origin sync | Complete | First DTS observed across either track seeds a shared `originDTSms`; each track's `nextDecode` initialises to the offset between its own first DTS and that origin (in the track's timescale). Without this, both counters defaulted to 0 and any source-side A/V skew (mp4 encoder pre-roll, edit lists, HLS audio-leads-video) was silently collapsed into "both tracks start together" — the bug user-visible as ~400ms drift on `file://*.mp4` sources |
| DASH — bidirectional inter-frame jump guard | Complete | `timestampJumpFromLast` checks the running `vDTS` / `aPTS` queue tail against the incoming packet's timestamp; ±`dashSourceSwitchJumpMs` (1 s) in either direction triggers `flushSegmentLocked` before the new sample is appended. Catches CDN HLS playlist resyncs (forward jump would otherwise bake a giant per-sample dur into `videoNextDecode`) and source switches (backward jump would underflow uint64 dur math); applies to H.264 / H.265 / AAC paths via shared helper |
| DASH — per-track drift cap (wallclock anchor) | Complete | `shouldSkipVideoLocked` / `shouldSkipAudioLocked` (called from `onTSFrame`) drop incoming frames when the projected segment timeline would race more than one `segDur` ahead of wallclock-since-AST. Video drops are IDR-aligned via the `videoSkipUntilIDR` latch (drops non-IDR until the next keyframe arrives, then re-anchors `videoNextDecode = elapsed` and accepts the IDR — clean segment boundary, no poisoned tfdt). Audio drops freely (sample-count-locked, no IDR concept). Necessary because raw-TS-path streams (HLS pull, mixer reading transcoded TS chunks) bypass the AV-path rebaser entirely — without a packager-level cap their MPDs would advertise content in the future of `publishTime` and strict players (dashjs / shaka) would refuse to play |
| DASH — uint64-underflow-safe per-sample dur | Complete | `writeVideoSegmentLocked` computes inter-frame dur with signed int64 deltas and an explicit `< uint32_max` clamp before casting back to `uint32`. A sub-second backward step that slips past the 1 s jump guard underflowed to ~2^64 in unsigned subtraction and cast to uint32 ≈ 47.7 s phantom dur, baking a permanent offset into `videoNextDecode` (test5 incident: DASH live edge ran ~1 .4 M seconds in the future of `publishTime` after sustained drift) |
| RTSP play | Complete | Shared listener; `gortsplib/v5`; `rtsp://host:port/live/<code>`. Output is wallclock-paced before each `WritePacketRTP` so bursty upstream delivery (HLS pulls, NVENC's faster-than-realtime output) reaches the wire smoothed back to realtime. RTP timestamps are monotonic-clamped (`rtpTS > lastRTP` always) so small in-window source DTS jitter no longer surfaces in clients as "non monotonically increasing dts" / dropped frames |
| RTMP play | Complete | Shared port with ingest (`:1935`); `rtmp://host:port/live/<code>`. Per-frame video tags carry slice NALUs only — SPS/PPS/AUD/SEI are stripped via `buildAvccSliceOnly` because strict players reject NALU tags containing non-slice NALUs. Sequence headers are sent at timestamp 0 either inline-extracted from the first IDR (`writeH264`'s pre-`avcSeqSent` branch) or via `PreloadAvcSeqHeader` triggered by the producer goroutine's raw-TS scan when gomedia's TSDemuxer drops standalone parameter-set NALUs before invoking `OnFrame`. AAC tags carry **one access unit each**: `RTMPFrameWriter.writeAAC` splits gomedia-bundled PES (typically 4–8 ADTS frames per delivery) into separate tags with monotonic per-frame DTS = `base + frameIndex × 1024 × 1000 / sampleRate`; without splitting, downstream pull-RTMP consumers collapse the bundle into one frame and audio sample counts under-report by the bundling factor (test5 incident: DASH audio segment durations declared ~0.5 s for ~4 s of actual data) |
| SRT play | Complete | Shared listener (`:9999`); `srt://host:port?streamid=live/<code>`; default latency 120ms |
| RTMP push out | Complete | `q191201771/lal` PushSession; `rtmp://` + `rtmps://`; custom codec adapter for proper PTS/DTS composition_time (B-frame friendly) |
| Per-protocol independent context | Complete | Each output (`hls`, `dash`, `rtsp`, `push:<url>`) has its own cancel func |
| `UpdateProtocols(old, new)` | Complete | Only changed protocols stop/start; live viewers preserved |
| Per-push state tracking | Complete | `runtime.publisher.pushes[]` — status (`starting`/`active`/`reconnecting`/`failed`), attempt, connected_at, last 5 errors |
| Listener hot-reload (RTMP / SRT / RTSP) | Complete | `publisher.SetListeners` + `ingestor.SetListeners` + `manager.SetConfig` swap `atomic.Pointer` snapshots before `diffService` restarts the affected goroutine — port / latency changes pick up on next restart cycle without losing other live viewers |

---

## DVR & Timeshift

| Feature | Status | Notes |
|---|---|---|
| Persistent recording (ID = stream code) | Complete | One recording per stream; survives restarts |
| Segment writing (MPEG-TS) | Complete | PTS-based cutting; wall-clock fallback |
| Gap detection + `#EXT-X-DISCONTINUITY` | Complete | Gap timer = 2× segment duration |
| Gap recording in `index.json` | Complete | `DVRGap{From, To, Duration}` |
| Resume after restart | Complete | Playlist parsing rebuilds in-memory segment list |
| `#EXT-X-PROGRAM-DATE-TIME` | Complete | Written before first segment + after every discontinuity |
| Retention by time + size | Complete | Both `retention_sec` (0=forever) and `max_size_gb` (0=unlimited) |
| VOD playlist + timeshift + segments | Complete | Unified `GET /recordings/{rid}/{file}` — `.m3u8` serves `playlist.m3u8` from disk by default, dispatches to dynamic timeshift slice when `?from=RFC3339` / `?offset_sec=N` (+ optional `?duration=N`) is present; `.ts` serves segment as-is. Path traversal sanitised |
| Info endpoint | Complete | `GET /recordings/{rid}/info` — range, gaps, count, total bytes |
| Configurable storage path | Complete | Per-stream `storage_path` overrides `./out/dvr/{streamCode}` default |

---

## Events & Hooks

| Feature | Status | Notes |
|---|---|---|
| In-process event bus | Complete | Typed events, bounded queue (512), worker pool sized via `hooks.worker_count` (default 4; rarely needs tuning after batching) |
| HTTP webhook delivery (batched) | Complete | Per-hook batcher; flushes on `BatchMaxItems` OR `BatchFlushIntervalSec`; POST body is a JSON array; `X-OpenStreamer-Batch-Size` header reports cardinality; HMAC `X-OpenStreamer-Signature` covers the array body |
| HTTP retry + re-queue on failure | Complete | Up to `max_retries` retries within a flush (1s/5s/30s backoff); failed batches re-queue at the front for next flush; queue capped by `BatchMaxQueueItems` (drops oldest on overflow) |
| File delivery | Complete | Appends one JSON line per event to an absolute target path; per-target mutex serialises concurrent writes; line-atomic at filesystem level via O_APPEND. **Not batched** to keep the JSON-lines contract intact |
| Per-hook event filter | Complete | `event_types[]` whitelist |
| Per-hook stream filter | Complete | `stream_codes.only[]` / `.except[]` |
| Per-hook metadata injection | Complete | Merged into payload as `metadata.*` |
| Per-hook MaxRetries / TimeoutSec | Complete | Defaults: 3 retries, 10s timeout (from `domain.Default*`) |
| Per-hook batch overrides | Complete | `batch_max_items` / `batch_flush_interval_sec` / `batch_max_queue_items` on Hook record override `hooks.batch_*` global defaults |
| Graceful drain on shutdown | Complete | Service.Start exits → each HTTP batcher gets a final best-effort flush before goroutine returns |
| Test endpoint (HTTP + File) | Complete | `POST /hooks/{id}/test` — for HTTP, signals an immediate flush so the test response is visible in seconds rather than waiting a full flush interval |
| Event documentation | Complete | See [APP_FLOW.md](./APP_FLOW.md#events-reference) |

---

## Runtime Status & Observability

All live state is exposed under `runtime.*` in `GET /streams/{code}` so the UI has one root for everything dynamic. Persisted config stays at the top level — runtime overlay never collides.

| Feature | Status | Notes |
|---|---|---|
| `runtime.status` + `pipeline_active` | Complete | Coordinator-resolved lifecycle: `active` / `degraded` / `stopped` / `idle` |
| `runtime.exhausted` | Complete | True when all inputs are degraded with no failover candidate |
| `runtime.active_input_priority` + `override_input_priority` | Complete | Manager state |
| `runtime.inputs[]` | Complete | Per-input snapshot: status, last_packet_at, bitrate_kbps, errors[] |
| `runtime.switches[]` | Complete | Last 20 active-input switches with reason + detail |
| `runtime.transcoder.profiles[]` | Complete | Per-rung restart_count + errors[]; subprocess error context embedded |
| `runtime.publisher.pushes[]` | Complete | Per-destination status + attempts + errors[]; resets on Active |
| Defensive snapshot copies | Complete | Caller-side mutation cannot leak back into service state |

---

## Configuration Defaults

Single source of truth: [internal/domain/defaults.go](../internal/domain/defaults.go). Exposed via `GET /config/defaults` for frontend placeholder rendering.

| Group | Constants |
|---|---|
| Buffer | `DefaultBufferCapacity=1024` |
| Manager | `DefaultInputPacketTimeoutSec=30` |
| Publisher HLS/DASH | `DefaultLiveSegmentSec=2`, `DefaultLiveWindow=12`, `DefaultLiveHistory=0` |
| DVR | `DefaultDVRSegmentDuration=4`, `DefaultDVRRoot="./out/dvr"` |
| Push | `DefaultPushTimeoutSec=10`, `DefaultPushRetryTimeoutSec=5` |
| Hook | `DefaultHookMaxRetries=3`, `DefaultHookTimeoutSec=10` |
| Video | `DefaultVideoBitrateK=2500`, `DefaultVideoResizeMode=pad` |
| Audio | `DefaultAudioBitrateK=128` |
| Listeners | `DefaultListenHost="0.0.0.0"`, `DefaultRTMPTimeoutSec=10`, `DefaultRTSPTimeoutSec=10`, `DefaultSRTLatencyMS=120` |
| Ingestor | `DefaultHLSPlaylistTimeoutSec=15`, `DefaultHLSSegmentTimeoutSec=60`, `DefaultHLSMaxSegmentBuffer=8` |
| Transcoder | `DefaultFFmpegPath="ffmpeg"` |

---

## Play Sessions (`internal/sessions`)

Tracks every active player so operators can answer "who is watching
this stream right now?". State is in-memory only — restart loses
records, viewers reconnect into fresh sessions.

| Feature | Status | Notes |
|---|---|---|
| HLS / DASH session tracking | Complete | `mediaserve.Mount` wrapped with `sessions.HTTPMiddleware`; each segment GET extends the session record; bytes counted from the `ResponseWriter` |
| RTMP session tracking | Complete | `push.PlayFunc` extended with `PlayInfo{RemoteAddr, FlashVer}`; bytes counted by wrapping `writeFrame` |
| SRT session tracking | Complete | `srtHandleSubscribe` opens a tracker session; bytes accumulated on every successful `conn.Write` |
| RTSP session tracking | Complete | gortsplib `OnPlay` / `OnSessionClose` hooks. Outbound bytes credited at close from `ServerSession.Stats().OutboundBytes` (gortsplib's per-session counter, covers RTP payload + RTP header + framing the library owns) — published as the session's `bytes` field, same shape as RTMP / SRT |
| Fingerprint session ID (HLS / DASH) | Complete | `sha256(stream + ip + ua + token)[0..16]` so repeated segment GETs collapse onto one record within the idle window |
| UUID session ID (RTMP / SRT / RTSP) | Complete | Connection-bound, generated at handshake; closed on TCP teardown |
| Idle reaper | Complete | Default 30s without activity → close + emit `EventSessionClosed`; tunable via `sessions.idle_timeout_sec` |
| Max-lifetime cap | Complete | Optional `sessions.max_lifetime_sec` hard-closes any session older than the cap |
| Hot-reload config | Complete | `sessions.UpdateConfig` swaps an `atomic.Pointer[runtimeConfig]` — toggling `enabled` / changing `idle_timeout_sec` takes effect on the next reaper tick without restart |
| Kick (force-close) | Complete | `DELETE /sessions/{id}` → reason=`kicked`; idempotent (404 on already-closed) |
| Filter / list | Complete | `GET /sessions?proto=…&status=…&limit=…` + per-stream `/streams/{code}/sessions` |
| Stats counters | Complete | `active`, `opened_total`, `closed_total`, `idle_closed_total`, `kicked_total` exposed in every list response |
| Event bus emit | Complete | `EventSessionOpened` / `EventSessionClosed` published — hooks can persist analytics or notify ops |
| GeoIP resolver (MaxMind .mmdb) | Complete | `GeoIPResolver` interface + `NullGeoIP` default. When `sessions.geoip_db_path` points to a MaxMind .mmdb (GeoLite2-Country / GeoLite2-City / commercial GeoIP2), `cmd/server/main.go` opens it via `sessions.NewMaxMindGeoIP` and registers as the resolver — `PlaySession.Country` then carries the ISO 3166-1 alpha-2 code. Empty path or open failure falls back to NullGeoIP (warn-logged); never blocks boot. Operators can swap in a custom resolver (IP2Location, in-house service) by replacing the DI binding. |

---

## Watermarks (`internal/watermarks` + transcoder filter graph)

| Feature | Status | Notes |
|---|---|---|
| Text overlay (drawtext) | Complete | `text` supports `%{localtime}` and friends; opacity folded into `fontcolor=…@α` |
| Image overlay (overlay+movie) | Complete | `movie=` source filter avoids second `-i`; opacity via `colorchannelmixer=aa` |
| GPU round-trip on NVENC | Complete | `hwdownload,format=nv12 → drawtext/overlay → hwupload_cuda`; portable across distros without `--enable-cuda-nvcc` |
| Per-rendition watermark | Complete | The filter graph is built for each rendition so every variant draws the watermark independently |
| Position presets | Complete | `top_left` / `top_right` / `bottom_left` / `bottom_right` / `center`; `offset_x` / `offset_y` act as edge padding |
| Custom position | Complete | `position=custom` + raw libavfilter expressions in `x` / `y` ("100", "main_w-overlay_w-50", "if(gt(t,5),10,-100)") |
| Asset library upload | Complete | `POST /watermarks` multipart; PNG / JPG / GIF sniffed via `http.DetectContentType`; cap 8 MiB image / 16 MiB request |
| Asset library list / get / raw / delete | Complete | Mirrors VOD UX; `/raw` serves with `Cache-Control: immutable` |
| Sidecar metadata | Complete | One `<id>.json` per asset → `os.ReadDir` rebuilds registry on restart, no DB |
| Asset-id reference from streams | Complete | `Stream.Watermark.AssetID` resolved by coordinator into `ImagePath` before each `tc.Start` (transcoder stays asset-agnostic) |
| Validation at API boundary | Complete | mutually exclusive `image_path` / `asset_id`; opacity 0..1; position-custom requires non-empty x/y; image / font path absoluteness + readability |

---

## Pending / Planned

Tracking what is intentionally NOT done. Each row is a deliberate scope decision.

| Priority | Feature | Status | Notes |
|---|---|---|---|
| Mid | Thumbnail | Schema only | Periodic JPEG snapshot from main buffer; needs ffmpeg `select=eq(pict_type\\,I)` chain |
| Low | RTMP `flashVer` capture | Schema only | gomedia doesn't expose `flashVer` from the connect command publicly; left empty in `PlayInfo.FlashVer` |
| Low | Sessions token-based auth | Not started | Token field reserved on PlaySession; no resolver / signed-URL verifier wired yet |
| Low | WebRTC publish / play | Not started | Pion-based subsystem; large surface (SDP, ICE, DTLS-SRTP) |

### Decided NOT (locked)

| Feature | Reason |
|---|---|
| Auto-recovery scheduler at coordinator level | Replaced by infinite per-module retry with backoff (transcoder retry forever, manager probe forever). No `MaxRestarts`. Pipeline never tears down on crash |
| RTMP ingest server lal migration | Current gomedia-based push server is stable with per-connection `recover()`. Cost-benefit doesn't justify refactor + retest matrix |
| Per-rendition push selection | Push always sends best rendition. Multi-tier publishing → run separate streams |
| Full `gomedia` → `lal` swap | TS infrastructure (`gomedia/go-mpeg2`) has no equivalent in lal. Hybrid stack is intentional |
| Sessions persistence (history beyond active set) | In-memory map is intentional — sessions are an operational view, not an audit log. Operators who need persistent history wire a hook on `session.opened` / `session.closed` and store downstream (their DB, S3, log pipeline). Avoids pulling a SQL/file backend into the server for a use case better served by the existing event bus |
| Local-packager error tracking (HLS/DASH runtime errors[]) | The success path is already covered by `publisher_segments_total{stream_code, format, profile}` — alert on `rate(...[2m]) == 0` for active streams catches every "segmenter stalled" case without per-error bookkeeping. Per-error-reason breakdown (`disk_full` vs `permission_denied` vs `demux_panic`) and a runtime API `errors[]` view would be useful for ops dashboards but duplicate what `slog` already records — log + Prometheus rate alert handles operational needs. Skip the bookkeeping until a concrete dashboard requirement justifies it |
| HLS / DASH push out (HTTP / S3) | Reverse-proxy CDN (Cloudflare / Fastly / Akamai sitting in front of the HLS/DASH serve endpoint) covers the same scale-out goal with a config-only change — the CDN pulls segments on first viewer request and caches them. Object-storage origins (S3 / R2) are handled by sidecar tools like `rclone sync` watching the segment dir. Implementing push out in-process would duplicate ~3-5 days of work (URL scheme parsing, retry/backoff, S3v4 signing, manifest sync, runtime status) for a use case the operator's existing CDN already solves. Reconsider only if a concrete deployment needs same-host transfer-style upload (compliance, multi-region origin replication) |

---

## Testing & Quality

| Feature | Status | Notes |
|---|---|---|
| Unit tests — protocol detection | Complete | |
| Unit tests — buffer ring / fan-out | Complete | |
| Unit tests — manager state machine + bypass-recovery + switch history | Complete | |
| Unit tests — encoder routing, native pipeline (decoder / encoder / scaler), audio reencode | Complete | |
| Unit tests — transcoder health detection (3-fail edge, sustain recovery, multi-profile aggregation) | Complete | |
| Unit tests — coordinator diff engine + degradation reconciliation | Complete | |
| Unit tests — publisher HLS/DASH segmenters, push state | Complete | |
| Unit tests — DVR playlist parsing, gap recording | Complete | |
| Unit tests — error history rings (manager / transcoder / push) | Complete | |
| Unit tests — runtime status snapshots (defensive copy, sort order) | Complete | |
| Unit tests — config defaults endpoint (shape, codec routing table, determinism) | Complete | |
| Unit tests — sessions tracker (HTTP + conn paths, idle reaper, kick, filter, hot-reload) | Complete | |
| Unit tests — sessions HTTP middleware (proto detection, byte counting, error path) | Complete | |
| Unit tests — sessions handler (list / get / kick / filter validation) | Complete | |
| Unit tests — watermark filter graph (text + image, CPU + GPU, custom position, position presets, escaping) | Complete | |
| Unit tests — watermark domain validation (mutual-exclusion, asset-id charset, opacity range, custom requires X/Y) | Complete | |
| Unit tests — watermarks asset service (save/list/get/delete, content-type sniff, rebuild from disk) | Complete | |
| Integration tests — coordinator.Update routing | Complete | 14 cases, spy implementations of all service interfaces |
| Tests — native libavcodec pipeline | Complete | Decoder / encoder / scaler / pipeline table tests link against libav; run in the builder image |
| CI (GitHub Actions) | Complete | `mod-tidy`, `test` (matrix Go 1.25.9 + stable), `lint` (allow-fail), `govulncheck` |
| Pre-commit hook (auto-regen swagger) | Complete | `make hooks-install` symlinks `scripts/git-hooks/pre-commit` |
| golangci-lint | Complete | 0 issues |
| gofumpt formatting | Complete | Enforced via lint |

### Benchmarking (`bench/`)

Operator-facing capacity tooling — runs sweeps across passthrough,
ABR (NVENC / libx264) and HLS+DASH multi-protocol phases. See
[`bench/README.md`](../bench/README.md) for the full sweep plan.

| Tool | Purpose |
|---|---|
| `bench/scripts/sample.sh` | 2s-cadence CSV of CPU% (jiffies-based, intersection of PID set across ticks) + RSS / GPU enc/dec / VRAM / network for the open-streamer process tree |
| `bench/scripts/run-bench.sh` | One-case driver: spin up N FFmpeg publishers → wait warmup → sample steady window → tear down |
| `bench/scripts/run-all.sh` | Full sweep — Phases A/B/C/F/H/D, auto-stops a phase on first SATURATED, full-stop on first FAIL |
| `bench/scripts/summarize.sh` | Per-run `summary.md` + auto-classify `PASS` / `SATURATED` / `FAIL` against thresholds |
| `bench/scripts/aggregate.sh` | Master report at `bench/reports/<sweep>/report.md` |
| `bench/scripts/notify.sh` | Optional Telegram webhook integration |

---

## Operational Notes

- **Transcoding links libavcodec.** The `open-streamer-transcoder` binary is built against libav (see `Dockerfile.builder`); its available encoders are fixed at build time. `ffmpeg_path` is only for auxiliary tasks (e.g. thumbnails).
- **HLS and DASH dirs must differ** when both publishers are active.
- **DVR is per-stream opt-in.** No global enable.
- **Failover timestamp jumps** produce `#EXT-X-DISCONTINUITY` in HLS and are logged at debug level.
- **`PUT /streams/{code}` is non-disruptive** when the stream is running — only changed components restart.
- **Pipeline never tears down on a subprocess crash.** The supervisor respawns forever with backoff. Status flips to `degraded` after 3 consecutive crashes; flips back to `active` after a sustained run (>30s) or hot-restart.
- **ABR-copy / ABR-mixer streams without a downstream transcoder report empty `runtime.media`** — these paths bypass the manager (their pipeline is N in-process taps, not an ingest worker), so the per-input track tracker that fills "Input Media" / "Output Media" / "Input throughput" panels is not exercised. Enable a downstream transcoder if you need those metrics — that routes the stream through the normal ingest+transcode pipeline where tracking is wired (input tracks observed by the ingestor, output tracks derived from the transcoder ladder config).
- **`mixer://` A/V drift with clock-independent sources** — combining two upstreams that don't share a sample/frame clock (e.g. live HLS video + file-paced audio) accumulates A/V drift mid-stream. The PTS Anchoring Layer's cross-track snap aligns V and A at startup, but the bursty source's GOP-by-GOP delivery keeps producing micro-drift after both tracks are seeded. Symptom: HLS player loads slowly (10–15 s waiting for V/A buffer alignment) but eventually plays smoothly. NOT a regression — production mixer usage should pair clock-coherent sources (e.g. two RTMP feeds from the same encoder).
- **Build version** stamped at compile time (`make build` runs `git describe --tags --always --dirty`); exposed via `GET /config.version`.
- **`build/reinstall.sh <tag>`** downloads + verifies + uninstalls + reinstalls a tagged release on Linux/systemd hosts. Data dir preserved.
