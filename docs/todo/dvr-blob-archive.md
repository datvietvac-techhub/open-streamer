# Blob-Archive DVR — Final Technical Design

A segmented CMAF blob archive replacing the per-`.ts` DVR. Per-hour
container files of concatenated fMP4 (CMAF) fragments, a per-hour binary range
index, a date-tree layout, multi-profile, hour/day retention, and on-the-fly
HLS + DASH timeshift from one store. It reuses `internal/publisher/dash`'s fMP4
**builders** (`BuildVideoFragment` / `BuildAudioFragment`) and its **segmenter
decision** (`Segmenter.Cut`) so an archived fragment is byte-compatible with a
live one — but it computes its own **timeline anchor** (tfdt) from frame PTS,
because the live tfdt helper is not crash-resumable (see §3.1.3).

This document folds in every high/medium adversarial-review finding. The most
load-bearing corrections vs. the synthesis draft:

- **Fragments start with `styp`, not `moof`.** Verified: `BuildVideoFragment`
  → `mp4.NewMediaSegment()` → `Styp: CreateStyp()`
  (`mediasegment.go:20-27`, mp4ff@v0.51.0). A fragment is the byte run
  `[styp][moof][mdat]`; `ByteOffset` points at the `styp`. Recovery walks
  `styp → moof → mdat`.
- **tfdt is computed from frame PTS + a persisted recording origin**, NOT from
  the reused `videoTfdtForSegment` (which seeds from emit-wallclock and chains
  from in-memory state that vanishes on restart). This makes the media timeline
  absolute, restart-resumable, and overlap-free by construction.
- **fsync never runs on the receive goroutine.** Each lane is split into a
  fast receive/ingest/cut goroutine and a separate flush goroutine, so a disk
  stall produces visible whole-fragment backpressure instead of silent
  mid-GOP frame drops from the 1024-deep buffer-hub subscriber channel.
- **Audio is recorded once (on the best/`p0` lane), not per profile.** The
  transcoder broadcasts identical AAC to every rendition buffer; per-profile
  `.cmfa` would be N bit-identical copies.

---

## 1. Goals & non-goals

### 1.1 Goals

1. Replace the per-`.ts` DVR storage with per-hour CMAF blob files
   (`HH.cmfv` + `HH.cmfa`) holding concatenated fMP4 fragments, a per-hour
   binary range index (`HH.ranges`), and a per-stream `catalog.json`.
2. **Multi-profile**: one blob set per rendition (`p0…pN`), audio recorded once.
3. Serve **both** HLS (fMP4 + `EXT-X-MAP`) and DASH (MPD) timeshift on-the-fly
   from the same store; serving is `ReadAt` byte slices, no second mux pass.
4. **Seek to any timestamp** with A/V sync preserved across hour boundaries.
5. Survive crash / partial-hour: append-only blob + rebuildable index +
   atomic catalog; recovery truncates the torn tail and never serves it.
6. **Hour/day/size retention** by whole-file deletes in one central pass.
7. **Migrate** existing per-`.ts` recordings; legacy serving keeps working
   until migrated (zero downtime).
8. **Phased behind a config flag**; legacy `.ts` is the default until the
   final phase.

### 1.2 Non-goals

- **Frame-exact scrubbing.** Seek precision is **fragment/GOP-granular**
  (snap to the nearest IDR ≤ target). The reused builder uses uniform
  per-sample durations and ms-granular CTO; frame-exact seek would require a
  new builder and is out of scope. The URL/player contract promises "seek to
  nearest GOP", not "seek to exact ms".
- **Re-muxing on read.** Stored bytes are served verbatim.
- **Re-stamping timestamps.** The Normaliser already anchored PTS/DTS in the
  buffer hub; the DVR never re-stamps.
- **Per-byte-range retention.** Retention only ever deletes whole hour files.
- **DASH multi-Period across an arbitrary window in v1.** A codec-param change
  splits Periods (Phase 4); a v1 DASH window that *spans* a codec change is
  clamped to the discontinuity (HLS handles it natively via
  `EXT-X-DISCONTINUITY`).

### 1.3 Invariants (add to CLAUDE.md "never violate" at Phase 6)

- One muxer (`dash.Build*Fragment`), one segmenter decision
  (`dash.Segmenter.Cut`), two sinks (live publisher / DVR). The DVR never
  re-implements fMP4 muxing, never muxes MPEG-TS, never re-stamps timestamps.
- A fragment is the byte run `[styp][moof][mdat]`; `ByteOffset` points at the
  `styp`, `ByteLen` covers the whole run.
- The **blob is the source of truth** (append-only, self-describing); `.ranges`
  is a rebuildable cache. Recovery validates counted records against blob size,
  then tail-scans for additional complete fragments, then rewrites the count.
- **MediaTicks (tfdt) is `round((frame0.PTSms − recordingMediaOriginMs) ×
  timescale / 1000)`**, monotonic-guarded, with `recordingMediaOriginMs`
  persisted in `catalog.json`. Consecutive same-track fragments are
  PTS-contiguous (`tfdt[n] == tfdt[n-1] + dur[n-1]`) except across a
  `discontinuity-before`. This makes the served MPD overlap-free by
  construction (no `behindPrevSegEnd` gate needed on the archive path).
- fsync **never** runs on the buffer-subscriber receive goroutine.
- Audio is recorded **once** (best/`p0` lane). Non-`p0` lanes are video-only.
- Retention deletes whole hour files across all profiles in one central pass,
  tolerating asymmetric per-profile hour sets; never deletes the in-progress
  hour, a crashed-but-resumable hour, or an hour younger than the read grace.
- Path-injection chokepoints: `<dvrRoot>/<streamCode>` via `resolveSegDir`'s
  containment guard (reused unchanged); profile id `^p[0-9]+$`; fragment-name
  strict regex. The 10-digit hour token in a fragment URL is a **lookup key**
  into server-built structures, never a path component joined onto disk.

---

## 2. On-disk format

### 2.1 Directory tree

Root resolves exactly as today: `dvrCfg.StoragePath` → else
`domain.DefaultDVRRoot`. `resolveSegDir(root, string(streamCode))`'s
containment guard builds `<root>/<streamCode>` (reused unchanged;
`streamCode` may contain `/` for namespacing). Everything below is
server-generated.

```
<root>/<streamCode>/
  catalog.json                       # per-stream catalog (atomic tmp+rename)
  catalog.json.tmp
  p0/                                # profile dir: p<N> = rendition INDEX N (NOT "best")
    2026/06/05/
      14.cmfv                        # hour 14 video: [init][frag][frag]...
      14.cmfa                        # hour 14 audio (ONLY on the audio-source profile)
      14.ranges                      # binary index for this hour (both tracks if cmfa present)
      14.ranges.tmp
      14.open                        # sentinel: present iff hour 14 is still being written
      15.cmfv  15.cmfa  15.ranges
  p1/
    2026/06/05/14.cmfv  14.ranges    # video-only (no .cmfa); audio served from the audio-source profile
```

- **Profile dir `p<N>`: N = rendition index** from
  `buffer.RenditionsForTranscoder` order (`track_(N+1)` → `pN`). Passthrough
  (`copy://`, no renditions) → single `p0` on buffer `stream.Code`.
  **`pN` is the dir name and is stable across restarts/ladder edits.** "Best"
  is recorded separately in the catalog as `best_profile` (via
  `BestRenditionIndex`) — it is NOT `p0` by definition (an operator listing
  profiles low-to-high makes the best rung the *last* index).
- **Audio source profile**: exactly one profile (the best rung) writes
  `.cmfa`. All other profiles are video-only. Catalog `audio_profile` names it;
  every profile's HLS audio-group URI and DASH shared-audio AdaptationSet point
  at it.
- Hour granularity keyed by **UTC wallclock of the fragment's FIRST frame**:
  `YYYY/MM/DD/HH`. Files are **created lazily** on the first ready fragment of
  that hour (so an hour crossed before the first IDR/init produces no files — an
  implicit gap).
- `.cmfv` / `.cmfa` are each `[init][frag][frag]…` — a self-contained,
  independently playable fMP4 (copy `HH.cmfv` out → valid file).

### 2.2 Hour blob layout (`HH.cmfv` / `HH.cmfa`)

```
offset 0:        ftyp+moov   (init = EncodeInit output)                  ← written ONCE at hour open
offset initLen:  styp+moof+mdat  (fragment 0 = BuildVideoFragment output)
offset …:        styp+moof+mdat  (fragment 1)
…
```

- **Init at head**: HLS `EXT-X-MAP` and DASH `initialization` point at the
  blob's `[0, initLen)` range — no separate init file, no duplication beyond
  the deliberate per-hour copy (the cost of self-contained whole-file
  retention; this is NOT "zero overhead", it is one ftyp+moov per hour per
  profile, ~1–2 KB).
- `initLen = len(EncodeInit(videoInit.Init))` **captured at write time** and
  stored in the `.ranges` header (`VideoInitLen` / `AudioInitLen`), never
  recomputed. `EncodeInit` emits exactly `ftyp+moov` (verified: it just calls
  `init.Encode`), so `[0, initLen)` decodes standalone.
- Init is built lazily from the first IDR's parameter sets
  (`ExtractParameterSets` / `ExtractHEVCParameterSets` →
  `BuildH264Init` / `BuildH265Init`; `BuildAACInit` from the first ADTS
  header). Until init exists, the IDR-startup gate drops frames — **no file is
  created and no fragment is written without a valid init** (resolves the
  lazy-init-vs-rotation collision: deferred file creation means an init-less
  hour simply has no files).
- **Codec-param change mid-hour** (resolution switch on a `copy://` source,
  or an ABR-ladder reorder detected via `buffer_slug` mismatch): detect by
  comparing SPS/PPS *before* push, then flush + seal the current hour, build a
  new init, force a fresh hour (or sub-hour) blob, and append the switch IDR as
  fragment 0 with `discontinuity-before` set.

### 2.3 The `.ranges` index — binary format

One `.ranges` per hour per profile. Append-only, fixed-width records → O(1)
crash truncation and O(log n) binary search by media time. Little-endian.
Readers use `RecordSize` from the header (forward-compat), not a constant.

**Header (64 bytes), rewritten in place at offset 0 on each checkpoint:**

| off | size | field | meaning |
|----|----|----|----|
| 0 | 4 | `Magic` | `"OSR1"` |
| 4 | 2 | `Version` | 1 |
| 6 | 2 | `RecordSize` | 40 |
| 8 | 2 | `Flags` | bit0 = hour sealed (closed cleanly); bit1 = started-with-discontinuity |
| 10 | 2 | _pad_ | |
| 12 | 4 | `VideoInitLen` | video init length (offset 0 in `.cmfv`); 0 if no video |
| 16 | 4 | `AudioInitLen` | audio init length (offset 0 in `.cmfa`); 0 if this profile has no `.cmfa` |
| 20 | 4 | `VideoTimescale` | 90000 |
| 24 | 4 | `AudioTimescale` | sample-rate Hz (0 until first audio frag) |
| 28 | 4 | `RecordCount` | valid fragment records appended (commit *hint*, not sole truth — see §3.6) |
| 32 | 8 | `HourStartUnixMs` | UTC ms at top of this hour (the `YYYYMMDDHH` anchor) |
| 40 | 8 | `BaseMediaTicksV` | absolute video `tfdt` of the first video fragment in this hour |
| 48 | 8 | `BaseMediaTicksA` | absolute audio `tfdt` of the first audio fragment |
| 56 | 8 | _reserved_ | pad to 64 |

> `recordingMediaOriginMs` / `recordingMediaOriginUnixMs` live in
> `catalog.json` (per recording, persisted at first fragment). The per-hour
> `BaseMediaTicks*` + `HourStartUnixMs` are derived conveniences for
> standalone-hour playback and recovery.

**Fragment record (40 bytes), appended after the header in blob-append order:**

| off | size | field | meaning |
|----|----|----|----|
| 0 | 1 | `Track` | 0=video, 1=audio |
| 1 | 1 | `Flags` | bit0 = keyframe (starts with IDR/IRAP); bit1 = discontinuity-before |
| 2 | 2 | `CRC16` | CRC-16 of bytes `[0..2)+[4..36)` of this record (torn-vs-valid discriminator) |
| 4 | 4 | `SampleCount` | frames in fragment (V: GOP-ish; A: AAC frames) |
| 8 | 8 | `WallTimeMs` | **derived** UTC ms = `recordingMediaOriginUnixMs + (MediaTicks − recordingMediaOriginTicks)×1000/timescale`; stored for fast search & PROGRAM-DATE-TIME |
| 16 | 8 | `MediaTicks` | absolute `tfdt` of first sample — **PTS-derived, continuous across hours** |
| 24 | 8 | `ByteOffset` | offset of this fragment's **`styp`** in the blob |
| 32 | 4 | `ByteLen` | fragment byte length (`styp+moof+mdat`) |
| 36 | 4 | `DurTicks` | fragment duration in track timescale (the exact `segDurTicks`) |

Properties:

- **Seek by media/wall time:** binary-search by `MediaTicks` (the authoritative
  clock). `WallTimeMs` is derived from `MediaTicks` via the persisted anchor, so
  the two keys never diverge (resolves the dual-clock-divergence high finding).
  For video, snap back to the nearest keyframe record ≤ target (`Flags` bit0).
- **Cross-hour continuity:** `MediaTicks` is absolute & monotonic across hours →
  the reader stitches consecutive `.ranges` with no recomputation.
- **Slice = `(ByteOffset, ByteLen)`:** serving = `pread(blob, off, len)` →
  exact `[styp][moof][mdat]` bytes, zero parsing. Init = `pread(blob, 0, initLen)`.
- **A/V sync:** V and A share one media origin (§3.1.3); reader pairs A to V by
  `MediaTicks` overlap, not naïve wall-span (resolves the seek-pairing finding).
- **Rebuildable & torn-safe:** `CRC16` distinguishes a torn final record from a
  valid one; a missing `.ranges` is reconstructed by walking
  `styp/moof/mdat` box headers (`mfhd.sequence_number`, `tfhd.track_ID`,
  `tfdt`, `trun` flags) via mp4ff's `DecodeHeader` / `DecodeBox`
  (verified present at `mp4/box.go:204,331`).

### 2.4 Top-level catalog (`catalog.json`)

Per-stream, atomic (`tmp`+rename, reusing `saveIndex`'s pattern). Single-writer:
**only the per-stream Service goroutine mutates it** (lanes and retention post
mutation requests to it; readers take an `atomic.Pointer` snapshot). Persisted
**only** on hour rotation, lane stop/close, and retention prune —
**never per-fragment and never per-tick** (`last_fragment_at` liveness is
in-memory only; the durable heartbeat, if needed, is the in-place
`.ranges` header rewrite, which is O(1)).

```jsonc
{
  "stream_code": "region/north/live",
  "format": "cmaf-blob-v1",
  "started_at": "2026-06-05T13:51:02Z",
  "video_timescale": 90000,
  "recording_media_origin_ticks": 351320,        // tfdt of the first recorded video frame
  "recording_media_origin_unix_ms": 1749132902120,// wall instant of that same frame
  "audio_profile": "p2",                          // the one profile that holds .cmfa
  "best_profile": "p2",                           // master default; from BestRenditionIndex
  "profiles": [
    { "id": "p0", "buffer_slug": "track_1", "codec": "avc1.4D401F",
      "width": 1280, "height": 720, "bandwidth": 3000000,
      "audio_codec": "mp4a.40.2", "audio_timescale": 48000, "audio_bandwidth": 128000,
      "available": [ { "from_ms": 1749132902120, "to_ms": 1749140102120 } ],
      "hours": [
        { "hour": "2026/06/05/14", "wall_from_ms": 1749132902120, "wall_to_ms": 1749136500000,
          "media_from_ticks_v": 351320, "media_to_ticks_v": 323712000,
          "sealed": true, "discontinuity": false, "frag_count_v": 900,
          "size_bytes": 612000000 }
      ] },
    { "id": "p2", "buffer_slug": "track_3", "codec": "avc1.4D4028",
      "width": 1920, "height": 1080, "bandwidth": 6000000,
      "audio_codec": "mp4a.40.2", "audio_timescale": 48000,
      "available": [ { "from_ms": 1749132902120, "to_ms": 1749140102120 } ],
      "hours": [ /* with frag_count_a + audio totals */ ] }
  ],
  "gaps": [ { "from_ms": 1749136500000, "to_ms": 1749136506000, "reason": "packet-drop" } ],
  "retention": { "max_age_sec": 86400, "max_size_gb": 50, "max_age_hours": 0 }
}
```

- `recording_media_origin_*`: the single wall↔media anchor. Survives restart;
  reloaded on resume so post-restart `WallTimeMs` uses the **same** origin
  (resolves the "origin not persisted" high finding).
- `available[]` per profile: media-time windows the profile actually covers,
  so a timeshift window touching a profile that started/stopped mid-recording
  resolves correctly instead of 404-ing (resolves the ABR-add/remove gap).
- `audio_profile` / `best_profile`: decouple audio source & master default
  from dir naming (resolves the `p0`-means-two-things finding).
- `hours[].wall_from_ms`/`wall_to_ms` are the **actual** first/last fragment
  wall times (not `floor(hour)`), so window selection uses real bounds and
  probes the adjacent hour within one GOP of a boundary
  (resolves the hour-bucketing mis-file finding). A rotation test asserts
  `wall_from_ms[N+1] == wall_to_ms[N]` (contiguous).
- `sealed:false` marks the in-progress hour AND any crashed mid-write hour;
  retention distinguishes the two by the `.open` sentinel + last-mtime
  (resolves the "never delete sealed:false leaks disk forever" gap).

---

## 3. Architecture

### 3.1 Writer

#### 3.1.1 Topology — N independent profile lanes, decoupled flush

`coordinator.Start` builds the profile list, then `blobDVR.StartRecording`
starts one lane per profile. Each lane owns its own `*buffer.Subscriber`,
`*dash.FrameQueue`, `*dash.Segmenter`, blob FDs, and **two goroutines**:

```
lane.receiveLoop (NEVER blocks on disk):
  sub := buf.Subscribe(profile.BufferID)   // explicitly enlarged channel (§3.7)
  loop pkt := sub.Recv():
    ingest(pkt) → FrameQueue               (shared dash.cmaf ingress; mutex-guarded)
    if dec := seg.Cut(now,q,…); dec.Ok:
       build fragment + range record IN MEMORY
       send {frag, record, rotateInfo} → flushCh   (bounded; whole-fragment backpressure)

lane.flushLoop (owns the FDs; does write(2)+fsync):
  loop item := <-flushCh:
    append item.frag to open blob (running offset)
    append item.record to .ranges
    on rotation boundary: seal+rotate (§3.4)
    on fsync timer / rotation / stop: fsync (§3.5)
```

A disk stall now stalls `flushLoop`, which fills `flushCh`, which applies
backpressure to `receiveLoop` at **whole-fragment** granularity (visible,
measurable) instead of silently dropping per-frame packets mid-GOP from the
1024-deep subscriber channel (resolves the fsync-on-receive high finding). On
any subscriber drop or detected gap, the lane forces `discontinuity-before` on
the next fragment and records a catalog `gap` so the reader emits
`EXT-X-DISCONTINUITY` / a new Period.

This mirrors the live packager exactly: a lane is structurally a
`dash.Packager` whose segment sink is the blob appender.

#### 3.1.2 Frame ingestion (shared `dash.cmaf` ingress)

Lanes subscribe to **rendition buffers** (post-transcode, per profile) — the
same buffers the DASH publisher reads. Ingress is the **same code** as
`dash.handleH264` / `handleAAC` / `onTSFrame`, promoted into a shared,
mutex-guarded `dash/cmaf.Ingress` (Phase 0):

1. `pkt.AV != nil`, video → `VideoFrame{AnnexB: clone(av.Data), PTSms, DTSms,
   IsIDR: av.KeyFrame}` → `q.PushVideo`. IDR-startup gate + SPS/PPS
   accumulation reused.
2. `pkt.AV != nil`, audio → `SplitADTSBundle` → per-frame
   `AudioFrame{Raw, PTSms = base + i×1024×1000/sr}` → `q.PushAudio`
   (only on the audio-source lane).
3. `pkt.TS != nil` (`copy://` passthrough on `stream.Code`): the lane reuses
   the **full** TS-demuxer lifecycle (`newTSBuffer` + `startDemuxer` +
   `alignTS`), where `onTSFrame` runs on a **separate goroutine** and calls
   back into the **mutex-guarded** queue — same discipline as the packager's
   `p.mu`. Wiring only `onAVPacket` would make every `copy://` recording empty
   (resolves the passthrough high finding). For a `copy://` mid-stream
   resolution change, the recorder parses SPS from the demuxed stream to detect
   the param change → forces rotation + `discontinuity-before`.
4. `pkt.SessionStart` (transcoder sets it on output packets) → **flush the
   in-progress fragment first** (so no recorded media is lost at
   failover/reconnect — the DVR flushes, unlike the live packager which drops),
   then mark the next fragment `discontinuity-before`, then `q.Reset()` +
   `seg.Reset()` to clear pairing/residual state. If codec params changed →
   force hour rotation (§3.4).

Because frames are already Normaliser-anchored, A/V sync and wallclock
anchoring are inherited; the lane adds no new timeline math beyond §3.1.3.

#### 3.1.3 Fragment formation + PTS-derived absolute timeline (CORE CORRECTION)

Per lane, run `seg.Cut(...)` (the verified pure decision) every tick. On
`dec.Ok`:

1. Pop `dec.VideoCount` video frames and `dec.AudioCount` audio frames.
2. `segDurTicks := dash.ComputeVideoSegDurTicks(frames, nextPTSms, hasNext)`
   (next-frame-peek; includes the last frame's own duration — the documented
   anti-stutter rule). Audio frag dur = `len(frames) × 1024`.
3. **Compute tfdt yourself — do NOT reuse `videoTfdtForSegment`:**
   - First-ever fragment of the recording (or first after a discontinuity):
     `tfdt = round((frame0.PTSms − recordingMediaOriginMs) × timescale / 1000)`,
     where `recordingMediaOriginMs` = the PTS of the very first recorded video
     frame, captured once and **persisted in `catalog.json`**. Audio uses the
     same origin instant (the same wall↔media anchor) so V and A start in
     lockstep across the whole recording.
   - Every later fragment: `tfdt = prevEnd = prev.MediaTicks + prev.DurTicks`
     (PTS-contiguous). A monotonic guard `tfdt = max(tfdt, lastTfdt+1)` covers
     NTP backward steps / non-monotonic PTS.
   - **Why not the reused helper:** `videoTfdtForSegment` (packager.go:982)
     seeds the first segment from `wallclockTicks(now, ast)` (emit-wallclock,
     not frame PTS) and chains from the in-memory `entries` slice, which is
     gone after restart → a tfdt cliff at every resume seam, and drift from
     true media time over long runs. The PTS-derived form is a pure function of
     frame PTS + persisted origin: absolute, restart-resumable, and
     overlap-free by construction (so the live `behindPrevSegEnd` MPD-overlap
     gate is **not needed** on the archive path).
4. `frag := dash.BuildVideoFragment(seqNum, videoInit.TrackID, tfdt, frames,
   isHEVC, segDurTicks)` (or `BuildAudioFragment(seqNum, audioInit.TrackID,
   tfdt, aud)`). The fragment bytes are `[styp][moof][mdat]`.
5. Append `frag` to the open hour blob; `off` = the running written offset
   (post-`write(2)`, NOT post-fsync — the reader of the same process reads
   page cache fine; a crash before fsync is handled by recovery §3.6).
6. Append a 40-byte `.ranges` record:
   `Track, Flags(keyframe=frames[0].IsIDR, disc=pendingDisc), CRC16,
   SampleCount, WallTimeMs(derived from MediaTicks+anchor), MediaTicks=tfdt,
   ByteOffset=off (styp start), ByteLen=len(frag), DurTicks`.
7. `seg.MarkCut(now)`. **`seqNum` is per-hour** (reset at rotation), so
   `moof.mfhd.sequence_number` (uint32) never wraps over a years-long always-on
   channel; fragments are addressed by `.ranges` offset, and `seqNum` is
   documented as non-authoritative (resolves the seqNum-overflow finding).

**Writer assertion + table test:** for every adjacent same-track pair,
`tfdt[n] == tfdt[n-1] + dur[n-1]` unless `discontinuity-before[n]` is set.

#### 3.1.4 Hour rotation

On each cut, compare `floor(frame0.wall / 1h)` to the open hour:

- **Crossing into a new hour → rotate at the next IDR** (new `.cmfv` starts on
  a SAP). Audio `.cmfa` rotates at the same fragment boundary. Files for the new
  hour are **created lazily** at the first ready fragment (init known).
- Ordered for crash safety:
  1. `flushLoop` fsyncs current `.cmfv` (and `.cmfa` if audio-source).
  2. Set `Flags.sealed` in current `.ranges` header; fsync `.ranges`.
  3. Post catalog mutation to the Service: `sealed:true`, real `wall_to_ms`,
     `media_to_ticks_v`; Service atomic-writes catalog.
  4. Remove `HH.open`.
  5. `mkdir -p YYYY/MM/DD`; create new `HH.*`; write init at offset 0
     (re-encode cached `videoInit`/`audioInit`); write new `.ranges` header
     (`VideoInitLen`/`AudioInitLen`, `HourStartUnixMs`, `BaseMediaTicks*` = the
     rotating fragment's absolute tfdt); create `HH.open`; **fsync the parent
     dir** (durable dirents).
  6. Append the rotating IDR fragment as fragment 0 of the new hour; reset
     per-hour `seqNum`.
- **Codec-param change** triggers the same rotation early with
  `discontinuity-before` on the new hour's first fragment.

#### 3.1.5 fsync strategy

Config `dvr.fsync_policy`: `per_fragment` | `per_n=k` | `interval=Nms`.
Default **`interval = 2×segDur` (≈8 s)** + always-fsync on rotation and stop.
Durability contract per checkpoint, run **on `flushLoop`**:

- Order: **fsync blob → write+fsync `.ranges` (header + appended records) →
  Service atomic catalog update.** This guarantees "a counted record's bytes
  are always durable". It does **not** make `RecordCount` the sole commit point
  — recovery also tail-scans for fragments whose bytes are durable but whose
  count fsync didn't land (those are KEPT, not truncated — see §3.6). This
  reconciles the §2.5/§2.6 contradiction the review flagged.
- fsync the parent dir after `create`/`rename` of new hour files.
- **Write/disk-full handling:** an `append` or `fsync` error MUST NOT bump
  `RecordCount`, MUST emit `dvr_blob_write_errors` + an event, and aborts the
  lane cleanly (seal what is durable, mark the hour `sealed:false`, stop).
  A transient `ENOSPC` after retention frees space is retried on the next tick.

#### 3.2 Crash / partial-hour recovery

On `StartRecording` resume, per profile (single-track scan, no full mux):

1. Find the dirty hour: the one with a present `HH.open` sentinel (else the
   newest). Open `.ranges`; validate `Magic`/`Version`/`RecordSize`.
2. Walk records; for each, verify `CRC16` AND `ByteOffset + ByteLen ≤
   fileSize(blob)`. The last record passing both is the last *committed*
   fragment. A record failing CRC is torn → stop.
3. **Tail re-scan** (the blob is the source of truth): from the last committed
   record's end to `blobSize`, parse top-level boxes via mp4ff `DecodeHeader`
   in order `styp → moof → mdat`. For each *complete* `styp+moof+mdat` run,
   append a recovered record (`MediaTicks` from `tfdt`, keyframe from `trun`
   first-sample flags; `WallTimeMs` derived as
   `recordingMediaOriginUnixMs + (MediaTicks − recordingMediaOriginTicks) ×
   1000/timescale` — using the persisted catalog origin, **never**
   `HourStartUnixMs + tfdt/timescale`, which would be wrong by
   `hour_start − recording_start`; resolves the recovery wall-formula
   inconsistency). Stop at the first incomplete box.
4. **Truncate** blob to the last complete fragment's end and `.ranges` to
   `64 + count×40`; rewrite header `RecordCount`; fsync both. Discards any torn
   final fragment.
5. Re-derive the catalog hour record from the repaired `.ranges`;
   `sealed:false` (resume point). Reload `recordingMediaOrigin*` from catalog.
   Resume per-hour `seqNum` and the tfdt continuation (`prevEnd` = last
   record's `MediaTicks+DurTicks`). Reload `videoInit`/`audioInit` by decoding
   the hour blob's `[0, initLen)` head (or, if that hour never got an init,
   from the latest sealed hour head / catalog codec params).
6. Seed the new `Segmenter` via `MarkCut(lastFragmentWallclock)` so the
   safety-net deadline is correct. The first post-resume fragment gets
   `discontinuity-before` (a one-audio-frame A/V phase reset at the seam is
   masked by `EXT-X-DISCONTINUITY` / DASH handling, which the reader MUST
   consume; resolves the residual-state recovery finding). A recovery test
   asserts A/V `tfdt` continuity within one audio-frame tolerance across the
   seam, and that a fully-written-but-uncounted fragment is KEPT.

Crash mid-`catalog.json` is harmless (atomic rename → old-or-new; catalog is
rebuildable from the tree + `.ranges` headers).

### 3.3 Reader / serving

A `BlobReader` per stream (cheap; opens FDs per request, holds them for the
whole request) backs **both** HLS and DASH timeshift. A
`BlobTimeshiftHandler` parallels the legacy `RecordingHandler`; routing reuses
`dispatchMedia` (§3.4.3).

#### 3.3.1 URL surface

```
# HLS timeshift
GET /<code>/index.m3u8?from=&dur=                       → master (one variant per profile)
GET /<code>/index.m3u8?from=&dur=&profile=p0           → media playlist (fMP4 + EXT-X-MAP)
GET /<code>/dvr/<profile>/init.mp4                      → init byte range ([0,initLen) of resolved hour)
GET /<code>/dvr/<profile>/<hour>-<track>-<seq>.m4s     → one fragment (resolved via .ranges, ReadAt)

# DASH timeshift
GET /<code>/index.mpd?from=&dur=                        → multi-profile MPD (static default; dynamic optional)
GET /<code>/dvr/<profile>/init_v.mp4 | init_a.mp4       → video/audio init byte range
GET /<code>/dvr/<profile>/v-<seq>.m4s | a-<seq>.m4s     → video/audio fragment
```

`isDVRPlaybackRequest` (existing `from`/`delay`/`ago`/`dur` detection,
dispatch.go:141) routes live vs. timeshift; live (no params) hits the live
packager unchanged. The 10-digit `<hour>` token is a **lookup key** into the
catalog/`.ranges` (server resolves the actual file path); it is never
`filepath.Join`-ed onto disk. Chokepoints: profile `^p[0-9]+$`,
fragment `^[0-9]{10}-[01]-[0-9]+\.m4s$` (and `^[va]-[0-9]+\.m4s$` for DASH),
`init(_[va])?\.mp4`.

#### 3.3.2 Time-window query → ranges → fragment slices

`BlobReader.Query(profile, from, dur) (*Window, error)`:

1. Parse `from`/`delay`/`ago`/`dur` with existing `parseTimeshiftStart` /
   `parseTimeshiftDuration` (reused unchanged). **Cap `dur`** (config
   `dvr.max_window_sec`, default e.g. 6 h) to bound manifest size / memory —
   a 24 h window at 6 s segments is ~14 400 entries; over the cap returns 416
   or clamps (resolves the unbounded-window gap).
2. Take an `atomic.Pointer` **catalog snapshot** (one per request). Intersect
   the requested `[from, from+dur)` with each profile's `available[]` windows
   (skip a profile that lacks the range instead of 404-ing the whole request).
3. Select hour records (per profile) whose **actual** `[wall_from_ms,
   wall_to_ms]` intersect the window. For the first hour, **open the blob FD
   once and hold it for the whole request** (Linux unlink-after-open keeps
   bytes readable even if retention deletes concurrently).
4. Binary-search video records by `MediaTicks`; **snap back to the nearest
   keyframe record ≤ target**, crossing into the **previous hour's** `.ranges`
   when the snap index < 0 in the first intersecting hour (the window's `PDT0`
   and first emitted fragment then come from that earlier hour;
   resolves the hour-boundary keyframe-snap finding).
5. **Audio pairing by media time:** select audio fragments whose
   `[tfdt, tfdt+dur)` overlaps `[videoStartTicks, windowEndTicks)`, INCLUDING
   the one audio fragment straddling `videoStartTicks`, so the player has audio
   samples covering the first video frame (A/V sync at seek-in).
6. Cross-hour windows concatenate records from consecutive `.ranges`;
   `MediaTicks` continuity guarantees a gap-free DASH timeline and correct HLS
   `EXTINF` sums.

Each output segment maps 1:1 to one stored fragment. Serving =
`blob.ReadAt(off, len)` (no re-mux). The reader re-reads the in-progress
hour's `.ranges` header `RecordCount` on each request (never caches it) and
never serves a record index ≥ the durable `RecordCount` (safe
live-read-during-write).

```go
type FragmentRef struct {
    Profile, HourPath string
    Track             uint8
    ByteOffset        int64
    ByteLen           int64
    MediaTicks        uint64
    DurTicks          uint64
    Keyframe          bool
    Discontinuity     bool
}
```

#### 3.3.3 On-the-fly HLS (fMP4 + `EXT-X-MAP`)

Media playlist per profile, version 7:

```m3u8
#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:6
#EXT-X-PLAYLIST-TYPE:VOD
#EXT-X-MAP:URI="dvr/p0/init.mp4"
#EXT-X-PROGRAM-DATE-TIME:2026-06-05T14:00:03.120Z
#EXTINF:6.000,
dvr/p0/2026060514-0-0.m4s
#EXTINF:6.000,
dvr/p0/2026060514-0-1.m4s
…
#EXT-X-DISCONTINUITY                       ← record Flags discontinuity-before bit
#EXT-X-MAP:URI="dvr/p0/init.mp4"           ← re-stated each hour / on codec-param change
#EXT-X-PROGRAM-DATE-TIME:2026-06-05T15:00:00.000Z
…
#EXT-X-ENDLIST
```

- `EXTINF` = `DurTicks / timescale`; `PROGRAM-DATE-TIME` = derived `WallTimeMs`
  (first segment & after each discontinuity).
- Audio is a separate `#EXT-X-MEDIA:TYPE=AUDIO` rendition group whose URI
  points at the **`audio_profile`** (one canonical audio source), so it cannot
  outlive/predate a pruned video profile (resolves the audio-group lifetime
  finding). Retention treats the audio-source hours as binding.
- Handler shape mirrors today's `ServeTimeshift`: parse → slice → render; only
  the body changes from v3-TS to v7-fMP4.

#### 3.3.4 On-the-fly DASH (MPD)

Reuses `dash.BuildManifest` for the single-Period (no codec change) case:
build a `ManifestInput` with one `TrackManifest` per profile (video) and the
shared audio track; segments from records:

```go
seg := dash.SegmentEntry{StartTicks: rec.MediaTicks, DurTicks: rec.DurTicks}
```

`MediaPattern` → `dvr/p0/v-$Number$.m4s`, `StartNumber` = first record's seq,
`InitFile` → `dvr/p0/init_v.mp4`. Because tfdt is PTS-contiguous (§3.1.3) the
`<S t=…>` entries never overlap.

- **Static MPD** (`type="static"` + `mediaPresentationDuration`) for the
  bounded `[from, from+dur)` window — the **primary** timeshift surface.
- **Dynamic MPD** (open-ended live-DVR): `AvailabilityStart` is derived
  **per fetch** from the newest fragment:
  `AST = latestWallTime − latestMediaTicks/timescale`, recomputed each manifest
  request, so `(now − AST) ≈ latest media time` regardless of source drift
  (resolves the dynamic-AST finding). Treated as a separate, carefully-tested
  mode; static is preferred.
- **Multi-Period for codec change (Phase 4 real feature, not a "wrapper"):**
  `BuildManifest` emits exactly one Period (manifest.go:195). A NEW
  `BuildMultiPeriodManifest([]PeriodInput)` emits one `<Period>` per
  discontinuity group, each with its own AdaptationSet/Representation+init.
  v1 scope: a window that *spans* a codec change is **clamped to the
  discontinuity** (or returns an explicit error); HLS handles the same case
  natively. Hour rotations are NOT new Periods (init re-stated per hour); only
  codec changes split Periods.

`ServeFragment` accepts both `<hour>-<track>-<seq>` and `$Number$`/`$Time$`
addressing (binary-search `.ranges` by seq or `MediaTicks`).

#### 3.3.5 Master / multivariant across profiles

HLS master from `catalog.profiles[]`, ordered/defaulted by `best_profile`:

```m3u8
#EXTM3U
#EXT-X-VERSION:7
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="audio",DEFAULT=YES,URI="index.m3u8?from=&dur=&profile=p2&track=a"
#EXT-X-STREAM-INF:BANDWIDTH=6000000,CODECS="avc1.4D4028,mp4a.40.2",RESOLUTION=1920x1080,AUDIO="aud"
index.m3u8?from=&dur=&profile=p2
#EXT-X-STREAM-INF:BANDWIDTH=3000000,CODECS="avc1.4D401F,mp4a.40.2",RESOLUTION=1280x720,AUDIO="aud"
index.m3u8?from=&dur=&profile=p0
```

`BANDWIDTH`/`RESOLUTION`/`CODECS` from `catalog.profiles[]`
(`TrackInfo` + `RenditionPlayout.BandwidthBps()`); the audio group URI is the
single `audio_profile`. DASH gets all profiles as Representations in one video
AdaptationSet.

#### 3.3.6 A/V sync & seek guarantees on read

- A/V sync: V and A share one media origin; the reader never re-stamps — it
  copies fragment bytes and emits stored `tfdt`-derived timing
  (MPD `<S t=>`, fMP4 `tfdt`) + derived `PROGRAM-DATE-TIME`. Audio is paired to
  video by `MediaTicks` overlap (§3.3.2 step 5). Cross-hour continuity via
  absolute `MediaTicks`.
- Seek: keyframe-snapped binary search on `MediaTicks` (hour-boundary-aware).
  Precision is fragment/GOP-granular (documented non-goal). `dur=0` ⇒ to end of
  archive (legacy semantics).

### 3.4 Multi-profile, retention, routing

#### 3.4.1 Coordinator wiring

`StreamDVRConfig` extends (all zero-value-inherit per template rules):

```go
Format         string `json:"format"`          // "" | "ts" (legacy) | "cmaf" ; default domain.DefaultDVRFormat
Profiles       string `json:"profiles"`        // "best" (audio-source profile only) | "all" ; default "best"
RetentionHours int    `json:"retention_hours"` // hour-granular retention (preferred for blob archive)
MaxWindowSec   int    `json:"max_window_sec"`  // reader window cap
FsyncPolicy    string `json:"fsync_policy"`    // "per_fragment" | "per_n" | "interval" (default)
// existing: Enabled, RetentionSec, SegmentDuration, StoragePath, MaxSizeGB (still honored)
```

`coordinator.Start` (coordinator.go:360) branches on `Format`:

- `"ts"` (default) → unchanged
  `c.dvr.StartRecording(ctx, code, PlaybackBufferID(...), cfg)`.
- `"cmaf"` → build `profiles` from
  `buffer.RenditionsForTranscoder(stream.Code, stream.Transcoder)`:
  - `len(rends)==0` (passthrough) → single `p0` on buffer `stream.Code`.
  - else `Profiles=="all"` → one `ProfileSub` per rendition
    (`RenditionBufferID(code, slug)`, id `p<index>`); `best_profile` =
    `pK` where K = `BestRenditionIndex`. `Profiles=="best"` → just the
    best rung as its own `p<index>` dir (still index-named, not forced `p0`).
  - **Audio recorded once**: only the `audio_profile` lane (the best rung)
    opens `.cmfa` and calls `PushAudio`; all others are video-only. Every
    profile's catalog audio fields point at `audio_profile`. (Resolves the
    duplicate-audio finding — folded into v1, not deferred.)
  - **Call the concrete blob service directly** (NOT via `dvrDep`): the
    multi-profile signature `StartRecording(ctx, code, []ProfileSub, cfg)`
    differs from `dvrDep`'s single-`StreamCode`. `coordinator` holds a
    `blobDVR *blob.Service` field alongside the legacy `dvr dvrDep`; the
    `cmaf` branch calls `c.blobDVR` directly. `IsRecording`/`StopRecording` are
    dispatched by checking both. (Resolves the dvrDep-signature gap.)

`reloadDVRIfBufferChanged` (coordinator.go:774) for `cmaf` becomes "reload if
the rung *set* changed": on a single profile add/remove, **clean-seal** the
removed lane (fsync `.cmfv`/`.cmfa`, set `sealed`, remove `.open`, post catalog
update) before closing FDs, and start the added lane staggered. Retention then
tolerates asymmetric per-profile hour sets (resolves the lane-reload finding).
A single best-rung change no longer cycles the whole DVR. Keep this behind a
sub-flag initially; fall back to full-cycle if uncertain.

`domain.Recording` keeps `ID = StreamCode`; `SegmentDir` →
`<root>/<streamCode>` (the stream root holding `catalog.json`). No
`RecordingRepository` schema break.

#### 3.4.2 Retention by hour / day / size (whole-file deletes, one central pass)

A single per-stream retention pass (not per-lane), on a 60 s timer and on
rotation, owned by the Service goroutine:

1. From the catalog snapshot compute total size (`Σ hours.size_bytes` across
   profiles) and the oldest hour across the **union** of all profiles' hour
   timestamps.
2. While `(now − oldestHourStart) > maxAge` OR `totalSize > maxSizeBytes` OR
   `oldestDay older than RetentionDays`:
   - For the oldest hour, delete that hour's files **only for the profiles that
     have it** (asymmetric-tolerant): `os.Remove` each present
     `p*/YYYY/MM/DD/HH.{cmfv,cmfa,ranges}`; `rmdir DD/` when a day empties.
   - Drop the catalog hour entries + shrink `available[]`; prune `gaps` ending
     before the new oldest.
3. **Never delete**: the in-progress hour; a crashed-but-resumable hour
   (`sealed:false` + present `.open` + recent mtime — distinguished from a
   normally-prunable sealed-false-due-to-crash-long-ago); or **any hour younger
   than a read grace** (e.g. `max(30s, 2×max_request_time)` beyond the
   retention boundary) so an in-flight reader's just-snapshotted FragmentRefs
   still resolve. Combined with hold-FD-for-whole-request + treat
   ENOENT/short-read as **404/410** (not 500), this resolves the
   retention-vs-reader race.

Whole-file delete = O(1) per hour, never leaves a half-deleted file; size cap
sums per-profile (and accounts for per-hour init + box overhead, see §7).

#### 3.4.3 Routing hook

In `dispatchMedia` (dispatch.go:88), branch on recording format (presence of
`catalog.json` vs `playlist.m3u8`):

- `mediaFileHLSIndex` + `isDVRPlaybackRequest` → `catalog.json` present →
  `blobH.ServeTimeshift`; else legacy `recordingH.ServeTimeshift`.
- `index.mpd` + `isDVRPlaybackRequest` → `blobH.ServeMPD` (new branch).
- `dvr/<profile>/…` → `blobH.ServeFragment` / `ServeInit`.
- Legacy `dvr_NNNNNN.ts` branch (dispatch.go:127) stays until the final phase.

No new abstraction — `BlobTimeshiftHandler` parallels `RecordingHandler`,
reuses `parseTimeshiftStart` / `parseTimeshiftDuration`.

---

## 4. Package / file layout + reused & changed code

### 4.1 New package `internal/dvr/blob`

```
internal/dvr/blob/
  service.go     Service: StartRecording([]ProfileSub)/StopRecording/IsRecording; per-stream owner
                 goroutine = sole catalog mutator + retention + reader-snapshot publisher
  lane.go        per-(stream,profile) receiveLoop + flushLoop (split goroutines); ingest→cut→build→flushCh
  blobfile.go    hourBlob: lazy create, append, fsync, rotate, truncate; init-at-head; running offset;
                 .open sentinel; parent-dir fsync
  ranges.go      RangesHeader + FragRecord binary encode/decode (CRC16); append; in-place header rewrite;
                 binary search by MediaTicks; IDR-snap (hour-boundary-aware caller)
  recovery.go    startup scan: validate counted records (CRC+size), tail re-scan styp/moof/mdat via
                 mp4ff DecodeHeader, truncate torn tail, repair catalog hour, reload origin/init
  catalog.go     Catalog load/save (atomic tmp+rename); profile/hour/available bookkeeping;
                 origin anchor; best_profile/audio_profile; gaps
  reader.go      BlobReader: Query(profile,from,dur)→*Window; ReadAt slice; init byte range; held FDs
  hls.go         master + media playlist (fMP4 + EXT-X-MAP) from records
  dash.go        timeshift MPD via BuildManifest (single-Period) + BuildMultiPeriodManifest (Phase 4);
                 static/dynamic; per-fetch AST for dynamic
  retention.go   central hour/day age+size pruning (whole-file, asymmetric-tolerant, grace window)
  migrate.go     legacy .ts → blob migrator (reuses ParsePlaylist + tsdemux + Normaliser)
  paths.go       p<N>/YYYY/MM/DD/HH builder (server-only components; resolveSegDir containment reuse)
  handler.go     BlobTimeshiftHandler: ServeTimeshift / ServeMPD / ServeMaster / ServeFragment / ServeInit
  *_test.go      table tests: ranges roundtrip+CRC, recovery (truncate torn / keep uncounted),
                 cross-hour seek + keyframe-snap across boundary, A/V media-time pairing, rotation
                 contiguity, retention asymmetric + grace, manifest output, multi-Period, copy://
                 passthrough, migration conservation
```

### 4.2 Shared `dash` refactor (Phase 0 — no behavior change; existing packager tests cover)

Export, in `internal/publisher/dash`:

| New exported symbol | Was | Use |
|---|---|---|
| `ComputeVideoSegDurTicks(frames, nextPTSms, hasNext) uint64` | `computeVideoSegDurTicks` (packager.go:1030) | DVR seg dur |
| `WallclockTicks(now, ast, timescale) uint64` | `wallclockTicks` (packager.go:1004) | dynamic-MPD AST math only |
| `SplitADTSBundle(...)` + the `onTSFrame`/TS-demux ingress as `dash/cmaf.Ingress` (mutex-guarded queue) | unexported ingress | shared recorder ingress |

**Do NOT export `videoTfdtForSegment` / `audioTfdtForSegment`** — the DVR
computes PTS-derived tfdt itself (§3.1.3). They stay internal to the live
packager.

New (Phase 4) `BuildMultiPeriodManifest([]PeriodInput) *mpdRoot` next to
`BuildManifest` (manifest.go) — a real multi-Period function with its own
tests.

### 4.3 Key Go types / interfaces

```go
// service.go
type ProfileSub struct {
    ID         string            // "p0" (= rendition index)
    Slug       string            // "track_1" (catalog buffer_slug; detects ladder reorder)
    BufferID   domain.StreamCode // RenditionBufferID(code, slug) (or code for passthrough)
    Width, Height, BitrateKbps int
    IsAudioSource bool           // exactly one true
}
type Service struct {
    buf *buffer.Service; bus events.Bus; m *metrics.Metrics; recRepo store.RecordingRepository
}
func (s *Service) StartRecording(ctx context.Context, code domain.StreamCode, profiles []ProfileSub, cfg *domain.StreamDVRConfig) (*domain.Recording, error)
func (s *Service) StopRecording(ctx context.Context, code domain.StreamCode) error
func (s *Service) IsRecording(code domain.StreamCode) bool

// ranges.go
type RangesHeader struct { /* §2.3 header, 64 bytes; little-endian */ }
type FragRecord struct {
    Track uint8; Keyframe, Discontinuity bool; CRC16 uint16
    SampleCount uint32; WallTimeMs int64
    MediaTicks, DurTicks uint64; ByteOffset uint64; ByteLen uint32
}
type RangesWriter interface { Append(FragRecord) error; Seal() error; Sync() error }
type RangesReader interface {
    Header() RangesHeader
    SearchByMedia(track uint8, ticks uint64) int // binary search; caller does IDR-snap
    Records(track uint8) []FragRecord            // bounded by durable RecordCount
}

// reader.go
type FragmentRef struct { /* §3.3.2 */ }
type Window struct {
    Profile string
    Video, Audio []FragmentRef
    VideoInit, AudioInit []byte
    PDT0 time.Time
    Discontinuities []int // record indices carrying discontinuity-before
}
type BlobReader struct { /* held FDs, catalog snapshot */ }
func (br *BlobReader) Query(profile string, from time.Time, dur time.Duration) (*Window, error)
func (br *BlobReader) Fragment(ref FragmentRef) (io.ReadSeeker, error) // ReadAt-backed, held FD
func (br *BlobReader) InitRange(profile string, track uint8) (io.ReadSeeker, error)

// catalog.go
type ProfileDesc struct {
    ID, BufferSlug, Codec, AudioCodec string
    Width, Height, Bandwidth, SampleRate int
    Available []MediaWindow
    Hours []HourRecord
}
type HourRecord struct {
    Hour string; WallFromMs, WallToMs int64; MediaFromV, MediaToV uint64
    Sealed, Discontinuity bool; FragCountV, FragCountA int; SizeBytes int64
}
type Catalog struct {
    StreamCode string; Format string
    RecordingMediaOriginTicks uint64; RecordingMediaOriginUnixMs int64
    AudioProfile, BestProfile string
    Profiles []ProfileDesc; Gaps []Gap; Retention RetentionCfg
}
func (c *Catalog) Save(dir string) error // atomic tmp+rename; called ONLY by Service owner
```

### 4.4 Reused / changed existing code (file:line)

**Reused without modification:**

- `internal/publisher/dash/fmp4_writer.go` — `BuildVideoFragment` (l.142),
  `BuildAudioFragment` (l.216), `BuildH264Init`/`BuildH265Init`/`BuildAACInit`/
  `EncodeInit`, `ExtractParameterSets` (l.262)/`ExtractHEVCParameterSets`.
- `internal/publisher/dash/frame_queue.go` — `VideoFrame`/`AudioFrame`/
  `FrameQueue`/`NewFrameQueue`.
- `internal/publisher/dash/segmenter.go` — `Segmenter`/`NewSegmenter`/`Cut`
  (pure decision, l.136)/`MarkCut`/`Reset`. (Per-lane instance; `Reset` on
  discontinuity clears `audioFrameResidualSamples`.)
- `internal/publisher/dash/manifest.go` — `SegmentEntry`/`TrackManifest`/
  `ManifestInput`/`BuildManifest` (single-Period reuse).
- `internal/buffer/rendition.go` — `RenditionsForTranscoder` (l.32),
  `RenditionBufferID` (l.11), `PlaybackBufferID` (l.87), `BestRenditionIndex`
  (l.69), `RenditionPlayout.BandwidthBps` (l.96).
- `internal/dvr/index.go` — `resolveSegDir` (l.24) containment guard,
  `saveIndex` atomic tmp+rename pattern (l.62).
- `internal/dvr/service.go` — `ParsePlaylist` (l.302) for migration.
- mp4ff@v0.51.0 — `mp4.DecodeHeader` (box.go:204) / `DecodeBox` (box.go:331)
  for recovery; `NewMediaSegmentWithoutStyp` (mediasegment.go:39) available if
  a styp-free DVR builder is ever chosen (NOT in v1 — v1 keeps verbatim
  builders, records `styp` start).

**Changed:**

- `internal/publisher/dash` Phase 0 exports (§4.2).
- `internal/publisher/dash/manifest.go` — add `BuildMultiPeriodManifest`
  (Phase 4).
- `internal/domain/recording.go` — extend `StreamDVRConfig` (l.60) with
  `Format`/`Profiles`/`RetentionHours`/`MaxWindowSec`/`FsyncPolicy`; add
  `domain.DefaultDVRFormat`.
- `internal/coordinator/coordinator.go` — add `blobDVR *blob.Service` field;
  branch `Start` (l.360), `IsRecording`/`StopRecording` call sites
  (l.467/597/759), `reloadDVRIfBufferChanged` (l.774) on `Format`.
- `internal/coordinator/deps.go` — add `blobDVR *blob.Service` (concrete, NOT
  via `dvrDep`).
- `internal/api/dispatch.go` — routing hook (l.88) + `index.mpd` timeshift
  branch.
- `cmd/server/main.go` — register `blob.Service` in DI.

---

## 5. Phased implementation plan

Each phase is independently shippable behind `StreamDVRConfig.Format`; legacy
`.ts` stays the **default** until Phase 6.

### Phase 0 — `dash` shared helpers
Files: `internal/publisher/dash/packager.go`, `manifest.go` (and a new
`dash/cmaf/ingress.go` if extracting the TS ingress).
- Export `ComputeVideoSegDurTicks`, `WallclockTicks`, `SplitADTSBundle`, and the
  mutex-guarded `cmaf.Ingress` (AV + TS-demux path). No behavior change.
- Test: existing packager tests stay green (rename-only). Unblocks everything.

### Phase 1 — Blob format + writer core (single profile, no serving)
Files: `internal/dvr/blob/{ranges.go,blobfile.go,catalog.go,lane.go,recovery.go,
service.go,paths.go}`; `internal/domain/recording.go`;
`internal/coordinator/{coordinator.go,deps.go}`; `cmd/server/main.go`.
- Single lane from `PlaybackBufferID`; PTS-derived tfdt; split receive/flush
  goroutines; fsync policy; lazy file create; rotation; CRC16 records;
  crash-tail recovery.
- **Includes copy:// passthrough** (full TS-demux ingress) — not deferred.
- Tests: ranges roundtrip+CRC; rotation contiguity
  (`wall_from[N+1]==wall_to[N]`); recovery truncates torn tail AND keeps a
  fully-written-but-uncounted fragment; tfdt-contiguity assertion; copy:// raw
  TS in → valid `[styp][moof][mdat]` out (mp4ff-decode a slice, assert first
  box == `styp` and round-trips); A/V `tfdt` continuity across a simulated
  resume seam within one audio-frame.
- Value: durable, mp4-tooling-verifiable CMAF archive on disk. Legacy DVR &
  serving untouched.

### Phase 2 — HLS timeshift read (single profile)
Files: `internal/dvr/blob/{reader.go,hls.go,handler.go}`;
`internal/api/dispatch.go`.
- `BlobReader.Query` (media-time search, keyframe-snap incl. hour-boundary
  cross, media-time audio pairing, held FDs, window cap); v7 fMP4 media
  playlist; `ServeInit`/`ServeFragment`. Wire `dispatchMedia` (try blob, fall
  back to legacy).
- Tests: seek 200 ms into a GOP near an hour boundary → first served video
  fragment is an IDR (possibly from prev hour), first audio fragment
  `tfdt ≤ video start tfdt`, both inits present; ENOENT/short-read → 404.
- Value: working fMP4 timeshift, single profile.

### Phase 3 — Multi-profile + retention
Files: `internal/dvr/blob/{retention.go,catalog.go,service.go,lane.go,hls.go}`;
`internal/coordinator/coordinator.go`.
- Fan out lanes to N rendition buffers; **audio recorded once** on
  `audio_profile`; `best_profile`/`audio_profile` in catalog; per-profile
  `available[]`; HLS master; central retention (asymmetric-tolerant, grace
  window); per-lane clean-seal on rung add/remove.
- Tests: add a rung mid-recording then prune → no dangling refs, catalog
  consistent; prune a non-audio profile → audio group URIs still resolve;
  retention grace lets an in-flight reader finish.
- Value: ABR timeshift.

### Phase 4 — DASH timeshift (incl. multi-Period)
Files: `internal/dvr/blob/dash.go`; `internal/publisher/dash/manifest.go`;
`internal/api/dispatch.go`.
- Static MPD (primary) via `BuildManifest`; dynamic MPD with per-fetch AST;
  `BuildMultiPeriodManifest` for codec-change windows (v1 clamps a spanning
  window otherwise); `index.mpd` timeshift route.
- Tests: cross-hour MPD timeline non-overlapping; dynamic MPD from a drifted
  recording → implied live edge within one segDur of newest; multi-Period
  across a codec change feeds correct per-Period init.
- Value: DASH parity with HLS.

### Phase 5 — Migration + catalog-rebuild tooling
Files: `internal/dvr/blob/migrate.go`; admin endpoint; CLI
`open-streamer dvr migrate <code>`.
- Offline `.ts` → blob replay through `tsdemux` → `timeline.Normaliser`
  (legacy `.ts` is not pre-normalized) → `cmaf.Ingress` → Segmenter →
  Build*Fragment; per-hour `.migrated` markers (after fsync+catalog) recording
  the source `.ts` range (idempotent/resumable).
- Tests: total media duration preserved within one fragment; every original
  `.ts` gap → exactly one `discontinuity-before`; migrated `WallTimeMs`
  monotonic & within original `[start,end]`; discontinuity-bearing legacy
  recording migrates cleanly.
- Value: existing recordings playable in the new format; legacy handler serves
  un-migrated ones meanwhile.

### Phase 6 — Flip default + legacy removal
Files: `internal/domain/recording.go`; `internal/dvr/service.go`;
`internal/api/dispatch.go`; CLAUDE.md.
- After soak, default `Format` to `cmaf`; delete the legacy `.ts` writer path,
  its `tsmux.FromAV` DVR usage, and the `dvr_NNNNNN.ts` dispatch branch; add
  §1.3 invariants to CLAUDE.md.
- Value: single archive format; smaller surface.

---

## 6. Migration, config, rollback

### 6.1 Config
- `dvr.format`: `ts` (default through Phase 5) | `cmaf`. Inherited per template
  zero-value semantics.
- `dvr.profiles`: `best` (default; audio-source profile only) | `all`. Cap
  ladder width recorded to bound FD/inode/disk.
- `dvr.fsync_policy`, `dvr.retention_hours`, `dvr.max_size_gb`,
  `dvr.max_window_sec`.
- Deploy docs: require `LimitNOFILE` ≥ 65536; the service logs its open-FD
  count vs. soft limit at start (FD budget: a video-only lane holds
  `.cmfv` + `.ranges` = 2 FDs; the audio-source lane +`.cmfa`).

### 6.2 Migration
- Detect legacy: `index.json` + `playlist.m3u8` + `dvr_*.ts`, no
  `catalog.json`. Migrator runs offline / against a stopped recording.
- Drive migrated `WallTimeMs` from the **demuxed frame PTS** mapped through the
  per-`.ts` `EXT-X-PROGRAM-DATE-TIME` anchor
  (`frame_wall = seg.wallTime + (frame.PTS − seg_first.PTS)`), NOT from the
  new (re-cut) fragment boundaries — wall time stays faithful even though
  Segmenter re-cuts at different IDR boundaries. Run frames through the
  Normaliser (Segmenter assumes pre-normalized input). Bucket by the frame's
  own wallclock. Carry a `discontinuity-before` onto the first new fragment at
  each original `.ts` gap.
- `.migrated` per-hour markers written **after** fsync+catalog for verifiable
  idempotency. `--prune` deletes legacy files; otherwise moved to
  `<dir>/legacy/`.

### 6.3 Rollback
- Per-stream: set `dvr.format: ts` → coordinator routes to the legacy writer;
  blob files on disk are inert. Old timeshift URLs resolve via the legacy
  handler (dispatch falls back when `catalog.json` is absent / format=ts).
- Global: keep Phase ≤ 5 (default `ts`); the blob path is opt-in. No data loss
  on rollback — blob and `.ts` recordings coexist; serving picks by catalog
  presence.

---

## 7. Risks & open questions

**Resolved in this design (review findings folded in):** dual-clock divergence
(single PTS-derived clock + persisted anchor); independent V/A tfdt desync
(shared origin, monotonic-guarded, per-lane Segmenter); origin not persisted
(in catalog, reloaded on resume); buffer-hub silent drops under fsync stall
(split receive/flush goroutines + enlarged channel + drop metric + forced
discontinuity); recovery commit-point ordering (CRC16 + blob-as-truth +
keep-uncounted tail-scan); hour-bucketing mis-file (key by first-fragment hour,
select by actual wall bounds, probe adjacent hour); retention-vs-reader race
(grace window + held FDs + 404-on-gone + snapshot); lazy-init-vs-rotation
(deferred file create); migration boundary mismatch (PTS-mapped wall + Normaliser
+ conservation tests); styp framing (`ByteOffset` at styp, recover styp→moof→mdat);
multi-Period DASH (real `BuildMultiPeriodManifest`, v1 clamp otherwise); copy://
passthrough (full TS-demux ingress in Phase 1); keyframe-snap hour-boundary +
media-time audio pairing; per-profile audio duplication (record once);
profile-id↔index ambiguity (`pN` = index, `best_profile`/`audio_profile` in
catalog); per-tick catalog churn (catalog persisted only on rotation/close/prune,
liveness in-memory); seqNum overflow (per-hour); dynamic-AST drift (per-fetch
AST); dvrDep signature (concrete `blob.Service` field).

**Open questions / to settle during implementation:**

1. **Per-sample-duration fidelity.** v1 reuses the uniform-duration builder
   (seek precision is GOP-granular, documented non-goal). The integer-division
   remainder in `frameDur` (fmp4_writer.go:162) leaves up to `len-1` ticks per
   fragment uncovered vs. `DurTicks`; inherited from live, but a 24 h archive
   amplifies exposure. **Open:** add a long-playback integration test (concat
   1000+ fragments → real player / mp4ff validator) before flipping the Phase 6
   default; if it stalls strict MSE pipelines, add a DVR builder that
   distributes the remainder across the first `segDurTicks mod len` samples
   (then it is NOT verbatim reuse — call it out).
2. **Audio sample-rate change mid-recording.** `AudioTimescale` is per-hour in
   the `.ranges` header, so a per-hour change is representable, but cross-hour
   `MediaTicks` (in samples) becomes non-comparable across the change and the
   DASH shared-audio Representation assumes one timescale. **Open:** treat an
   audio-SR change like a codec-param change (force rotation + discontinuity +
   new audio init); v1 may simply reject DASH windows spanning it.
3. **Disk-overhead vs. legacy.** fMP4 box overhead per fragment + a re-encoded
   init per hour per profile, vs. the legacy single-best `.ts`. With
   `profiles: all` this is a multi-x increase. **Open:** publish a concrete
   bytes/hour/profile estimate and confirm `MaxSizeGB` accounting (§3.4.2 sums
   per-profile sizes including init + box overhead) so operators' caps still
   bound disk as expected.
4. **SessionStart flush-vs-drop policy** is specified as *flush-then-mark*
   (DVR must not lose recorded media at failover). **Open:** confirm the
   transcoder's `SessionStart` cadence doesn't make the pre-flush fragment
   pathologically short; if so, merge it into the next fragment with the
   discontinuity flag instead of emitting a sub-segDur fragment.
5. **Catalog mutation serialization** is centralized in the Service owner
   goroutine (single writer). **Open:** confirm rotation-driven catalog updates
   from N lanes don't queue-starve retention under a wide ladder; if so, batch
   catalog mutations on the same fsync-interval timer.
