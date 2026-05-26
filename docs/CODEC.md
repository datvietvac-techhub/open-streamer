# Open Streamer — Codec Support Matrix

What codecs can be **ingested** (recognised by the data path), **encoded**
(produced by the transcoder), **muxed** (re-wrapped into MPEG-TS), and
**delivered** (HLS / DASH / MPEG-TS / RTMP / SRT / RTSP). For the
operator-facing stream config knobs see
[CONFIG.md](./CONFIG.md); for the data-path topology see
[ARCHITECTURE.md](./ARCHITECTURE.md).

---

## 1. Reading the matrix

A codec lands in this server through one of three doors:

- **Ingest path** — recognised at byte level (passthrough always works) AND
  at AVPacket level (needed for stats panel, mixer extraction).
- **Encoder path** — `transcoder.video.codec` / `transcoder.audio.codec`
  resolves to an FFmpeg encoder. Without this, the user setting is silently
  fallen back to a default (libx264 / aac) — set the right value or use
  `copy: true` for passthrough.
- **Re-mux path** — `tsmux.FromAV` knows how to wrap the AVPacket into
  MPEG-TS for downstream segmenters, push handlers, and the `mixer://`
  reader.

Three independent capabilities. A codec might be ingest-recognised but
not encodable (you can stat it, can't transcode TO it), or encodable but
not ingest-recognised (you can encode TO it, but the stats panel shows
"unknown" if the source uses it).

---

## 2. Audio codecs

| Codec | Label | Ingest TS | Ingest RTMP | Ingest RTSP | Stats panel | Encode OUTPUT | tsmux re-mux | mixer:// extract | HLS browser |
|---|---|---|---|---|---|---|---|---|---|
| AAC | `aac` | ✅ | ✅ | ✅ | ✅ | ✅ `aac` | ✅ | ✅ | ✅ |
| MPEG-1/2 Audio Layer I/II | `mp2a` | ✅ | ❌ | ❌ | ✅ | ✅ `mp2` | ✅ | ✅ | ❌ (transcode → AAC) |
| MPEG-1/2 Audio Layer III | `mp3` | ✅ frame parse | ❌ | ❌ | ✅ | ✅ `libmp3lame` | ✅ | ✅ | ✅ |
| AC-3 (Dolby Digital) | `ac3` | ✅ ATSC + DVB | ❌ | ❌ | ✅ | ✅ `ac3` | ⚠️ skip-with-comment | ❌ (no frame extractor) | ⚠️ Safari/Chrome macOS only |
| E-AC-3 (Dolby Digital Plus) | `eac3` | ✅ ATSC + DVB | ❌ | ❌ | ✅ | ✅ `eac3` | ⚠️ skip-with-comment | ❌ (no frame extractor) | ⚠️ same as AC-3 |

### TS recognition for audio

| Source signal | Detected as |
|---|---|
| `stream_type` 0x03 (MPEG-1 audio) | `mp2a` (upgraded to `mp3` if first PES sync detects Layer III) |
| `stream_type` 0x04 (MPEG-2 audio) | `mp2a` (upgraded to `mp3` likewise) |
| `stream_type` 0x0F (AAC ADTS) | `aac` |
| `stream_type` 0x11 (AAC LATM) | `aac` |
| `stream_type` 0x81 (ATSC AC-3) | `ac3` |
| `stream_type` 0x87 (ATSC E-AC-3) | `eac3` |
| `stream_type` 0x06 + descriptor 0x6A (DVB AC-3) | `ac3` |
| `stream_type` 0x06 + descriptor 0x7A (DVB E-AC-3) | `eac3` |

---

## 3. Video codecs

| Codec | Label | Ingest TS | Ingest RTMP | Ingest RTSP | Stats panel | Resolution detect | Encode OUTPUT | tsmux re-mux | mixer:// extract | HLS browser |
|---|---|---|---|---|---|---|---|---|---|---|
| H.264 / AVC | `h264` | ✅ | ✅ | ✅ | ✅ | ✅ SPS parse | ✅ libx264 / NVENC / VAAPI / QSV / VideoToolbox | ✅ | ✅ | ✅ |
| H.265 / HEVC | `h265` | ✅ | ✅ Enhanced RTMP | ✅ | ✅ | ✅ SPS parse | ✅ libx265 / NVENC / VAAPI / QSV / VideoToolbox | ✅ | ✅ | ⚠️ Safari + modern Chrome |
| MPEG-2 Video (H.262) | `mp2v` | ✅ stream_type 0x02 | ❌ | ❌ | ✅ | ❌ (no sequence_header parser) | ✅ `mpeg2video` | ⚠️ skip-with-comment | ❌ (no frame extractor) | ❌ |
| AV1 | `av1` | ✅ DVB descriptor "AV01" | ❌ | ❌ | ✅ | ❌ (no OBU parser) | ✅ `libsvtav1` | ⚠️ skip-with-comment | ❌ (no frame extractor) | ⚠️ recent Chrome / Edge / Firefox |

### TS recognition for video

| Source signal | Detected as |
|---|---|
| `stream_type` 0x01 (MPEG-1 Video) | `mp2v` (lumped with MPEG-2) |
| `stream_type` 0x02 (MPEG-2 Video) | `mp2v` |
| `stream_type` 0x1B (H.264) | `h264` |
| `stream_type` 0x24 (H.265) | `h265` |
| `stream_type` 0x06 + registration descriptor format ID `"AV01"` | `av1` |

---

## 4. FFmpeg encoder mappings

`transcoder.audio.codec` and `transcoder.video.codec` resolve to FFmpeg
encoder names via [`internal/domain/resolvers.go`](../internal/domain/resolvers.go).
Empty / unknown codec values silently fall back to `libx264` (video) or
`aac` (audio) — set the right value or use `copy: true` to passthrough.

### Audio

| User config | FFmpeg encoder | External library required |
|---|---|---|
| `aac` (default) | `aac` | none (built-in) |
| `mp2a` | `mp2` | none (built-in) |
| `mp3` | `libmp3lame` | yes — `--enable-libmp3lame` |
| `ac3` | `ac3` | none (built-in) |
| `eac3` | `eac3` | none (built-in) |
| `copy` | (no encode) | n/a |

### Video

| User config | HW=none | HW=NVENC | HW=VAAPI/QSV/VideoToolbox |
|---|---|---|---|
| `h264` / `avc` (default) | `libx264` | `h264_nvenc` | (use explicit encoder name) |
| `h265` / `hevc` | `libx265` | `hevc_nvenc` | (use explicit encoder name) |
| `av1` | `libsvtav1` | (no GPU path) | (no GPU path) |
| `mp2v` / `mpeg2video` | `mpeg2video` | (no GPU path) | (no GPU path) |
| explicit encoder name (e.g. `h264_qsv`) | preserved | preserved | preserved |
| `copy` | (no encode) | (no encode) | (no encode) |

External libraries (`libmp3lame`, `libsvtav1`, `libx264`, `libx265`)
require the FFmpeg build to enable them at compile time. The probe
endpoint (`POST /config/transcoder/probe`) reports which encoders are
actually present in the binary the operator points to.

---

## 5. Architecture notes

### 5.1 Two recognition paths run in parallel

Raw-TS sources (UDP / HLS / SRT / File) flow bytes through the buffer hub
unchanged — *no codec recognition is required for the data path itself*.
The two independent recognisers run in side channels:

1. **gompeg2 demuxer** (in `tsdemux_packet_reader.go`) — produces full
   AVPackets (with PTS/DTS/keyframe flags) for the codecs it understands:
   H.264, H.265, AAC, MPEG audio. Used by RTSP/RTMP-style downstreams that
   need real frames (mixer extraction, transcoder when fed AVPackets).

2. **PMT scanner** (in `stats_pmt_scanner.go`) — parses PSI tables and
   maps every PMT-listed PID to its codec via stream_type + DVB
   descriptors. Emits stats AVPackets for *every* recognised PID, including
   AC-3, E-AC-3, MPEG-2 Video, and AV1 (codecs gompeg2 drops on the
   floor). Used only for the input "tracks" panel — does not produce
   frames.

The PMT scanner replaced an earlier gompeg2-based stats path; without it,
DVB AC-3 / MPEG-2 / AV1 sources showed an empty "tracks" panel in the UI
even when the data path was healthy.

### 5.2 Why mixer:// can't extract these new codecs

`mixer://video,audio` re-interleaves AVPackets at frame granularity. The
gompeg2 library does not extract frames for AC-3, E-AC-3, MPEG-2 Video,
or AV1, so the side path that powers the mixer reader has no source to
draw from.

Building a custom frame extractor for each codec is a separate piece of
work: AC-3 needs frame-sync scanning (`0x0B77`), MPEG-2 needs
sequence-header / picture-header parsing, AV1 needs OBU walking. None of
these are difficult but each is its own ~150-line module with its own
test set. The current decision is to add them on demand rather than
preemptively.

### 5.3 Why tsmux skips these codecs

`tsmux.FromAV` (see [internal/tsmux/fromav.go](../internal/tsmux/fromav.go))
has explicit `case` arms for codec → TS stream_type. AC-3 / E-AC-3 /
MPEG-2 Video / AV1 are present in the switch for compile-time
exhaustiveness but return immediately — there are no AVPackets to mux
because of §5.2. The cases exist so a future contributor wiring frame
extraction sees the muxer call site and doesn't chase a silent-skip bug.

### 5.4 Resolution detection

`MediaTrackInfo.Width / Height` is populated by parsing the codec's
sequence parameters on the first observed keyframe. Currently implemented
only for H.264 (AVC SPS) and H.265 (HEVC SPS). MPEG-2 Video (sequence
header) and AV1 (sequence header OBU) would each need ~80–150 lines.
Audio codecs have no concept of resolution.

### 5.5 RTMP codec-config delivery

RTMP delivers codec configuration (`AVCDecoderConfigurationRecord` for
H.264, equivalent for H.265, `AudioSpecificConfig` for AAC) **out of
band** as separate "sequence header" tags, not in the per-frame data.
That makes the moment-of-arrival of the first sequence-header tag the
only window for downstream muxers to learn the parameters they need to
build init segments / generate decodable PES. The RTMP pull and push
paths both ride
[`internal/ingestor/pull/rtmp_msg_converter.go`](../internal/ingestor/pull/rtmp_msg_converter.go)
and the same invariant applies to both.

#### Pull side — IDR access units must always carry param sets

`RTMPMsgConverter.ensureKeyFrameHasParamSets` evaluates a 3-tier
resolution order on every key NALU message:

1. **Inline scan** — the IDR's Annex-B is scanned for SPS/PPS NALUs
   (NAL types 7/8 for AVC; 32/33/34 for HEVC). Found ⇒ cache as the
   prefix and emit `annexB` unchanged. This handles encoders that emit
   parameter sets in-band on every IDR (most modern x264/x265 with
   `repeat_headers=1`, NVENC's default).
2. **Cached prefix** — if `captureVideoSeqHeader` populated the prefix
   from a prior `AVCPacketType=0` tag, prepend it to the slice-only
   IDR. This handles encoders that send param sets only via the
   sequence-header tag (FFmpeg `-bsf:v dump_extra=freq=keyframe`
   disabled, encoder default).
3. **Drop** — neither found ⇒ return `nil` so the buffer hub never
   sees an IDR without init params. Without this drop, the IDR would
   land in the buffer hub, every downstream HLS/DASH muxer would queue
   an undecodable access unit, and DASH's `videoPSGiveUp` cap (4 MiB)
   would eventually latch the stream into permanent audio-only.

The bug fix that made this reliable: lal's `SpsPpsSeqHeader2Annexb`
expects the FULL FLV tag payload (5-byte header + AVCDecoderConfig)
and verifies `payload[:5] == {0x17, 0, 0, 0, 0}` internally; passing
`msg.Payload[5:]` cuts off the FLV header and the function silently
fails. `captureVideoSeqHeader` now passes `msg.Payload` unmodified.

The RTSP pull side mirrors the same 3-tier resolution. The H264 and
H265 callbacks in [`pull/rtsp.go`](../internal/ingestor/pull/rtsp.go)
maintain a per-stream parameter-set cache seeded from the SDP
`sprop-parameter-sets` (or `sprop-vps` / `sprop-sps` / `sprop-pps`
for HEVC) and refreshed on every IDR whose NAL units include inline
params (scanned via `findH264SPSPPSInNALUs` / `findH265VPSSPSPPSInNALUs`
in [`pull/h264_helpers.go`](../internal/ingestor/pull/h264_helpers.go)).
Slice-only IDRs get the cached prefix prepended; IDRs with neither
inline nor cached params are dropped (with a rate-limited warn log).
Required because some RTSP servers omit `sprop-parameter-sets` from
SDP (relying on the encoder to repeat in-band) AND some encoders
omit in-band SPS/PPS (relying on SDP) — without the dual-source
cache + drop fallback the empty-intersection case poisoned downstream
muxers with un-init-able keyframes.

#### Push side (RTMP play out) — slice-only NALU tags + bundle-split AAC

`internal/ingestor/push/rtmp_writer.go` strips SPS/PPS/AUD/SEI NALUs
from per-frame video tags via `buildAvccSliceOnly` because strict
players (strict players) reject NALU tags that contain non-slice
NALUs — those belong in the AVCDecoderConfigurationRecord. Sequence
headers ride either:

- the inline-extract path in `writeH264` (first IDR with in-band SPS/PPS), or
- the explicit `PreloadAvcSeqHeader` call from
  [`publisher/serve_rtmp.go`](../internal/publisher/serve_rtmp.go) after
  the producer goroutine scans raw TS bytes for SPS/PPS — needed when
  gomedia's TSDemuxer drops standalone parameter-set NAL units before
  invoking `OnFrame`.

For AAC, gomedia delivers the **whole PES payload** through `OnFrame`
and a single PES often carries 4–8 concatenated ADTS frames (encoders
bundle for efficiency — at 48 kHz, 8 frames = 170 ms per delivery).
RTMP convention is one raw AAC access unit per audio tag, so
`RTMPFrameWriter.writeAAC` splits the bundle and emits per-frame tags
with monotonically increasing DTS:

```text
frame_dts_ms = base_dts_ms + frame_index × 1024 × 1000 / sample_rate
```

Without splitting, downstream pull-RTMP consumers receive one tag
containing N frames, treat it as a single frame with `Dur=1024`, and
audio sample counts collapse by the bundling factor. Symptoms: DASH
audio segments declare `~24576/48000 ≈ 0.5 s` while the file actually
contains ~4 s of data, MPD timeline grows ~8× slower than wallclock,
and players report A/V drift or refuse to load the manifest.

---

## 6. Limitations & workarounds

### 6.1 Browser HLS playback

Apple's HLS spec lists AAC, AC-3, EC-3, MP3 as audio codecs and AVC, HEVC
as video. Browsers implement subsets:

| Browser | Audio | Video |
|---|---|---|
| Safari (macOS / iOS) | AAC, AC-3, EC-3, MP3 | H.264, H.265 (HW only) |
| Chrome (desktop) | AAC, MP3, AC-3 (macOS only) | H.264, H.265 (recent), AV1 |
| Firefox | AAC, MP3 | H.264, AV1 (recent) |
| Edge (Chromium) | AAC, MP3, AC-3 (Windows only) | H.264, H.265, AV1 |

For maximum compatibility, transcode anything else to **AAC + H.264** for
HLS output. MP2 audio in particular is **not** in the HLS spec and most
browsers won't decode it. Set `transcoder.audio.codec=aac` to convert.

### 6.2 mixer:// codec coverage

Only AAC / MP2 / MP3 audio + H.264 / H.265 video can be extracted by the
mixer. To use a new-codec audio source as a mixer audio input:

```yaml
streams:
  - code: src_ac3
    inputs:
      - url: udp://...
    transcoder:
      audio: { copy: false, codec: aac, bitrate: 192 }
      video: { copy: true }   # we only need to re-encode audio

  - code: mixed
    inputs:
      - url: mixer://video_stream,src_ac3   # mixer sees AAC, works
```

### 6.3 RTMP / RTSP codec map

The `gomedia/go-rtmp` RTMP reader recognises **H.264, H.265 (Enhanced
RTMP), and AAC**. The gortsplib RTSP reader recognises H.264, H.265, AAC.
Other codecs (Opus, AC-3, MP3, MP2, AV1, MPEG-2) over those transports
are dropped at the reader — gomedia's FLV demuxer doesn't surface them
yet, and gortsplib's payload format mappings to our AVCodec enum aren't
wired for non-H.26x / non-AAC audio. Users needing MP2 / AC-3 / EAC-3 /
AV1 / MPEG-2 ingest should use UDP / HLS / SRT / File where the TS path
recognises them via stream_type + DVB descriptors.

### 6.4 Resolution unknown for non-H.26x video

`MediaTrackInfo.Width / Height` will be 0 for MPEG-2 Video and AV1
sources. The UI should hide the resolution row for tracks where
`Width × Height == 0` rather than rendering "0x0".

### 6.5 Encoding to specific HW backends

AV1 / MPEG-2 Video have no entry in `hwOptionalEncoders` for NVENC /
VAAPI / QSV / VideoToolbox. They run on the CPU regardless of the global
`hw` setting. If a HW path is needed (e.g. `h264_nvenc` for AV1's input
decode) the operator must spell the explicit encoder name in
`transcoder.video.codec`.

### 6.6 Bitrate accuracy

The PMT scanner reports bitrate at the **TS payload level** (includes PES
headers and adaptation field). gompeg2 (the older path) reported it at
the **elementary stream level** (PES headers stripped). For typical IPTV
streams the difference is ~3–5% — TS-level matches what `ss -lump`,
`tcpdump`, and operator-monitor display.

---

## 7. Configuration examples

### 7.1 Pure relay (lowest CPU, lowest latency)

Source codec doesn't matter — bytes flow through unchanged.

```yaml
streams:
  - code: ch1_relay
    inputs:
      - url: udp://ens5@239.0.113.1:5001
    protocols:
      mpegts: true     # raw-TS HTTP relay endpoint
```

### 7.2 DVB SD source → modern HLS playback

MPEG-2 Video + MP2 audio source needs full re-encode for browser HLS.

```yaml
streams:
  - code: ch1
    inputs:
      - url: udp://ens5@239.0.113.1:5001
    transcoder:
      mode: multi
      video:
        copy: false
        profiles:
          - { width: 1280, height: 720, bitrate: 3000, codec: h264, preset: veryfast }
      audio:
        copy: false
        codec: aac
        bitrate: 128
      global:
        hw: nvenc
    protocols:
      hls: true
```

### 7.3 Feed-back to legacy DVB headend

Modern source → encode MPEG-2 + MP2 for transmitter chain.

```yaml
streams:
  - code: ch1_legacy
    inputs:
      - url: copy://ch1   # any modern stream code
    transcoder:
      video:
        copy: false
        profiles:
          - { codec: mp2v, bitrate: 5000, width: 720, height: 576 }
      audio:
        copy: false
        codec: mp2a
        bitrate: 192
    protocols:
      mpegts: true
    push:
      - url: srt://transmitter.lan:9000
        enabled: true
```

### 7.4 Audio-only radio (MP2)

```yaml
streams:
  - code: voh_fm_877
    inputs:
      - url: udp://ens5@239.0.113.1:5001
    transcoder:
      audio:
        copy: false
        codec: aac
        bitrate: 96
      video:
        copy: true   # source has no video
    protocols:
      hls: true
      mpegts: true
```

### 7.5 Premium HD with AC-3 source

```yaml
streams:
  - code: hbo_hd
    inputs:
      - url: udp://ens5@239.0.113.7:5001
    transcoder:
      video:
        copy: false
        profiles:
          - { width: 1920, height: 1080, bitrate: 5000, codec: h264, preset: veryfast }
      audio:
        copy: false
        codec: aac     # transcode AC-3 → AAC for HLS browser playback
        bitrate: 192
        channels: 2    # AC-3 is often 5.1; downmix to stereo for browser
      global:
        hw: nvenc
    protocols:
      hls: true
      mpegts: true
```

---

## 8. Roadmap

Items not yet implemented but tractable:

| Feature | Priority | Effort estimate |
|---|---|---|
| Frame extractor for AC-3 / E-AC-3 (enables mixer://) | medium | ~250 lines + tests |
| Frame extractor for MPEG-2 Video (enables mixer://) | medium | ~200 lines + tests |
| Frame extractor for AV1 (enables mixer://) | low | ~400 lines + tests (OBU parsing) |
| MPEG-2 Video resolution parse (sequence_header) | low | ~80 lines |
| AV1 resolution parse (sequence header OBU) | low | ~150 lines |
| RTMP reader codec map (Enhanced RTMP for AV1) | low | ~50 lines once gomedia's FLV demuxer adds them |
| RTSP reader codec map (gortsplib already supports more) | low | ~80 lines wiring |
| HW-accelerated MPEG-2 / AV1 encoding | low | depends on FFmpeg build |

Add only when a concrete use case lands. The current set covers DVB IPTV
+ IP camera + modern HLS / DASH / MPEG-TS-relay scenarios fully.

---

## 9. Reference files

| Concern | File |
|---|---|
| AVCodec enum + IsVideo / IsAudio | [internal/domain/avpacket.go](../internal/domain/avpacket.go) |
| Codec → UI label | [internal/domain/media_track.go](../internal/domain/media_track.go) |
| User-config codec → FFmpeg encoder | [internal/domain/resolvers.go](../internal/domain/resolvers.go) |
| Output codec constants (config dropdown) | [internal/domain/transcoder.go](../internal/domain/transcoder.go) |
| TS demux (gompeg2-based, frame-level) | [internal/ingestor/pull/tsdemux_packet_reader.go](../internal/ingestor/pull/tsdemux_packet_reader.go) |
| PMT scanner (custom, codec-only) | [internal/ingestor/pull/stats_pmt_scanner.go](../internal/ingestor/pull/stats_pmt_scanner.go) |
| TS muxing (AV → wire) | [internal/tsmux/fromav.go](../internal/tsmux/fromav.go) |
| RTMP message → AVPacket (pull side, codec-config invariant) | [internal/ingestor/pull/rtmp_msg_converter.go](../internal/ingestor/pull/rtmp_msg_converter.go) |
| RTMP play out (slice-only video, ADTS-bundle-split audio) | [internal/ingestor/push/rtmp_writer.go](../internal/ingestor/push/rtmp_writer.go) |
| Transcoder capability probe (no-op stub) | [internal/api/handler/config_probe.go](../internal/api/handler/config_probe.go) |
| Per-codec runtime track stats | [internal/manager/tracks.go](../internal/manager/tracks.go) |
