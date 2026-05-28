# DASH ABR: video renditions have misaligned segment timelines

## Symptom
On DASH playback of a transcoded (multi-rendition) stream, audio/video can be
out of sync, and the amount of skew **depends on which rendition the player is
showing**. Switching renditions (ABR up/down) produces a visible A/V jump.
The HLS output of the **same** stream is in sync. The top rendition is ~in
sync; the lower renditions lag.

> Note: this is distinct from the long-runtime ingest A/V drift (fixed in the
> timeline Normaliser — see `av-drift-timeline-fix-finalize`). This one lives in
> the DASH packager and only bites on the lower renditions / on an ABR switch.

## Evidence
The MPD has one video `AdaptationSet` with N `Representation`s and one audio
`Representation`. The video representations' `SegmentTimeline` `t` bases do not
match — the top rendition's first-segment `t` was observed ~200 ms ahead of the
lower renditions, i.e. `18000` ticks at `timescale=90000` (= 0.2 s),
**constant across the whole timeline**. Audio is anchored once.

Measured via `ffprobe` on the live MPD:
- top rendition: `audio_start - video_start ≈ -0.02 s` (≈ in sync)
- lower renditions: `audio_start - video_start ≈ +0.18 s` (audio lags ~180 ms)

So the audio↔video offset changes by ~200 ms when the player switches between
the top and lower renditions.

## Why HLS is not affected
HLS muxes video + audio into a single MPEG-TS per rendition; the player syncs
by the in-band PTS, and every rendition carries the same muxed timeline. DASH
uses separate per-track fMP4 segments whose timelines are anchored
independently.

## Root-cause direction
All video renditions come from **one decode → N scaled encodes**, so they
share the same source PTS. The DASH packager appears to anchor each
rendition's segment timeline (`tfdt` / `<S t=...>`) from that rendition's own
first frame instead of from a **common per-stream reference**, so small
differences in when each encoder emits its first IDR / segment bake a fixed
inter-rendition offset.

## Proposed fix
In `internal/publisher/dash/`, anchor every video rendition of a stream to a
**single shared timeline origin** (and keep audio on the same reference) so
representations are byte-for-byte switchable without an A/V shift and
`segmentAlignment="true"` actually holds. Likely touch points: `packager.go`,
`segmenter.go`, `state.go`, `manifest.go`. Add a test asserting all video
representations share the same first-segment `t` and the same `<S t=...>`
sequence.

## Verify
`ffprobe` the live `.mpd`: every video representation's `start_time` equal;
`audio_start - video_start` constant and small regardless of selected
rendition; ABR switch produces no A/V jump.
