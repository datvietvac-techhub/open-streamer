# Transcoder A/V output clock — drift fixes + the remaining unification

## Status
The two drift mechanisms that were causing visible A/V desync are **fixed and
validated**:
1. Bursty-input audio race → `releaseAudio` audio-gate.
2. Source-fps ≠ configured-fps re-timing → `rebaseVideoPTS` now paces its
   monotonic guard by the **observed source frame interval**.

The broader "single source-PES master clock" refactor (below) is **optional
future hardening**, not required to resolve the observed bugs.

## Problem — one root, several manifestations
The transcoder clocks video and audio **semi-independently**: each track maps
its source PTS onto the output timeline through its own function and its own
monotonic guard, sharing only one `ptsOffset` set per input switch. They stay
aligned only when the input is "well-behaved" (real-time delivery **and**
framerate matching the configured output fps). Deviations drift the two clocks
apart, differently per deviation:

| Deviation | Manifestation | Fix |
|---|---|---|
| Bursty delivery (HLS-pull dumps a playlist window at a switch) | cheap audio passthrough outruns the heavy N×encode video path → audio leads by seconds, baked in | `releaseAudio` gate (holds audio leading video > cap) — **done** |
| Source fps ≠ configured fps (e.g. a 30 fps looping file vs a 25 fps ladder) | video re-timed to the configured fps → ~(1 − srcFps/cfgFps) drift | `rebaseVideoPTS` paces guard by observed interval — **done** |
| Long-run PTS wrap / clock re-anchor | per-track hard re-anchor leaves a constant offset | `timeline.Normaliser` partner-shift (ingest) — done earlier |

Resolution differences are handled by the scaler; bitrate is irrelevant to
sync. The real axes are **delivery timing** and **framerate**.

## Confirmed root of the fps drift (instrumented, not theory)
Two earlier hypotheses were wrong and ruled out by instrumentation: it is **not**
the `f.Pts() <= 0 → Encoder.NextPTS()` fallback (never fired), and **not** the
MP4 file reader (it emits V/A within ~22 ms per loop).

The actual mechanism is the monotonic guard in `rebaseVideoPTS`:
```
out := src + ptsOffset
if out <= lastVideoOut { out = lastVideoOut + videoFrameDurMs }  // static config-fps interval
```
`videoFrameDurMs` is the **configured** fps interval (40 ms = 25 fps). For a
30 fps source (33 ms real interval) the guard advances 40 ms while `src+offset`
advances 33 ms, so `lastVideoOut` outpaces `src+offset` by 7 ms every frame —
the guard condition stays true and **never releases**. Video locks to 25 fps
while 30 frames/s arrive, running ~20 % fast; audio keeps real time, so it
drifts behind ~0.2 s/s (measured to −49 s before the fix). 25 fps sources are
unaffected (interval == guard step). Captured live: `decDelta=33` (source
30 fps) while `vOut` advanced 40 ms/frame, `aOut` real-time, skew growing
−926 → −7840 ms.

## Fix (done)
`rebaseVideoPTS` learns the source frame interval from forward PTS deltas
(`videoSrcIntervalMs`, capped at `maxSaneFrameIntervalMs`) and advances the
guard by **that**, falling back to `videoFrameDurMs` only until the first
interval is observed or during a genuine stall (no forward delta). Relearned
per input switch. So the guard, even when engaged, paces at the source rate and
cannot re-time video off the audio clock.

Validated live across ~3 file loops + both regression directions:
- file (30 fps loop): **bounded ±0.5 s** (was drifting to −15/−49 s),
- HLS switch: +0.0 s (audio-gate intact),
- UDP: −0.27 s (unchanged).
Unit tests: `TestRebaseVideoPTS_ClampPacesAtSourceRateNotConfigFps`,
`_StalledSourcePacesAtFrameRate`, `_MonotonicSourceTracksSource`,
`TestReleaseAudio_*`.

## Optional future hardening — single source-PES master clock
The two fixes above are point-fixes on the same weakness (independent per-track
clocks). The robust end state: **one master timeline = the source PES timeline**
(what the copy path already does, which is why copy is always synced), with
both tracks slaved symmetrically:
1. Video output PTS = the frame's source PES PTS, recovered across the decoder
   reorder-aware (a naive decode-order FIFO is wrong for B-frame sources — use a
   sorted/min-ordered mapping or the decoder best-effort timestamp).
2. Symmetric stall fallback for both tracks (shared wall-clock step), replacing
   the per-track guards entirely.
3. Real-time input pacing at ingest so no source delivers a burst the encoder
   can't drain (the gate then becomes pure defence-in-depth).
This would make UDP / HLS / file / any fps synced through one mechanism instead
of layered guards. Not scheduled — the observed bugs are resolved without it.

## Affected code
`internal/transcoder/native/stream_pipeline.go` — `rebaseVideoPTS`,
`rebaseAudioPTS`, `anchorRebase`, `releaseAudio`, `videoFrameDurMs` /
`videoSrcIntervalMs`. Reference target: the `tsmux`/copy path (carries source
PES verbatim).

## Risk
The PTS path is shared by every transcoded stream; the fps fix was gated on the
matched-fps inputs (UDP/HLS, 25 fps) staying unaffected — confirmed in
validation.
