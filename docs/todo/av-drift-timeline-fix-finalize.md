# Finalize the A/V-drift timeline fix (validate → merge → release)

## Context
Long-runtime audio/video desync — lip-sync that drifts over hours and is reset
by a stream restart — was root-caused to the per-stream `timeline.Normaliser`
performing a **hard re-anchor of video and audio independently** to wallclock.
Two triggers:
1. **33-bit MPEG-TS PTS wrap** (~26.5 h): the counter wrap looks like a
   backward jump (`monotonic_regress`) and re-anchors each track separately.
2. **Clock drift / source stall** past the jump threshold: a forward re-anchor
   jumps one track to wallclock while the partner lags.

Because video and audio cross each threshold at slightly different moments, the
per-track re-anchor leaves a permanent A/V offset until the next restart.

## Fix (branch `fix/timeline-av-drift`, commit `3f94a4a`)
- **Unwrap** the 33-bit PTS counter (add one period) so a wrap keeps the input
  timeline continuous and never triggers a re-anchor; both tracks unwrap
  consistently, preserving their offset.
- **A/V-pair-aware re-anchor**: a forward re-anchor now shifts the partner
  track by the same delta, preserving the A/V offset.
- Repro unit tests added (fail before / pass after) plus a B-frame large-gap
  joint-seed test. Files: `internal/timeline/normaliser.go`, `normaliser_test.go`.

## Status
Deployed as an **uncommitted test build** (`v4.0.0-avsync-test`) on the test
server; the previous binary is kept alongside for rollback. A sampler logs the
A/V offset every 30 min.
- Early result: a clock-drift forward re-anchor fired post-deploy and the
  offset stayed flat → fix for trigger (2) confirmed live.
- The PTS-wrap fix (trigger (1)) still needs the run to cross the ~26.5 h wrap
  boundary with offsets staying flat and **no** `monotonic_regress
  drift_ms≈-95443xxx` events in the journal.

## Remaining actions
1. Confirm >26.5 h of flat A/V offset across the wrap (sampler log).
2. Push branch `fix/timeline-av-drift` → open PR → merge.
3. Cut a new release (the running binary is an uncommitted build).
