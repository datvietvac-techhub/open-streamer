# TODO — open issues to fix

One file per issue. Each file states the symptom, evidence, root-cause
direction, affected code, and a proposed fix. Remove a file once its issue is
fixed and verified.

- **dash-abr-rendition-timeline-misalignment** — DASH video renditions have
  misaligned segment timelines; A/V skew varies by rendition and jumps on ABR
  switch (only the lower renditions / on switch; the top rendition is in sync).
  HLS unaffected. `internal/publisher/dash`.
- **dash-codec-string-profile-mismatch** — DASH MPD advertises Main profile in
  `codecs` for every rendition; HLS reports the real profiles. Cosmetic (strict
  players only). `internal/publisher/dash`.
- **realip-chi-deprecation-migration** — migrate off deprecated
  `middleware.RealIP` (chi v5.3) to `ClientIPFrom*` + `GetClientIP`; currently
  `//nolint`-suppressed. `internal/api`, `internal/publisher`.
- **codeql-ruby-language-misconfig** — CodeQL default setup analyses Ruby on a
  Ruby-free repo → the Ruby job fails on every run. repo Settings / `.github`.
- **av-drift-timeline-fix-finalize** — (in progress) validate the A/V-drift
  Normaliser fix past the ~26.5 h PTS-wrap boundary, then merge the branch and
  cut a release (currently an uncommitted test build). `internal/timeline`.
