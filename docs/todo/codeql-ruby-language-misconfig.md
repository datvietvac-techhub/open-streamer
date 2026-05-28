# CodeQL default setup analyses Ruby on a Ruby-free repo

## Symptom
The CodeQL workflow's `Analyze (ruby)` job fails on every run with:

> CodeQL detected code written in Go and GitHub Actions, but not any written in
> Ruby. Confirm that there is some source code for Ruby in the project.

The `Analyze (go)` and `Analyze (actions)` jobs pass.

## Context
CodeQL runs via GitHub **default setup** (configured in repo Settings, not via
a `.github/workflows/codeql.yml`). The configured language list includes
**Ruby**, but the repository is Go-only — so there is no Ruby source and the
Ruby job errors out every time.

## Impact
A persistently-failing CodeQL check on every push. Not a build/code failure,
but noise that can mask genuine failures and, if CodeQL is a required check,
can block merges / branch protection.

## Fix options
- (a) Repo **Settings → Code security and analysis → CodeQL analysis → Default
  setup → Edit** → remove **Ruby** from the language list (keep Go + Actions).
  No code change. Smallest fix.
- (b) Add an **advanced-setup** `.github/workflows/codeql.yml` pinning
  `languages: [go, actions]`, which overrides default setup.

## Verify
The CodeQL check has no failing `Analyze (ruby)` job on subsequent pushes.
