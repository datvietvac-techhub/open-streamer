# Migrate off deprecated `middleware.RealIP` (chi v5.3)

## Context
chi v5.3 **deprecated** `middleware.RealIP` (staticcheck SA1019). The
deprecation reason is a security one: `RealIP` mutates `r.RemoteAddr` from the
`True-Client-IP` / `X-Real-IP` / `X-Forwarded-For` headers **whether or not the
infrastructure actually sets them**, so a directly-reachable server can be fed
a spoofed client IP. (See GHSA-3fxj-6jh8-hvhx, GHSA-rjr7-jggh-pgcp,
GHSA-9g5q-2w5x-hmxf.)

It is currently kept and suppressed with `//nolint:staticcheck` in
`internal/api/server.go` (`buildRouter`), because the deployment runs behind a
trusted reverse proxy. This file tracks the proper migration.

## Replacement (added in chi v5.3)
A new family that stores the client IP in the **request context** (read via
`middleware.GetClientIP(r)`) and **never mutates `r.RemoteAddr`**:
- `ClientIPFromXFFTrustedProxies(n)` — trust the last `n` proxy hops in XFF.
- `ClientIPFromXFF(trustedIPPrefixes...)` — trust specific proxy CIDRs.
- `ClientIPFromHeader(trustedHeader)` — single trusted header.
- `ClientIPFromRemoteAddr` — just the TCP peer (no header trust).

## Migration scope (small)
1. Replace `r.Use(middleware.RealIP)` with the variant matching the deployment:
   - behind a trusted proxy/CDN: `ClientIPFromXFFTrustedProxies(N)` (N = trusted
     proxy hops in front of the server), or
   - direct-to-client: `ClientIPFromRemoteAddr`.
2. Update the **one** functional HTTP consumer of `r.RemoteAddr` —
   `internal/publisher/sessions_helper.go:165` (HLS/DASH play-session client
   IP) — to read `middleware.GetClientIP(r)`, falling back to `r.RemoteAddr`
   when empty.
3. Remove the `//nolint:staticcheck` from `internal/api/server.go`.

Not affected: RTMP / SRT / RTSP read their own protocol connection
`RemoteAddr`. `serve_mpegts.go`'s `r.RemoteAddr` is logging-only.

## Decision needed
How many trusted proxy hops sit in front of the HTTP server (picks the
`ClientIPFrom*` variant and its `n`).

## Verify
`golangci-lint run ./...` clean without the `//nolint`; play-session entries
still record the real client IP (not the proxy IP) in a proxied setup.
