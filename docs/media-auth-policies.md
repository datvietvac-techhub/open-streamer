# Media-Plane Auth — Playback Policies

Playback authorization decides **who may watch a stream** over HLS, DASH,
HTTP-MPEGTS, RTMP, SRT, and RTSP. It is the media-plane counterpart to the
control-plane (admin API) auth, and it is **policy-based**: every rule lives on a
first-class `Policy` entity in the store. There is **no global media-auth
config**.

- Source of truth: [internal/domain/policy.go](../internal/domain/policy.go)
- Decision engine: [internal/mediaauth/mediaauth.go](../internal/mediaauth/mediaauth.go)
- HTTP gate: [internal/api/dispatch.go](../internal/api/dispatch.go)
- RTMP/SRT/RTSP gate: [internal/publisher/playauth.go](../internal/publisher/playauth.go)
- REST CRUD: [internal/api/handler/policy.go](../internal/api/handler/policy.go)

---

## 1. The `Policy` entity

A policy is a reusable, named bundle of rules. It is fully self-contained — it
carries its **own** token secret and its **own** allow/deny chain, so revoking
one policy's key never affects another.

| Field (JSON) | Type | Meaning | Format |
|---|---|---|---|
| `code` | string | Unique key (primary key) | `A-Z a-z 0-9 _ -`, ≤ 128 chars |
| `name` | string | Operator-facing label | free-form |
| `description` | string | Operator-facing notes | free-form |
| `require_token` | bool | Require a valid signed token on every request | — |
| `token_secret` | string | HMAC-SHA256 key for **this** policy's tokens | required when `require_token=true` |
| `allow_ips` | []string | If non-empty, client IP **must** match | exact IP or CIDR |
| `deny_ips` | []string | Client IP on this list is rejected | exact IP or CIDR |
| `allow_countries` | []string | If non-empty, client country **must** appear | ISO 3166-1 alpha-2 (needs GeoIP DB) |
| `deny_countries` | []string | Client country on this list is rejected | ISO 3166-1 alpha-2 |
| `allow_user_agents` | []string | If non-empty, UA **must** contain one entry | case-insensitive substring |
| `deny_user_agents` | []string | UA containing any entry is rejected | case-insensitive substring |
| `allowed_domains` | []string | If non-empty, HTTP `Referer` host **must** match | exact host or parent domain (`example.com` covers `play.example.com`) |

`Policy.Validate()` runs at save time: it rejects a bad `code`, a missing
`token_secret` when `require_token=true`, and any malformed IP/CIDR or country
code — so a silently-dropped CIDR can never weaken a deny list.

---

## 2. Binding a policy to a stream

A stream binds **at most one** policy via `Stream.PlaybackPolicy` (JSON
`playback_policy`). Templates carry the same field and `ResolveStream` inherits
it with **zero-value = inherit** semantics (a non-empty value on the stream
overrides the template).

| Binding state | Result |
|---|---|
| No policy (empty) | **Public** — allow-all, returns `allow` immediately |
| Valid policy code | Rules evaluated (see chain below) |
| Unknown policy code | **Fail-closed** — `deny "unknown policy"` |

An unknown code only arises from a direct store edit: the delete handler refuses
to remove a referenced policy (see §8).

### Policy resolution (which policy applies)

`PolicyResolver` ([cmd/server/main.go](../cmd/server/main.go), `wireMediaAuth`)
resolves a stream code to its policy code in two tiers:

1. **Live (hot path):** `publisher.Service.PlaybackPolicy(code)` — O(1)
   in-memory lookup, no store read per segment.
2. **Stopped / DVR fallback:** `streamRepo.FindByCode` → if the stream has a
   template, `domain.ResolveStream` merges the template's `playback_policy`
   before returning the effective code. Store error → `""` (treated as public).

---

## 3. Evaluation chain

For one playback request the engine runs **deny → allow → token**
([mediaauth.go](../internal/mediaauth/mediaauth.go), `evaluate`):

```
0. No policy bound .......................... ALLOW (public)
1. Policy code unknown ...................... DENY  "unknown policy"  (fail-closed)
2. DENY lists  (deny wins) .................. ip / country / user-agent on a deny list → DENY
3. ALLOW gates (every configured list must pass)
      allow_ips        non-empty & no match → DENY
      allow_countries  non-empty & no match → DENY
      allow_user_agents non-empty & no match → DENY
      allowed_domains  non-empty & no match → DENY
4. TOKEN gate (only when require_token=true)
      no token_secret .................... → DENY "token required but policy has no secret"
      verify() fails ..................... → DENY "missing or invalid token"
5. ............................................ ALLOW
```

The compiled policy set lives in an `atomic.Pointer` and is hot-swapped by
`SetPolicies` (see §8); `Authorize` itself does zero allocations.

---

## 4. Playback tokens

Tokens are **client-signed, server-verify-only**. The server never mints tokens
on the hot path; `mediaauth.SignToken` is the reference implementation for
clients.

### Wire format

```
token = "<exp>.<base64url-nopad-sig>"

  exp = Unix expiry (seconds, decimal)
  sig = HMAC-SHA256( token_secret, "<stream_code>|<exp>" )   // raw 32 bytes
        encoded with base64 URL-safe, no padding
```

The MAC binds to **both** the stream code and the expiry, so a token minted for
one stream never authorizes another — even under the same policy secret.

- **Expiry / clock skew:** a token is valid while `now − 60s ≤ exp` (60 s skew
  tolerance).
- **Verification:** constant-time compare (`subtle.ConstantTimeCompare`).

### Minting a token (example, shell)

```bash
CODE="mychannel"
SECRET="s3cr3t"
EXP=$(( $(date +%s) + 3600 ))                       # valid 1 hour
SIG=$(printf '%s|%s' "$CODE" "$EXP" \
      | openssl dgst -sha256 -hmac "$SECRET" -binary \
      | basenc --base64url | tr -d '=')
TOKEN="$EXP.$SIG"
```

### Token transport per protocol

| Protocol | How the token is carried |
|---|---|
| HLS / DASH / HTTP-MPEGTS | URL query `?token=<TOKEN>` (on **every** request, incl. each segment) |
| RTSP | URL query `rtsp://host:554/live/<code>?token=<TOKEN>` |
| SRT | inside the streamid: `srt://host:9999?streamid=live/<code>?token=<TOKEN>` |
| RTMP | URL query on the play URL: `rtmp://host:1935/live/<code>?token=<TOKEN>` |

---

## 5. Enforcement matrix (protocol × rule)

Which policy rule actually takes effect depends on whether the delivery protocol
can carry that signal. This is the core of the design — **not every rule applies
to every protocol.**

| Policy rule | HLS / DASH / HTTP-MPEGTS | RTMP play | SRT play | RTSP play |
|---|:---:|:---:|:---:|:---:|
| **Token** (`require_token`) | ✅ `?token=` | ✅ play-URL query | ✅ via `streamid` | ✅ `?token=` |
| **IP** (`allow_ips` / `deny_ips`) | ✅ | ✅ | ✅ | ✅ |
| **Country** (`allow_countries` / `deny_countries`) | ✅ | ✅ | ✅ | ✅ |
| **User-Agent** (`allow_user_agents` / `deny_user_agents`) | ✅ | ❌ | ❌ | ⚠️ |
| **Referer domain** (`allowed_domains`) | ✅ | ❌ | ❌ | ❌ |

**Legend**

- ✅ **Enforced** — the signal is captured; the rule works as intended.
- ⚠️ **Conditional** — captured only if the client sends it (RTSP `User-Agent`);
  an **allow-list** may block legitimate clients that omit the header.
- ❌ **Not available** — the signal is never captured (the gate passes an empty
  string). Effect depends on list polarity:
  - **deny-list** → inert (empty never matches; harmless no-op);
  - **allow-list** → **denies every client of that protocol** (empty never
    matches a required list). See §7.

Country is derived from the client IP via the GeoIP DB, so it is enforceable
exactly when IP is — on all protocols.

### Where each gate lives

| Protocol | Gate call (file:line) |
|---|---|
| HLS / DASH / HTTP-MPEGTS | `mediaAllowed(r, code)` — [dispatch.go:164](../internal/api/dispatch.go#L164) (one gate for all three, before file routing) |
| RTMP | `playAllowed(code, "rtmp", addr, token, "")` — token from the play-URL query — [serve_rtmp.go](../internal/publisher/serve_rtmp.go) |
| SRT | `playAllowed(code, "srt", addr, srtTok, "")` — [serve_srt.go:129](../internal/publisher/serve_srt.go#L129) |
| RTSP | `playAllowed(code, "rtsp", remote, TokenFromQuery, ua)` — [serve_rtsp.go:253](../internal/publisher/serve_rtsp.go#L253) |

---

## 6. Per-protocol behaviour

### HLS / DASH / HTTP-MPEGTS

All three are gated **identically** in the API dispatcher, once at the top of
`dispatchMedia` before any per-file branching — so the manifest, **every
segment** (`.ts` / `.m4s` / `.mp4`), the `/<code>/mpegts` endpoint, and DVR blob
paths are all protected. A token must be present on **every** request, not just
the manifest. The ABR rendition slug (`/<code>/track_N/…`) is normalised to
`/<code>` before auth, so a token minted for the parent code authorizes all
renditions. Full signal set available: IP, country, token, User-Agent, Referer.

### RTMP play

Carries the token on the play-URL query
(`rtmp://host:1935/live/<code>?token=…`): the push server forwards the session's
raw query and the publisher extracts the token with the same helper as SRT/RTSP.
**IP / country / token** are effective. **User-Agent** and **Referer** are always
empty (the RTMP handshake carries neither), so those rules are inert. The query
rides as a genuine query string, so it never pollutes the stream code.

### SRT play

Carries a token inside the `streamid` (`…?token=…`); the code lookup strips
everything after the first `?` so the token does not bleed into the stream code.
IP / country / token are effective; User-Agent and Referer are always empty. The
gate fires at the **subscribe** phase (`srtHandleSubscribe`), not at connect — a
denied client is dropped before any buffer subscriber is allocated.

### RTSP play

Carries a token in the URL query (`?token=`). IP / country / token are
effective. **User-Agent** is read from the request header **if the client sends
it** (VLC/ffmpeg do; headless/embedded clients may not). **Referer** is never
available. Denial returns `401 Unauthorized` before the connection cap check.

---

## 7. Footguns & caveats

1. **RTMP token rides the play URL.** The token must be appended to the RTMP
   play URL as a query (`rtmp://host:1935/live/<code>?token=…`). Players differ
   in where they place the query (on the app/tcURL vs. the play stream name) —
   test your encoder/player; the server reads it from the session's raw query.
   Like every URL-borne token it appears in logs/proxies, so keep `exp` short.

2. **Allow-lists on an unavailable signal block everyone.** Setting a *positive*
   `allow_user_agents` on a policy used by RTMP/SRT, or `allowed_domains` on
   RTMP/SRT/RTSP, denies **every** client of that protocol (the captured value
   is empty and can never satisfy a required list). Deny-lists on the same
   signal are inert (harmless). Rule of thumb: only put `allow_user_agents` /
   `allowed_domains` on policies whose streams are served over **HTTP**
   (HLS/DASH/MPEGTS), where the header is always present.

3. **RTSP `User-Agent` allow-list may block valid clients.** Because the header
   is optional, an `allow_user_agents` list will reject any RTSP client that
   omits it.

4. **IP / country trust the forwarded headers.** The HTTP path derives the
   client IP from chi's `RealIP` middleware (`True-Client-IP` → `X-Real-IP` →
   left-most `X-Forwarded-For`, then TCP peer). Without a trusted reverse proxy
   stripping/setting these, a client can spoof its IP — and therefore bypass
   IP/country rules — by sending a forged header. Terminate at a trusted proxy
   that overwrites these headers before relying on IP/country gates.

---

## 8. Managing policies

### REST API (`/policies`, admin-authenticated)

| Method | Path | Action |
|---|---|---|
| `GET` | `/policies/` | List all policies → `{data: [...], total: N}` |
| `GET` | `/policies/{code}` | Get one → `{data: policy}` or 404 |
| `POST` | `/policies/{code}` | Create (201) or replace (200); URL `{code}` wins over body |
| `DELETE` | `/policies/{code}` | Delete → 204, or 409 if referenced |

### Hot reload

Every `POST` / `DELETE` calls `reloadAuthorizer`, which re-lists all policies and
calls `Authorizer.SetPolicies` — the compiled set is swapped atomically with no
restart and no in-flight disruption. At boot, `wireMediaAuth` loads the set once;
a load failure logs a WARN and leaves the set empty (all playback public until
the first reload).

### Delete guard (`POLICY_IN_USE`)

`DELETE` scans all streams and templates for references. If any depend on the
policy it returns **409** with the dependents listed:

```json
{
  "error": "POLICY_IN_USE",
  "message": "policy is referenced by 2 stream(s) / 1 template(s)",
  "streams": ["..."],
  "templates": ["..."]
}
```

Detach each dependent (set `playback_policy` to null/empty) before retrying.

---

## 9. Worked examples

### Token-gated browser playback (HLS), domain-locked

```json
POST /policies/web-embed
{
  "code": "web-embed",
  "name": "Website embeds only, token required",
  "require_token": true,
  "token_secret": "s3cr3t",
  "allowed_domains": ["example.com"]
}
```
Bind to a stream (`"playback_policy": "web-embed"`). Players load
`https://host/<code>/index.m3u8?token=<TOKEN>` from a page on `example.com`.
RTMP clients of the same stream pass the token on the play URL
(`rtmp://host:1935/live/<code>?token=<TOKEN>`); note `allowed_domains` does not
apply to RTMP (no Referer), so the domain lock is browser-only.

### Geo-restricted, RTMP-friendly (no token)

```json
POST /policies/vn-only
{
  "code": "vn-only",
  "name": "Vietnam only",
  "allow_countries": ["VN"]
}
```
Works uniformly on HLS/DASH/MPEGTS/RTMP/SRT/RTSP — IP/country apply to every
protocol.

### Block a set of abusive networks (deny-list)

```json
POST /policies/block-bad-nets
{
  "code": "block-bad-nets",
  "deny_ips": ["203.0.113.0/24", "198.51.100.7"]
}
```
Deny-lists are safe on every protocol (an absent signal simply never matches).
