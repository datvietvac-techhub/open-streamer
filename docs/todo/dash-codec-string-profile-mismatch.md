# DASH MPD advertises the wrong H.264 profile in `codecs`

## Symptom
In the DASH manifest, every video `Representation` advertises the **Main**
profile in its `codecs` attribute (`avc1.4D40xx`), regardless of the
rendition's real encoded profile. The HLS master playlist for the same stream
reports the correct per-rendition profiles.

## Evidence
For a 3-rung ladder (1080p / 720p / 480p), observed:

| Rendition | DASH `codecs` | HLS `CODECS` (real) |
|-----------|---------------|---------------------|
| 1080p | `avc1.4D4028` (Main) | `avc1.640028` (**High**) |
| 720p  | `avc1.4D401F` (Main) | `avc1.4d401f` (Main) |
| 480p  | `avc1.4D401E` (Main) | `avc1.42e01e` (**Baseline**) |

`4D` (profile_idc 77 = Main) is emitted for all three; only the level byte
varies. The HLS values (`64` High, `4d` Main, `42` Baseline) are the truth.

## Why it matters
Strict DASH / MSE players use the `codecs` string for `SourceBuffer`
initialization and `isTypeSupported` / `canPlayType`. Advertising **Main** for
a stream that is actually **High** profile can cause a player to reject the
representation or mis-initialize the decoder. Lenient browsers ignore the
mismatch (which is why playback currently works), so this is low-severity but
technically wrong.

## Root-cause direction
The DASH packager hardcodes or mis-derives the `avc1.PPCCLL` bytes
(profile_idc / constraint_set flags / level_idc) instead of reading them from
each rendition's SPS. The HLS path derives them correctly from the SPS, so the
parsing logic already exists and can be reused.

## Proposed fix
In `internal/publisher/dash/` (manifest / `codecs` construction), derive the
`avc1.` string per rendition from the rendition's actual SPS, the same source
the HLS path uses.

## Verify
Each DASH video `Representation`'s `codecs` matches the corresponding HLS
variant's `CODECS` profile/level bytes.
