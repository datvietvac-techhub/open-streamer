package blob

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/tsdemux"
	"github.com/datvietvac-techhub/open-streamer/internal/tsmux"
)

// migrate.go — offline migration of a legacy per-`.ts` DVR recording into the
// CMAF blob archive, in place.
//
// The legacy layout is `index.json` + `playlist.m3u8` + `dvr_NNNNNN.ts`. Each
// `.ts` is demuxed and its frames drive the SAME profileWriter the live recorder
// uses — so a migrated fragment is byte-identical to a freshly recorded one.
//
// Timeline: each segment's PTS/DTS is rebased onto its EXT-X-PROGRAM-DATE-TIME
// anchor, so the migrated wall time is faithful to the recording (not the re-cut
// fragment boundaries) and is monotonic across segments. Rebasing PER SEGMENT
// makes the result robust to a 33-bit PTS wrap or reset between segments — the
// per-segment offset absorbs it — without depending on the live Normaliser
// (which exists to correct live source drift and mishandles an offline replay's
// reset-across-gap). A no-gap boundary stays continuous because the next
// segment's anchor equals the previous anchor plus its duration; an
// EXT-X-DISCONTINUITY feeds one SessionStart so exactly one emitted fragment is
// flagged discontinuous.
//
// Migration is idempotent: a present `catalog.json` short-circuits it (that file
// is also the dispatcher's signal to serve the stream from the blob handler), so
// a re-run is a no-op unless the caller removes the catalog.

const (
	legacyPlaylist = "playlist.m3u8"
	legacyIndex    = "index.json"
	migratedMarker = ".migrated"
)

// MigrateOptions controls a legacy `.ts` → blob migration.
type MigrateOptions struct {
	StreamCode string        // catalog stream code; defaults to the dir's base name
	SegDur     time.Duration // target fragment duration; defaults to DefaultSegDur
	Prune      bool          // delete legacy dvr_*.ts + playlist.m3u8 + index.json on success
}

// MigrateResult summarises a completed migration.
type MigrateResult struct {
	Segments     int   // legacy .ts segments replayed
	Hours        int   // per-hour blobs produced
	VideoFrags   int   // video fragments written
	AudioFrags   int   // audio fragments written
	Gaps         int   // discontinuities carried across
	SourceFromMs int64 // first legacy segment wall time
	SourceToMs   int64 // last legacy segment wall time + duration
}

// IsLegacyRecording reports whether segDir holds a legacy `.ts` recording that
// has not yet been migrated (playlist.m3u8 present, catalog.json absent).
func IsLegacyRecording(segDir string) bool {
	if HasCatalog(segDir) {
		return false
	}
	_, err := os.Stat(filepath.Join(segDir, legacyPlaylist))
	return err == nil
}

// Migrate replays a legacy `.ts` recording at segDir into the blob archive in
// place. The caller must ensure the recording is stopped (no live writer).
func Migrate(ctx context.Context, segDir string, opts MigrateOptions) (*MigrateResult, error) {
	if HasCatalog(segDir) {
		return nil, fmt.Errorf("blob: %s already migrated (catalog.json present)", segDir)
	}
	segs, err := parseLegacyPlaylist(segDir)
	if err != nil {
		return nil, err
	}
	if len(segs) == 0 {
		return nil, fmt.Errorf("blob: %s has no legacy segments to migrate", segDir)
	}
	code := opts.StreamCode
	if code == "" {
		code = filepath.Base(segDir)
	}
	segDur := opts.SegDur
	if segDur <= 0 {
		segDur = DefaultSegDur
	}

	var hours []HourRecord
	sink := func(_ string, hr HourRecord) { hours = append(hours, hr) }
	w := newProfileWriter(segDir, "p0", true, segDur, sink)

	res := &MigrateResult{Segments: len(segs)}
	migStartMs := segs[0].wallTime.UnixMilli()
	res.SourceFromMs = migStartMs
	last := segs[len(segs)-1]
	res.SourceToMs = last.wallTime.Add(last.duration).UnixMilli()

	sawHEVC := false
	for i, seg := range segs {
		if seg.discontinuity && i > 0 {
			// One SessionStart per original gap → one discontinuous fragment.
			if err := w.Ingest(buffer.Packet{SessionStart: true}, seg.wallTime); err != nil {
				return nil, err
			}
			res.Gaps++
		}
		if err := migrateSegment(ctx, w, filepath.Join(segDir, seg.file), seg.wallTime.UnixMilli(), migStartMs, &sawHEVC); err != nil {
			return nil, fmt.Errorf("blob: migrate %s: %w", seg.file, err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	_, originWallMs, set := w.Origin()
	if !set {
		return nil, fmt.Errorf("blob: %s produced no video frames", segDir)
	}
	cat := buildMigratedCatalog(code, originWallMs, sawHEVC, hours)
	if err := cat.Save(segDir); err != nil {
		return nil, err
	}
	for _, hr := range hours {
		res.Hours++
		res.VideoFrags += hr.FragCountV
		res.AudioFrags += hr.FragCountA
	}
	// Marker after catalog so its presence implies a fully-committed migration.
	writeMigratedMarker(segDir, res)
	if opts.Prune {
		pruneLegacy(segDir, segs)
	}
	return res, nil
}

// migrateSegment demuxes one legacy `.ts` and drives its frames into the writer,
// rebasing the segment's decode timeline onto its PDT anchor: the first frame's
// DTS maps to (segWallMs − migStartMs) and every other frame keeps its original
// offset from it. The constant per-segment offset preserves intra-segment
// PTS/DTS (B-frame ordering) exactly while absorbing any inter-segment PTS reset
// or 33-bit wrap.
func migrateSegment(ctx context.Context, w *profileWriter, path string, segWallMs, migStartMs int64, sawHEVC *bool) error {
	f, err := os.Open(path) //nolint:gosec // path = segDir/<validated dvr_*.ts base name>
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	var (
		offset   int64
		haveBase bool
		cbErr    error
	)
	dmx := tsdemux.New()
	dmx.OnFrame = func(cid tsdemux.StreamType, frame []byte, ptsMs, dtsMs uint64) {
		if cbErr != nil || len(frame) == 0 {
			return
		}
		if !haveBase {
			offset = (segWallMs - migStartMs) - int64(dtsMs) //nolint:gosec // ms values fit int64
			haveBase = true
		}
		synthDTS := int64(dtsMs) + offset //nolint:gosec // ms values fit int64
		synthPTS := int64(ptsMs) + offset //nolint:gosec // ms values fit int64
		if synthDTS < 0 {
			synthDTS = 0
		}
		if synthPTS < 0 {
			synthPTS = 0
		}
		frameWall := time.UnixMilli(migStartMs + synthDTS)
		av := &domain.AVPacket{Data: frame, PTSms: uint64(synthPTS), DTSms: uint64(synthDTS)} //nolint:gosec // clamped >= 0
		switch cid {
		case tsdemux.StreamTypeH264:
			av.Codec = domain.AVCodecH264
			av.KeyFrame = tsmux.KeyFrameH264(frame)
		case tsdemux.StreamTypeH265:
			av.Codec = domain.AVCodecH265
			av.KeyFrame = tsmux.KeyFrameH265(frame)
			*sawHEVC = true
		case tsdemux.StreamTypeAAC:
			av.Codec = domain.AVCodecAAC
		default:
			return // unsupported elementary stream — skip
		}
		if err := w.Ingest(buffer.Packet{AV: av}, frameWall); err != nil {
			cbErr = err
		}
	}
	if err := dmx.Input(ctx, f); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return cbErr
}

// buildMigratedCatalog assembles the per-stream catalog from the migrated hours.
// Width/height are left zero (the renderers fall back to defaults); only the
// codec family is recorded, derived from the demuxed stream type.
func buildMigratedCatalog(code string, originWallMs int64, hevc bool, hours []HourRecord) *Catalog {
	codec := "avc1.4d401f"
	if hevc {
		codec = "hvc1.1.6.L93.B0"
	}
	cat := &Catalog{
		StreamCode: code, Format: CatalogFormat, VideoTimescale: videoTimescale,
		RecordingMediaOriginUnixMs: originWallMs, RecordingMediaOriginTicks: 0,
		AudioProfile: "p0", BestProfile: "p0",
		Profiles: []ProfileDesc{{ID: "p0", Codec: codec}},
	}
	for _, hr := range hours {
		upsertHour(&cat.Profiles[0], hr)
		extendAvailable(&cat.Profiles[0], hr)
	}
	return cat
}

// legacySeg is one parsed playlist entry.
type legacySeg struct {
	file          string
	wallTime      time.Time
	duration      time.Duration
	discontinuity bool
}

// parseLegacyPlaylist reads playlist.m3u8 into ordered segments, deriving each
// segment's wall time from its EXT-X-PROGRAM-DATE-TIME anchor (and accumulating
// EXTINF durations for segments that share an anchor with a predecessor).
func parseLegacyPlaylist(segDir string) ([]legacySeg, error) {
	f, err := os.Open(filepath.Join(segDir, legacyPlaylist))
	if err != nil {
		return nil, fmt.Errorf("blob: open legacy playlist: %w", err)
	}
	defer func() { _ = f.Close() }()

	var (
		segs        []legacySeg
		pendingDur  time.Duration
		pendingDisc bool
		havePDT     bool
		pdt         time.Time
		lastWall    time.Time
		lastDur     time.Duration
		haveLast    bool
	)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case line == "":
			continue
		case line == "#EXT-X-DISCONTINUITY":
			pendingDisc = true
		case strings.HasPrefix(line, "#EXT-X-PROGRAM-DATE-TIME:"):
			t, err := parsePDT(strings.TrimPrefix(line, "#EXT-X-PROGRAM-DATE-TIME:"))
			if err != nil {
				return nil, err
			}
			pdt, havePDT = t, true
		case strings.HasPrefix(line, "#EXTINF:"):
			pendingDur = parseExtinf(line)
		case strings.HasPrefix(line, "#"):
			continue
		default:
			file := filepath.Base(line)
			if !validLegacySegName(file) {
				return nil, fmt.Errorf("blob: unexpected playlist URI %q", line)
			}
			wall := lastWall.Add(lastDur)
			if havePDT {
				wall, havePDT = pdt, false
			} else if !haveLast {
				wall = time.Time{}
			}
			segs = append(segs, legacySeg{file: file, wallTime: wall, duration: pendingDur, discontinuity: pendingDisc})
			lastWall, lastDur, haveLast = wall, pendingDur, true
			pendingDur, pendingDisc = 0, false
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("blob: scan playlist: %w", err)
	}
	return segs, nil
}

func parsePDT(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000Z07:00"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("blob: unrecognised EXT-X-PROGRAM-DATE-TIME %q", s)
}

func parseExtinf(line string) time.Duration {
	v := strings.TrimPrefix(line, "#EXTINF:")
	if i := strings.IndexByte(v, ','); i >= 0 {
		v = v[:i]
	}
	sec, _ := strconv.ParseFloat(strings.TrimSpace(v), 64)
	return time.Duration(sec * float64(time.Second))
}

func validLegacySegName(name string) bool {
	return name == filepath.Base(name) &&
		strings.HasPrefix(name, "dvr_") && strings.HasSuffix(name, ".ts")
}

// writeMigratedMarker records the source range + counts after the catalog is
// committed, as provenance for a verified migration.
func writeMigratedMarker(segDir string, res *MigrateResult) {
	body := fmt.Sprintf("segments=%d hours=%d video_frags=%d audio_frags=%d gaps=%d source_from_ms=%d source_to_ms=%d\n",
		res.Segments, res.Hours, res.VideoFrags, res.AudioFrags, res.Gaps, res.SourceFromMs, res.SourceToMs)
	_ = os.WriteFile(filepath.Join(segDir, migratedMarker), []byte(body), 0o644) //nolint:gosec // provenance marker, not a secret
}

// pruneLegacy removes the legacy files once a migration is committed. Best
// effort: the catalog already governs serving, so a leftover file is inert.
func pruneLegacy(segDir string, segs []legacySeg) {
	for _, seg := range segs {
		_ = os.Remove(filepath.Join(segDir, seg.file))
	}
	_ = os.Remove(filepath.Join(segDir, legacyPlaylist))
	_ = os.Remove(filepath.Join(segDir, legacyIndex))
}
