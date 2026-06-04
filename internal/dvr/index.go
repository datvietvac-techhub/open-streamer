package dvr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

const indexFileName = "index.json"

// resolveSegDir joins root and code into an absolute segment directory and
// rejects any result that would escape root. Stream codes are validated to
// reject `..` segments, but `/` is allowed so the joined path can legally
// have multiple segments — this guard makes path containment explicit so
// every downstream filepath.Join sees a path the caller has already proven
// safe (and CodeQL's path-injection tracker sees a sink-side validator).
func resolveSegDir(root, code string) (string, error) {
	cleanCode := filepath.Clean("/" + code)
	if cleanCode == "/" {
		return "", fmt.Errorf("dvr: stream code is empty")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("dvr: resolve dvr root %q: %w", root, err)
	}
	segDir, err := filepath.Abs(filepath.Join(rootAbs, cleanCode))
	if err != nil {
		return "", fmt.Errorf("dvr: resolve segment dir: %w", err)
	}
	rootPrefix := rootAbs + string(filepath.Separator)
	if segDir != rootAbs && !strings.HasPrefix(segDir, rootPrefix) {
		return "", fmt.Errorf("dvr: stream code %q escapes dvr root", code)
	}
	return segDir, nil
}

// loadIndex reads index.json from segDir.
// Returns nil, nil if the file does not exist (fresh recording).
func loadIndex(segDir string) (*domain.DVRIndex, error) {
	data, err := os.ReadFile(filepath.Join(segDir, indexFileName))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var idx domain.DVRIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, err
	}
	return &idx, nil
}

// saveIndex atomically writes index.json to segDir (write tmp → rename).
func saveIndex(segDir string, idx *domain.DVRIndex) error {
	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(segDir, indexFileName+".tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(segDir, indexFileName))
}

// segmentMeta is the in-memory per-segment info used for playlist writing and retention.
// Not persisted to index.json.
type segmentMeta struct {
	index         int
	wallTime      time.Time
	duration      time.Duration
	size          int64
	discontinuity bool
}

// parsePlaylist reads an existing playlist.m3u8 and reconstructs the in-memory
// segment list. Called on resume to continue appending without losing history.
//
// Parses:
//   - #EXT-X-PROGRAM-DATE-TIME → wallTime (applies to next segment and resets after DISCONTINUITY)
//   - #EXT-X-DISCONTINUITY    → discontinuity flag for next segment
//   - #EXTINF:X.XXX,           → duration
//   - filename (e.g. 000003.ts) → index derived from base name
func parsePlaylist(segDir string) ([]segmentMeta, error) {
	f, err := os.Open(filepath.Join(segDir, "playlist.m3u8"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	var (
		segments     []segmentMeta
		pendingWall  time.Time
		pendingDur   time.Duration
		pendingDisc  bool
		hasDur       bool
		needDateTime = true
	)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case strings.HasPrefix(line, "#EXT-X-PROGRAM-DATE-TIME:"):
			raw := strings.TrimPrefix(line, "#EXT-X-PROGRAM-DATE-TIME:")
			if t, err := time.Parse("2006-01-02T15:04:05.000Z", raw); err == nil {
				pendingWall = t
				needDateTime = false
			}

		case line == "#EXT-X-DISCONTINUITY":
			pendingDisc = true
			needDateTime = true // expect a DATE-TIME after discontinuity

		case strings.HasPrefix(line, "#EXTINF:"):
			raw := strings.TrimPrefix(line, "#EXTINF:")
			raw = strings.TrimSuffix(raw, ",")
			if s, err := strconv.ParseFloat(raw, 64); err == nil {
				pendingDur = time.Duration(s * float64(time.Second))
				hasDur = true
			}

		case hasDur && !strings.HasPrefix(line, "#") && line != "":
			// This is the segment filename line. Accept both the dvr_NNNNNN.ts
			// form and the legacy NNNNNN.ts form (recordings written before the
			// prefix was introduced) so old playlists still parse.
			base := strings.TrimPrefix(strings.TrimSuffix(filepath.Base(line), ".ts"), "dvr_")
			idx, err := strconv.Atoi(base)
			if err != nil {
				hasDur = false
				pendingDisc = false
				continue
			}
			// Try to get file size from disk. line comes from the playlist
			// file's contents, so Base() it to keep the lookup confined to
			// segDir even if the playlist were tampered with.
			var size int64
			if fi, err := os.Stat(filepath.Join(segDir, filepath.Base(line))); err == nil {
				size = fi.Size()
			}
			segments = append(segments, segmentMeta{
				index:         idx,
				wallTime:      pendingWall,
				duration:      pendingDur,
				size:          size,
				discontinuity: pendingDisc,
			})
			// Advance pendingWall by this segment's duration so subsequent
			// segments without their own EXT-X-PROGRAM-DATE-TIME inherit a
			// correctly accumulating wall-clock — writePlaylist only emits
			// PDT once per discontinuity group, not per segment, so without
			// this advance every segment in a group ends up with the same
			// wallTime as the first one. That collapse is what made the
			// timeshift handler return NO_SEGMENTS_IN_RANGE for any `from`
			// past the first segment's end (every segEnd = first.wallTime
			// + first.duration, never reaching the requested startTime).
			if !pendingWall.IsZero() {
				pendingWall = pendingWall.Add(pendingDur)
			}
			pendingDisc = false
			hasDur = false
			_ = needDateTime
		}
	}
	return segments, scanner.Err()
}
