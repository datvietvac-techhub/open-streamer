package blob

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ResolveStreamDir builds <root>/<streamCode> with a containment guard so a
// crafted stream code can never escape the DVR root. streamCode may contain
// '/' for namespacing (e.g. "region/north/live").
func ResolveStreamDir(root string, streamCode string) (string, error) {
	cleanRoot := filepath.Clean(root)
	dir := filepath.Clean(filepath.Join(cleanRoot, streamCode))
	if dir != cleanRoot && !strings.HasPrefix(dir, cleanRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("blob: stream code %q escapes dvr root", streamCode)
	}
	return dir, nil
}

// profileDirPath is <streamDir>/<profileID> (e.g. ".../p0").
func profileDirPath(streamDir, profileID string) string {
	return filepath.Join(streamDir, profileID)
}

// hourDirPath is <profileDir>/YYYY/MM/DD for the UTC hour of t.
func hourDirPath(profileDir string, t time.Time) string {
	u := t.UTC()
	return filepath.Join(profileDir, fmt.Sprintf("%04d", u.Year()), fmt.Sprintf("%02d", int(u.Month())), fmt.Sprintf("%02d", u.Day()))
}

// hourToken is the two-digit UTC hour ("14"), the HH.* file stem.
func hourToken(t time.Time) string { return fmt.Sprintf("%02d", t.UTC().Hour()) }

// hourStem joins a day dir and hour token into the per-hour path stem; callers
// append .cmfv / .cmfa / .ranges / .open.
func hourStem(dir, hour string) string { return filepath.Join(dir, hour) }

// hourRel is the catalog hour key "YYYY/MM/DD/HH".
func hourRel(t time.Time) string {
	u := t.UTC()
	return fmt.Sprintf("%04d/%02d/%02d/%02d", u.Year(), int(u.Month()), u.Day(), u.Hour())
}

// hourFloor truncates t to the top of its UTC hour.
func hourFloor(t time.Time) time.Time { return t.UTC().Truncate(time.Hour) }

// ensureDir mkdir -p's a directory (the YYYY/MM/DD tree).
func ensureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("blob: mkdir %s: %w", dir, err)
	}
	return nil
}

// fsyncDir flushes a directory's entries so a create/rename is durable.
func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("blob: open dir %s: %w", dir, err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("blob: fsync dir %s: %w", dir, err)
	}
	return nil
}

// sentinelPath is the ".open" in-progress marker for an hour blob stem.
func sentinelPath(stem string) string { return stem + ".open" }

// createSentinel writes the ".open" marker (hour is being written).
func createSentinel(stem string) error {
	if err := os.WriteFile(sentinelPath(stem), nil, 0o644); err != nil {
		return fmt.Errorf("blob: create sentinel %s: %w", stem, err)
	}
	return nil
}

// removeSentinel deletes the ".open" marker (hour sealed).
func removeSentinel(stem string) error {
	if err := os.Remove(sentinelPath(stem)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("blob: remove sentinel %s: %w", stem, err)
	}
	return nil
}
