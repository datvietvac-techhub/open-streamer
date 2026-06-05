package blob

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RepairStream scans every profile of a stream for crash-dirty hours (those
// still carrying a ".open" sentinel) and repairs each in place. The blob is the
// source of truth: a record is kept only if it passes CRC AND its fragment
// bytes are durable in the corresponding track blob; the first record that
// fails either ends the valid run. The blobs are then truncated to their last
// durable fragment, the index to the valid count, the hour sealed, and the
// sentinel removed. After repair the writer opens a fresh hour with a
// discontinuity, so the archive is consistent however the process died.
func RepairStream(streamDir string) error {
	entries, err := os.ReadDir(streamDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("blob: repair: read %s: %w", streamDir, err)
	}
	for _, e := range entries {
		if !e.IsDir() || !isProfileDir(e.Name()) {
			continue
		}
		if err := repairProfile(filepath.Join(streamDir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// isProfileDir reports whether name matches the pN profile-dir convention.
func isProfileDir(name string) bool {
	if len(name) < 2 || name[0] != 'p' {
		return false
	}
	for _, c := range name[1:] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func repairProfile(profileDir string) error {
	var stems []string
	err := filepath.WalkDir(profileDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() && strings.HasSuffix(path, ".open") {
			stems = append(stems, strings.TrimSuffix(path, ".open"))
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("blob: repair: walk %s: %w", profileDir, err)
	}
	for _, stem := range stems {
		if err := repairHour(stem); err != nil {
			return err
		}
	}
	return nil
}

// repairHour repairs one crash-dirty hour identified by its path stem.
func repairHour(stem string) error {
	rangesPath := stem + ".ranges"
	data, err := os.ReadFile(rangesPath)
	if err != nil {
		// A sentinel with no index: nothing recoverable; just drop the marker.
		if os.IsNotExist(err) {
			return removeSentinel(stem)
		}
		return fmt.Errorf("blob: repair: read %s: %w", rangesPath, err)
	}
	hdr, err := UnmarshalRangesHeader(data)
	if err != nil {
		return err
	}
	vSize := fileSize(stem + ".cmfv")
	aSize := fileSize(stem + ".cmfa")

	var validCount uint32
	lastVEnd := int64(hdr.VideoInitLen)
	lastAEnd := int64(hdr.AudioInitLen)
	for off := RangesHeadSize; off+FragRecordSize <= len(data); off += FragRecordSize {
		rec, ok := UnmarshalFragRecord(data[off : off+FragRecordSize])
		if !ok {
			break // torn record → end of the valid run
		}
		end := int64(rec.ByteOffset) + int64(rec.ByteLen)
		if rec.Track == TrackVideo {
			if end > vSize {
				break // fragment bytes not durable
			}
			lastVEnd = end
		} else {
			if end > aSize {
				break
			}
			lastAEnd = end
		}
		validCount++
	}

	if err := truncateFile(stem+".cmfv", lastVEnd); err != nil {
		return err
	}
	if aSize > 0 {
		if err := truncateFile(stem+".cmfa", lastAEnd); err != nil {
			return err
		}
	}
	hdr.Flags |= HeaderFlagSealed
	if err := rewriteRangesHeader(rangesPath, hdr, validCount); err != nil {
		return err
	}
	return removeSentinel(stem)
}

// fileSize returns the file size, or 0 if it doesn't exist.
func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// truncateFile truncates path to size (no-op if the file is absent).
func truncateFile(path string, size int64) error {
	if err := os.Truncate(path, size); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("blob: repair: truncate %s: %w", path, err)
	}
	return nil
}

// rewriteRangesHeader truncates the index to count records and rewrites its
// header (count + flags), then fsyncs.
func rewriteRangesHeader(path string, hdr RangesHeader, count uint32) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("blob: repair: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Truncate(int64(RangesHeadSize) + int64(count)*FragRecordSize); err != nil {
		return fmt.Errorf("blob: repair: truncate index %s: %w", path, err)
	}
	hdr.RecordCount = count
	head := MarshalRangesHeader(hdr)
	if _, err := f.WriteAt(head[:], 0); err != nil {
		return fmt.Errorf("blob: repair: rewrite header %s: %w", path, err)
	}
	return f.Sync()
}
