package blob

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// retentionInterval is how often a recording's retention pass runs.
const retentionInterval = 60 * time.Second

// runRetention is a per-recording goroutine that prunes whole hours past the
// configured age or total-size budget. Only sealed hours are eligible, so the
// in-progress (and any crash-dirty, not-yet-sealed) hour is never deleted.
func (s *Service) runRetention(ctx context.Context, rec *recording, cfg *domain.StreamDVRConfig) {
	defer rec.wg.Done()
	if cfg == nil || (cfg.RetentionSec <= 0 && cfg.MaxSizeGB <= 0) {
		return // keep forever
	}
	t := time.NewTicker(retentionInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rec.pruneOnce(cfg, time.Now())
		}
	}
}

// pruneOnce deletes oldest sealed hours until within the age + size budget.
func (r *recording) pruneOnce(cfg *domain.StreamDVRConfig, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	maxAge := time.Duration(cfg.RetentionSec) * time.Second
	var maxSize int64
	if cfg.MaxSizeGB > 0 {
		maxSize = int64(cfg.MaxSizeGB * 1e9)
	}

	changed := false
	for {
		hour, wallToMs, found := oldestSealedHour(r.cat)
		if !found {
			break
		}
		overAge := maxAge > 0 && now.Sub(time.UnixMilli(wallToMs)) > maxAge
		overSize := maxSize > 0 && catalogTotalSize(r.cat) > maxSize
		if !overAge && !overSize {
			break
		}
		deleteHourEverywhere(r.streamDir, r.cat, hour)
		changed = true
	}
	if changed {
		recomputeAllAvailable(r.cat)
		if err := r.cat.Save(r.streamDir); err != nil {
			return
		}
	}
}

// oldestSealedHour returns the lexicographically smallest sealed hour across all
// profiles (YYYY/MM/DD/HH sorts chronologically) and its last-fragment wall ms.
func oldestSealedHour(cat *Catalog) (hour string, wallToMs int64, found bool) {
	for i := range cat.Profiles {
		for _, hr := range cat.Profiles[i].Hours {
			if !hr.Sealed {
				continue
			}
			if !found || hr.Hour < hour {
				hour, wallToMs, found = hr.Hour, hr.WallToMs, true
			}
		}
	}
	return hour, wallToMs, found
}

// catalogTotalSize sums every profile's hour sizes.
func catalogTotalSize(cat *Catalog) int64 {
	var total int64
	for i := range cat.Profiles {
		for _, hr := range cat.Profiles[i].Hours {
			total += hr.SizeBytes
		}
	}
	return total
}

// deleteHourEverywhere removes one hour's files for every profile that has it
// (asymmetric-tolerant) and drops the hour from the catalog. Empty day
// directories are cleaned best-effort.
func deleteHourEverywhere(streamDir string, cat *Catalog, hour string) {
	for i := range cat.Profiles {
		pd := &cat.Profiles[i]
		idx := -1
		for j := range pd.Hours {
			if pd.Hours[j].Hour == hour {
				idx = j
				break
			}
		}
		if idx < 0 {
			continue
		}
		stem := filepath.Join(profileDirPath(streamDir, pd.ID), filepath.FromSlash(hour))
		for _, ext := range []string{".cmfv", ".cmfa", ".ranges", ".open"} {
			_ = os.Remove(stem + ext)
		}
		_ = os.Remove(filepath.Dir(stem)) // rmdir the day dir if now empty
		pd.Hours = append(pd.Hours[:idx], pd.Hours[idx+1:]...)
	}
}

// recomputeAllAvailable rebuilds each profile's coverage windows from its
// remaining (sorted) hours, merging adjacent ones.
func recomputeAllAvailable(cat *Catalog) {
	for i := range cat.Profiles {
		pd := &cat.Profiles[i]
		pd.Available = pd.Available[:0]
		for _, hr := range pd.Hours {
			n := len(pd.Available)
			if n > 0 && hr.WallFromMs <= pd.Available[n-1].ToMs+1 {
				if hr.WallToMs > pd.Available[n-1].ToMs {
					pd.Available[n-1].ToMs = hr.WallToMs
				}
				continue
			}
			pd.Available = append(pd.Available, MediaWindow{FromMs: hr.WallFromMs, ToMs: hr.WallToMs})
		}
	}
}
