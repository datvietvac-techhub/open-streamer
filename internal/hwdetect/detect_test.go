package hwdetect

import (
	"runtime"
	"testing"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

func TestAvailableAlwaysIncludesNone(t *testing.T) {
	accels := Available()
	if len(accels) == 0 || accels[0] != domain.HWAccelNone {
		t.Fatalf("HWAccelNone must be first entry, got %v", accels)
	}
}

func TestAvailableHonoursPlatform(t *testing.T) {
	accels := Available()
	contains := func(target domain.HWAccel) bool {
		for _, a := range accels {
			if a == target {
				return true
			}
		}
		return false
	}

	switch runtime.GOOS {
	case "darwin":
		if !contains(domain.HWAccelVideoToolbox) {
			t.Errorf("darwin must report VideoToolbox, got %v", accels)
		}
	case "linux":
		// Linux detection depends on host devices — only assert that no
		// macOS-only backends sneak in.
		if contains(domain.HWAccelVideoToolbox) {
			t.Errorf("linux must not report VideoToolbox, got %v", accels)
		}
	default:
		// Other platforms (windows, freebsd, …) get only HWAccelNone.
		if len(accels) != 1 {
			t.Errorf("unsupported OS %s should report only HWAccelNone, got %v", runtime.GOOS, accels)
		}
	}
}

func TestAvailableNoDuplicates(t *testing.T) {
	accels := Available()
	seen := make(map[domain.HWAccel]bool)
	for _, a := range accels {
		if seen[a] {
			t.Errorf("duplicate backend %s in %v", a, accels)
		}
		seen[a] = true
	}
}
