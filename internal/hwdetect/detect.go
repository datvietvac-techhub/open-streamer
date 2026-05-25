// Package hwdetect probes the host OS for available hardware acceleration backends.
// Detection is based on device files and PCI vendor IDs — no subprocess is spawned.
package hwdetect

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ntt0601zcoder/open-streamer/internal/domain"
)

// Available returns the list of HWAccel backends that appear to be present on
// the current host. domain.HWAccelNone (CPU) is always included first.
//
// Detection rules (Linux):
//   - NVENC:  /dev/nvidia0 exists
//   - VAAPI:  any /dev/dri/renderD* device exists
//   - QSV:    VAAPI device present AND at least one PCI device with Intel vendor (0x8086)
//
// Detection rules (macOS):
//   - VideoToolbox: always available on darwin
func Available() []domain.HWAccel {
	accels := []domain.HWAccel{domain.HWAccelNone}

	switch runtime.GOOS {
	case "darwin":
		accels = append(accels, domain.HWAccelVideoToolbox)
	case "linux":
		if hasNVIDIADevice() {
			accels = append(accels, domain.HWAccelNVENC)
		}
		if hasDRIDevice() {
			accels = append(accels, domain.HWAccelVAAPI)
			if hasIntelGPU() {
				accels = append(accels, domain.HWAccelQSV)
			}
		}
	}

	return accels
}

// hasNVIDIADevice checks for the primary NVIDIA character device.
func hasNVIDIADevice() bool {
	_, err := os.Stat("/dev/nvidia0")
	return err == nil
}

// hasDRIDevice checks for at least one DRM render node (Intel, AMD, or any GPU on Linux).
func hasDRIDevice() bool {
	matches, err := filepath.Glob("/dev/dri/renderD*")
	return err == nil && len(matches) > 0
}

// hasIntelGPU scans PCI device vendor IDs for Intel (0x8086).
func hasIntelGPU() bool {
	vendors, err := filepath.Glob("/sys/bus/pci/devices/*/vendor")
	if err != nil {
		return false
	}
	for _, path := range vendors {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(data)) == "0x8086" {
			return true
		}
	}
	return false
}
