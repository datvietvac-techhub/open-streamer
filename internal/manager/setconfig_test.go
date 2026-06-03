package manager

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// TestSetConfigSwapsThreshold pins the user-facing contract: the next
// monitor tick (read via packetTimeoutNs) sees the new value without any
// service restart.
func TestSetConfigSwapsThreshold(t *testing.T) {
	t.Parallel()
	svc := &Service{}
	svc.packetTimeoutNs.Store(int64(time.Duration(domain.DefaultInputPacketTimeoutSec) * time.Second))

	got := time.Duration(svc.packetTimeoutNs.Load())
	assert.Equal(t, time.Duration(domain.DefaultInputPacketTimeoutSec)*time.Second, got)

	svc.SetConfig(config.ManagerConfig{InputPacketTimeoutSec: 90})
	got = time.Duration(svc.packetTimeoutNs.Load())
	assert.Equal(t, 90*time.Second, got)
}

// TestSetConfigZeroFallsBackToDefault — operators who clear the field
// expect the package default rather than a zero-second timeout (which
// would degrade every input on the first tick).
func TestSetConfigZeroFallsBackToDefault(t *testing.T) {
	t.Parallel()
	svc := &Service{}
	svc.SetConfig(config.ManagerConfig{InputPacketTimeoutSec: 60})

	svc.SetConfig(config.ManagerConfig{InputPacketTimeoutSec: 0})
	got := time.Duration(svc.packetTimeoutNs.Load())
	assert.Equal(t, time.Duration(domain.DefaultInputPacketTimeoutSec)*time.Second, got)
}

// TestSetConfigRaceSafe catches a regression if SetConfig stops using
// atomic and goes back to a plain field. -race flags the torn read/write.
func TestSetConfigRaceSafe(t *testing.T) {
	t.Parallel()
	svc := &Service{}
	svc.SetConfig(config.ManagerConfig{InputPacketTimeoutSec: 30})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			svc.SetConfig(config.ManagerConfig{InputPacketTimeoutSec: 30 + i%10})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = time.Duration(svc.packetTimeoutNs.Load())
		}
	}()
	wg.Wait()
}
