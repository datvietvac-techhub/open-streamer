package manager

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/metrics"
)

// newTestMetrics returns a Metrics instance whose collectors are independent
// of the global Prometheus registry, so each test can construct its own
// without triggering "duplicate metrics collector registration" panics
// (production code uses promauto which registers globally).
//
// Only the collectors actually touched by the manager are populated — other
// fields are nil and would panic if manager ever expanded into them, which
// tests would catch immediately.
func newTestMetrics() *metrics.Metrics {
	return &metrics.Metrics{
		ManagerInputHealth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_manager_input_health",
		}, []string{"stream_code", "input_priority"}),
		ManagerFailoversTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_manager_failovers_total",
		}, []string{"stream_code"}),
		IngestorErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_ingestor_errors_total",
		}, []string{"stream_code", "reason"}),
	}
}

// fakeIngestor is a stub of ingestorDep that records calls instead of
// running real pull workers. Tests inspect the recorded calls to verify
// manager behaviour (e.g. "Register triggers ingestor.Start with the
// best-priority input"). All fields are guarded by mu — the manager spawns
// goroutines (monitor, runProbe, tryFailover) that race with the test's
// inspection.
type fakeIngestor struct {
	mu sync.Mutex

	starts   []startCall
	probes   []domain.Input
	stops    []domain.StreamCode
	probeErr error // when set, Probe returns this; nil = success
	startErr error // when set, Start returns this; nil = success

	// onStart fires synchronously inside Start (under lock-free); used by
	// tests that need to simulate "first packet arrived" right after a
	// successful start.
	onStart func(streamID domain.StreamCode, input domain.Input)

	// Observers wired via SetPacketObserver / SetInputErrorObserver. The
	// manager invokes these on packet flow / source errors; tests can also
	// invoke them directly to drive RecordPacket / ReportInputError without
	// going through a real ingest worker.
	onPacket atomic.Pointer[func(domain.StreamCode, int)]
	onErr    atomic.Pointer[func(domain.StreamCode, int, error)]
}

type startCall struct {
	StreamID      domain.StreamCode
	Input         domain.Input
	BufferWriteID domain.StreamCode
}

func (f *fakeIngestor) Start(_ context.Context, streamID domain.StreamCode, input domain.Input, bufID domain.StreamCode) error {
	f.mu.Lock()
	if f.startErr != nil {
		err := f.startErr
		f.mu.Unlock()
		return err
	}
	f.starts = append(f.starts, startCall{StreamID: streamID, Input: input, BufferWriteID: bufID})
	cb := f.onStart
	f.mu.Unlock()
	if cb != nil {
		cb(streamID, input)
	}
	return nil
}

func (f *fakeIngestor) Probe(_ context.Context, input domain.Input) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probes = append(f.probes, input)
	return f.probeErr
}

func (f *fakeIngestor) Stop(streamID domain.StreamCode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops = append(f.stops, streamID)
}

func (f *fakeIngestor) SetPacketObserver(fn func(domain.StreamCode, int)) {
	f.onPacket.Store(&fn)
}

func (f *fakeIngestor) SetInputErrorObserver(fn func(domain.StreamCode, int, error)) {
	f.onErr.Store(&fn)
}

// SetMediaPacketObserver is satisfied for the ingestorDep interface but
// unused by manager unit tests (no media packets are synthesized).
func (f *fakeIngestor) SetMediaPacketObserver(_ func(domain.StreamCode, int, *domain.AVPacket)) {
	// no-op: see method comment.
}

// startCallsCopy returns a snapshot of recorded Start calls; safe for
// concurrent reads while the manager continues to mutate state.
func (f *fakeIngestor) startCallsCopy() []startCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]startCall, len(f.starts))
	copy(out, f.starts)
	return out
}

func (f *fakeIngestor) stopCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.stops)
}
