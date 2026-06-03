package ingestor

// service_test.go covers the configuration setters, pure helpers, and the
// non-network code paths of Service. The DI constructor (New) and the
// listener/Run path are integration territory and exercised separately.

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/q191201771/lal/pkg/rtmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor/push"
)

// capturingBus records every Publish call so tests can assert which
// lifecycle events fired. Subscribe is a no-op (nothing in the ingestor
// subscribes back to the bus during these unit tests).
type capturingBus struct {
	mu     sync.Mutex
	events []domain.Event
}

func (b *capturingBus) Publish(_ context.Context, ev domain.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, ev)
}

func (b *capturingBus) Subscribe(_ domain.EventType, _ events.HandlerFunc) func() {
	return func() {}
}

// newTestService assembles a Service with just enough fields to exercise
// the synchronous setters and helpers. Network-bound fields (rtmpSrv,
// real metrics) stay zero — tests must avoid Run / Start to skip them.
// The bus is the in-package capturingBus so OnConnect / OnDisconnect
// callbacks (which publish events) don't nil-deref.
func newTestService() *Service {
	buf := buffer.NewServiceForTesting(8)
	svc := &Service{
		cfg:      config.IngestorConfig{},
		buf:      buf,
		bus:      &capturingBus{},
		registry: NewRegistry(),
		workers:  make(map[domain.StreamCode]*pullWorkerEntry),
	}
	cp := config.ListenersConfig{}
	svc.listenersPtr.Store(&cp)
	return svc
}

// ─── pure helpers ────────────────────────────────────────────────────────────

func TestRTMPListenAddr_DefaultsHostTo0000(t *testing.T) {
	t.Parallel()
	addr := rtmpListenAddr(config.RTMPListenerConfig{Port: 1935})
	assert.Contains(t, addr, ":1935")
	// DefaultListenHost is published in the domain package; just assert the
	// host segment is non-empty rather than baking the exact constant.
	assert.NotEqual(t, ":1935", addr, "host segment must default to non-empty")
}

func TestRTMPListenAddr_HonoursExplicitHost(t *testing.T) {
	t.Parallel()
	addr := rtmpListenAddr(config.RTMPListenerConfig{ListenHost: "127.0.0.1", Port: 1936})
	assert.Equal(t, "127.0.0.1:1936", addr)
}

func TestPushStreamKey_PublishURLUsesStreamCode(t *testing.T) {
	t.Parallel()
	key := pushStreamKey("live", domain.Input{URL: "publish://"})
	assert.Equal(t, "live", key)
}

func TestPushStreamKey_LegacyWildcardUsesLastSegment(t *testing.T) {
	t.Parallel()
	key := pushStreamKey("ignored-for-legacy", domain.Input{URL: "rtmp://0.0.0.0:1935/live/encoder1"})
	assert.Equal(t, "encoder1", key)
}

func TestPushStreamKey_TrailingSlashIsTrimmed(t *testing.T) {
	t.Parallel()
	key := pushStreamKey("live", domain.Input{URL: "rtmp://0.0.0.0:1935/app/key/"})
	assert.Equal(t, "key", key)
}

func TestPushStreamKey_EmptyURLReturnsEmpty(t *testing.T) {
	t.Parallel()
	// publish:// detection short-circuits to streamID; force the legacy
	// path with a non-publish URL that has no segments.
	assert.Equal(t, "", pushStreamKey("live", domain.Input{URL: ""}))
}

// ─── SetListeners / currentListeners ─────────────────────────────────────────

func TestService_SetListenersHotSwaps(t *testing.T) {
	t.Parallel()
	svc := newTestService()
	svc.SetListeners(config.ListenersConfig{
		RTMP: config.RTMPListenerConfig{Enabled: true, Port: 1936},
	})
	got := svc.currentListeners()
	assert.True(t, got.RTMP.Enabled)
	assert.Equal(t, 1936, got.RTMP.Port)
}

func TestService_CurrentListenersZeroValueWhenUnset(t *testing.T) {
	t.Parallel()
	// listenersPtr never populated → fall back to zero value, never nil-deref.
	svc := &Service{}
	got := svc.currentListeners()
	assert.False(t, got.RTMP.Enabled)
}

// ─── observer setters ────────────────────────────────────────────────────────

func TestService_SetPacketObserver(t *testing.T) {
	t.Parallel()
	svc := newTestService()
	var fired int
	svc.SetPacketObserver(func(_ domain.StreamCode, _ int) { fired++ })

	// Verify the observer is reachable via pushStreamCallbacks (which
	// resolves it at fire time).
	cb := svc.pushStreamCallbacks("live", 0, "rtmp")
	cb.OnPacket()
	assert.Equal(t, 1, fired)
}

func TestService_SetInputErrorObserver(t *testing.T) {
	t.Parallel()
	svc := newTestService()
	var captured error
	svc.SetInputErrorObserver(func(_ domain.StreamCode, _ int, err error) { captured = err })

	cb := svc.pushStreamCallbacks("live", 0, "rtmp")
	wantErr := errors.New("disconnected")
	cb.OnDisconnect(wantErr)
	assert.Equal(t, wantErr, captured)
}

func TestService_SetMediaPacketObserver(t *testing.T) {
	t.Parallel()
	svc := newTestService()
	var seen int
	svc.SetMediaPacketObserver(func(_ domain.StreamCode, _ int, _ *domain.AVPacket) { seen++ })

	cb := svc.pushStreamCallbacks("live", 0, "rtmp")
	cb.OnMedia(&domain.AVPacket{})
	assert.Equal(t, 1, seen)
}

func TestService_SetStreamLookupStores(t *testing.T) {
	t.Parallel()
	svc := newTestService()
	called := false
	svc.SetStreamLookup(func(_ domain.StreamCode) (*domain.Stream, bool) {
		called = true
		return nil, false
	})
	svc.mu.Lock()
	fn := svc.streamLookup
	svc.mu.Unlock()
	require.NotNil(t, fn)
	_, _ = fn("any")
	assert.True(t, called)
}

// ─── SetRTMPPlayHandler ──────────────────────────────────────────────────────

func TestService_SetRTMPPlayHandler_StoresWhenServerNotRunning(t *testing.T) {
	t.Parallel()
	svc := newTestService()
	fn := push.PlayFunc(func(_ context.Context, _ string, _ push.PlayInfo, _ *rtmp.ServerSession) error {
		return nil
	})
	svc.SetRTMPPlayHandler(fn)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	require.NotNil(t, svc.pendingPlayFunc, "play handler must be queued for deferred wiring")
}

// ─── Stop ────────────────────────────────────────────────────────────────────

func TestService_StopCancelsAndUnregistersWorker(t *testing.T) {
	t.Parallel()
	svc := newTestService()

	cancelled := false
	cancel := func() { cancelled = true }
	svc.workers["live"] = &pullWorkerEntry{inputPriority: 0, cancel: cancel}

	svc.Stop("live")

	assert.True(t, cancelled, "Stop must cancel the worker context")
	svc.mu.Lock()
	defer svc.mu.Unlock()
	assert.NotContains(t, svc.workers, domain.StreamCode("live"))
}

func TestService_StopMissingStreamIsNoOp(t *testing.T) {
	t.Parallel()
	svc := newTestService()
	// Should not panic / not error when the stream was never started.
	svc.Stop("never-started")
	svc.mu.Lock()
	defer svc.mu.Unlock()
	assert.Empty(t, svc.workers)
}

func TestService_StopAlsoClearsPendingPushCallbacks(t *testing.T) {
	t.Parallel()
	svc := newTestService()
	svc.pendingPushCallbacks = map[domain.StreamCode]push.StreamCallbacks{
		"live": {},
	}
	svc.Stop("live")
	svc.mu.Lock()
	defer svc.mu.Unlock()
	_, ok := svc.pendingPushCallbacks["live"]
	assert.False(t, ok)
}

// ─── pushStreamCallbacks closure semantics ───────────────────────────────────

// OnConnect / OnDisconnect publish events through the wired bus.
func TestService_PushStreamCallbacks_OnConnectEmitsEvent(t *testing.T) {
	t.Parallel()
	svc := newTestService()

	cb := svc.pushStreamCallbacks("live", 0, "rtmp")
	cb.OnConnect()

	bus := svc.bus.(*capturingBus)
	bus.mu.Lock()
	defer bus.mu.Unlock()
	require.Len(t, bus.events, 1)
	assert.Equal(t, domain.EventInputConnected, bus.events[0].Type)
	assert.Equal(t, domain.StreamCode("live"), bus.events[0].StreamCode)
}

func TestService_PushStreamCallbacks_OnDisconnectEmitsEvent(t *testing.T) {
	t.Parallel()
	svc := newTestService()

	cb := svc.pushStreamCallbacks("live", 0, "rtmp")
	cb.OnDisconnect(errors.New("network"))

	bus := svc.bus.(*capturingBus)
	bus.mu.Lock()
	defer bus.mu.Unlock()
	require.Len(t, bus.events, 1)
	assert.Equal(t, domain.EventInputFailed, bus.events[0].Type)
}

// ─── shouldFailoverImmediately ───────────────────────────────────────────────

func TestShouldFailoverImmediately(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error never failovers", nil, false},
		{"unrelated error", errors.New("io: short read"), false},
		{"TLS handshake failure", errors.New("tls: handshake failure"), true},
		{"x509 cert untrusted", errors.New("x509: certificate signed by unknown authority"), true},
		{"DNS resolution failure", errors.New("dial tcp: lookup foo: no such host"), true},
		{"HTTP 401", errors.New("hls: fetch failed http 401 unauthorized"), true},
		{"HTTP 403", errors.New("hls: http 403"), true},
		{"HTTP 404", errors.New("hls: http 404 not found"), true},
		{"HTTP 410", errors.New("http 410 gone"), true},
		{"HTTP 429", errors.New("rate limited http 429"), true},
		{"HTTP 5xx", errors.New("http 503 unavailable"), true},
		{"HTTP 200 does not failover", errors.New("http 200 ok"), false},
		{"malformed status", errors.New("http abc"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, shouldFailoverImmediately(tc.err))
		})
	}
}
