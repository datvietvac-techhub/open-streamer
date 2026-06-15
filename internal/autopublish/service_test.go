package autopublish

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

// ─── fakes ───────────────────────────────────────────────────────────────────

type fakeTemplateRepo struct {
	mu sync.Mutex
	m  map[domain.TemplateCode]*domain.Template
}

func newFakeTemplateRepo() *fakeTemplateRepo {
	return &fakeTemplateRepo{m: make(map[domain.TemplateCode]*domain.Template)}
}

func (r *fakeTemplateRepo) put(t *domain.Template) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *t
	r.m[t.Code] = &cp
}

func (r *fakeTemplateRepo) Save(_ context.Context, t *domain.Template) error {
	r.put(t)
	return nil
}

func (r *fakeTemplateRepo) FindByCode(_ context.Context, code domain.TemplateCode) (*domain.Template, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if v, ok := r.m[code]; ok {
		cp := *v
		return &cp, nil
	}
	return nil, store.ErrNotFound
}

func (r *fakeTemplateRepo) List(_ context.Context) ([]*domain.Template, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*domain.Template, 0, len(r.m))
	for _, v := range r.m {
		cp := *v
		out = append(out, &cp)
	}
	return out, nil
}

func (r *fakeTemplateRepo) Delete(_ context.Context, code domain.TemplateCode) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, code)
	return nil
}

// stubCoord records Start/Stop calls so tests can assert lifecycle. It
// also creates / deletes the matching buffer slot — production
// coordinator.Start does this as part of its pipeline setup, and the
// autopublish liveness observer relies on it being there. A stub that
// skipped buffer creation would let the observer hit subscribe-failed
// and unwind the entry, breaking the test's "ResolveOrCreate inserted
// an entry" assertion.
type stubCoord struct {
	mu       sync.Mutex
	buf      *buffer.Service
	started  []*domain.Stream
	stopped  []domain.StreamCode
	startErr error
}

func (c *stubCoord) Start(_ context.Context, s *domain.Stream) error {
	if c.startErr != nil {
		return c.startErr
	}
	c.mu.Lock()
	c.started = append(c.started, s)
	c.mu.Unlock()
	if c.buf != nil {
		c.buf.Create(s.Code)
	}
	return nil
}

func (c *stubCoord) Stop(_ context.Context, code domain.StreamCode) {
	c.mu.Lock()
	c.stopped = append(c.stopped, code)
	c.mu.Unlock()
	if c.buf != nil {
		c.buf.Delete(code)
	}
}

func (c *stubCoord) IsRunning(_ domain.StreamCode) bool { return false }

func newServiceWithDeps(t *testing.T) (*Service, *fakeTemplateRepo, *stubCoord) {
	t.Helper()
	buf := buffer.NewServiceForTesting(1024)
	tr := newFakeTemplateRepo()
	cd := &stubCoord{buf: buf}
	s := &Service{
		templates: tr,
		coord:     cd,
		buf:       buf,
		entries:   make(map[domain.StreamCode]*runtimeEntry),
	}
	s.cur.Store(newMatcher(nil))
	return s, tr, cd
}

// ─── tests ───────────────────────────────────────────────────────────────────

// ResolveOrCreate must find a matching template, verify it accepts push,
// build the resolved stream, and dispatch coordinator.Start. The runtime
// registry should hold a single entry afterwards.
func TestResolveOrCreate_HappyPath(t *testing.T) {
	s, tr, cd := newServiceWithDeps(t)
	tr.put(&domain.Template{
		Code:     "profile-a",
		Prefixes: []string{"live/"},
		Inputs:   []domain.Input{{URL: "publish://"}},
	})
	require.NoError(t, s.RefreshTemplates(context.Background()))

	code, err := s.ResolveOrCreate(context.Background(), "live/foo", "")
	require.NoError(t, err)
	assert.Equal(t, domain.StreamCode("live/foo"), code)

	cd.mu.Lock()
	require.Len(t, cd.started, 1, "coordinator.Start must fire once for a fresh runtime stream")
	started := cd.started[0]
	cd.mu.Unlock()
	assert.Equal(t, domain.StreamCode("live/foo"), started.Code)
	require.NotNil(t, started.Template)
	assert.Equal(t, domain.TemplateCode("profile-a"), *started.Template)
	require.Len(t, started.Inputs, 1, "runtime stream must inherit publish:// input from template")
	assert.Equal(t, "publish://", started.Inputs[0].URL)
}

// Concurrent callers racing on the same path must not produce two Start
// calls — the first wins, the others observe an already-running stream
// and return its code.
func TestResolveOrCreate_DedupsConcurrentCallers(t *testing.T) {
	s, tr, cd := newServiceWithDeps(t)
	tr.put(&domain.Template{
		Code:     "profile-a",
		Prefixes: []string{"live"},
		Inputs:   []domain.Input{{URL: "publish://"}},
	})
	require.NoError(t, s.RefreshTemplates(context.Background()))

	var wg sync.WaitGroup
	var ok atomic.Int32
	for range 25 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.ResolveOrCreate(context.Background(), "live/foo", ""); err == nil {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(25), ok.Load(), "every caller should see success")
	cd.mu.Lock()
	defer cd.mu.Unlock()
	assert.Len(t, cd.started, 1, "Start may fire only once")
}

func TestResolveOrCreate_NoMatchReturnsErr(t *testing.T) {
	s, _, _ := newServiceWithDeps(t)
	require.NoError(t, s.RefreshTemplates(context.Background()))

	_, err := s.ResolveOrCreate(context.Background(), "unknown/foo", "")
	require.ErrorIs(t, err, ErrNoMatch)
}

// A matched template that lacks a publish:// input must produce
// ErrTemplateNoPush; the runtime registry stays empty.
func TestResolveOrCreate_TemplateWithoutPushRejected(t *testing.T) {
	s, tr, cd := newServiceWithDeps(t)
	tr.put(&domain.Template{
		Code:     "profile-a",
		Prefixes: []string{"live"},
		Inputs:   []domain.Input{{URL: "rtmp://upstream/key"}}, // not publish://
	})
	require.NoError(t, s.RefreshTemplates(context.Background()))

	_, err := s.ResolveOrCreate(context.Background(), "live/foo", "")
	require.ErrorIs(t, err, ErrTemplateNoPush)
	cd.mu.Lock()
	defer cd.mu.Unlock()
	assert.Empty(t, cd.started)
}

// A template with a StreamKey requires a matching ?key= secret (S-8): a
// wrong/absent secret is rejected with ErrUnauthorized and no stream is
// started; the correct secret materialises the stream.
func TestResolveOrCreate_StreamKeyEnforced(t *testing.T) {
	s, tr, cd := newServiceWithDeps(t)
	tr.put(&domain.Template{
		Code:      "profile-a",
		Prefixes:  []string{"live"},
		Inputs:    []domain.Input{{URL: "publish://"}},
		StreamKey: "s3cr3t",
	})
	require.NoError(t, s.RefreshTemplates(context.Background()))

	_, err := s.ResolveOrCreate(context.Background(), "live/foo", "wrong")
	require.ErrorIs(t, err, ErrUnauthorized)
	_, err = s.ResolveOrCreate(context.Background(), "live/foo", "")
	require.ErrorIs(t, err, ErrUnauthorized)
	cd.mu.Lock()
	assert.Empty(t, cd.started, "an unauthorised push must not start a runtime stream")
	cd.mu.Unlock()

	code, err := s.ResolveOrCreate(context.Background(), "live/foo", "s3cr3t")
	require.NoError(t, err)
	assert.Equal(t, domain.StreamCode("live/foo"), code)
	cd.mu.Lock()
	assert.Len(t, cd.started, 1, "the correct secret must materialise the stream")
	cd.mu.Unlock()
}

// Invalid stream codes (e.g. contain "..") must be rejected at the
// boundary even when a template prefix accepts the path.
func TestResolveOrCreate_InvalidStreamCodeRejected(t *testing.T) {
	s, tr, _ := newServiceWithDeps(t)
	tr.put(&domain.Template{
		Code:     "profile-a",
		Prefixes: []string{"live"},
		Inputs:   []domain.Input{{URL: "publish://"}},
	})
	require.NoError(t, s.RefreshTemplates(context.Background()))

	_, err := s.ResolveOrCreate(context.Background(), "live/../escape", "")
	require.Error(t, err)
}

// A Start failure must surface to the caller AND leave no entry behind
// (otherwise the reaper would never clean it up).
func TestResolveOrCreate_StartFailureLeavesNoEntry(t *testing.T) {
	s, tr, cd := newServiceWithDeps(t)
	cd.startErr = errors.New("boom")
	tr.put(&domain.Template{
		Code:     "profile-a",
		Prefixes: []string{"live"},
		Inputs:   []domain.Input{{URL: "publish://"}},
	})
	require.NoError(t, s.RefreshTemplates(context.Background()))

	_, err := s.ResolveOrCreate(context.Background(), "live/foo", "")
	require.Error(t, err)
	assert.False(t, s.IsRuntime("live/foo"))
}

// reapOnce must stop any entry whose lastPacketAt is older than IdleTimeout
// and leave fresher entries untouched.
func TestReapOnce_EvictsIdleEntries(t *testing.T) {
	s, _, cd := newServiceWithDeps(t)

	stale := &runtimeEntry{code: "live/old", templateCode: "p"}
	stale.lastPacketAt.Store(time.Now().Add(-2 * IdleTimeout).UnixNano())
	stale.cancelObs = func() {}

	fresh := &runtimeEntry{code: "live/new", templateCode: "p"}
	fresh.lastPacketAt.Store(time.Now().UnixNano())
	fresh.cancelObs = func() {}

	s.entries[stale.code] = stale
	s.entries[fresh.code] = fresh

	s.reapOnce(context.Background())

	cd.mu.Lock()
	defer cd.mu.Unlock()
	require.Len(t, cd.stopped, 1)
	assert.Equal(t, domain.StreamCode("live/old"), cd.stopped[0])
	assert.True(t, s.IsRuntime("live/new"), "fresh entry must stay registered")
	assert.False(t, s.IsRuntime("live/old"), "idle entry must be unregistered")
}

// IsRuntime is the cheap read path used by the stream handler to tag list
// entries; verify under both branches.
func TestIsRuntime(t *testing.T) {
	s, _, _ := newServiceWithDeps(t)
	entry := &runtimeEntry{code: "live/foo", cancelObs: func() {}}
	entry.lastPacketAt.Store(time.Now().UnixNano())
	s.entries[entry.code] = entry

	assert.True(t, s.IsRuntime("live/foo"))
	assert.False(t, s.IsRuntime("live/nope"))
}

// ListRuntime returns one entry per live runtime stream with the
// template code attached.
func TestListRuntime_ReturnsAllEntries(t *testing.T) {
	s, _, _ := newServiceWithDeps(t)
	for _, code := range []domain.StreamCode{"live/a", "live/b"} {
		e := &runtimeEntry{code: code, templateCode: "p", cancelObs: func() {}}
		e.lastPacketAt.Store(time.Now().UnixNano())
		s.entries[code] = e
	}

	list := s.ListRuntime()
	assert.Len(t, list, 2)
}

// Regression test for the "delete-then-republish" bug: when the buffer
// hub for a runtime stream is torn down by ANYONE other than the idle
// reaper (operator DELETE /streams/<code> is the realistic case), the
// liveness observer must clear the entry. Otherwise ResolveOrCreate's
// fast path keeps returning the stale entry while the push registry
// has no matching slot, so every subsequent push is rejected.
func TestObserveLiveness_BufferCloseClearsEntry(t *testing.T) {
	s, _, _ := newServiceWithDeps(t)

	const code domain.StreamCode = "live/foo"
	s.buf.Create(code)
	entry := &runtimeEntry{code: code, templateCode: "profile-a"}
	entry.lastPacketAt.Store(time.Now().UnixNano())
	ctx, cancel := context.WithCancel(context.Background())
	entry.cancelObs = cancel
	s.entries[code] = entry

	done := make(chan struct{})
	go func() {
		s.observeLiveness(ctx, entry)
		close(done)
	}()

	// Simulate the operator-initiated coordinator.Stop tearing down the
	// buffer for this runtime stream — Delete closes every subscriber
	// channel, which is what the observer must detect.
	s.buf.Delete(code)

	select {
	case <-done:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("observer did not exit after buffer close")
	}

	assert.False(t, s.IsRuntime(code),
		"observer must clear the entry so the next push can re-materialise the runtime stream")
}

// Defensive: if a parallel push has already replaced the entry with a
// fresh one (same code, new struct) before the old observer's cleanup
// runs, the cleanup must NOT stomp the newer entry — otherwise we'd
// lose a live stream every time the reaper / external teardown raced
// with re-materialisation.
func TestObserveLiveness_DoesNotEvictReplacementEntry(t *testing.T) {
	s, _, _ := newServiceWithDeps(t)

	const code domain.StreamCode = "live/foo"
	original := &runtimeEntry{code: code, templateCode: "profile-a"}
	original.lastPacketAt.Store(time.Now().UnixNano())

	replacement := &runtimeEntry{code: code, templateCode: "profile-a"}
	replacement.lastPacketAt.Store(time.Now().UnixNano())
	s.entries[code] = replacement // simulate parallel re-create

	s.removeOrphanedEntry(context.Background(), original, "buffer_closed")

	assert.True(t, s.IsRuntime(code), "replacement entry must survive an old observer's cleanup")
	s.mu.RLock()
	defer s.mu.RUnlock()
	assert.Same(t, replacement, s.entries[code])
}
