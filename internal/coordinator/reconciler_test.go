package coordinator

// reconciler_test.go — guards the "every enabled stream must be running"
// invariant. Without these tests it's easy to regress the reconciler into
// either over-eagerly starting disabled streams (operator's intent ignored)
// or under-eagerly leaving stopped-but-should-run streams idle (the bug
// users report as "stream stuck at Stopped after create").

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

// fakeStreamRepo serves a pre-loaded list to reconcileOnce. Save / Delete /
// FindByCode are stubbed because the reconciler only calls List.
type fakeStreamRepo struct {
	mu        sync.Mutex
	streams   []*domain.Stream
	listCalls int
	listErr   error
}

func (r *fakeStreamRepo) List(_ context.Context, _ store.StreamFilter) ([]*domain.Stream, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listCalls++
	if r.listErr != nil {
		return nil, r.listErr
	}
	out := make([]*domain.Stream, len(r.streams))
	copy(out, r.streams)
	return out, nil
}

func (r *fakeStreamRepo) Save(_ context.Context, _ *domain.Stream) error {
	return nil
}

func (r *fakeStreamRepo) FindByCode(_ context.Context, _ domain.StreamCode) (*domain.Stream, error) {
	return nil, errors.New("not implemented")
}

func (r *fakeStreamRepo) Delete(_ context.Context, _ domain.StreamCode) error {
	return nil
}

// runnableStream returns a minimal Stream that the coordinator's Start path
// will accept (one input, not disabled). Avoids transcoder / push fields so
// the spy services don't need to handle every code path.
func runnableStream(code string) *domain.Stream {
	return &domain.Stream{
		Code:     domain.StreamCode(code),
		Disabled: false,
		Inputs: []domain.Input{
			{URL: "udp://example.invalid:1234", Priority: 0},
		},
	}
}

// reconcileOnce must Start every enabled stream that the coordinator is not
// already running. Streams already running (or disabled, or with no inputs)
// must be left untouched.
func TestReconcileOnceStartsStoppedEnabledStreams(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	defer h.coord.Stop(context.Background(), "running")

	// Pretend "running" is already up; it must not be Start'd again.
	require.NoError(t, h.coord.Start(context.Background(), runnableStream("running")))
	startedBefore := len(h.pub.started)

	repo := &fakeStreamRepo{streams: []*domain.Stream{
		runnableStream("running"),
		runnableStream("stopped"),
		{
			Code:     "disabled",
			Disabled: true,
			Inputs:   []domain.Input{{URL: "udp://x:1", Priority: 0}},
		},
		{
			Code:   "no-inputs",
			Inputs: nil,
		},
	}}
	h.coord.streamRepo = repo

	h.coord.reconcileOnce(context.Background())

	assert.Equal(t, 1, repo.listCalls, "reconcile must call List exactly once per pass")
	assert.True(t, h.coord.IsRunning("running"), "already-running stream stays running")
	assert.True(t, h.coord.IsRunning("stopped"), "stopped enabled stream gets started")
	assert.False(t, h.coord.IsRunning("disabled"), "disabled stream must not be started")
	assert.False(t, h.coord.IsRunning("no-inputs"), "stream with no inputs must not be started")

	// pub.Start fires once per stream Coordinator.Start brought up. We expect
	// exactly one new call (for "stopped") on top of the pre-existing
	// "running" startup.
	assert.Len(t, h.pub.started, startedBefore+1,
		"pub.Start must be called exactly once for the stopped enabled stream")
}

// reconcileOnce must be idempotent — calling it twice in a row when nothing
// has changed must not double-start anything (regression guard against
// IsRunning checks being bypassed).
func TestReconcileOnceIsIdempotent(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	defer h.coord.Stop(context.Background(), "s1")

	repo := &fakeStreamRepo{streams: []*domain.Stream{runnableStream("s1")}}
	h.coord.streamRepo = repo

	h.coord.reconcileOnce(context.Background())
	startsAfterFirst := len(h.pub.started)

	h.coord.reconcileOnce(context.Background())
	assert.Equal(t, startsAfterFirst, len(h.pub.started),
		"second reconcile pass must not re-Start the already-running stream")
}

// reconcileOnce must survive a repo error without panicking and without
// trying to start anything. The next tick will retry.
func TestReconcileOnceRepoErrorIsNonFatal(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	repo := &fakeStreamRepo{listErr: errors.New("repo blew up")}
	h.coord.streamRepo = repo

	require.NotPanics(t, func() {
		h.coord.reconcileOnce(context.Background())
	})
	assert.Empty(t, h.pub.started, "repo error must not start anything")
}

// reconcileOnce must skip nil entries — defensive guard against a repo
// returning a sparse slice (some YAML-store backends do this on parse
// errors).
func TestReconcileOnceSkipsNilStreams(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	defer h.coord.Stop(context.Background(), "ok")

	repo := &fakeStreamRepo{streams: []*domain.Stream{
		nil,
		runnableStream("ok"),
		nil,
	}}
	h.coord.streamRepo = repo

	require.NotPanics(t, func() {
		h.coord.reconcileOnce(context.Background())
	})
	assert.True(t, h.coord.IsRunning("ok"))
}

// fakeTemplateRepoRec is a minimal TemplateRepository for the template-
// inheritance reconcile test (B-5).
type fakeTemplateRepoRec struct{ tpl *domain.Template }

func (r *fakeTemplateRepoRec) Save(context.Context, *domain.Template) error { return nil }
func (r *fakeTemplateRepoRec) FindByCode(context.Context, domain.TemplateCode) (*domain.Template, error) {
	if r.tpl != nil {
		return r.tpl, nil
	}
	return nil, errors.New("template not found")
}
func (r *fakeTemplateRepoRec) List(context.Context) ([]*domain.Template, error)  { return nil, nil }
func (r *fakeTemplateRepoRec) Delete(context.Context, domain.TemplateCode) error { return nil }

// B-5: a stream that inherits its Inputs from a template (none on the raw
// record) must still be started by the reconciler. Before the fix the raw
// len(Inputs)==0 gate ran before resolveTemplate, so such a stream was
// skipped forever.
func TestReconcileOnceStartsTemplateInheritedInputs(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	tplCode := domain.TemplateCode("tpl-inherit")
	h.coord.templateRepo = &fakeTemplateRepoRec{tpl: &domain.Template{
		Inputs: []domain.Input{{URL: "udp://example.invalid:1234", Priority: 0}},
	}}
	h.coord.streamRepo = &fakeStreamRepo{streams: []*domain.Stream{
		{Code: "inherits", Template: &tplCode}, // no own Inputs
	}}
	defer h.coord.Stop(context.Background(), "inherits")

	h.coord.reconcileOnce(context.Background())

	assert.True(t, h.coord.IsRunning("inherits"),
		"stream inheriting Inputs from a template must be started (B-5)")
}

// D-2: a non-transcoded raw-TS source (UDP/HLS-pull/HTTP-TS/SRT/file) writes TS
// chunks the blob writer can't ingest — recording it produced ZERO bytes while
// status said "recording". blobProfiles must refuse it (no lane) so no catalog
// or Recording row is created. A non-transcoded AV source still records.
func TestBlobProfiles_RefusesRawTSSource(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	raw := &domain.Stream{Code: "udp-dvr", DVR: &domain.StreamDVRConfig{Enabled: true}, Inputs: []domain.Input{{URL: "udp://239.0.0.1:1234"}}}
	require.Nil(t, h.coord.blobProfiles(raw), "raw-TS source must not get a DVR lane (D-2)")

	av := &domain.Stream{Code: "rtmp-dvr", DVR: &domain.StreamDVRConfig{Enabled: true}, Inputs: []domain.Input{{URL: "rtmp://host/app/key"}}}
	require.NotEmpty(t, h.coord.blobProfiles(av), "non-transcoded AV source records via the p0 lane")
}
