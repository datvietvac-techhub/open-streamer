package hooks

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
)

// fakeHookRepo is an in-memory hook store for tests.
type fakeHookRepo struct {
	mu      sync.Mutex
	hooks   map[domain.HookID]*domain.Hook
	listFn  func(ctx context.Context) ([]*domain.Hook, error)
	listErr error
}

func newFakeHookRepo() *fakeHookRepo {
	return &fakeHookRepo{hooks: make(map[domain.HookID]*domain.Hook)}
}

func (r *fakeHookRepo) Save(_ context.Context, h *domain.Hook) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hooks[h.ID] = h
	return nil
}

func (r *fakeHookRepo) FindByID(_ context.Context, id domain.HookID) (*domain.Hook, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.hooks[id]; ok {
		return h, nil
	}
	return nil, errors.New("not found")
}

func (r *fakeHookRepo) List(ctx context.Context) ([]*domain.Hook, error) {
	if r.listFn != nil {
		return r.listFn(ctx)
	}
	if r.listErr != nil {
		return nil, r.listErr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*domain.Hook, 0, len(r.hooks))
	for _, h := range r.hooks {
		out = append(out, h)
	}
	return out, nil
}

func (r *fakeHookRepo) Delete(_ context.Context, id domain.HookID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.hooks, id)
	return nil
}

func newSvc(repo *fakeHookRepo) *Service {
	return &Service{
		cfg:       config.HooksConfig{},
		hookRepo:  repo,
		bus:       events.New(1, 16),
		client:    &http.Client{Timeout: 5 * time.Second},
		batchers:  make(map[domain.HookID]*httpBatcher),
		fileLocks: make(map[string]*sync.Mutex),
	}
}

// fastBatchHook returns an HTTP hook tuned for tests: batch size 1 so
// every enqueue immediately triggers a flush, plus a short flush interval
// for the timer-driven test.
func fastBatchHook(target string) *domain.Hook {
	return &domain.Hook{
		ID:                    "h",
		Type:                  domain.HookTypeHTTP,
		Target:                target,
		BatchMaxItems:         1,
		BatchFlushIntervalSec: 1,
		BatchMaxQueueItems:    100,
		MaxRetries:            0, // tests want crisp pass/fail without retry padding
	}
}

func TestBatcherFlushOnSizeTrigger(t *testing.T) {
	var got []domain.Event
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var batch []domain.Event
		if err := json.Unmarshal(body, &batch); err != nil {
			t.Errorf("server got non-array body: %s", body)
		}
		mu.Lock()
		got = append(got, batch...)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	hook := &domain.Hook{
		ID:                    "h",
		Type:                  domain.HookTypeHTTP,
		Target:                srv.URL,
		BatchMaxItems:         3, // ship after every 3rd event
		BatchFlushIntervalSec: 60,
		BatchMaxQueueItems:    100,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := mergeBatchConfig(hook, batchGlobalDefaults{})
	b := newHTTPBatcher(ctx, cfg, &http.Client{Timeout: 2 * time.Second}, batchMetrics{})
	defer b.stop()

	for i := 0; i < 3; i++ {
		b.enqueue(domain.Event{ID: "e" + string(rune('0'+i)), Type: domain.EventStreamStarted})
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == 3 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected 3 events delivered, got %d", len(got))
}

func TestBatcherFlushOnTimerWhenBelowSize(t *testing.T) {
	var deliveredBatches atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		deliveredBatches.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := mergeBatchConfig(fastBatchHook(srv.URL), batchGlobalDefaults{})
	cfg.maxItems = 1000 // stays below cap
	b := newHTTPBatcher(ctx, cfg, &http.Client{Timeout: 2 * time.Second}, batchMetrics{})
	defer b.stop()

	// One event, well below batch cap. Timer (~1s) should ship it anyway.
	b.enqueue(domain.Event{ID: "e1", Type: domain.EventStreamStarted})

	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if deliveredBatches.Load() >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timer did not flush within 2.5s")
}

func TestBatcherSendsArrayBodyWithHMACAndBatchSize(t *testing.T) {
	var (
		mu            sync.Mutex
		capturedBody  []byte
		capturedSig   string
		capturedBatch string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		capturedBody = body
		capturedSig = r.Header.Get("X-OpenStreamer-Signature")
		capturedBatch = r.Header.Get("X-OpenStreamer-Batch-Size")
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	hook := fastBatchHook(srv.URL)
	hook.Secret = "topsecret"
	hook.BatchMaxItems = 2
	cfg := mergeBatchConfig(hook, batchGlobalDefaults{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := newHTTPBatcher(ctx, cfg, &http.Client{Timeout: 2 * time.Second}, batchMetrics{})
	defer b.stop()

	b.enqueue(domain.Event{ID: "a", Type: domain.EventStreamCreated})
	b.enqueue(domain.Event{ID: "b", Type: domain.EventStreamStarted})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := len(capturedBody)
		mu.Unlock()
		if got > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	body := capturedBody
	sig := capturedSig
	batchHeader := capturedBatch
	mu.Unlock()

	if len(body) == 0 {
		t.Fatal("server received no body")
	}

	var batch []domain.Event
	if err := json.Unmarshal(body, &batch); err != nil {
		t.Fatalf("body not JSON array: %v\n%s", err, body)
	}
	if len(batch) != 2 || batch[0].ID != "a" || batch[1].ID != "b" {
		t.Errorf("batch contents = %+v", batch)
	}

	if batchHeader != "2" {
		t.Errorf("X-OpenStreamer-Batch-Size = %q, want 2", batchHeader)
	}

	mac := hmac.New(sha256.New, []byte("topsecret"))
	mac.Write(body)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if sig != want {
		t.Errorf("sig=%s want %s", sig, want)
	}
}

func TestBatcherRequeuesFailedBatch(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// First attempt fails, second succeeds.
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	hook := fastBatchHook(srv.URL)
	hook.BatchMaxItems = 2
	hook.BatchFlushIntervalSec = 1
	cfg := mergeBatchConfig(hook, batchGlobalDefaults{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := newHTTPBatcher(ctx, cfg, &http.Client{Timeout: 2 * time.Second}, batchMetrics{})
	defer b.stop()

	b.enqueue(domain.Event{ID: "x", Type: domain.EventStreamStarted})
	b.enqueue(domain.Event{ID: "y", Type: domain.EventStreamStarted})

	// First flush fails; events re-queue. Next tick (≤ 1s later) re-attempts.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if attempts.Load() >= 2 && b.queueLen() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected 2 attempts and empty queue; attempts=%d queueLen=%d", attempts.Load(), b.queueLen())
}

func TestBatcherDropsOldestOnQueueOverflow(t *testing.T) {
	// Use a deliberately broken target so deliver always fails — we want
	// the queue to fill up.
	cfg := batchConfig{
		maxItems:      5,
		flushInterval: 50 * time.Millisecond,
		maxQueue:      3,
		maxRetries:    0,
		timeout:       100 * time.Millisecond,
		target:        "http://127.0.0.1:1/dead", // refused
		hookID:        "overflow-test",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := newHTTPBatcher(ctx, cfg, &http.Client{Timeout: 200 * time.Millisecond}, batchMetrics{})
	defer b.stop()

	for i := 0; i < 10; i++ {
		b.enqueue(domain.Event{ID: string(rune('0' + i)), Type: domain.EventStreamStarted})
	}

	// Some flushes have happened by now and re-queued failures; the queue
	// should never exceed maxQueue.
	time.Sleep(300 * time.Millisecond)
	if got := b.queueLen(); got > cfg.maxQueue {
		t.Errorf("queueLen=%d exceeded maxQueue=%d", got, cfg.maxQueue)
	}
}

func TestBatcherStopDrainsRemaining(t *testing.T) {
	var deliveredBatches atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		deliveredBatches.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	hook := fastBatchHook(srv.URL)
	hook.BatchMaxItems = 100        // never trigger size-flush
	hook.BatchFlushIntervalSec = 60 // never trigger timer-flush in test
	cfg := mergeBatchConfig(hook, batchGlobalDefaults{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := newHTTPBatcher(ctx, cfg, &http.Client{Timeout: 2 * time.Second}, batchMetrics{})

	for i := 0; i < 3; i++ {
		b.enqueue(domain.Event{ID: string(rune('a' + i)), Type: domain.EventStreamStarted})
	}

	// stop() must drain the queue before returning.
	b.stop()
	if got := deliveredBatches.Load(); got < 1 {
		t.Errorf("expected drain to deliver pending batch, got 0")
	}
}

func TestServiceDispatchRoutesHTTPThroughBatcher(t *testing.T) {
	var bodies [][]byte
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	repo := newFakeHookRepo()
	hook := fastBatchHook(srv.URL)
	hook.Enabled = true
	repo.hooks[hook.ID] = hook

	svc := newSvc(repo)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.startCtx = ctx

	for i := 0; i < 2; i++ {
		_ = svc.dispatch(ctx, domain.Event{
			ID:   string(rune('e' + i)),
			Type: domain.EventStreamCreated,
		})
	}

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(bodies)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) < 2 {
		t.Fatalf("got %d batches, want ≥ 2", len(bodies))
	}
	for i, body := range bodies {
		var batch []domain.Event
		if err := json.Unmarshal(body, &batch); err != nil {
			t.Errorf("batch %d not JSON array: %s", i, body)
		}
	}
}

func TestServiceDispatchRoutesFileSynchronously(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "events.log")

	repo := newFakeHookRepo()
	hook := &domain.Hook{
		ID:      "f",
		Type:    domain.HookTypeFile,
		Target:  target,
		Enabled: true,
	}
	repo.hooks[hook.ID] = hook

	svc := newSvc(repo)

	if err := svc.dispatch(context.Background(), domain.Event{
		ID: "e1", Type: domain.EventStreamStarted,
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	// File hook delivers synchronously — no flush window to wait for.
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		t.Errorf("expected one JSON line ending with newline, got %q", data)
	}
}

func TestDeliverFileAppendsJSONLines(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "events.log")

	svc := newSvc(newFakeHookRepo())
	hook := &domain.Hook{ID: "h", Type: domain.HookTypeFile, Target: target}

	for i := 0; i < 3; i++ {
		ev := domain.Event{
			ID:         "e" + string(rune('1'+i)),
			Type:       domain.EventStreamStarted,
			StreamCode: "live",
		}
		if err := svc.deliverFile(hook, ev); err != nil {
			t.Fatalf("deliver %d: %v", i, err)
		}
	}

	f, err := os.Open(target)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		var ev domain.Event
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			t.Errorf("line %d not JSON: %v\n%s", count, err, scanner.Text())
		}
		count++
	}
	if count != 3 {
		t.Errorf("got %d lines, want 3", count)
	}
}

func TestDeliverFileRequiresAbsolutePath(t *testing.T) {
	svc := newSvc(newFakeHookRepo())
	hook := &domain.Hook{ID: "h", Type: domain.HookTypeFile, Target: "relative/events.log"}
	err := svc.deliverFile(hook, domain.Event{Type: domain.EventStreamStarted})
	if err == nil {
		t.Fatal("expected error for relative path")
	}
}

func TestDeliverFileEmptyTargetFails(t *testing.T) {
	svc := newSvc(newFakeHookRepo())
	hook := &domain.Hook{ID: "h", Type: domain.HookTypeFile, Target: ""}
	if err := svc.deliverFile(hook, domain.Event{}); err == nil {
		t.Fatal("expected error for empty target")
	}
}

func TestDeliverFileConcurrentSerialised(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "concurrent.log")

	svc := newSvc(newFakeHookRepo())
	hook := &domain.Hook{ID: "h", Type: domain.HookTypeFile, Target: target}

	const writers, perWriter = 8, 50
	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				ev := domain.Event{
					ID:   "w-" + string(rune('A'+w)) + "-" + string(rune('0'+i%10)),
					Type: domain.EventStreamStarted,
				}
				_ = svc.deliverFile(hook, ev)
			}
		}(w)
	}
	wg.Wait()

	// Every line must be valid JSON — interleaving would corrupt at least one.
	f, err := os.Open(target)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		var ev domain.Event
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			t.Errorf("corrupt line %d: %v\n%s", count, err, scanner.Text())
		}
		count++
	}
	if count != writers*perWriter {
		t.Errorf("got %d lines, want %d", count, writers*perWriter)
	}
}

func TestMergeBatchConfigLayering(t *testing.T) {
	defaults := batchGlobalDefaults{
		maxItems:         50,
		flushIntervalSec: 7,
		maxQueueItems:    1000,
	}

	t.Run("hook overrides global", func(t *testing.T) {
		got := mergeBatchConfig(&domain.Hook{
			BatchMaxItems:         42,
			BatchFlushIntervalSec: 3,
			BatchMaxQueueItems:    99,
		}, defaults)
		if got.maxItems != 42 || got.flushInterval != 3*time.Second || got.maxQueue != 99 {
			t.Errorf("hook override not applied: %+v", got)
		}
	})

	t.Run("global wins when hook unset", func(t *testing.T) {
		got := mergeBatchConfig(&domain.Hook{}, defaults)
		if got.maxItems != defaults.maxItems ||
			got.flushInterval != time.Duration(defaults.flushIntervalSec)*time.Second ||
			got.maxQueue != defaults.maxQueueItems {
			t.Errorf("global default not used: %+v", got)
		}
	})

	t.Run("code default wins when both unset", func(t *testing.T) {
		got := mergeBatchConfig(&domain.Hook{}, batchGlobalDefaults{})
		if got.maxItems != domain.DefaultHookBatchMaxItems ||
			got.flushInterval != time.Duration(domain.DefaultHookBatchFlushIntervalSec)*time.Second ||
			got.maxQueue != domain.DefaultHookBatchMaxQueueItems {
			t.Errorf("code default not used: %+v", got)
		}
	})
}

func TestMatchesEventTypeFilter(t *testing.T) {
	svc := newSvc(newFakeHookRepo())
	h := &domain.Hook{EventTypes: []domain.EventType{domain.EventStreamStarted}}

	if !svc.matches(h, domain.Event{Type: domain.EventStreamStarted}) {
		t.Error("matching event type should pass")
	}
	if svc.matches(h, domain.Event{Type: domain.EventStreamStopped}) {
		t.Error("non-matching event type should fail")
	}
}

func TestMatchesEmptyEventTypesAllowsAll(t *testing.T) {
	svc := newSvc(newFakeHookRepo())
	h := &domain.Hook{}
	if !svc.matches(h, domain.Event{Type: domain.EventStreamStarted}) {
		t.Error("empty EventTypes should match everything")
	}
}

func TestMatchesStreamCodeFilter(t *testing.T) {
	svc := newSvc(newFakeHookRepo())

	t.Run("only", func(t *testing.T) {
		h := &domain.Hook{
			StreamCodes: &domain.StreamCodeFilter{
				Only: []domain.StreamCode{"s1", "s2"},
			},
		}
		if !svc.matches(h, domain.Event{StreamCode: "s1"}) {
			t.Error("s1 should match only=[s1,s2]")
		}
		if svc.matches(h, domain.Event{StreamCode: "s9"}) {
			t.Error("s9 should NOT match only=[s1,s2]")
		}
	})

	t.Run("except", func(t *testing.T) {
		h := &domain.Hook{
			StreamCodes: &domain.StreamCodeFilter{
				Except: []domain.StreamCode{"bad"},
			},
		}
		if !svc.matches(h, domain.Event{StreamCode: "good"}) {
			t.Error("good should match except=[bad]")
		}
		if svc.matches(h, domain.Event{StreamCode: "bad"}) {
			t.Error("bad should NOT match except=[bad]")
		}
	})
}

func TestWithMetadataPrefixesKeys(t *testing.T) {
	ev := domain.Event{
		Type:    domain.EventStreamStarted,
		Payload: map[string]any{"original": 1},
	}
	out := withMetadata(ev, map[string]string{"env": "prod", "region": "ap-1"})
	if out.Payload["original"] != 1 {
		t.Errorf("original payload lost")
	}
	if out.Payload["meta.env"] != "prod" {
		t.Errorf("meta.env not injected: %+v", out.Payload)
	}
	if out.Payload["meta.region"] != "ap-1" {
		t.Errorf("meta.region not injected: %+v", out.Payload)
	}
}
