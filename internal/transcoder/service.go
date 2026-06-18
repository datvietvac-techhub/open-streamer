// Package transcoder is the public surface for stream transcoding.
//
// Transcoding runs in-process via libavcodec. Service supervises one
// `open-streamer-transcoder` subprocess per transcoded stream (the `native`
// subpackage, built as cmd/open-streamer-transcoder): it streams raw
// packets to the subprocess and reads encoded packets back over a
// bidirectional gRPC `Run` stream on a Unix-domain socket. The subprocess
// decodes the source once and fans the decoded frames out to every
// rendition. Passthrough streams (no transcoder config) never start one.
//
// The subprocess owns all renditions, so there is no per-rung lifecycle: any
// ladder or transcoder-config change (audio, profiles, global, decoder)
// restarts the whole subprocess (see Coordinator.reloadTranscoderFull).
package transcoder

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/metrics"
	"github.com/samber/do/v2"
)

// Profile defines a single transcoding output rendition.
// Rendition label in logs and URLs is track_<n> from ladder order (see buffer.VideoTrackSlug).
type Profile struct {
	Width            int
	Height           int
	Bitrate          string // e.g. "4000k"
	Codec            string // e.g. "h264_nvenc", "libx264"
	Preset           string
	CodecProfile     string
	CodecLevel       string
	MaxBitrate       int
	Framerate        float64
	KeyframeInterval int
	Bframes          *int
	Refs             *int
	SAR              string
	ResizeMode       string
}

const maxTranscoderErrorHistory = 5

// ProcessStatus reports the CURRENT health of a stream's transcoder subprocess.
type ProcessStatus string

// ProcessStatus values.
const (
	ProcessStatusHealthy   ProcessStatus = "healthy"
	ProcessStatusUnhealthy ProcessStatus = "unhealthy"
)

// RenditionSnapshot identifies one output rendition (ABR rung) the subprocess
// produces. Health is reported once at the subprocess level (see RuntimeStatus),
// not per rendition, because one subprocess transcodes them all.
type RenditionSnapshot struct {
	Index int    `json:"index"`
	Track string `json:"track"`
}

// RuntimeStatus is a JSON-safe snapshot of a stream's transcoder subprocess.
// One subprocess produces every rendition, so status / restart_count / errors
// describe the subprocess as a whole; renditions lists the ABR rungs it emits.
type RuntimeStatus struct {
	Status       ProcessStatus       `json:"status"`
	RestartCount int                 `json:"restart_count"`
	Errors       []domain.ErrorEntry `json:"errors,omitempty"`
	Renditions   []RenditionSnapshot `json:"renditions"`
}

// streamWorker holds one stream's transcoder handle: the supervisor that owns
// the open-streamer-transcoder subprocess + gRPC connection. One subprocess
// transcodes every rendition, so health state (restartCount / errors) is
// per-subprocess, not per-rendition. baseCtx / baseCancel scope the subprocess
// lifetime; rawIngest / tc capture what it transcodes; done closes when the
// supervisor goroutine exits.
type streamWorker struct {
	baseCtx    context.Context //nolint:containedctx // pipelinex spawn pattern; cancel exposed via baseCancel
	baseCancel context.CancelFunc
	rawIngest  domain.StreamCode
	tc         *domain.TranscoderConfig
	supervisor *supervisor // the native subprocess + gRPC supervisor; nil only in tests
	done       chan struct{}

	mu           sync.Mutex
	renditions   []int // rendition track indices (0..N-1) the subprocess emits
	restartCount int
	errors       []domain.ErrorEntry // newest first, capped at maxTranscoderErrorHistory
}

// Service is the public transcoder entry point: it starts / stops /
// restarts per-stream transcoder subprocesses and reports their runtime
// status to the coordinator and API handlers.
type Service struct {
	buf     *buffer.Service
	bus     events.Bus
	m       *metrics.Metrics
	mu      sync.Mutex
	workers map[domain.StreamCode]*streamWorker

	onUnhealthy func(streamID domain.StreamCode, reason string)
	onHealthy   func(streamID domain.StreamCode)

	healthMu  sync.Mutex
	unhealthy map[domain.StreamCode]struct{}
}

// New creates a Service and registers it with the DI injector.
func New(i do.Injector) (*Service, error) {
	buf := do.MustInvoke[*buffer.Service](i)
	bus := do.MustInvoke[events.Bus](i)
	m := do.MustInvoke[*metrics.Metrics](i)

	return &Service{
		buf:       buf,
		bus:       bus,
		m:         m,
		workers:   make(map[domain.StreamCode]*streamWorker),
		unhealthy: make(map[domain.StreamCode]struct{}),
	}, nil
}

// RuntimeStatus returns a snapshot of the stream's transcoder subprocess.
// Returns ok=false if the stream has no transcoder pipeline running.
func (s *Service) RuntimeStatus(streamID domain.StreamCode) (RuntimeStatus, bool) {
	s.mu.Lock()
	sw, ok := s.workers[streamID]
	s.mu.Unlock()
	if !ok {
		return RuntimeStatus{}, false
	}

	s.healthMu.Lock()
	_, unhealthy := s.unhealthy[streamID]
	s.healthMu.Unlock()

	sw.mu.Lock()
	defer sw.mu.Unlock()

	status := ProcessStatusHealthy
	if unhealthy {
		status = ProcessStatusUnhealthy
	}
	out := RuntimeStatus{
		Status:       status,
		RestartCount: sw.restartCount,
		Renditions:   make([]RenditionSnapshot, 0, len(sw.renditions)),
	}
	if len(sw.errors) > 0 {
		out.Errors = make([]domain.ErrorEntry, len(sw.errors))
		copy(out.Errors, sw.errors)
	}
	for _, idx := range sw.renditions {
		out.Renditions = append(out.Renditions, RenditionSnapshot{
			Index: idx,
			Track: buffer.VideoTrackSlug(idx),
		})
	}
	return out, true
}

// recordError bumps the subprocess restart count and appends a crash entry to
// the stream's transcoder error history (newest first, capped). Called by the
// supervisor when the open-streamer-transcoder subprocess exits and respawns.
func (s *Service) recordError(streamID domain.StreamCode, msg string) {
	s.mu.Lock()
	sw, ok := s.workers[streamID]
	s.mu.Unlock()
	if !ok {
		return
	}
	// Count every supervisor respawn. Nil-guard: tests construct &Service{}
	// without a metrics handle and call recordError directly.
	if s.m != nil && s.m.TranscoderRestartsTotal != nil {
		s.m.TranscoderRestartsTotal.WithLabelValues(string(streamID)).Inc()
	}
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.restartCount++
	e := domain.ErrorEntry{Message: msg, At: time.Now()}
	if len(sw.errors) >= maxTranscoderErrorHistory {
		copy(sw.errors[1:], sw.errors[:maxTranscoderErrorHistory-1])
		sw.errors[0] = e
		return
	}
	sw.errors = append([]domain.ErrorEntry{e}, sw.errors...)
}

// SetUnhealthyCallback registers a function the Service calls the FIRST
// time a stream transitions to "transcoder unhealthy".
func (s *Service) SetUnhealthyCallback(fn func(streamID domain.StreamCode, reason string)) {
	s.mu.Lock()
	s.onUnhealthy = fn
	s.mu.Unlock()
}

// SetHealthyCallback registers a function the Service calls the FIRST
// time every previously-failing profile in a stream has recovered.
func (s *Service) SetHealthyCallback(fn func(streamID domain.StreamCode)) {
	s.mu.Lock()
	s.onHealthy = fn
	s.mu.Unlock()
}

// markUnhealthy marks the stream's transcoder unhealthy. Returns true on the
// healthy → unhealthy edge.
func (s *Service) markUnhealthy(streamID domain.StreamCode) bool {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	if _, already := s.unhealthy[streamID]; already {
		return false
	}
	s.unhealthy[streamID] = struct{}{}
	return true
}

// markHealthy clears the stream's transcoder unhealthy flag. Returns true on
// the unhealthy → healthy edge.
func (s *Service) markHealthy(streamID domain.StreamCode) bool {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	if _, present := s.unhealthy[streamID]; !present {
		return false
	}
	delete(s.unhealthy, streamID)
	return true
}

// fireUnhealthyIfTransitioned invokes onUnhealthy on the transition edge.
func (s *Service) fireUnhealthyIfTransitioned(streamID domain.StreamCode, reason string) {
	if !s.markUnhealthy(streamID) {
		return
	}
	s.mu.Lock()
	cb := s.onUnhealthy
	s.mu.Unlock()
	if cb != nil {
		cb(streamID, reason)
	}
}

// fireHealthyIfTransitioned mirrors fireUnhealthyIfTransitioned for recovery.
func (s *Service) fireHealthyIfTransitioned(streamID domain.StreamCode) {
	if !s.markHealthy(streamID) {
		return
	}
	s.mu.Lock()
	cb := s.onHealthy
	s.mu.Unlock()
	if cb != nil {
		cb(streamID)
	}
}

// dropHealthState clears the stream's transcoder unhealthy flag.
func (s *Service) dropHealthState(streamID domain.StreamCode) {
	s.healthMu.Lock()
	_, hadEntry := s.unhealthy[streamID]
	delete(s.unhealthy, streamID)
	s.healthMu.Unlock()
	if !hadEntry {
		return
	}
	s.mu.Lock()
	cb := s.onHealthy
	s.mu.Unlock()
	if cb != nil {
		cb(streamID)
	}
}

// Start launches the native transcoder subprocess for a stream and
// wires it to the raw-ingest + per-rendition buffer-hub entries.
//
// Resolves the open-streamer-transcoder binary, spawns it on a unique
// Unix-domain socket, opens the gRPC stream, forwards Configure +
// raw-ingest bytes, and pipes encoded output back into the rendition
// buffers. The supervisor goroutine survives subprocess crashes by
// respawning with exponential backoff, so a subprocess crash never tears
// down the stream.
//
// Returns an error when the subprocess binary isn't reachable so the
// coordinator unwinds buffers / publisher / manager entries cleanly
// rather than starting a stream whose transcoder will never wake.
func (s *Service) Start(
	ctx context.Context,
	logStreamCode domain.StreamCode,
	rawIngestID domain.StreamCode,
	tc *domain.TranscoderConfig,
	targets []RenditionTarget,
) error {
	if len(targets) == 0 {
		return fmt.Errorf("transcoder: no rendition targets for stream %s", logStreamCode)
	}
	// Probe the binary path up-front so the caller sees the failure on
	// Start instead of as a respawn loop seconds later.
	if _, err := s.resolveBinaryPath(); err != nil {
		return fmt.Errorf("transcoder: %w", err)
	}

	s.mu.Lock()
	if _, ok := s.workers[logStreamCode]; ok {
		s.mu.Unlock()
		return fmt.Errorf("transcoder: stream %s already running", logStreamCode)
	}
	baseCtx, baseCancel := context.WithCancel(ctx)
	sw := &streamWorker{
		baseCtx:    baseCtx,
		baseCancel: baseCancel,
		rawIngest:  rawIngestID,
		tc:         tc,
		done:       make(chan struct{}),
		renditions: make([]int, len(targets)),
	}
	// One subprocess produces every rendition; renditions[i] = i records the
	// ladder so RuntimeStatus can list the track slugs (track_0, track_1, ...).
	for i := range targets {
		sw.renditions[i] = i
	}
	supervisor := newSupervisor(s, logStreamCode, rawIngestID, tc, targets)
	sw.supervisor = supervisor
	s.workers[logStreamCode] = sw
	s.mu.Unlock()

	slog.Info("transcoder: stream job started",
		"stream_code", logStreamCode,
		"profiles", len(targets),
		"read_from", rawIngestID,
		"backend", string(tc.Global.HW),
	)
	// One subprocess transcodes every rendition, so "workers" is the subprocess
	// count (1 while up), not the rendition count — that is QualitiesActive.
	s.m.TranscoderWorkersActive.WithLabelValues(string(logStreamCode)).Set(1)
	s.m.TranscoderQualitiesActive.WithLabelValues(string(logStreamCode)).Set(float64(len(targets)))
	//nolint:contextcheck // event bus consumers must outlive baseCtx
	s.bus.Publish(context.Background(), domain.Event{
		Type:       domain.EventTranscoderStarted,
		StreamCode: logStreamCode,
		Payload: map[string]any{
			"profiles":      len(targets),
			"raw_ingest_id": string(rawIngestID),
			"backend":       string(tc.Global.HW),
		},
	})

	go func() {
		defer close(sw.done)
		supervisor.Run(baseCtx)
	}()
	return nil
}

// NotifyInputSwitch tells the active supervisor (if any) that the
// upstream ingestor has switched to a new raw ingest buffer ID. Called
// by the Manager from its switch handler; the supervisor forwards the
// hint as a Switch message over gRPC so the subprocess swaps its
// decoder before the new source's first packet arrives.
//
// No-op when the stream has no transcoder running.
func (s *Service) NotifyInputSwitch(streamID, newRawIngestID domain.StreamCode) {
	s.mu.Lock()
	sw, ok := s.workers[streamID]
	s.mu.Unlock()
	if !ok || sw.supervisor == nil {
		return
	}
	sw.supervisor.NotifySwitchInput(newRawIngestID)
}

// resolveBinaryPath locates the open-streamer-transcoder binary. Looks:
//  1. Next to the running open-streamer binary (production install
//     ships them together via install.sh).
//  2. In $PATH as a fallback for local dev where the dev runs
//     `go run` from the repo root.
//
// Returns an error with both attempted paths in the message when
// neither yields an executable.
func (s *Service) resolveBinaryPath() (string, error) {
	var attempts []string
	if exe, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(exe), transcoderBinaryName)
		attempts = append(attempts, candidate)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	if found, err := exec.LookPath(transcoderBinaryName); err == nil {
		return found, nil
	}
	attempts = append(attempts, "$PATH lookup")
	return "", fmt.Errorf("%s binary not found (tried: %s); run `make build-all` and ensure both binaries are installed together",
		transcoderBinaryName, strings.Join(attempts, ", "))
}

// Stop cancels the transcoder pipeline for a stream.
func (s *Service) Stop(streamID domain.StreamCode) {
	s.mu.Lock()
	sw, ok := s.workers[streamID]
	if !ok {
		s.mu.Unlock()
		return
	}
	delete(s.workers, streamID)
	s.mu.Unlock()

	sw.baseCancel()
	<-sw.done // wait for the supervisor goroutine (subprocess) to exit

	s.m.TranscoderWorkersActive.WithLabelValues(string(streamID)).Set(0)
	s.m.TranscoderQualitiesActive.WithLabelValues(string(streamID)).Set(0)
	s.dropHealthState(streamID)
	//nolint:contextcheck // baseCtx is cancelled; publish must outlive it
	s.bus.Publish(context.Background(), domain.Event{
		Type:       domain.EventTranscoderStopped,
		StreamCode: streamID,
	})
}
