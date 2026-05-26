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
// The subprocess owns all renditions, so there is no per-rung lifecycle:
// StartProfile returns ErrNotImplemented and a ladder change restarts the
// whole subprocess (see Coordinator.reloadTranscoderFull).
package transcoder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ntt0601zcoder/open-streamer/internal/buffer"
	"github.com/ntt0601zcoder/open-streamer/internal/domain"
	"github.com/ntt0601zcoder/open-streamer/internal/events"
	"github.com/ntt0601zcoder/open-streamer/internal/metrics"
	"github.com/samber/do/v2"
)

// ErrNotImplemented is returned by StartProfile: the subprocess produces
// every rendition from a single decode, so one rung cannot be started or
// stopped on its own — callers restart the whole subprocess instead.
var ErrNotImplemented = errors.New("transcoder: per-profile start is not supported; the subprocess owns all renditions")

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

// profileWorker tracks a single rendition's runtime state. RuntimeStatus
// surfaces RestartCount + Errors per rendition to the UI; index 0 carries
// the live subprocess state, the rest are bookkeeping shells.
type profileWorker struct {
	cancel       context.CancelFunc
	done         chan struct{}
	restartCount int
	errors       []domain.ErrorEntry
}

const maxProfileErrorHistory = 5

// recordProfileErrorEntry prepends an entry, capped at maxProfileErrorHistory.
// Caller must hold the parent streamWorker.mu.
func recordProfileErrorEntry(pw *profileWorker, msg string, at time.Time) {
	e := domain.ErrorEntry{Message: msg, At: at}
	if len(pw.errors) >= maxProfileErrorHistory {
		copy(pw.errors[1:], pw.errors[:maxProfileErrorHistory-1])
		pw.errors[0] = e
		return
	}
	pw.errors = append([]domain.ErrorEntry{e}, pw.errors...)
}

// ProfileStatus reports the CURRENT health of one profile encoder.
type ProfileStatus string

// ProfileStatus values.
const (
	ProfileStatusHealthy   ProfileStatus = "healthy"
	ProfileStatusUnhealthy ProfileStatus = "unhealthy"
)

// ProfileSnapshot is a serialisable copy of one profile encoder's state.
type ProfileSnapshot struct {
	Index        int                 `json:"index"`
	Track        string              `json:"track"`
	Status       ProfileStatus       `json:"status"`
	RestartCount int                 `json:"restart_count"`
	Errors       []domain.ErrorEntry `json:"errors,omitempty"`
}

// RuntimeStatus is a JSON-safe snapshot of transcoder state for one stream.
type RuntimeStatus struct {
	Profiles []ProfileSnapshot `json:"profiles"`
}

// streamWorker holds one stream's transcoder handle: the supervisor that
// owns the open-streamer-transcoder subprocess + gRPC connection, plus a
// profileWorker per rendition for RuntimeStatus. baseCtx / baseCancel
// scope the subprocess lifetime; rawIngest / tc capture what it transcodes.
type streamWorker struct {
	baseCtx    context.Context //nolint:containedctx // pipelinex spawn pattern; cancel exposed via baseCancel
	baseCancel context.CancelFunc
	rawIngest  domain.StreamCode
	tc         *domain.TranscoderConfig
	supervisor *supervisor // the native subprocess + gRPC supervisor; nil only in tests
	mu         sync.Mutex
	profiles   map[int]*profileWorker
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

	healthMu          sync.Mutex
	unhealthyProfiles map[domain.StreamCode]map[int]struct{}
}

// New creates a Service and registers it with the DI injector.
func New(i do.Injector) (*Service, error) {
	buf := do.MustInvoke[*buffer.Service](i)
	bus := do.MustInvoke[events.Bus](i)
	m := do.MustInvoke[*metrics.Metrics](i)

	return &Service{
		buf:               buf,
		bus:               bus,
		m:                 m,
		workers:           make(map[domain.StreamCode]*streamWorker),
		unhealthyProfiles: make(map[domain.StreamCode]map[int]struct{}),
	}, nil
}

// RuntimeStatus returns a snapshot of per-profile encoder state.
// Returns ok=false if the stream has no transcoder pipeline running.
func (s *Service) RuntimeStatus(streamID domain.StreamCode) (RuntimeStatus, bool) {
	s.mu.Lock()
	sw, ok := s.workers[streamID]
	s.mu.Unlock()
	if !ok {
		return RuntimeStatus{}, false
	}

	s.healthMu.Lock()
	unhealthy := make(map[int]struct{}, len(s.unhealthyProfiles[streamID]))
	for idx := range s.unhealthyProfiles[streamID] {
		unhealthy[idx] = struct{}{}
	}
	s.healthMu.Unlock()

	sw.mu.Lock()
	defer sw.mu.Unlock()

	out := RuntimeStatus{Profiles: make([]ProfileSnapshot, 0, len(sw.profiles))}
	for idx, pw := range sw.profiles {
		status := ProfileStatusHealthy
		if _, bad := unhealthy[idx]; bad {
			status = ProfileStatusUnhealthy
		}
		snap := ProfileSnapshot{
			Index:        idx,
			Track:        buffer.VideoTrackSlug(idx),
			Status:       status,
			RestartCount: pw.restartCount,
		}
		if len(pw.errors) > 0 {
			snap.Errors = make([]domain.ErrorEntry, len(pw.errors))
			copy(snap.Errors, pw.errors)
		}
		out.Profiles = append(out.Profiles, snap)
	}
	sort.Slice(out.Profiles, func(i, j int) bool { return out.Profiles[i].Index < out.Profiles[j].Index })
	return out, true
}

// recordProfileError appends a crash entry to the profile's history.
// Called by the native runner supervisor when an encoder pipeline restarts.
//
//nolint:unparam // tests pass profileIndex=0; native runner (P1) will iterate per target.
func (s *Service) recordProfileError(streamID domain.StreamCode, profileIndex int, msg string) {
	s.mu.Lock()
	sw, ok := s.workers[streamID]
	s.mu.Unlock()
	if !ok {
		return
	}
	sw.mu.Lock()
	defer sw.mu.Unlock()
	pw, ok := sw.profiles[profileIndex]
	if !ok {
		return
	}
	pw.restartCount++
	recordProfileErrorEntry(pw, msg, time.Now())
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

// markProfileUnhealthy adds (streamID, profileIndex) to the unhealthy set.
// Returns true on the healthy → unhealthy edge.
func (s *Service) markProfileUnhealthy(streamID domain.StreamCode, profileIndex int) bool {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	set, ok := s.unhealthyProfiles[streamID]
	if !ok {
		set = make(map[int]struct{})
		s.unhealthyProfiles[streamID] = set
	}
	wasEmpty := len(set) == 0
	if _, already := set[profileIndex]; already {
		return false
	}
	set[profileIndex] = struct{}{}
	return wasEmpty
}

// markProfileHealthy removes (streamID, profileIndex) from the unhealthy set.
// Returns true on the unhealthy → healthy edge.
func (s *Service) markProfileHealthy(streamID domain.StreamCode, profileIndex int) bool {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	set, ok := s.unhealthyProfiles[streamID]
	if !ok {
		return false
	}
	if _, present := set[profileIndex]; !present {
		return false
	}
	delete(set, profileIndex)
	if len(set) == 0 {
		delete(s.unhealthyProfiles, streamID)
		return true
	}
	return false
}

// fireUnhealthyIfTransitioned invokes onUnhealthy on the transition edge.
func (s *Service) fireUnhealthyIfTransitioned(streamID domain.StreamCode, profileIndex int, reason string) {
	if !s.markProfileUnhealthy(streamID, profileIndex) {
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
//
//nolint:unparam // profileIndex=0 today (single-encoder supervisor); per-profile granularity returns in P5+ multi-rendition phase.
func (s *Service) fireHealthyIfTransitioned(streamID domain.StreamCode, profileIndex int) {
	if !s.markProfileHealthy(streamID, profileIndex) {
		return
	}
	s.mu.Lock()
	cb := s.onHealthy
	s.mu.Unlock()
	if cb != nil {
		cb(streamID)
	}
}

// dropHealthState clears every profile entry for a stream.
func (s *Service) dropHealthState(streamID domain.StreamCode) {
	s.healthMu.Lock()
	_, hadEntries := s.unhealthyProfiles[streamID]
	delete(s.unhealthyProfiles, streamID)
	s.healthMu.Unlock()
	if !hadEntries {
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
		profiles:   make(map[int]*profileWorker, len(targets)),
	}
	// Maintain one profileWorker per rendition so RuntimeStatus
	// continues to surface per-profile restart counts + errors.
	// The supervisor calls recordProfileError on profile index 0
	// for now (single-encoder pipeline); shadow entries for the
	// other indices keep the JSON shape the UI expects.
	pw0 := &profileWorker{cancel: func() {}, done: make(chan struct{})}
	sw.profiles[0] = pw0
	for i := 1; i < len(targets); i++ {
		sw.profiles[i] = &profileWorker{cancel: func() {}, done: pw0.done}
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
	s.m.TranscoderWorkersActive.WithLabelValues(string(logStreamCode)).Set(float64(len(targets)))
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
		defer close(pw0.done)
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

	sw.mu.Lock()
	profiles := make(map[int]*profileWorker, len(sw.profiles))
	for k, v := range sw.profiles {
		profiles[k] = v
	}
	sw.mu.Unlock()
	for _, pw := range profiles {
		<-pw.done
	}

	s.m.TranscoderWorkersActive.WithLabelValues(string(streamID)).Set(0)
	s.m.TranscoderQualitiesActive.WithLabelValues(string(streamID)).Set(0)
	s.dropHealthState(streamID)
	//nolint:contextcheck // baseCtx is cancelled; publish must outlive it
	s.bus.Publish(context.Background(), domain.Event{
		Type:       domain.EventTranscoderStopped,
		StreamCode: streamID,
	})
}

// StopProfile stops a single encoder for one profile index.
func (s *Service) StopProfile(streamID domain.StreamCode, profileIndex int) {
	s.mu.Lock()
	sw, ok := s.workers[streamID]
	s.mu.Unlock()
	if !ok {
		return
	}

	sw.mu.Lock()
	pw, ok := sw.profiles[profileIndex]
	if !ok {
		sw.mu.Unlock()
		return
	}
	delete(sw.profiles, profileIndex)
	sw.mu.Unlock()

	pw.cancel()
	<-pw.done

	slog.Info("transcoder: profile stopped",
		"stream_code", streamID,
		"profile", buffer.VideoTrackSlug(profileIndex),
	)
	s.updateMetrics(streamID, sw)
}

// StartProfile starts a single encoder for one profile index.
//
// StartProfile is unsupported: the subprocess produces every rendition, so a
// single rung can't be started on its own. Returns ErrNotImplemented; the
// coordinator surfaces it and restarts the whole subprocess instead.
func (s *Service) StartProfile(streamID domain.StreamCode, profileIndex int, target RenditionTarget) error {
	_ = target
	slog.Warn("transcoder: StartProfile refused — the subprocess owns all renditions",
		"stream_code", streamID,
		"profile_index", profileIndex,
	)
	return fmt.Errorf("transcoder: profile %d: %w", profileIndex, ErrNotImplemented)
}

// updateMetrics refreshes the active worker/quality gauge for a stream.
func (s *Service) updateMetrics(streamID domain.StreamCode, sw *streamWorker) {
	sw.mu.Lock()
	n := float64(len(sw.profiles))
	sw.mu.Unlock()
	s.m.TranscoderWorkersActive.WithLabelValues(string(streamID)).Set(n)
	s.m.TranscoderQualitiesActive.WithLabelValues(string(streamID)).Set(n)
}
