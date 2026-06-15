// Package manager implements the Stream Manager — the failover engine.
// It monitors input health (bitrate, FPS, packet loss, timeout) and seamlessly
// switches to the best available input when the active one degrades or fails.
// Failover is handled entirely in Go — FFmpeg is never restarted for this purpose.
//
// # Concurrency model
//
// Each stream has its own [streamState] protected by state.mu.
// The global Service.mu is an RWMutex that guards only the streams map and is
// never held while performing I/O or while acquiring state.mu (consistent lock
// ordering: Service.mu → state.mu prevents deadlocks).
//
// RecordPacket (hot path) does a global RLock to locate the state pointer, then
// a per-stream Lock to update LastPacketAt and Status.
// At typical packet rates (< 100/s per stream) the per-packet mutex overhead
// is negligible while being completely race-free.
//
// # Failover state machine
//
//	StatusIdle → StatusActive   (first packet arrives on the active input)
//	StatusActive → StatusDegraded (timeout detected by monitor, or error from ingestor)
//	StatusDegraded → StatusIdle   (background probe succeeds; input is a failback candidate)
//	StatusIdle → StatusActive   (ingestor switches to this input after tryFailover)
package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor"
	"github.com/datvietvac-techhub/open-streamer/internal/metrics"
	"github.com/datvietvac-techhub/open-streamer/internal/publisher"
	"github.com/datvietvac-techhub/open-streamer/internal/transcoder"
	"github.com/samber/do/v2"
)

const (
	monitorInterval        = 2 * time.Second
	failbackProbeCooldown  = 8 * time.Second  // min time after degradation before first probe
	failbackSwitchCooldown = 12 * time.Second // min time between input switches
	probeTimeout           = 3 * time.Second
)

// InputHealth tracks the runtime health of one input source.
// All mutable fields are protected by the parent streamState.mu, EXCEPT
// `tracks` which has its own internal mutex so the per-packet hot path can
// update without contending on the broader state lock.
type InputHealth struct {
	Input        domain.Input
	LastPacketAt time.Time
	PacketLoss   float64 // percent (reserved)
	Status       domain.StreamStatus
	// tracks accumulates per-codec byte / SPS info for the runtime "input
	// media info" panel. Snapshot via tracks.snapshot() / totalBitrateKbps()
	// — it has its own mutex so packet observers stay off state.mu.
	tracks *inputTrackStats
	// Errors is a bounded rolling history (newest at index 0, max maxInputErrorHistory).
	// Persists for the lifetime of the manager registration — cleared only when
	// the stream pipeline is stopped/restarted (Unregister drops the whole state).
	Errors []domain.ErrorEntry
}

const maxInputErrorHistory = 5

// recordInputError prepends an entry, capped at maxInputErrorHistory.
// Caller must hold the parent streamState.mu.
func recordInputError(h *InputHealth, msg string, at time.Time) {
	e := domain.ErrorEntry{Message: msg, At: at}
	if len(h.Errors) >= maxInputErrorHistory {
		copy(h.Errors[1:], h.Errors[:maxInputErrorHistory-1])
		h.Errors[0] = e
		return
	}
	h.Errors = append([]domain.ErrorEntry{e}, h.Errors...)
}

// SwitchReason names why the active input changed. The set is closed —
// every callsite of tryFailover must pick one so the rolling history is
// always interpretable in the UI.
type SwitchReason string

// SwitchReason values. Add a new constant before introducing a new
// trigger path so the UI legend and frontend filter dropdowns can stay
// in sync (currently maintained manually).
const (
	// SwitchReasonInitial — the very first activation of a stream when
	// Register picks the best-priority input. From=-1 (no previous active);
	// recorded so the UI history shows a baseline event for streams that
	// haven't switched yet.
	SwitchReasonInitial SwitchReason = "initial"
	// SwitchReasonError — ingestor reported a non-recoverable error on
	// the active input (RTMP disconnect, HLS playlist 404, SRT broken …).
	SwitchReasonError SwitchReason = "error"
	// SwitchReasonTimeout — no packet from the active input for the full
	// packet-timeout window (default 30s).
	SwitchReasonTimeout SwitchReason = "timeout"
	// SwitchReasonManual — operator forced the switch via the
	// /streams/{code}/switch API.
	SwitchReasonManual SwitchReason = "manual"
	// SwitchReasonFailback — a higher-priority input recovered after
	// having been degraded; manager switched back to honour priority.
	SwitchReasonFailback SwitchReason = "failback"
	// SwitchReasonRecovery — every input had degraded (exhausted state)
	// and one probed clean; pipeline restarted on it. May land on the
	// same priority as before in the single-input case.
	SwitchReasonRecovery SwitchReason = "recovery"
	// SwitchReasonInputAdded — UpdateInputs added an input with a
	// priority lower (= higher precedence) than the current active one.
	SwitchReasonInputAdded SwitchReason = "input_added"
	// SwitchReasonInputRemoved — UpdateInputs removed the active input;
	// manager promoted the next-best candidate.
	SwitchReasonInputRemoved SwitchReason = "input_removed"
)

// SwitchEvent records one active-input switch for the rolling history.
// From = -1 means "no previous active" — used for the initial activation
// recorded by Register so the UI has a baseline event for fresh streams.
type SwitchEvent struct {
	At     time.Time    `json:"at"`
	From   int          `json:"from"`
	To     int          `json:"to"`
	Reason SwitchReason `json:"reason"`
	// Detail is human-readable extra context (error message, timeout
	// duration, …). Empty for reasons that have no extra context (manual,
	// failback, input_added, input_removed).
	Detail string `json:"detail,omitempty"`
}

const maxSwitchHistory = 20

// recordSwitch prepends an entry, capped at maxSwitchHistory.
// Caller must hold the parent streamState.mu.
func recordSwitch(state *streamState, e SwitchEvent) {
	if len(state.switchHistory) >= maxSwitchHistory {
		copy(state.switchHistory[1:], state.switchHistory[:maxSwitchHistory-1])
		state.switchHistory[0] = e
		return
	}
	state.switchHistory = append([]SwitchEvent{e}, state.switchHistory...)
}

// streamState holds all monitoring data for a single stream.
// Lock order: Service.mu (read or write) → state.mu — never in reverse.
type streamState struct {
	mu sync.Mutex

	inputs        map[int]*InputHealth // keyed by Input.Priority
	active        int                  // active Input.Priority
	bufferWriteID domain.StreamCode
	degradedAt    map[int]time.Time // when each input was last marked degraded
	probing       map[int]bool      // true while a probe goroutine is in-flight for that priority
	lastSwitchAt  time.Time         // time of the most recent active-input switch
	// switchHistory is a bounded rolling log of active-input switches
	// (newest at index 0, max maxSwitchHistory). Persists for the lifetime
	// of the manager registration; cleared only on Unregister.
	switchHistory []SwitchEvent

	// overridePriority is set by a manual SwitchInput call.
	// selectBest treats this input as highest-priority (always wins if healthy).
	// Cleared automatically when the overridden input degrades permanently.
	overridePriority *int

	// dead is set by Unregister. All goroutines that touch state must bail immediately on sight.
	dead bool
	// exhausted is set when all inputs are degraded and no failover candidate exists.
	// Cleared when a new input becomes active.
	exhausted bool

	// monCtx is cancelled when the stream is unregistered.
	// It is used as the base context for ingestor operations to ensure they
	// are tied to the stream's lifetime and not context.Background.
	monCtx context.Context
	cancel context.CancelFunc
}

// RuntimeStatus is the single "runtime" envelope returned by the API for a
// stream. Sub-systems contribute their own sections — manager owns input
// health; transcoder owns its subprocess state — but the API exposes one shape
// so clients have a single root for all live data.
//
// Status and PipelineActive are populated by the API handler from the
// coordinator (not by manager itself). Transcoder and Publisher are also
// handler-populated and named after the subsystem they wrap, so frontend
// reads `runtime.transcoder` and `runtime.publisher.pushes[]` —
// no collision with the persisted `transcoder` / `push` config fields on
// domain.Stream.
//
// Exhausted is true when every input has degraded and no failover candidate
// remains — the stream is effectively offline at the source.
type RuntimeStatus struct {
	Status                domain.StreamStatus   `json:"status"`
	PipelineActive        bool                  `json:"pipeline_active"`
	ActiveInputPriority   int                   `json:"active_input_priority"`
	OverrideInputPriority *int                  `json:"override_input_priority,omitempty"`
	Exhausted             bool                  `json:"exhausted"`
	Inputs                []InputHealthSnapshot `json:"inputs"`
	// Switches is the rolling history of active-input changes (newest at
	// index 0, capped at maxSwitchHistory). Stream-level — switches happen
	// BETWEEN inputs, so this lives next to Inputs rather than inside one.
	Switches   []SwitchEvent             `json:"switches,omitempty"`
	Transcoder *transcoder.RuntimeStatus `json:"transcoder,omitempty"`
	Publisher  *publisher.RuntimeStatus  `json:"publisher,omitempty"`

	// StartedAt is the wallclock moment the pipeline went live. Populated
	// by the stream handler from coordinator.StreamStartedAt; nil for a
	// stopped stream so the UI can show "—" instead of an epoch zero.
	StartedAt *time.Time `json:"started_at,omitempty"`
	// UptimeSec is the precomputed elapsed seconds since StartedAt at the
	// moment of the response. Bundled so frontend can render uptime without
	// reading wallclock + doing the subtraction itself (and getting clock-
	// skew weirdness when the browser is hours off from the server).
	UptimeSec int64 `json:"uptime_sec,omitempty"`

	// Media is the UI-friendly summary of the current input → output track
	// shape (what the dashboard renders as "Input media info / Output media
	// info / 954kbit/s -> 2577kbit/s"). Populated by the API handler — the
	// manager doesn't know the output config.
	Media *MediaSummary `json:"media,omitempty"`
}

// MediaSummary aggregates the input + output media-track info for one stream.
// Numbers are convenience aggregates over Inputs/Outputs and may be
// recomputed by clients if they want different rounding.
type MediaSummary struct {
	InputBitrateKbps  int                     `json:"input_bitrate_kbps"`
	OutputBitrateKbps int                     `json:"output_bitrate_kbps"`
	Inputs            []domain.MediaTrackInfo `json:"inputs,omitempty"`
	Outputs           []domain.MediaTrackInfo `json:"outputs,omitempty"`
}

// InputHealthSnapshot is a serialisable copy of one input's health.
// Errors is a bounded rolling history (newest first, max maxInputErrorHistory)
// of human-readable failure reasons (packet timeout, ingestor error, …).
// History persists for the lifetime of the manager registration and is only
// cleared when the stream pipeline is stopped. Frontend should treat each
// message as a diagnostic string, not a code.
//
// BitrateKbps is the aggregate input bitrate (sum of per-codec track bitrates).
// Tracks lists each detected elementary stream (codec, resolution, kbps) and is
// nil/empty until the first AVPackets arrive.
type InputHealthSnapshot struct {
	InputPriority int                     `json:"input_priority"`
	LastPacketAt  time.Time               `json:"last_packet_at"`
	BitrateKbps   int                     `json:"bitrate_kbps"`
	PacketLoss    float64                 `json:"packet_loss"`
	Status        domain.StreamStatus     `json:"status"`
	Tracks        []domain.MediaTrackInfo `json:"tracks,omitempty"`
	Errors        []domain.ErrorEntry     `json:"errors,omitempty"`
}

// probeTask carries the arguments for a background probe goroutine.
// All fields are immutable value copies so the goroutine is safe after state.mu is released.
type probeTask struct {
	priority int
	h        *InputHealth // pointer is stable for the stream's lifetime
	input    domain.Input // value copy of the input (URL, headers, etc.)
}

// ingestorDep is the slice of ingestor.Service that manager actually uses.
// Defined as an interface so unit tests can substitute a stub without
// constructing a full pull/push ingest stack — same pattern coordinator uses
// for its mgrDep / tcDep / pubDep deps.
type ingestorDep interface {
	Start(ctx context.Context, streamID domain.StreamCode, input domain.Input, bufferWriteID domain.StreamCode) error
	Probe(ctx context.Context, input domain.Input) error
	SetPacketObserver(fn func(streamID domain.StreamCode, inputPriority int))
	SetInputErrorObserver(fn func(streamID domain.StreamCode, inputPriority int, err error))
	SetMediaPacketObserver(fn func(streamID domain.StreamCode, inputPriority int, p *domain.AVPacket))
	Stop(streamID domain.StreamCode)
}

// Service monitors all streams and orchestrates source failover.
type Service struct {
	bus      events.Bus
	ingestor ingestorDep
	m        *metrics.Metrics

	// packetTimeoutNs is the active-input silence threshold in nanoseconds.
	// Stored atomically so SetConfig can hot-swap the value while the
	// monitor's checkHealth tick reads it without locking.
	packetTimeoutNs atomic.Int64

	mu      sync.RWMutex
	streams map[domain.StreamCode]*streamState

	onExhausted func(streamCode domain.StreamCode) // all inputs degraded — no ingest possible
	onRestored  func(streamCode domain.StreamCode) // at least one input active again after exhaustion
}

// packetTimeoutFromCfg normalises ManagerConfig.InputPacketTimeoutSec into
// a usable Duration, falling back to the package default for zero / negative
// values. Used by both New constructors and SetConfig so the resolution
// rule stays in one place.
func packetTimeoutFromCfg(sec int) time.Duration {
	if sec <= 0 {
		sec = domain.DefaultInputPacketTimeoutSec
	}
	return time.Duration(sec) * time.Second
}

// SetConfig hot-swaps the manager's runtime parameters. Currently only
// InputPacketTimeoutSec is exposed for live updates; future fields can
// be threaded through here as they get added. The new value takes effect
// on the next monitor tick (≤ monitorInterval = 2s).
//
// Does NOT restart the monitor goroutine, ingestor workers, or any
// downstream pipeline — only the threshold value is replaced. Streams
// that were degraded under the old threshold stay degraded; the new
// threshold applies to subsequent silence-gap evaluations.
func (s *Service) SetConfig(cfg config.ManagerConfig) {
	s.packetTimeoutNs.Store(int64(packetTimeoutFromCfg(cfg.InputPacketTimeoutSec)))
}

// SetExhaustedCallback registers a function called when all inputs for a stream are
// degraded and no failover candidate is available.
func (s *Service) SetExhaustedCallback(fn func(domain.StreamCode)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onExhausted = fn
}

// SetRestoredCallback registers a function called when a failover succeeds after
// a period where all inputs were exhausted.
func (s *Service) SetRestoredCallback(fn func(domain.StreamCode)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onRestored = fn
}

// NewForTesting builds a Service from pre-constructed deps. Used by unit
// tests that stub the ingestor (no real pull workers needed) and skip the
// DI plumbing. packetTimeoutSec mirrors ManagerConfig.InputPacketTimeoutSec
// (defaults to 30s when ≤ 0).
func NewForTesting(bus events.Bus, ing ingestorDep, m *metrics.Metrics, packetTimeoutSec int) *Service {
	svc := &Service{
		bus:      bus,
		ingestor: ing,
		m:        m,
		streams:  make(map[domain.StreamCode]*streamState),
	}
	svc.packetTimeoutNs.Store(int64(packetTimeoutFromCfg(packetTimeoutSec)))
	ing.SetPacketObserver(svc.RecordPacket)
	ing.SetInputErrorObserver(svc.ReportInputError)
	ing.SetMediaPacketObserver(svc.RecordMediaPacket)
	return svc
}

// New creates a Service and registers it with the DI injector.
func New(i do.Injector) (*Service, error) {
	cfg := do.MustInvoke[config.ManagerConfig](i)
	bus := do.MustInvoke[events.Bus](i)
	ing := do.MustInvoke[*ingestor.Service](i)
	m := do.MustInvoke[*metrics.Metrics](i)
	svc := &Service{
		bus:      bus,
		ingestor: ing,
		m:        m,
		streams:  make(map[domain.StreamCode]*streamState),
	}
	svc.packetTimeoutNs.Store(int64(packetTimeoutFromCfg(cfg.InputPacketTimeoutSec)))
	ing.SetPacketObserver(svc.RecordPacket)
	ing.SetInputErrorObserver(svc.ReportInputError)
	ing.SetMediaPacketObserver(svc.RecordMediaPacket)
	return svc, nil
}

// Register starts health monitoring for a stream and begins ingesting on the best input.
// bufferWriteID is the Buffer Hub slot for ingest writes; empty defaults to stream.Code.
func (s *Service) Register(ctx context.Context, stream *domain.Stream, bufferWriteID domain.StreamCode) error {
	if bufferWriteID == "" {
		bufferWriteID = stream.Code
	}

	monCtx, cancel := context.WithCancel(ctx)
	state := &streamState{
		inputs:        make(map[int]*InputHealth, len(stream.Inputs)),
		bufferWriteID: bufferWriteID,
		degradedAt:    make(map[int]time.Time),
		probing:       make(map[int]bool),
		lastSwitchAt:  time.Now(),
		monCtx:        monCtx,
		cancel:        cancel,
	}
	regNow := time.Now()
	for _, inp := range stream.Inputs {
		state.inputs[inp.Priority] = &InputHealth{
			Input:  inp,
			Status: domain.StatusIdle,
			// Stamp a grace window so the packet-timeout monitor gives the
			// initial active input `timeout` to connect + deliver its first
			// packet before it can be declared timed-out (see
			// collectTimeoutIfNeeded — which now fails over an active input
			// that never reaches StatusActive). commitSwitch re-stamps on every
			// switch for the same reason.
			LastPacketAt: regNow,
			tracks:       newInputTrackStats(),
		}
	}

	s.mu.Lock()
	if _, exists := s.streams[stream.Code]; exists {
		// Already registered: overwriting would orphan the prior monitor
		// goroutine + ticker (cancel unreachable) and let a concurrent
		// Unregister stop the new worker. Refuse — the coordinator's
		// per-stream lifecycle lock makes this a defensive backstop (C-1).
		s.mu.Unlock()
		cancel()
		return fmt.Errorf("manager: stream %q already registered", stream.Code)
	}
	s.streams[stream.Code] = state
	s.mu.Unlock()

	go s.monitor(monCtx, stream.Code)

	// Start ingesting on the best available input.
	state.mu.Lock()
	best := selectBest(state)
	if best != nil {
		state.active = best.Input.Priority
	}
	state.mu.Unlock()

	if best != nil {
		if err := s.ingestor.Start(monCtx, stream.Code, best.Input, bufferWriteID); err != nil {
			slog.Error("manager: initial ingest start failed",
				"stream_code", stream.Code,
				"input_priority", best.Input.Priority,
				"err", err,
			)
			// Route the synchronous start failure through the normal degrade
			// path so the input is marked degraded, error history recorded, and
			// failover / exhausted-handling engage. Without this the stream sat
			// Idle while StreamStatus reported Active and the reconciler was
			// blind (A-5): multi-input promotes a backup; single-input flips to
			// Degraded and the probe loop keeps trying to recover.
			s.ReportInputError(stream.Code, best.Input.Priority, err)
		} else {
			// Record the baseline event so the UI's switch history shows
			// "stream came up on input N at T" before any failover happens.
			// From=-1 distinguishes this from a real input-to-input switch.
			state.mu.Lock()
			recordSwitch(state, SwitchEvent{
				At:     time.Now(),
				From:   -1,
				To:     best.Input.Priority,
				Reason: SwitchReasonInitial,
			})
			state.mu.Unlock()
		}
	}
	return nil
}

// Unregister stops ingest and health monitoring for streamID.
func (s *Service) Unregister(streamID domain.StreamCode) {
	s.mu.Lock()
	state, ok := s.streams[streamID]
	if !ok {
		s.mu.Unlock()
		return
	}
	// Mark dead while holding Service.mu so any goroutine that reads dead under
	// state.mu (after releasing Service.mu) will see it immediately. Snapshot
	// the input priorities so we can drop their health-gauge series after the
	// stream is gone (otherwise they report their last value forever).
	state.mu.Lock()
	state.dead = true
	prios := make([]int, 0, len(state.inputs))
	for p := range state.inputs {
		prios = append(prios, p)
	}
	state.mu.Unlock()
	state.cancel() // cancels monCtx → stops monitor + aborts in-flight probes
	delete(s.streams, streamID)
	s.mu.Unlock()

	for _, p := range prios {
		s.m.ManagerInputHealth.DeleteLabelValues(string(streamID), strconv.Itoa(p))
	}

	s.ingestor.Stop(streamID)
}

// IsRegistered reports whether streamID is under manager supervision.
func (s *Service) IsRegistered(streamID domain.StreamCode) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.streams[streamID]
	return ok
}

// RegisteredStreams returns the codes of every stream currently under
// manager supervision (snapshot — caller is free to mutate the slice).
func (s *Service) RegisteredStreams() []domain.StreamCode {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.StreamCode, 0, len(s.streams))
	for code := range s.streams {
		out = append(out, code)
	}
	return out
}

// RuntimeStatus returns a snapshot of runtime input health, or ok=false if not registered.
func (s *Service) RuntimeStatus(streamID domain.StreamCode) (RuntimeStatus, bool) {
	s.mu.RLock()
	state, ok := s.streams[streamID]
	s.mu.RUnlock()
	if !ok {
		return RuntimeStatus{}, false
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	out := RuntimeStatus{
		ActiveInputPriority:   state.active,
		OverrideInputPriority: state.overridePriority,
		Exhausted:             state.exhausted,
		Inputs:                make([]InputHealthSnapshot, 0, len(state.inputs)),
	}
	if len(state.switchHistory) > 0 {
		// Defensive copy — caller must not see future mutations under state.mu.
		out.Switches = make([]SwitchEvent, len(state.switchHistory))
		copy(out.Switches, state.switchHistory)
	}
	for _, h := range state.inputs {
		snap := InputHealthSnapshot{
			InputPriority: h.Input.Priority,
			LastPacketAt:  h.LastPacketAt,
			PacketLoss:    h.PacketLoss,
			Status:        h.Status,
		}
		if h.tracks != nil {
			// Snapshot is taken under tracks.mu; safe to release state.mu
			// — tracks struct has its own lifetime tied to the InputHealth.
			snap.Tracks = h.tracks.snapshot()
			snap.BitrateKbps = h.tracks.totalBitrateKbps()
		}
		if len(h.Errors) > 0 {
			// Defensive copy — caller must not see future mutations under state.mu.
			snap.Errors = make([]domain.ErrorEntry, len(h.Errors))
			copy(snap.Errors, h.Errors)
		}
		out.Inputs = append(out.Inputs, snap)
	}
	return out, true
}

// RecordMediaPacket folds one decoded AVPacket into the per-input track
// stats (codec-bucketed bitrate, SPS-derived resolution). Hot path: one
// map lookup + one int add per packet, plus an SPS parse on the first
// keyframe of each codec.
//
// Skips silently when the stream is unknown (typical on race between
// teardown and a still-flushing reader). Does NOT update LastPacketAt /
// Status — those live in RecordPacket which the ingestor already calls
// per packet.
func (s *Service) RecordMediaPacket(streamID domain.StreamCode, inputPriority int, p *domain.AVPacket) {
	if p == nil {
		return
	}
	s.mu.RLock()
	state, ok := s.streams[streamID]
	s.mu.RUnlock()
	if !ok {
		return
	}
	state.mu.Lock()
	h, found := state.inputs[inputPriority]
	state.mu.Unlock()
	if !found || h.tracks == nil {
		return
	}
	h.tracks.observe(p, time.Now())
}

// RecordPacket updates the last-seen timestamp for an input.
// Called by the ingestor per packet — must be fast and contention-free.
func (s *Service) RecordPacket(streamID domain.StreamCode, inputPriority int) {
	s.mu.RLock()
	state, ok := s.streams[streamID]
	s.mu.RUnlock()
	if !ok {
		return
	}

	now := time.Now()
	// recoveredFromExhaustion captures the post-recovery transition under the
	// lock so we can fire the onRestored callback after unlocking. The
	// callback re-enters the coordinator (StatusDegraded → StatusActive
	// transition) and we must not hold state.mu across that — risk of lock
	// inversion with handlers that call back into the manager.
	recoveredFromExhaustion := false
	var recoveryCtx context.Context

	state.mu.Lock()
	if !state.dead {
		if h, found := state.inputs[inputPriority]; found {
			h.LastPacketAt = now
			if h.Status != domain.StatusActive {
				h.Status = domain.StatusActive
				// Recovery wipes the error history. Without this, transient
				// faults (file-loop EOF gap, brief network blip) leave a
				// stale "1 error" badge in the UI on a now-healthy input —
				// users can't distinguish "currently broken" from "broke
				// 2 minutes ago, recovered". Diagnostic history of past
				// failures is still observable via hooks (EventInputDegraded
				// / EventInputFailover) and slog warn lines, which are the
				// right place for "what happened over time".
				h.Errors = nil
				delete(state.degradedAt, inputPriority)
				s.m.ManagerInputHealth.WithLabelValues(string(streamID), strconv.Itoa(inputPriority)).Set(1)

				// Bypass-probe recovery: ingestor reader (HLS pull, RTMP
				// reconnect, …) repaired itself before the manager's probe
				// cycle (~8s cooldown) had a chance to run. Without this
				// the exhausted flag stays true forever — UI shows "all
				// inputs exhausted" alert despite packets flowing — and no
				// switch event is recorded for the recovery moment.
				//
				// Only the active input clears exhausted: a non-active
				// input recovering doesn't bring the pipeline back online
				// (we'd still need to switch to it via tryFailover).
				if state.exhausted && inputPriority == state.active {
					state.exhausted = false
					state.lastSwitchAt = now
					recordSwitch(state, SwitchEvent{
						At:     now,
						From:   inputPriority,
						To:     inputPriority,
						Reason: SwitchReasonRecovery,
						Detail: "ingestor auto-reconnect",
					})
					recoveredFromExhaustion = true
					recoveryCtx = state.monCtx
				}
			}
		}
	}
	state.mu.Unlock()

	if recoveredFromExhaustion {
		s.m.ManagerFailoversTotal.WithLabelValues(string(streamID)).Inc()
		//nolint:contextcheck // event publish must outlive any single-packet ctx
		s.bus.Publish(recoveryCtx, domain.Event{
			Type:       domain.EventInputFailover,
			StreamCode: streamID,
			Payload: map[string]any{
				"from":   inputPriority,
				"to":     inputPriority,
				"reason": string(SwitchReasonRecovery),
			},
		})
		s.notifyRestored(true, streamID)
	}
}

// ReportInputError marks an input degraded immediately and triggers failover.
// Called by the ingestor on non-retriable source errors.
func (s *Service) ReportInputError(streamID domain.StreamCode, inputPriority int, err error) {
	s.mu.RLock()
	state, ok := s.streams[streamID]
	s.mu.RUnlock()
	if !ok {
		return
	}

	now := time.Now()
	state.mu.Lock()
	if state.dead {
		state.mu.Unlock()
		return
	}
	// Single-input streams (eg. an auto-publish runtime stream with only a
	// publish:// input) treat ErrNoPusherConnected as a no-op: the error
	// exists to fast-track failover to a LOWER-priority input, but when
	// there's nothing to fall over to, the only effect is to flip the
	// stream to Degraded, fire onExhausted, and race the first inbound
	// packet's recovery callback. The race is non-deterministic; under
	// adverse ordering the Degraded flag stays set even though packets
	// flow normally. Leaving the input as Idle here lets RecordPacket
	// promote it to Active on the first packet without ever touching
	// the exhausted bit.
	if errors.Is(err, ingestor.ErrNoPusherConnected) && len(state.inputs) <= 1 {
		state.mu.Unlock()
		return
	}
	h, found := state.inputs[inputPriority]
	if !found || h.Status == domain.StatusDegraded || h.Status == domain.StatusStopped {
		state.mu.Unlock()
		return
	}
	h.Status = domain.StatusDegraded
	recordInputError(h, err.Error(), now)
	state.degradedAt[inputPriority] = now
	state.mu.Unlock()

	slog.Warn("manager: input degraded by ingestor error",
		"stream_code", streamID,
		"input_priority", inputPriority,
		"err", err,
	)
	s.m.ManagerInputHealth.WithLabelValues(string(streamID), strconv.Itoa(inputPriority)).Set(0)
	s.bus.Publish(state.monCtx, domain.Event{
		Type:       domain.EventInputDegraded,
		StreamCode: streamID,
		Payload:    map[string]any{"input_priority": inputPriority, "reason": err.Error()},
	})
	s.tryFailover(streamID, state, SwitchReasonError, err.Error())
}

func (s *Service) monitor(ctx context.Context, streamID domain.StreamCode) {
	ticker := time.NewTicker(monitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkHealth(streamID) //nolint:contextcheck // checkHealth uses state.monCtx internally
		}
	}
}

// checkHealth detects active-input timeouts, schedules probes for degraded
// inputs, and sweeps for pending failbacks. It holds state.mu only briefly
// to collect work items, then acts outside the lock.
func (s *Service) checkHealth(streamID domain.StreamCode) {
	s.mu.RLock()
	state, ok := s.streams[streamID]
	s.mu.RUnlock()
	if !ok {
		return
	}

	now := time.Now()
	timeout := time.Duration(s.packetTimeoutNs.Load())
	timedOutPriority := -1
	timedOutDetail := ""
	var probeTasks []probeTask
	needsFailback := false

	state.mu.Lock()
	if !state.dead {
		for priority, h := range state.inputs {
			s.collectTimeoutIfNeeded(state, h, priority, now, timeout, &timedOutPriority, &timedOutDetail)
			s.collectProbeIfNeeded(state, h, priority, now, &probeTasks)
		}
		needsFailback = shouldFailbackNow(state, now)
	}
	state.mu.Unlock()

	if timedOutPriority >= 0 {
		slog.Warn("manager: active input timed out",
			"stream_code", streamID,
			"input_priority", timedOutPriority,
		)
		s.m.ManagerInputHealth.WithLabelValues(string(streamID), strconv.Itoa(timedOutPriority)).Set(0)
		s.bus.Publish(state.monCtx, domain.Event{
			Type:       domain.EventInputDegraded,
			StreamCode: streamID,
			Payload:    map[string]any{"input_priority": timedOutPriority},
		})
		s.tryFailover(streamID, state, SwitchReasonTimeout, timedOutDetail)
	}
	for _, task := range probeTasks {
		go s.runProbe(streamID, state, task) //nolint:contextcheck // probe uses state.monCtx internally
	}
	if needsFailback {
		// Idempotent: tryFailover re-evaluates selectBest under its own lock
		// and early-returns when best already equals active. Calling it on
		// every tick costs a constant-time check, with no side effects when
		// state has changed between this collect and the actual failover.
		s.tryFailover(streamID, state, SwitchReasonFailback, "")
	}
}

// shouldFailbackNow reports whether the stream has a healthy higher-priority
// input that should preempt the current active. Caller must hold state.mu.
//
// Why this lives outside runProbe: the existing failback trigger (line ~987)
// only fires when a probe completes AFTER failbackSwitchCooldown has elapsed.
// If a probe runs and succeeds within the cooldown window (e.g. probeCooldown=8s
// vs switchCooldown=12s — a 4s race window), the input promotes to Idle but
// no failback ever fires, leaving the stream stuck on the lower-priority
// fallback (typically a maintenance VOD) until manual intervention. The
// The production failback-stuck incident this fix targets was exactly this race.
//
// The sweeper here re-evaluates every monitor tick (2s), so a "missed"
// failback at probe time gets retried on the next tick with full cooldown
// elapsed. Idempotent — when best already equals active, tryFailover is a
// no-op (line ~800 early return).
//
// Honors operator overrides via selectBest: if overridePriority pins to a
// specific input, selectBest returns that input regardless of its actual
// priority value, which makes `best.Input.Priority < state.active`
// evaluate against the pinned choice rather than the natural lowest.
func shouldFailbackNow(state *streamState, now time.Time) bool {
	if state.exhausted {
		// Exhausted recovery is RecordPacket / runProbe's job — they emit
		// SwitchReasonRecovery which restarts the worker even when best
		// equals active. The sweeper handles only true failbacks
		// (preempting a lower-priority fallback).
		return false
	}
	if now.Sub(state.lastSwitchAt) < failbackSwitchCooldown {
		return false
	}
	best := selectBest(state)
	if best == nil {
		return false
	}
	return best.Input.Priority < state.active
}

// collectTimeoutIfNeeded sets active input as degraded if no packet arrived within the timeout.
// Caller must hold state.mu.
func (s *Service) collectTimeoutIfNeeded(
	state *streamState,
	h *InputHealth,
	priority int,
	now time.Time,
	timeout time.Duration,
	timedOut *int,
	timedOutDetail *string,
) {
	if priority != state.active {
		return
	}
	// Skip inputs already degraded — a failover is in flight / the probe loop is
	// handling them. Do NOT require StatusActive here: an input only reaches
	// StatusActive on its FIRST packet (RecordPacket), so a switch/start onto an
	// input that never connects (e.g. SSRF-blocked, dead source) would otherwise
	// stay non-Active forever and never time out → permanent freeze with no
	// failover. LastPacketAt is stamped at Register/commitSwitch, giving every
	// active input a fair `timeout` grace before this fires (A-5 blind spot).
	if h.Status == domain.StatusDegraded {
		return
	}
	if now.Sub(h.LastPacketAt) <= timeout {
		return
	}
	msg := fmt.Sprintf("no packet for %s (timeout %s)", now.Sub(h.LastPacketAt).Truncate(time.Second), timeout)
	h.Status = domain.StatusDegraded
	recordInputError(h, msg, now)
	state.degradedAt[priority] = now
	*timedOut = priority
	*timedOutDetail = msg
}

// collectProbeIfNeeded queues a probe task for a degraded input past its cooldown.
// Caller must hold state.mu.
//
// We normally skip the active priority because the live ingestor worker is
// already trying to reconnect on its own. EXCEPTION: when the stream is
// exhausted, the active worker has stopped (handleReadError returned true on
// EOF / non-retriable error), so nobody is reconnecting. In that case the
// "active" priority is stale — we must probe it ourselves or single-input
// streams stay permanently exhausted.
func (s *Service) collectProbeIfNeeded(
	state *streamState,
	h *InputHealth,
	priority int,
	now time.Time,
	tasks *[]probeTask,
) {
	if h.Status != domain.StatusDegraded || state.probing[priority] {
		return
	}
	if priority == state.active && !state.exhausted {
		return
	}
	at, seen := state.degradedAt[priority]
	if !seen || now.Sub(at) < failbackProbeCooldown {
		return
	}
	state.probing[priority] = true
	*tasks = append(*tasks, probeTask{priority: priority, h: h, input: h.Input})
}

// tryFailover picks the best healthy input and seamlessly switches to it.
// It is safe to call concurrently; concurrent calls are idempotent.
//
// reason / detail are recorded in the switch history when the failover
// actually commits (caller's intent — what triggered this attempt). Every
// callsite must supply a reason; the closed SwitchReason enum makes
// "forgot to label" a compile error.
func (s *Service) tryFailover(streamID domain.StreamCode, state *streamState, reason SwitchReason, detail string) {
	state.mu.Lock()
	if state.dead {
		state.mu.Unlock()
		return
	}
	best := selectBest(state)
	clearOverrideIfNeeded(streamID, state, best)
	if best == nil {
		s.handleExhausted(streamID, state)
		return
	}
	// Skip when best is already the active worker — except when exhausted, in
	// which case the active worker has stopped (EOF / error) and we must
	// restart it. Common single-input case: priority 0 degrades, exhausts,
	// later probes clean → best.Priority == state.active == 0 → must restart.
	if best.Input.Priority == state.active && !state.exhausted {
		state.mu.Unlock()
		return
	}
	prevPriority := state.active
	wasExhausted := state.exhausted
	bestInput := best.Input
	bufID := state.bufferWriteID
	ctx := state.monCtx
	state.mu.Unlock()

	slog.Info("manager: switching input",
		"stream_code", streamID,
		"from", prevPriority,
		"to", bestInput.Priority,
		"reason", reason,
	)

	if err := s.ingestor.Start(ctx, streamID, bestInput, bufID); err != nil {
		slog.Error("manager: failed to start new ingestor",
			"stream_code", streamID,
			"input_priority", bestInput.Priority,
			"err", err,
		)
		return
	}

	commitSwitch(state, prevPriority, bestInput, reason, detail)

	// The previous input is no longer active — clear its health gauge so a
	// still-delivering backup isn't reported as healthy=1 after we switch away
	// (it returns to 1 via RecordPacket if it becomes active again). Guard
	// against same-priority restarts, where the gauge should stay as-is.
	if prevPriority != bestInput.Priority {
		s.m.ManagerInputHealth.WithLabelValues(string(streamID), strconv.Itoa(prevPriority)).Set(0)
	}

	s.m.ManagerFailoversTotal.WithLabelValues(string(streamID)).Inc()
	s.bus.Publish(ctx, domain.Event{
		Type:       domain.EventInputFailover,
		StreamCode: streamID,
		Payload:    map[string]any{"from": prevPriority, "to": bestInput.Priority, "reason": string(reason)},
	})
	s.notifyRestored(wasExhausted, streamID)
}

// handleExhausted marks the stream as having no healthy inputs and fires the callback once.
// Caller must hold state.mu; releases it before returning.
func (s *Service) handleExhausted(streamID domain.StreamCode, state *streamState) {
	wasExhausted := state.exhausted
	state.exhausted = true
	state.mu.Unlock()

	slog.Error("manager: no healthy input available", "stream_code", streamID)
	if wasExhausted {
		return
	}
	s.mu.RLock()
	cb := s.onExhausted
	s.mu.RUnlock()
	if cb != nil {
		go cb(streamID)
	}
}

// commitSwitch atomically updates streamState after a successful ingestor start.
// Records the switch in the rolling history with the caller-supplied reason
// (re-checked under the lock to avoid stale state).
func commitSwitch(state *streamState, prevPriority int, bestInput domain.Input, reason SwitchReason, detail string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.dead || state.active != prevPriority {
		return
	}
	if prevH := state.inputs[prevPriority]; prevH != nil && prevH.Status == domain.StatusActive {
		// Only downgrade Active → Idle; never override StatusDegraded that triggered the switch.
		prevH.Status = domain.StatusIdle
	}
	now := time.Now()
	state.active = bestInput.Priority
	state.lastSwitchAt = now
	state.exhausted = false
	// Stamp LastPacketAt so the packet-timeout monitor doesn't fire on the new input
	// before it has had a chance to connect (pre-connect handoff window).
	if newH := state.inputs[bestInput.Priority]; newH != nil {
		newH.LastPacketAt = now
	}
	recordSwitch(state, SwitchEvent{
		At:     now,
		From:   prevPriority,
		To:     bestInput.Priority,
		Reason: reason,
		Detail: detail,
	})
}

// notifyRestored fires the onRestored callback when the stream recovers from exhaustion.
func (s *Service) notifyRestored(wasExhausted bool, streamID domain.StreamCode) {
	if !wasExhausted {
		return
	}
	s.mu.RLock()
	cb := s.onRestored
	s.mu.RUnlock()
	if cb != nil {
		go cb(streamID)
	}
}

// clearOverrideIfNeeded clears the manual input override when the overridden input is no longer
// the selected best (meaning it has degraded permanently). Caller must hold state.mu.
func clearOverrideIfNeeded(streamID domain.StreamCode, state *streamState, best *InputHealth) {
	if state.overridePriority == nil {
		return
	}
	if best != nil && best.Input.Priority == *state.overridePriority {
		return
	}
	slog.Info("manager: manual input override cleared due to permanent failure",
		"stream_code", streamID,
		"override_priority", *state.overridePriority,
	)
	state.overridePriority = nil
}

// runProbe verifies whether a degraded input has recovered.
// On success it promotes the input to Idle and triggers a failback if warranted.
func (s *Service) runProbe(streamID domain.StreamCode, state *streamState, task probeTask) {
	defer func() {
		state.mu.Lock()
		state.probing[task.priority] = false
		state.mu.Unlock()
	}()

	// Use monCtx as parent so the probe is automatically cancelled when the stream is unregistered.
	probeCtx, cancel := context.WithTimeout(state.monCtx, probeTimeout)
	defer cancel()

	if err := s.ingestor.Probe(probeCtx, task.input); err != nil {
		// Probe failure: input is still unhealthy. Record so dashboards can
		// distinguish "probe loop alive but target down" from "probe loop
		// dead" (= no probe metrics for N minutes — alert).
		if s.m != nil && s.m.ManagerProbeAttemptsTotal != nil {
			s.m.ManagerProbeAttemptsTotal.WithLabelValues(string(streamID), "failure").Inc()
		}
		return // stays StatusDegraded; probe is retried after failbackProbeCooldown
	}
	if s.m != nil && s.m.ManagerProbeAttemptsTotal != nil {
		s.m.ManagerProbeAttemptsTotal.WithLabelValues(string(streamID), "success").Inc()
	}

	state.mu.Lock()
	if state.dead {
		state.mu.Unlock()
		return
	}
	task.h.Status = domain.StatusIdle
	delete(state.degradedAt, task.priority)
	currentActive := state.active
	wasExhausted := state.exhausted
	sinceSwitch := time.Since(state.lastSwitchAt)
	state.mu.Unlock()

	slog.Info("manager: degraded input recovered via probe",
		"stream_code", streamID,
		"input_priority", task.priority,
		"was_exhausted", wasExhausted,
	)

	// Emit a dedicated `input.recovered` event distinct from the
	// `input.failover` reason="failback"/"recovery" path. Consumers
	// subscribing to recovery transitions (oncall dashboards, automatic
	// page-resolve hooks) shouldn't have to inspect the failover payload
	// reason field — `input.recovered` is the unambiguous signal that a
	// previously-degraded source is healthy again. The follow-on
	// failover (if any) still emits `input.failover` separately.
	s.bus.Publish(state.monCtx, domain.Event{
		Type:       domain.EventInputRecovered,
		StreamCode: streamID,
		Payload: map[string]any{
			"input_priority": task.priority,
			"was_exhausted":  wasExhausted,
		},
	})

	// Two reasons to failover after a successful probe:
	//   1. Exhausted recovery — the active worker died (EOF / non-retriable
	//      error) so there's nothing running. Restart on whatever input just
	//      probed clean, regardless of priority comparison (might be the same
	//      priority that was active before).
	//   2. Failback — recovered input has higher priority than the fallback
	//      we're currently running on; switch back when cooldown elapsed.
	switch {
	case wasExhausted:
		s.tryFailover(streamID, state, SwitchReasonRecovery, "")
	case task.priority < currentActive && sinceSwitch >= failbackSwitchCooldown:
		s.tryFailover(streamID, state, SwitchReasonFailback, "")
	}
}

// SwitchInput forces the stream to use the given input priority regardless of its
// configured priority value. The override persists until the input degrades permanently,
// at which point the manager reverts to normal priority-based selection.
// Switching to a different priority replaces any previous override.
func (s *Service) SwitchInput(streamID domain.StreamCode, priority int) error {
	s.mu.RLock()
	state, ok := s.streams[streamID]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("manager: stream %s is not active", streamID)
	}

	state.mu.Lock()
	if state.dead {
		state.mu.Unlock()
		return fmt.Errorf("manager: stream %s is not active", streamID)
	}
	h, exists := state.inputs[priority]
	if !exists {
		state.mu.Unlock()
		return fmt.Errorf("manager: input priority %d not found in stream %s", priority, streamID)
	}
	if h.Status == domain.StatusDegraded || h.Status == domain.StatusStopped {
		state.mu.Unlock()
		return fmt.Errorf("manager: input priority %d is degraded and cannot be switched to", priority)
	}
	state.overridePriority = &priority
	state.mu.Unlock()

	slog.Info("manager: manual input switch requested",
		"stream_code", streamID,
		"priority", priority,
	)
	s.tryFailover(streamID, state, SwitchReasonManual, "")
	return nil
}

// UpdateInputs patches the live input routing table while the monitor is running.
//   - removed: deleted from state.inputs; if the active input is removed, failover is triggered.
//   - added: inserted as StatusIdle; if a higher-priority input is added, failover is triggered.
//   - updated: Input field replaced; if the active input is updated, the ingestor is restarted.
func (s *Service) UpdateInputs(
	streamID domain.StreamCode,
	added, removed, updated []domain.Input,
) {
	s.mu.RLock()
	state, ok := s.streams[streamID]
	s.mu.RUnlock()
	if !ok {
		return
	}

	// failoverReason captures WHY a failover is needed so the switch
	// history shows the right cause. Input removal of the active source
	// takes precedence over a higher-priority addition because the
	// removal alone would force a switch even if no add had happened.
	var failoverReason SwitchReason
	var restartInput *domain.Input

	state.mu.Lock()
	if state.dead {
		state.mu.Unlock()
		return
	}

	for _, inp := range removed {
		delete(state.inputs, inp.Priority)
		delete(state.degradedAt, inp.Priority)
		delete(state.probing, inp.Priority)
		if inp.Priority == state.active {
			failoverReason = SwitchReasonInputRemoved
		}
		slog.Info("manager: input removed", "stream_code", streamID, "priority", inp.Priority)
	}

	for _, inp := range added {
		state.inputs[inp.Priority] = &InputHealth{
			Input:  inp,
			Status: domain.StatusIdle,
			tracks: newInputTrackStats(),
		}
		if inp.Priority < state.active && failoverReason == "" {
			failoverReason = SwitchReasonInputAdded
		}
		slog.Info("manager: input added", "stream_code", streamID, "priority", inp.Priority)
	}

	for _, inp := range updated {
		if h, ok := state.inputs[inp.Priority]; ok {
			h.Input = inp
			if inp.Priority == state.active {
				ri := inp
				restartInput = &ri
			}
			slog.Info("manager: input updated", "stream_code", streamID, "priority", inp.Priority)
		}
	}
	bufID := state.bufferWriteID
	ctx := state.monCtx
	state.mu.Unlock()

	if failoverReason != "" {
		s.tryFailover(streamID, state, failoverReason, "")
	} else if restartInput != nil {
		if err := s.ingestor.Start(ctx, streamID, *restartInput, bufID); err != nil {
			slog.Error("manager: restart active input failed",
				"stream_code", streamID,
				"input_priority", restartInput.Priority,
				"err", err,
			)
		}
	}
}

// UpdateBufferWriteID changes the buffer slot where the ingestor writes packets.
// Used when adding/removing the transcoder (buffer topology change).
// The active ingestor is restarted to write to the new buffer.
func (s *Service) UpdateBufferWriteID(streamID domain.StreamCode, newBufID domain.StreamCode) {
	s.mu.RLock()
	state, ok := s.streams[streamID]
	s.mu.RUnlock()
	if !ok {
		return
	}

	state.mu.Lock()
	if state.dead {
		state.mu.Unlock()
		return
	}
	state.bufferWriteID = newBufID
	activeH := state.inputs[state.active]
	ctx := state.monCtx
	state.mu.Unlock()

	if activeH == nil {
		return
	}

	slog.Info("manager: buffer write ID updated",
		"stream_code", streamID,
		"new_buffer_id", newBufID,
	)

	if err := s.ingestor.Start(ctx, streamID, activeH.Input, newBufID); err != nil {
		slog.Error("manager: restart ingestor for new buffer failed",
			"stream_code", streamID,
			"err", err,
		)
	}
}

// selectBest returns the input to activate next. Caller must hold state.mu.
// If a manual override is set and the overridden input is healthy, it always wins
// regardless of its actual priority value. Otherwise the lowest priority value wins.
func selectBest(state *streamState) *InputHealth {
	if state.overridePriority != nil {
		if h, ok := state.inputs[*state.overridePriority]; ok &&
			h.Status != domain.StatusDegraded && h.Status != domain.StatusStopped {
			return h
		}
	}
	var best *InputHealth
	for _, h := range state.inputs {
		if h.Status == domain.StatusDegraded || h.Status == domain.StatusStopped {
			continue
		}
		if best == nil || h.Input.Priority < best.Input.Priority {
			best = h
		}
	}
	return best
}
