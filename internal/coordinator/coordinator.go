// Package coordinator wires buffer, stream manager, transcoder, and publisher
// for a single stream lifecycle. Used by the HTTP API and server bootstrap.
package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/dvr"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/manager"
	"github.com/datvietvac-techhub/open-streamer/internal/metrics"
	"github.com/datvietvac-techhub/open-streamer/internal/publisher"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
	"github.com/datvietvac-techhub/open-streamer/internal/transcoder"
	"github.com/datvietvac-techhub/open-streamer/internal/watermarks"
	"github.com/samber/do/v2"
)

// Coordinator starts and stops the full per-stream pipeline.
type Coordinator struct {
	buf          *buffer.Service
	mgr          mgrDep
	tc           tcDep
	pub          pubDep
	dvr          dvrDep
	bus          events.Bus
	m            *metrics.Metrics
	streamRepo   store.StreamRepository
	templateRepo store.TemplateRepository

	// upstreamLookup resolves another stream by code; used by detectABRCopy
	// to inspect the upstream's transcoder ladder shape before deciding
	// which pipeline branch to wire. Nil in unit tests that don't exercise
	// ABR copy — detectABRCopy short-circuits to the normal path.
	upstreamLookup func(domain.StreamCode) (*domain.Stream, bool)

	// wmAssets resolves Stream.Watermark.Filename into an absolute on-disk
	// path before each transcoder.Start. Optional — nil disables asset
	// resolution and only Stream.Watermark.ImagePath is honoured. Wired
	// from DI in main.go.
	wmAssets *watermarks.Service

	rendMu     sync.Mutex
	renditions map[domain.StreamCode][]string // ABR rendition slugs per stream (for buffer teardown)

	abrMu     sync.RWMutex
	abrCopies map[domain.StreamCode]*abrCopyEntry  // streams currently running as ABR copy
	abrMixers map[domain.StreamCode]*abrMixerEntry // streams currently running as ABR mixer (mirror video ladder + audio fan-out)

	statusMu sync.RWMutex
	// degradation tracks per-stream "what's broken right now" so the
	// derived StreamStatus can reflect both input failover state AND
	// transcoder crash loops. Status is Active iff the stream has no
	// entry in the map (or the entry has all flags false). The
	// presence of an entry with any flag set → StatusDegraded.
	degradation map[domain.StreamCode]*streamDegradation
	// startedAt records when each currently-running stream's pipeline
	// went live. Set inside Start once every subsystem (manager,
	// publisher, transcoder, dvr) has been wired without error and
	// cleared on Stop. The pre-existing Prometheus
	// StreamStartTimeSeconds metric serves the dashboards; this map
	// powers the same value over REST (GET /streams returns the runtime
	// envelope including started_at + uptime_sec) so frontend can
	// render uptime without a Prometheus round-trip.
	startedAt map[domain.StreamCode]time.Time
}

// streamDegradation flags the discrete failure modes that should pull a
// stream into StatusDegraded. Multiple flags can be true at once
// (transcoder crashing AND inputs exhausted) — the stream stays
// degraded until ALL flags clear.
type streamDegradation struct {
	inputsExhausted     bool // manager: every input is in StatusDegraded
	transcoderUnhealthy bool // transcoder: FFmpeg crashing in a loop
}

// any reports whether the degradation entry currently warrants
// StatusDegraded. Returns false when every flag is cleared so the
// caller can drop the map entry and let the default (Active) take over.
func (d *streamDegradation) any() bool {
	return d.inputsExhausted || d.transcoderUnhealthy
}

// New registers a Coordinator with the DI injector.
func New(i do.Injector) (*Coordinator, error) {
	repo := do.MustInvoke[store.StreamRepository](i)
	templates := do.MustInvoke[store.TemplateRepository](i)
	c := &Coordinator{
		buf:          do.MustInvoke[*buffer.Service](i),
		mgr:          do.MustInvoke[*manager.Service](i),
		tc:           do.MustInvoke[*transcoder.Service](i),
		pub:          do.MustInvoke[*publisher.Service](i),
		dvr:          do.MustInvoke[*dvr.Service](i),
		bus:          do.MustInvoke[events.Bus](i),
		m:            do.MustInvoke[*metrics.Metrics](i),
		streamRepo:   repo,
		templateRepo: templates,
		upstreamLookup: func(code domain.StreamCode) (*domain.Stream, bool) {
			s, err := repo.FindByCode(context.Background(), code)
			if err != nil {
				return nil, false
			}
			return s, true
		},
		renditions:  make(map[domain.StreamCode][]string),
		abrCopies:   make(map[domain.StreamCode]*abrCopyEntry),
		abrMixers:   make(map[domain.StreamCode]*abrMixerEntry),
		degradation: make(map[domain.StreamCode]*streamDegradation),
		startedAt:   make(map[domain.StreamCode]time.Time),
	}
	// Watermarks library is optional — coordinator runs fine without it
	// (only direct ImagePath watermarks resolve). Tolerate "no provider".
	if w, err := do.Invoke[*watermarks.Service](i); err == nil {
		c.wmAssets = w
	}
	c.mgr.SetExhaustedCallback(c.handleAllInputsExhausted)
	c.mgr.SetRestoredCallback(c.handleInputRestored)
	c.tc.SetUnhealthyCallback(c.handleTranscoderUnhealthy)
	c.tc.SetHealthyCallback(c.handleTranscoderHealthy)
	return c, nil
}

// resolveTemplate merges a stream with its referenced template, if any.
// Returns the stream unchanged when there is no template reference or the
// template lookup fails (the template repo is allowed to be nil in tests).
// All bootstrap / reconcile call sites use this helper so the pipeline
// always sees the inherited config.
func (c *Coordinator) resolveTemplate(ctx context.Context, s *domain.Stream) *domain.Stream {
	if s == nil || s.Template == nil || c.templateRepo == nil {
		return s
	}
	tpl, err := c.templateRepo.FindByCode(ctx, *s.Template)
	if err != nil {
		return s
	}
	return domain.ResolveStream(s, tpl)
}

// newForTesting builds a Coordinator from pre-constructed deps, for use in tests.
func newForTesting(
	buf *buffer.Service,
	mgr mgrDep,
	tc tcDep,
	pub pubDep,
	dvr dvrDep,
	bus events.Bus,
	m *metrics.Metrics,
) *Coordinator {
	c := &Coordinator{
		buf:         buf,
		mgr:         mgr,
		tc:          tc,
		pub:         pub,
		dvr:         dvr,
		bus:         bus,
		m:           m,
		renditions:  make(map[domain.StreamCode][]string),
		abrCopies:   make(map[domain.StreamCode]*abrCopyEntry),
		abrMixers:   make(map[domain.StreamCode]*abrMixerEntry),
		degradation: make(map[domain.StreamCode]*streamDegradation),
		startedAt:   make(map[domain.StreamCode]time.Time),
	}
	c.mgr.SetExhaustedCallback(c.handleAllInputsExhausted)
	c.mgr.SetRestoredCallback(c.handleInputRestored)
	return c
}

// SetUpstreamLookupForTesting injects an upstream stream resolver — used by
// tests that exercise the ABR-copy branch without spinning up a store backend.
func (c *Coordinator) SetUpstreamLookupForTesting(fn func(domain.StreamCode) (*domain.Stream, bool)) {
	c.upstreamLookup = fn
}

// StreamStatus returns the runtime status of a stream pipeline.
// It is derived purely from in-memory state — never read from the store.
//
//   - StatusStopped  — pipeline is not registered (never started, or was stopped)
//   - StatusActive   — pipeline is running and no degradation source is active
//   - StatusDegraded — pipeline is running but at least one degradation source
//     is active (all inputs exhausted, OR transcoder crashing in a loop)
func (c *Coordinator) StreamStatus(code domain.StreamCode) domain.StreamStatus {
	if !c.IsRunning(code) {
		return domain.StatusStopped
	}
	c.statusMu.RLock()
	d := c.degradation[code]
	c.statusMu.RUnlock()
	if d != nil && d.any() {
		return domain.StatusDegraded
	}
	return domain.StatusActive
}

// updateDegradation toggles a single degradation flag and prunes the
// map entry when no flag remains set, so the default-Active path stays
// allocation-free for healthy streams. mutator receives the current
// (or freshly created) degradation entry under the lock.
func (c *Coordinator) updateDegradation(code domain.StreamCode, mutator func(*streamDegradation)) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	d := c.degradation[code]
	if d == nil {
		d = &streamDegradation{}
	}
	mutator(d)
	if d.any() {
		c.degradation[code] = d
	} else {
		delete(c.degradation, code)
	}
}

// clearDegradation drops every flag for a stream — used by the Start
// paths so a fresh pipeline never inherits stale degraded state from a
// prior run with the same code.
func (c *Coordinator) clearDegradation(code domain.StreamCode) {
	c.statusMu.Lock()
	delete(c.degradation, code)
	c.statusMu.Unlock()
}

// recordStartedAt stamps the moment a stream's pipeline went live so the
// API can compute uptime over REST without scraping Prometheus. Stored
// under statusMu (same lock as degradation) to avoid a second mutex when
// most stream-status reads already touch this critical section.
func (c *Coordinator) recordStartedAt(code domain.StreamCode, t time.Time) {
	c.statusMu.Lock()
	c.startedAt[code] = t
	c.statusMu.Unlock()
}

// clearStartedAt drops the startedAt entry on Stop. Returning to "not
// running" must not leave a stale start time the next StreamStartedAt
// query could surface — uptime semantics demand the timestamp track the
// CURRENT pipeline incarnation, not a prior one.
func (c *Coordinator) clearStartedAt(code domain.StreamCode) {
	c.statusMu.Lock()
	delete(c.startedAt, code)
	c.statusMu.Unlock()
}

// StreamStartedAt returns the wallclock moment the named stream's
// pipeline went live, or (zero, false) when the stream is not running.
// Used by the stream handler to populate runtime.started_at /
// runtime.uptime_sec on GET /streams responses.
func (c *Coordinator) StreamStartedAt(code domain.StreamCode) (time.Time, bool) {
	c.statusMu.RLock()
	t, ok := c.startedAt[code]
	c.statusMu.RUnlock()
	return t, ok
}

// Start creates the buffer, registers the stream with the manager (ingest + failover),
// starts publisher outputs, and optionally a transcoder worker.
//
// When the stream's first input is `copy://X` and X has an ABR ladder the
// pipeline is wired through startABRCopy instead — no ingest worker, no
// transcoder, just N tap goroutines re-publishing upstream rendition packets.
func (c *Coordinator) Start(ctx context.Context, stream *domain.Stream) error {
	if stream == nil {
		return fmt.Errorf("coordinator: nil stream")
	}
	if c.IsRunning(stream.Code) {
		return nil
	}
	if stream.Disabled {
		return fmt.Errorf("coordinator: stream %q is disabled", stream.Code)
	}
	if len(stream.Inputs) == 0 {
		return fmt.Errorf("coordinator: stream %q has no inputs", stream.Code)
	}

	if upstream := c.detectABRCopy(stream); upstream != nil {
		return c.startABRCopy(ctx, stream, upstream)
	}
	if videoUp, audioUp, ok := c.detectABRMixer(stream); ok {
		return c.startABRMixer(ctx, stream, videoUp, audioUp)
	}

	c.buf.Create(stream.Code)

	ingestWriteID := stream.Code
	if shouldRunTranscoder(stream) {
		ingestWriteID = buffer.RawIngestBufferID(stream.Code)
		c.buf.Create(ingestWriteID)
	}

	// Build the rendition targets and create their buffers BEFORE starting the
	// publisher. The publisher goroutines (serveHLSAdaptive, serveDASHAdaptive)
	// subscribe to rendition buffers synchronously on a different goroutine; if
	// those buffers don't exist yet the subscribe fails and the rendition is
	// silently dropped from the master playlist.
	var (
		transcoderTargets []transcoder.RenditionTarget
		renditionSlugs    []string
	)
	if shouldRunTranscoder(stream) {
		profiles := transcoderProfilesFromDomain(&stream.Transcoder.Video)
		for i := range profiles {
			slug := buffer.VideoTrackSlug(i)
			bid := buffer.RenditionBufferID(stream.Code, slug)
			c.buf.Create(bid)
			renditionSlugs = append(renditionSlugs, slug)
			transcoderTargets = append(transcoderTargets, transcoder.RenditionTarget{
				BufferID: bid,
				Profile:  profiles[i],
			})
		}
	}

	if err := c.mgr.Register(ctx, stream, ingestWriteID); err != nil {
		for _, slug := range renditionSlugs {
			c.buf.Delete(buffer.RenditionBufferID(stream.Code, slug))
		}
		c.buf.Delete(stream.Code)
		if ingestWriteID != stream.Code {
			c.buf.Delete(ingestWriteID)
		}
		return fmt.Errorf("coordinator: register manager: %w", err)
	}

	if err := c.pub.Start(ctx, stream); err != nil {
		c.mgr.Unregister(stream.Code)
		for _, slug := range renditionSlugs {
			c.buf.Delete(buffer.RenditionBufferID(stream.Code, slug))
		}
		c.buf.Delete(stream.Code)
		if ingestWriteID != stream.Code {
			c.buf.Delete(ingestWriteID)
		}
		return fmt.Errorf("coordinator: publisher: %w", err)
	}

	if len(transcoderTargets) > 0 {
		rawID := buffer.RawIngestBufferID(stream.Code)
		tcRT := c.transcoderConfigWithWatermark(stream)
		if err := c.tc.Start(ctx, stream.Code, rawID, tcRT, transcoderTargets); err != nil {
			for _, slug := range renditionSlugs {
				c.buf.Delete(buffer.RenditionBufferID(stream.Code, slug))
			}
			c.pub.Stop(stream.Code)
			c.mgr.Unregister(stream.Code)
			c.buf.Delete(stream.Code)
			c.buf.Delete(rawID)
			return fmt.Errorf("coordinator: transcoder: %w", err)
		}
		c.rendMu.Lock()
		c.renditions[stream.Code] = renditionSlugs
		c.rendMu.Unlock()
	}

	if stream.DVR != nil && stream.DVR.Enabled {
		mediaBuf := buffer.PlaybackBufferID(stream.Code, stream.Transcoder)
		if _, err := c.dvr.StartRecording(ctx, stream.Code, mediaBuf, stream.DVR); err != nil {
			slog.Warn("coordinator: dvr start failed", "stream_code", stream.Code, "err", err)
		}
	}

	now := time.Now()
	if c.m != nil {
		c.m.StreamStartTimeSeconds.WithLabelValues(string(stream.Code)).Set(float64(now.Unix()))
	}
	c.recordStartedAt(stream.Code, now)

	c.clearDegradation(stream.Code)

	c.bus.Publish(ctx, domain.Event{
		Type:       domain.EventStreamStarted,
		StreamCode: stream.Code,
	})

	p := domain.OutputProtocols{}
	if stream.Protocols != nil {
		p = *stream.Protocols
	}
	slog.Info("coordinator: stream pipeline started",
		"stream_code", stream.Code,
		"inputs", len(stream.Inputs),
		"transcoder", shouldRunTranscoder(stream),
		"renditions", len(renditionSlugs),
		"hls", p.HLS,
		"dash", p.DASH,
		"rtsp", p.RTSP,
		"push_targets", len(stream.Push),
		"dvr", stream.DVR != nil && stream.DVR.Enabled,
	)
	return nil
}

// IsRunning reports whether the stream pipeline is active in memory.
// Covers the normal pipeline (manager-registered) plus the in-process bypass
// paths (ABR copy, ABR mixer) that don't go through the manager.
func (c *Coordinator) IsRunning(streamID domain.StreamCode) bool {
	if c.mgr.IsRegistered(streamID) {
		return true
	}
	c.abrMu.RLock()
	_, copyOK := c.abrCopies[streamID]
	_, mixerOK := c.abrMixers[streamID]
	c.abrMu.RUnlock()
	return copyOK || mixerOK
}

// RunningStreams returns the codes of every stream whose pipeline is
// currently active. Used by runtime.diff to enumerate streams that need
// to be restarted when global config (e.g. transcoder.multi_output) changes.
//
// Snapshot semantics: the returned slice is decoupled from the live maps,
// so a stream torn down between this call and the caller acting on it
// will safely no-op via Stop's existing "not running" branch.
func (c *Coordinator) RunningStreams() []domain.StreamCode {
	seen := make(map[domain.StreamCode]struct{})

	for _, code := range c.mgr.RegisteredStreams() {
		seen[code] = struct{}{}
	}
	c.abrMu.RLock()
	for code := range c.abrCopies {
		seen[code] = struct{}{}
	}
	for code := range c.abrMixers {
		seen[code] = struct{}{}
	}
	c.abrMu.RUnlock()

	out := make([]domain.StreamCode, 0, len(seen))
	for code := range seen {
		out = append(out, code)
	}
	return out
}

// Stop tears down publisher, transcoder, manager (ingest), and the buffer.
// ctx is used for the DVR stop and the EventStreamStopped publish; cleanup
// of in-memory state always proceeds even if ctx is cancelled.
func (c *Coordinator) Stop(ctx context.Context, streamID domain.StreamCode) {
	c.abrMu.Lock()
	abr, isABR := c.abrCopies[streamID]
	if isABR {
		delete(c.abrCopies, streamID)
	}
	mix, isMix := c.abrMixers[streamID]
	if isMix {
		delete(c.abrMixers, streamID)
	}
	c.abrMu.Unlock()
	if isABR {
		slog.Info("coordinator: stopping abr copy pipeline", "stream_code", streamID)
		c.stopABRCopy(ctx, streamID, abr)
		return
	}
	if isMix {
		slog.Info("coordinator: stopping abr mixer pipeline", "stream_code", streamID)
		c.stopABRMixer(ctx, streamID, mix)
		return
	}

	slog.Info("coordinator: stopping stream pipeline", "stream_code", streamID)
	if c.dvr.IsRecording(streamID) {
		if err := c.dvr.StopRecording(ctx, streamID); err != nil {
			slog.Warn("coordinator: dvr stop failed", "stream_code", streamID, "err", err)
		}
	}
	c.pub.Stop(streamID)
	c.tc.Stop(streamID)
	c.mgr.Unregister(streamID)

	c.rendMu.Lock()
	slugs := c.renditions[streamID]
	delete(c.renditions, streamID)
	c.rendMu.Unlock()
	for _, slug := range slugs {
		c.buf.Delete(buffer.RenditionBufferID(streamID, slug))
	}

	c.buf.Delete(buffer.RawIngestBufferID(streamID))
	c.buf.Delete(streamID)

	if c.m != nil {
		c.m.StreamStartTimeSeconds.DeleteLabelValues(string(streamID))
	}
	c.clearStartedAt(streamID)

	c.clearDegradation(streamID)

	c.bus.Publish(ctx, domain.Event{
		Type:       domain.EventStreamStopped,
		StreamCode: streamID,
	})
}

// Update hot-reloads only the components that changed between old and new stream configs.
// The caller must persist the new config to the store BEFORE calling Update.
// Returns early with c.Stop() when the stream transitions to Disabled.
func (c *Coordinator) Update(ctx context.Context, old, new *domain.Stream) error {
	if old == nil || new == nil {
		return fmt.Errorf("coordinator: Update requires both old and new stream")
	}

	// ABR copy / ABR mixer use custom pipelines (no ingestor / transcoder),
	// so the per-component diff routing doesn't apply. Whenever either side
	// is on one of these paths we full-cycle: stop whatever is running, then
	// start fresh.
	c.abrMu.RLock()
	wasABRCopy := c.abrCopies[old.Code] != nil
	wasABRMixer := c.abrMixers[old.Code] != nil
	c.abrMu.RUnlock()
	willBeABRCopy := !new.Disabled && c.detectABRCopy(new) != nil
	_, _, willBeABRMixer := c.detectABRMixer(new)
	willBeABRMixer = willBeABRMixer && !new.Disabled
	if wasABRCopy || wasABRMixer || willBeABRCopy || willBeABRMixer {
		c.Stop(ctx, old.Code)
		if new.Disabled || len(new.Inputs) == 0 {
			return nil
		}
		return c.Start(ctx, new)
	}

	diff := ComputeDiff(old, new)

	slog.Info("coordinator: applying stream update",
		"stream_code", new.Code,
		"now_disabled", diff.NowDisabled,
		"transcoder_topology_changed", diff.TranscoderTopologyChanged,
		"transcoder_changed", diff.TranscoderChanged,
		"inputs_changed", diff.InputsChanged,
		"protocols_changed", diff.ProtocolsChanged,
		"push_changed", diff.PushChanged,
		"dvr_changed", diff.DVRChanged,
	)

	if diff.NowDisabled {
		c.Stop(ctx, new.Code)
		return nil
	}

	// Transcoder topology changes (nil↔non-nil, mode change, video.copy change)
	// require rebuilding the whole buffer layout → full pipeline reload.
	if diff.TranscoderTopologyChanged {
		return c.reloadTranscoderFull(ctx, old, new)
	}

	// Per-profile transcoder changes: reload only the affected profiles.
	if diff.TranscoderChanged && diff.ProfilesDiff != nil {
		//nolint:contextcheck // StartProfile derives from streamWorker.baseCtx; by design
		if err := c.reloadProfiles(new, diff.ProfilesDiff); err != nil {
			return fmt.Errorf("coordinator: reload profiles: %w", err)
		}
		// Push updated profile metadata to the live HLS master playlist writer so
		// resolution/bandwidth info reflects the new profile without a publisher restart.
		if len(diff.ProfilesDiff.Updated) > 0 {
			c.pub.UpdateABRMasterMeta(new.Code, abrMetaFromUpdated(new, diff.ProfilesDiff.Updated))
		}
	}

	if diff.InputsChanged {
		c.mgr.UpdateInputs(new.Code, diff.AddedInputs, diff.RemovedInputs, diff.UpdatedInputs)
	}

	// Profile add/remove: restart only HLS+DASH (RTSP/SRT viewers unaffected).
	if diff.ProfilesDiff != nil && diff.ProfilesDiff.HasAddedOrRemoved() {
		if err := c.pub.RestartHLSDASH(ctx, new); err != nil {
			return fmt.Errorf("coordinator: restart hls/dash: %w", err)
		}
	}

	// Protocol/push changes: surgically stop/start only affected goroutines.
	if diff.ProtocolsChanged || diff.PushChanged {
		if err := c.pub.UpdateProtocols(ctx, old, new); err != nil {
			return fmt.Errorf("coordinator: update protocols: %w", err)
		}
	}

	if diff.DVRChanged {
		c.reloadDVR(ctx, new)
	} else if diff.ProfilesDiff != nil && diff.ProfilesDiff.HasAddedOrRemoved() {
		// Best rendition may have changed (profile added/removed); re-evaluate DVR buffer.
		c.reloadDVRIfBufferChanged(ctx, old, new)
	}

	return nil
}

// reloadTranscoderFull rebuilds the entire pipeline when transcoder topology changes.
// This is the fallback for changes that affect buffer layout (nil↔non-nil, mode change).
func (c *Coordinator) reloadTranscoderFull(ctx context.Context, old, new *domain.Stream) error {
	slog.Info("coordinator: full transcoder reload", "stream_code", new.Code)

	if c.dvr.IsRecording(new.Code) {
		if err := c.dvr.StopRecording(ctx, new.Code); err != nil {
			slog.Warn("coordinator: dvr stop during reload failed", "stream_code", new.Code, "err", err)
		}
	}
	c.pub.Stop(new.Code)
	//nolint:contextcheck // tc.Stop uses its own baseCtx from Start; by design
	c.tc.Stop(new.Code)

	// Tear down old rendition buffers.
	c.rendMu.Lock()
	oldSlugs := c.renditions[new.Code]
	delete(c.renditions, new.Code)
	c.rendMu.Unlock()
	for _, slug := range oldSlugs {
		c.buf.Delete(buffer.RenditionBufferID(new.Code, slug))
	}

	// Rebuild buffers based on new config.
	oldIngestID := new.Code
	if shouldRunTranscoder(old) {
		oldIngestID = buffer.RawIngestBufferID(new.Code)
	}

	newIngestID := new.Code
	var transcoderTargets []transcoder.RenditionTarget
	var renditionSlugs []string

	if shouldRunTranscoder(new) {
		newIngestID = buffer.RawIngestBufferID(new.Code)
		if oldIngestID != newIngestID {
			c.buf.Create(newIngestID)
		}
		profiles := transcoderProfilesFromDomain(&new.Transcoder.Video)
		for i := range profiles {
			slug := buffer.VideoTrackSlug(i)
			bid := buffer.RenditionBufferID(new.Code, slug)
			c.buf.Create(bid)
			renditionSlugs = append(renditionSlugs, slug)
			transcoderTargets = append(transcoderTargets, transcoder.RenditionTarget{
				BufferID: bid,
				Profile:  profiles[i],
			})
		}
	} else if oldIngestID != newIngestID {
		// Old had raw buffer, new doesn't → delete it.
		c.buf.Delete(oldIngestID)
	}

	// Swap buffer write target on the active ingestor.
	c.mgr.UpdateBufferWriteID(new.Code, newIngestID)

	if len(transcoderTargets) > 0 {
		tcRT := c.transcoderConfigWithWatermark(new)
		if err := c.tc.Start(ctx, new.Code, newIngestID, tcRT, transcoderTargets); err != nil {
			return fmt.Errorf("transcoder start: %w", err)
		}
		c.rendMu.Lock()
		c.renditions[new.Code] = renditionSlugs
		c.rendMu.Unlock()
	}

	if err := c.pub.Start(ctx, new); err != nil {
		return fmt.Errorf("publisher start: %w", err)
	}

	c.reloadDVR(ctx, new)
	return nil
}

// reloadProfiles applies per-profile transcoder changes without touching unchanged profiles.
// Updated profiles: stop+start the corresponding rendition.
// Added profiles: create rendition buffer + start the rendition.
// Removed profiles: stop the rendition + delete its rendition buffer.
func (c *Coordinator) reloadProfiles(new *domain.Stream, pd *ProfilesDiff) error {
	newProfiles := transcoderProfilesFromDomain(&new.Transcoder.Video)

	// Removed: stop encoders and delete their buffers.
	for _, ch := range pd.Removed {
		c.tc.StopProfile(new.Code, ch.Index)
		slug := buffer.VideoTrackSlug(ch.Index)
		c.buf.Delete(buffer.RenditionBufferID(new.Code, slug))

		c.rendMu.Lock()
		slugs := c.renditions[new.Code]
		for i, s := range slugs {
			if s == slug {
				c.renditions[new.Code] = append(slugs[:i], slugs[i+1:]...)
				break
			}
		}
		c.rendMu.Unlock()
	}

	// Updated: stop + restart each changed encoder.
	for _, ch := range pd.Updated {
		if ch.Index >= len(newProfiles) {
			continue
		}
		c.tc.StopProfile(new.Code, ch.Index)
		slug := buffer.VideoTrackSlug(ch.Index)
		bid := buffer.RenditionBufferID(new.Code, slug)
		if err := c.tc.StartProfile(new.Code, ch.Index, transcoder.RenditionTarget{
			BufferID: bid,
			Profile:  newProfiles[ch.Index],
		}); err != nil {
			return fmt.Errorf("restart profile %d: %w", ch.Index, err)
		}
	}

	// Added: create buffer + start new encoder.
	for _, ch := range pd.Added {
		if ch.Index >= len(newProfiles) {
			continue
		}
		slug := buffer.VideoTrackSlug(ch.Index)
		bid := buffer.RenditionBufferID(new.Code, slug)
		c.buf.Create(bid)
		if err := c.tc.StartProfile(new.Code, ch.Index, transcoder.RenditionTarget{
			BufferID: bid,
			Profile:  newProfiles[ch.Index],
		}); err != nil {
			c.buf.Delete(bid)
			return fmt.Errorf("start added profile %d: %w", ch.Index, err)
		}

		c.rendMu.Lock()
		c.renditions[new.Code] = append(c.renditions[new.Code], slug)
		c.rendMu.Unlock()
	}

	return nil
}

// abrMetaFromUpdated converts ProfilesDiff.Updated entries to publisher.ABRRepMeta slices
// so the live HLS master playlist can be rewritten without restarting the publisher.
func abrMetaFromUpdated(stream *domain.Stream, updated []ProfileChange) []publisher.ABRRepMeta {
	profiles := transcoderProfilesFromDomain(&stream.Transcoder.Video)
	hasAudio := stream.Transcoder.Audio.Copy || stream.Transcoder.Audio.Codec != ""
	out := make([]publisher.ABRRepMeta, 0, len(updated))
	for _, ch := range updated {
		if ch.Index >= len(profiles) {
			continue
		}
		p := profiles[ch.Index]
		bwBps := 0
		if ch.New != nil {
			bwBps = ch.New.Bitrate * 1000
		}
		out = append(out, publisher.ABRRepMeta{
			Slug:     buffer.VideoTrackSlug(ch.Index),
			BwBps:    bwBps,
			Width:    p.Width,
			Height:   p.Height,
			HasAudio: hasAudio,
		})
	}
	return out
}

// reloadDVR stops any active recording and starts a new one when DVR is enabled.
func (c *Coordinator) reloadDVR(ctx context.Context, new *domain.Stream) {
	if c.dvr.IsRecording(new.Code) {
		if err := c.dvr.StopRecording(ctx, new.Code); err != nil {
			slog.Warn("coordinator: dvr stop failed", "stream_code", new.Code, "err", err)
		}
	}
	if new.DVR != nil && new.DVR.Enabled {
		mediaBuf := buffer.PlaybackBufferID(new.Code, new.Transcoder)
		if _, err := c.dvr.StartRecording(ctx, new.Code, mediaBuf, new.DVR); err != nil {
			slog.Warn("coordinator: dvr start failed", "stream_code", new.Code, "err", err)
		}
	}
}

// reloadDVRIfBufferChanged reloads DVR only when the playback buffer changed
// (e.g. best rendition shifted when a higher-resolution profile was added/removed).
func (c *Coordinator) reloadDVRIfBufferChanged(ctx context.Context, old, new *domain.Stream) {
	oldBuf := buffer.PlaybackBufferID(old.Code, old.Transcoder)
	newBuf := buffer.PlaybackBufferID(new.Code, new.Transcoder)
	if oldBuf != newBuf {
		c.reloadDVR(ctx, new)
	}
}

// handleAllInputsExhausted is called by the manager when all inputs are degraded.
// Sets the input-source degradation flag; the transcoder flag is left
// untouched so a stream that was ALSO transcoder-broken stays degraded
// after inputs recover.
func (c *Coordinator) handleAllInputsExhausted(streamCode domain.StreamCode) {
	slog.Warn("coordinator: all inputs exhausted, stream degraded", "stream_code", streamCode)
	c.updateDegradation(streamCode, func(d *streamDegradation) {
		d.inputsExhausted = true
	})
	c.bus.Publish(context.Background(), domain.Event{
		Type:       domain.EventInputDegraded,
		StreamCode: streamCode,
		Payload:    map[string]any{"reason": "all_inputs_exhausted"},
	})
}

// handleInputRestored is called by the manager when failover succeeds after all inputs
// were previously exhausted. Clears ONLY the input flag — if transcoder
// is also unhealthy the stream stays degraded.
func (c *Coordinator) handleInputRestored(streamCode domain.StreamCode) {
	slog.Info("coordinator: input restored", "stream_code", streamCode)
	c.updateDegradation(streamCode, func(d *streamDegradation) {
		d.inputsExhausted = false
	})
}

// handleTranscoderUnhealthy is called by the transcoder when its
// subprocess has crashed N consecutive times — signal that downstream
// publishers (HLS / DASH / RTMP) are receiving no packets and the stream
// is effectively broken even though inputs may still be flowing.
//
// reason carries the latest error message ("native transcoder: exit
// status 234; …") for ops visibility via slog. UI sees only the
// StatusDegraded transition; the subprocess error history (already
// populated via recordError) provides the detailed crash trail.
func (c *Coordinator) handleTranscoderUnhealthy(streamCode domain.StreamCode, reason string) {
	slog.Warn("coordinator: transcoder unhealthy, stream degraded",
		"stream_code", streamCode, "reason", reason)
	c.updateDegradation(streamCode, func(d *streamDegradation) {
		d.transcoderUnhealthy = true
	})
	c.bus.Publish(context.Background(), domain.Event{
		Type:       domain.EventTranscoderError,
		StreamCode: streamCode,
		Payload: map[string]any{
			"reason":  "consecutive_crashes",
			"message": reason,
		},
	})
}

// handleTranscoderHealthy is called by the transcoder when an FFmpeg
// worker has run continuously past the sustain threshold after a prior
// degradation — indicates the crash loop has resolved (operator fixed
// config, transient resource pressure cleared, etc.). Clears ONLY the
// transcoder flag.
func (c *Coordinator) handleTranscoderHealthy(streamCode domain.StreamCode) {
	slog.Info("coordinator: transcoder recovered", "stream_code", streamCode)
	c.updateDegradation(streamCode, func(d *streamDegradation) {
		d.transcoderUnhealthy = false
	})
}

// BootstrapPersistedStreams starts the pipeline for every non-disabled stream that
// has at least one input configured.  Stream status is never persisted, so all
// eligible streams are started fresh on every boot regardless of their last
// known runtime state.
func BootstrapPersistedStreams(ctx context.Context, log *slog.Logger, repo store.StreamRepository, coord *Coordinator) {
	streams, err := repo.List(ctx, store.StreamFilter{})
	if err != nil {
		log.Error("bootstrap: list streams failed", "err", err)
		return
	}
	for _, st := range streams {
		if st == nil {
			continue
		}
		if st.Disabled {
			log.Debug("bootstrap: skip stream (disabled)", "stream_code", st.Code)
			continue
		}
		if len(st.Inputs) == 0 {
			log.Debug("bootstrap: skip stream (no inputs)", "stream_code", st.Code)
			continue
		}
		resolved := coord.resolveTemplate(ctx, st)
		if err := coord.Start(ctx, resolved); err != nil {
			log.Warn("bootstrap: stream start failed", "stream_code", st.Code, "err", err)
			continue
		}
		log.Info("bootstrap: stream pipeline started", "stream_code", st.Code)
	}
}

// reconcileInterval is the cadence at which the reconciler verifies that
// every enabled stream is running. 10s is short enough that an operator
// creating a stream sees it come up "immediately" (worst case ~10s after
// the API write), and long enough that a healthy fleet pays effectively
// zero cost — one repo.List + a hashmap probe per stream.
const reconcileInterval = 10 * time.Second

// RunReconciler enforces the invariant "every stream that is not disabled
// must be running". It re-lists the persisted streams every reconcileInterval
// and Start()s any enabled stream the coordinator is not currently running.
//
// This is the safety net behind every code path that can leave a stream
// stopped against the operator's intent — e.g. a bootstrap Start that
// failed because the upstream HLS source was briefly unreachable, a
// restart that errored mid-flight, an API edge case where Start was never
// dispatched (POST /streams creating a brand-new stream and the handler
// only branched on "was previously disabled"). Without this loop those
// failures stick: the stream is saved as `disabled: false` but stays in
// StatusStopped until someone manually hits /restart.
//
// Start is idempotent (it short-circuits when the pipeline is already
// running), and concurrent Update / Stop calls from the API path race
// safely against the reconciler — Start checks IsRunning under its own
// lock, and a Stop arriving after our IsRunning probe simply means the
// next tick will re-Start it (which is the desired behaviour: if the
// operator wants the stream stopped, they should toggle Disabled).
//
// Call once at boot from the runtime Manager; the loop exits when ctx
// is cancelled (process shutdown).
func (c *Coordinator) RunReconciler(ctx context.Context) {
	// One immediate pass so a freshly-created stream that the create
	// handler missed comes up well before the first ticker tick.
	c.reconcileOnce(ctx)

	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reconcileOnce(ctx)
		}
	}
}

// reconcileOnce is one pass of the reconcile loop, factored out so tests
// can drive it deterministically without spinning up the ticker.
func (c *Coordinator) reconcileOnce(ctx context.Context) {
	streams, err := c.streamRepo.List(ctx, store.StreamFilter{})
	if err != nil {
		slog.Warn("coordinator: reconciler list failed", "err", err)
		return
	}
	for _, st := range streams {
		if st == nil || st.Disabled || len(st.Inputs) == 0 {
			continue
		}
		if c.IsRunning(st.Code) {
			continue
		}
		slog.Info("coordinator: reconciler starting stopped stream", "stream_code", st.Code)
		if err := c.Start(ctx, c.resolveTemplate(ctx, st)); err != nil {
			slog.Warn("coordinator: reconciler start failed",
				"stream_code", st.Code, "err", err)
		}
	}
	c.refreshStatusGauge(streams)
	if c.m != nil && c.m.ReconcilerLastRunSeconds != nil {
		// Stamp wall-clock seconds so a Grafana panel can compute "time
		// since last reconcile" via `time() - this gauge`. Set on every
		// successful pass — the absence of advancement across scrapes is
		// the alert signal.
		c.m.ReconcilerLastRunSeconds.Set(float64(time.Now().Unix()))
	}
}

// refreshStatusGauge updates open_streamer_streams_total per status bucket.
// Computes the snapshot from the just-listed stream set + current
// coordinator status query so dashboards see one consistent picture per
// reconciler tick. Buckets that go to zero (e.g. last degraded stream
// just recovered) are written explicitly rather than dropped — Prometheus
// gauges that disappear and reappear cause Grafana panels to render
// "No Data" gaps instead of a clean horizontal line at zero.
func (c *Coordinator) refreshStatusGauge(streams []*domain.Stream) {
	if c.m == nil || c.m.StreamsTotal == nil {
		return
	}
	// StreamStatus only ever returns active|degraded|stopped — seed those so a
	// bucket that drops to zero is still Set (not left stale). Idle is an
	// input-level state, never a top-level stream status, so it is not seeded.
	counts := map[domain.StreamStatus]int{
		domain.StatusActive:   0,
		domain.StatusDegraded: 0,
		domain.StatusStopped:  0,
	}
	seen := make(map[domain.StreamCode]struct{})
	for _, st := range streams {
		if st == nil {
			continue
		}
		seen[st.Code] = struct{}{}
		counts[c.StreamStatus(st.Code)]++
	}
	// Include runtime (auto-publish) pipelines, which run without a persisted
	// record and would otherwise be invisible in the active count.
	for _, code := range c.RunningStreams() {
		if _, ok := seen[code]; ok {
			continue
		}
		counts[c.StreamStatus(code)]++
	}
	for status, n := range counts {
		c.m.StreamsTotal.WithLabelValues(string(status)).Set(float64(n))
	}
}

// transcoderConfigWithWatermark returns a shallow clone of stream.Transcoder
// with the runtime-only Watermark field populated from stream.Watermark.
//
// We clone instead of mutating in place because stream.Transcoder is a
// pointer into the persisted Stream record — overwriting the Watermark
// field on it would surface the runtime overlay on subsequent GET responses
// and break the API contract that says watermark lives at Stream.Watermark.
//
// When the watermark references an uploaded asset (AssetID != ""), the
// coordinator resolves it into ImagePath here before handing the config
// to the transcoder, so the FFmpeg layer never sees AssetID and stays
// agnostic of the asset library.
func (c *Coordinator) transcoderConfigWithWatermark(stream *domain.Stream) *domain.TranscoderConfig {
	if stream == nil || stream.Transcoder == nil {
		return nil
	}
	clone := *stream.Transcoder
	clone.Watermark = c.resolvedWatermark(stream.Watermark, stream.Code)
	return &clone
}

// resolvedWatermark expands a filename reference into a concrete on-disk path.
// On any resolution error it logs and returns nil — a missing asset must
// not crash the start path. The transcoder treats nil as "watermark
// disabled" and the rest of the pipeline keeps running.
func (c *Coordinator) resolvedWatermark(wm *domain.WatermarkConfig, code domain.StreamCode) *domain.WatermarkConfig {
	if wm == nil || c.wmAssets == nil {
		return wm
	}
	if strings.TrimSpace(string(wm.Filename)) == "" {
		return wm
	}
	clone := *wm
	path, err := c.wmAssets.ResolvePath(wm.Filename)
	if err != nil {
		slog.Warn("coordinator: watermark asset resolve failed",
			"stream_code", code, "filename", wm.Filename, "err", err)
		return nil
	}
	clone.ImagePath = path
	clone.Filename = ""
	return &clone
}

func shouldRunTranscoder(stream *domain.Stream) bool {
	if stream == nil || stream.Transcoder == nil {
		return false
	}
	tc := stream.Transcoder
	// Both video and audio copy → raw MPEG-TS passes through without FFmpeg.
	return !tc.Video.Copy || !tc.Audio.Copy
}

// transcoderProfilesFromDomain maps stream video settings to FFmpeg ladder profiles.
// When video.copy is true or profiles is empty, returns a single passthrough profile
// (mpegts copy — one worker, no ABR). Explicit non-empty profiles with copy false
// build a multi-rendition encode ladder.
func transcoderProfilesFromDomain(video *domain.VideoTranscodeConfig) []transcoder.Profile {
	if video == nil {
		return singleOriginCopyProfile()
	}
	if video.Copy || len(video.Profiles) == 0 {
		return singleOriginCopyProfile()
	}
	out := make([]transcoder.Profile, 0, len(video.Profiles))
	for _, p := range video.Profiles {
		// Leave codec/preset empty when not set — buildFFmpegArgs +
		// normalizeVideoEncoder route on Global.HW (e.g. "" → h264_nvenc when
		// hw=nvenc, libx264 when hw=none). Defaulting to "libx264"/"fast" here
		// hardcodes the CPU encoder and silently ignores the HW config.
		codec := string(p.Codec)
		if codec == string(domain.VideoCodecCopy) {
			codec = ""
		}
		br := strconv.Itoa(p.Bitrate) + "k"
		if p.Bitrate <= 0 {
			br = strconv.Itoa(domain.DefaultVideoBitrateK) + "k"
		}
		out = append(out, transcoder.Profile{
			Width:            p.Width,
			Height:           p.Height,
			Bitrate:          br,
			Codec:            codec,
			Preset:           p.Preset,
			CodecProfile:     p.Profile,
			CodecLevel:       p.Level,
			MaxBitrate:       p.MaxBitrate,
			Framerate:        p.Framerate,
			KeyframeInterval: p.KeyframeInterval,
			Bframes:          p.Bframes,
			Refs:             p.Refs,
			SAR:              p.SAR,
			ResizeMode:       string(p.ResizeMode),
		})
	}
	return out
}

func singleOriginCopyProfile() []transcoder.Profile {
	// Empty codec/preset → buildFFmpegArgs picks based on Global.HW.
	return []transcoder.Profile{{
		Bitrate: strconv.Itoa(domain.DefaultVideoBitrateK) + "k",
	}}
}
