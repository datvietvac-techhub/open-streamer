// abr_copy.go — coordinator branch for `copy://` inputs whose upstream has an
// ABR ladder. Unlike single-stream copy (handled by ingestor.pull.CopyReader),
// ABR copy bypasses the ingest worker AND the transcoder entirely:
//
//	Upstream rendition_track_1  ──tap1──▶  Downstream rendition_track_1
//	Upstream rendition_track_2  ──tap2──▶  Downstream rendition_track_2
//	... (one tap goroutine per rung)
//
// The publisher then serves the downstream's rendition buffers as a normal
// ABR HLS/DASH master playlist — clients can't tell the rungs are copies.
//
// v1 limitations (documented):
//   - upstream profile changes after Start are NOT picked up; operator must
//     restart the downstream stream to re-snapshot the ladder.
//   - chained ABR copy (B = copy://A where A itself is ABR-copy) is not
//     supported because the in-memory ladder mirror is local to coordinator
//     and not visible through the StreamRepository. Configure C → A directly.
//   - failover not applicable: ABR-copy requires `copy://X` as the SOLE
//     input (enforced by domain.ValidateCopyShape).

package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/manager"
)

// abrCopyEntry holds the runtime handles needed to tear down an ABR-copy stream.
type abrCopyEntry struct {
	cancel context.CancelFunc
	wg     *sync.WaitGroup
	slugs  []string // rendition slugs; used to delete downstream buffers on stop
	// upstream is captured for the synthetic input snapshot returned to the
	// API — gives the operator the URL of the input that's actually feeding
	// the pipeline (`copy://<upstreamCode>`).
	upstream domain.StreamCode
	// lastPacketAtNanos is updated on every successful tap forward (any rung).
	// Atomic so the N tap goroutines + the API status reader can race-free.
	// Zero value = no packet has flowed yet.
	lastPacketAtNanos atomic.Int64
}

// detectABRCopy reports whether `s` should run on the ABR-copy mirror path
// (N tap goroutines, one per upstream rung, no transcoder). Returns the
// upstream stream when ALL of the following hold:
//   - the first input is `copy://X` and parses cleanly
//   - X exists in the lookup
//   - X has an ABR ladder (≥1 video profile, video.copy=false)
//   - downstream has NO transcoder configured
//
// Otherwise returns nil — the caller falls back to the normal pipeline,
// which (when upstream is ABR + downstream has transcoder) will single-tap
// the best rendition of the upstream and feed it into downstream's encoder.
//
// The first input is sufficient to detect because ValidateCopyShape rejects
// ABR-copy + fallback inputs at write time.
func (c *Coordinator) detectABRCopy(s *domain.Stream) *domain.Stream {
	if c.upstreamLookup == nil || s == nil || len(s.Inputs) == 0 {
		return nil
	}
	if s.Transcoder != nil {
		// Downstream has its own encoder → we don't mirror the ladder; the
		// normal ingestor pipeline (CopyReader picking best rendition) feeds
		// the encoder, which produces a fresh ladder per downstream config.
		return nil
	}
	first := s.Inputs[0]
	if !domain.IsCopyInput(first) {
		return nil
	}
	target, err := domain.CopyInputTarget(first)
	if err != nil {
		return nil
	}
	upstream, ok := c.upstreamLookup(target)
	if !ok {
		return nil
	}
	if !upstreamHasABRLadder(upstream) {
		return nil
	}
	return upstream
}

// upstreamHasABRLadder mirrors domain.streamHasRenditions — kept inline to
// avoid widening the domain export surface for one call site.
func upstreamHasABRLadder(s *domain.Stream) bool {
	if s == nil || s.Transcoder == nil {
		return false
	}
	if s.Transcoder.Video.Copy {
		return false
	}
	return len(s.Transcoder.Video.Profiles) > 0
}

// mirrorTranscoderForCopy clones the upstream's video profiles into a fresh
// TranscoderConfig used purely as a buffer-topology hint. The clone is never
// persisted and never feeds the transcoder service — its only role is to make
// `buffer.RenditionsForTranscoder(downstream)` return the correct N rungs so
// the publisher serves an ABR master playlist with matching slugs.
func mirrorTranscoderForCopy(src *domain.TranscoderConfig) *domain.TranscoderConfig {
	if src == nil {
		return nil
	}
	profiles := make([]domain.VideoProfile, len(src.Video.Profiles))
	copy(profiles, src.Video.Profiles)
	return &domain.TranscoderConfig{
		Video: domain.VideoTranscodeConfig{Profiles: profiles},
	}
}

// startABRCopy wires the ABR-copy pipeline:
//  1. shallow-clone downstream and inject the mirrored transcoder so publisher
//     and buffer helpers see N rungs;
//  2. create one downstream rendition buffer per rung;
//  3. start the publisher (it'll subscribe to the rendition buffers);
//  4. spawn N tap goroutines, each forwarding one upstream rendition into
//     its matching downstream rendition.
//
// On any setup failure rolls back partial state in reverse order.
// Concurrency: holds c.abrMu for the entire body so two callers racing on
// the same downstream.Code can't both reach pub.Start. See startABRMixer
// for the full rationale — the same race manifested there with
// "publisher: stream X already running" and downstream subscribe failures
// when the rollback path deleted buffers the first start was using.
func (c *Coordinator) startABRCopy(ctx context.Context, downstream, upstream *domain.Stream) error {
	c.abrMu.Lock()
	defer c.abrMu.Unlock()

	// Idempotent guard against the unlocked IsRunning → startABRCopy gap.
	if _, exists := c.abrCopies[downstream.Code]; exists {
		return nil
	}

	upRends := buffer.RenditionsForTranscoder(upstream.Code, upstream.Transcoder)
	if len(upRends) == 0 {
		return fmt.Errorf("coordinator: abr copy: upstream %q has no rendition ladder", upstream.Code)
	}

	mirrored := *downstream // shallow copy — we only mutate Transcoder pointer
	mirrored.Transcoder = mirrorTranscoderForCopy(upstream.Transcoder)
	downRends := buffer.RenditionsForTranscoder(mirrored.Code, mirrored.Transcoder)
	if len(downRends) != len(upRends) {
		return fmt.Errorf("coordinator: abr copy: rung count mismatch (upstream=%d downstream=%d)",
			len(upRends), len(downRends))
	}

	slugs := make([]string, 0, len(downRends))
	for _, r := range downRends {
		c.buf.Create(r.BufferID)
		slugs = append(slugs, r.Slug)
	}

	if err := c.pub.Start(ctx, &mirrored); err != nil {
		for _, r := range downRends {
			c.buf.Delete(r.BufferID)
		}
		return fmt.Errorf("coordinator: abr copy publisher: %w", err)
	}

	// Tap context is detached from the request ctx — the request is for
	// "start the pipeline", not "run forever". Stop() cancels this ctx.
	tapCtx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	entry := &abrCopyEntry{cancel: cancel, wg: wg, slugs: slugs, upstream: upstream.Code}

	c.abrCopies[downstream.Code] = entry

	for i := range downRends {
		wg.Add(1)
		//nolint:contextcheck // tapCtx is intentionally detached from request ctx; cancelled by Stop()
		go c.runABRCopyTap(tapCtx, wg, entry, downstream.Code, upRends[i].BufferID, downRends[i].BufferID, downRends[i].Slug)
	}

	now := time.Now()
	if c.m != nil {
		c.m.StreamStartTimeSeconds.WithLabelValues(string(downstream.Code)).Set(float64(now.Unix()))
	}
	c.recordStartedAt(downstream.Code, now)
	c.clearDegradation(downstream.Code)
	c.bus.Publish(ctx, domain.Event{
		Type:       domain.EventStreamStarted,
		StreamCode: downstream.Code,
	})

	slog.Info("coordinator: abr copy pipeline started",
		"stream_code", downstream.Code,
		"upstream", upstream.Code,
		"renditions", len(downRends),
	)
	return nil
}

// stopABRCopy tears down an ABR-copy pipeline. Caller is responsible for
// removing the entry from c.abrCopies BEFORE calling so concurrent IsRunning
// returns false promptly.
func (c *Coordinator) stopABRCopy(ctx context.Context, code domain.StreamCode, abr *abrCopyEntry) {
	abr.cancel()
	c.pub.Stop(code)
	abr.wg.Wait() // taps must exit before deleting their target buffers

	for _, slug := range abr.slugs {
		c.buf.Delete(buffer.RenditionBufferID(code, slug))
	}

	if c.m != nil {
		c.m.StreamStartTimeSeconds.DeleteLabelValues(string(code))
	}
	c.clearStartedAt(code)
	c.clearDegradation(code) // clears the per-stream status entry

	c.bus.Publish(ctx, domain.Event{
		Type:       domain.EventStreamStopped,
		StreamCode: code,
	})

	slog.Info("coordinator: abr copy pipeline stopped",
		"stream_code", code,
		"renditions", len(abr.slugs),
	)
}

// runABRCopyTap subscribes to one upstream rendition buffer and forwards every
// packet into the matching downstream rendition buffer until ctx is cancelled.
// On upstream tear-down (subscriber chan closed) it retries with capped
// exponential backoff so an upstream restart resumes copy automatically.
func (c *Coordinator) runABRCopyTap(
	ctx context.Context,
	wg *sync.WaitGroup,
	entry *abrCopyEntry,
	downstreamCode, upBufID, downBufID domain.StreamCode,
	slug string,
) {
	defer wg.Done()

	const initialBackoff = time.Second
	const maxBackoff = 10 * time.Second
	backoff := initialBackoff

	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if c.abrCopyTapForward(ctx, entry, upBufID, downBufID) {
			return // ctx cancelled
		}
		// Subscribe failed or upstream chan closed — wait + retry.
		slog.Info("coordinator: abr copy tap reconnecting",
			"stream_code", downstreamCode,
			"slug", slug,
			"upstream_buffer", upBufID,
			"retry_in", backoff,
		)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// abrCopyTapForward runs one subscribe-and-forward cycle. Returns true when
// ctx cancellation caused the exit (caller should not retry).
func (c *Coordinator) abrCopyTapForward(ctx context.Context, entry *abrCopyEntry, upBufID, downBufID domain.StreamCode) bool {
	sub, err := c.buf.Subscribe(upBufID)
	if err != nil {
		return false
	}
	defer c.buf.Unsubscribe(upBufID, sub)

	// Mint a StreamSession on the downstream rendition buffer for every
	// (re)start of the tap. Same rationale as abrMixerVideoForward: an
	// upstream cycle is the copy mirror's reconnect equivalent.
	_ = c.buf.SetSession(downBufID, domain.SessionStartMixerCycle, nil, nil)

	for {
		select {
		case <-ctx.Done():
			return true
		case pkt, ok := <-sub.Recv():
			if !ok {
				return false // upstream torn down — tell caller to retry
			}
			// Write may fail with "stream not found" if the downstream
			// buffer was already torn down between Stop() invalidating
			// our cancel and our defer firing. Silently drop in that
			// race; the next ctx.Done check breaks us out.
			if err := c.buf.Write(downBufID, pkt); err == nil {
				// Stamp the entry's last-packet timestamp so the API can
				// report the synthetic input as ACTIVE. Atomic store —
				// any of the N tap goroutines may race here.
				entry.lastPacketAtNanos.Store(time.Now().UnixNano())
			}
		}
	}
}

// abrCopyInputStaleAfter is the threshold for marking the synthetic input
// as Degraded vs Active. Mirrors manager.packetTimeout so behaviour is
// consistent with the normal pipeline path.
const abrCopyInputStaleAfter = 30 * time.Second

// ABRCopyRuntimeStatus returns a synthetic manager.RuntimeStatus for an
// ABR-copy stream (which doesn't go through the manager and therefore has no
// real RuntimeStatus). The returned snapshot lets the API surface a single
// "input 0" with status derived from tap activity:
//
//   - lastPacketAt within abrCopyInputStaleAfter → StatusActive
//   - older than the threshold (or no packet yet) → StatusDegraded
//
// Returns ok=false when the stream isn't running as ABR-copy — caller should
// fall back to the normal manager.RuntimeStatus path.
func (c *Coordinator) ABRCopyRuntimeStatus(code domain.StreamCode) (manager.RuntimeStatus, bool) {
	c.abrMu.RLock()
	entry, ok := c.abrCopies[code]
	c.abrMu.RUnlock()
	if !ok {
		return manager.RuntimeStatus{}, false
	}

	lastNanos := entry.lastPacketAtNanos.Load()
	var lastPacketAt time.Time
	status := domain.StatusDegraded
	if lastNanos > 0 {
		lastPacketAt = time.Unix(0, lastNanos)
		if time.Since(lastPacketAt) <= abrCopyInputStaleAfter {
			status = domain.StatusActive
		}
	}

	return manager.RuntimeStatus{
		Status:              domain.StatusActive, // pipeline-level (filled by handler)
		PipelineActive:      true,
		ActiveInputPriority: 0,
		Inputs: []manager.InputHealthSnapshot{{
			InputPriority: 0,
			LastPacketAt:  lastPacketAt,
			Status:        status,
		}},
	}, true
}
