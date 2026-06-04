package ingestor

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// stallWatchdog watches the time-since-last-write on a worker's
// readLoop and emits a SessionStartStallRecovery marker when the gap
// exceeds stallThreshold. The marker fronts the next packet written
// (buffer.Service auto-stamps SessionStart=true on the next write
// after SetSession), so downstream consumers (DASH packager's
// onSessionBoundary, HLS segmenter, RTSP/RTMP re-stream) flush their
// accumulated state at a clean boundary instead of accreting a giant
// per-sample dur or an A/V skew.
//
// The watchdog fires AT MOST ONCE per stall event — stallSignaled
// latches when a marker is emitted and clears as soon as a fresh
// packet write resets the lastWriteAt timestamp. This avoids a flood
// of session boundaries during a long outage.
//
// Scope: applies to all reader paths (raw-TS / AV). Raw-TS streams
// (HLS pull / SRT / UDP / file) benefit most because they bypass the
// Normaliser's MaxBehindMs re-anchor; AV-path streams already get a
// per-track re-anchor via the Normaliser but still benefit from the
// downstream session-boundary signal.
// Production defaults for the watchdog timing knobs. Pass these into
// runStallWatchdog from production call sites; tests pass shorter
// values so they don't have to sleep > 15 s of real time per case.
const (
	// DefaultStallThreshold is the gap-since-last-write that the watchdog
	// treats as a source stall. Sources at 4 Mbps pause for 200–500 ms
	// regularly during HLS chunk fetches, and bursty HLS-pull sources
	// commonly deliver one ~6 s chunk per ~6 s of wallclock — between
	// chunks there's a ~5 s legitimate silence. A 3 s threshold fired
	// on every HLS chunk gap, producing `EXT-X-DISCONTINUITY` on every
	// HLS segment and spurious `SessionStartStallRecovery` events.
	// 15 s tolerates typical HLS-pull cadence (with margin) while still
	// catching real stalls quickly enough to signal before the player's
	// buffer underruns (timeShiftBufferDepth defaults to 24 s).
	DefaultStallThreshold = 15 * time.Second
	// DefaultStallCheckInterval is how often the watchdog ticks. Smaller
	// than DefaultStallThreshold so detection latency is bounded.
	DefaultStallCheckInterval = 1 * time.Second
)

// runStallWatchdog blocks until ctx is cancelled, emitting a session
// boundary on `buf` for `streamID` whenever the gap since lastWriteAt
// exceeds `threshold`. Safe to run as a goroutine alongside the
// worker's read loop; the readLoop bumps lastWriteAt on every
// successful buffer.Write.
//
// Production callers pass DefaultStallThreshold + DefaultStallCheckInterval;
// tests pass shorter values to avoid > 15 s real-time waits. Passing the
// knobs in rather than reading shared vars keeps the goroutine race-free
// across tests that override them.
func runStallWatchdog(
	ctx context.Context,
	streamID, bufferWriteID domain.StreamCode,
	buf *buffer.Service,
	lastWriteAt *atomic.Int64,
	threshold, interval time.Duration,
	onStall func(),
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	stallSignaled := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			last := time.Unix(0, lastWriteAt.Load())
			gap := time.Since(last)
			switch {
			case gap >= threshold && !stallSignaled:
				slog.Info("ingestor: source stall detected, signalling session boundary",
					"stream_code", streamID,
					"gap_ms", gap.Milliseconds(),
					"threshold_ms", threshold.Milliseconds(),
				)
				buf.SetSession(bufferWriteID, domain.SessionStartStallRecovery, nil, nil)
				if onStall != nil {
					onStall()
				}
				stallSignaled = true
			case gap < threshold && stallSignaled:
				// A fresh write reset the clock — clear the latch so a
				// later stall on the same readLoop can fire again.
				stallSignaled = false
			}
		}
	}
}
