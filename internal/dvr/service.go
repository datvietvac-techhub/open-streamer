// Package dvr implements the DVR (Digital Video Recorder).
//
// Storage layout per stream:
//
//	./dvr/{streamCode}/
//	  index.json      — lightweight metadata: stream info, segment count, gaps
//	  playlist.m3u8   — HLS EVENT/VOD playlist with EXT-X-PROGRAM-DATE-TIME
//	  000000.ts
//	  000001.ts
//	  ...
//
// index.json is intentionally lean — no per-segment details.
// Per-segment timeline (wall time, duration, discontinuity) lives in playlist.m3u8.
//
// On server restart: index.json + playlist.m3u8 are read to resume state.
// The first new segment after resume is tagged #EXT-X-DISCONTINUITY.
//
// On signal loss (gap > 2× segment duration): partial segment is flushed,
// TS muxer is reset, gap is recorded in index.json, next segment tagged
// #EXT-X-DISCONTINUITY with updated #EXT-X-PROGRAM-DATE-TIME.
package dvr

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/metrics"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
	"github.com/datvietvac-techhub/open-streamer/internal/tsmux"
	"github.com/datvietvac-techhub/open-streamer/pkg/logger"
	"github.com/samber/do/v2"
)

const (
	defaultRootDir         = domain.DefaultDVRRoot
	defaultSegmentDuration = time.Duration(domain.DefaultDVRSegmentDuration) * time.Second
	// gapTimeoutFactor: gap timer = factor × segmentDuration.
	gapTimeoutFactor = 2
)

// recordingSession tracks an in-memory active DVR recording.
//
// `streamCode` and `recordingID` are immutable copies of the corresponding
// `recording` fields, captured once at session creation. The recording
// goroutine (record → flushSegment → applyRetention) reads these instead
// of `recording.*` so log/event/metric formatting is race-free against
// `StopRecording` which sets `recording.StoppedAt` and `Status` from the
// caller goroutine.
//
// `done` is closed by the recording goroutine when it exits. StopRecording
// waits on it before mutating `recording.{StoppedAt,Status}` so the goroutine
// has finished its last `recRepo.Save(sess.recording)` call (which
// reads the struct as a whole) before any field is written. Without
// this synchronisation the race detector trips reliably under retention
// pruning loads where the goroutine is still flushing when the test
// stops the recording.
type recordingSession struct {
	recording   *domain.Recording
	streamCode  domain.StreamCode
	recordingID domain.RecordingID
	index       *domain.DVRIndex
	segments    []segmentMeta // in-memory per-segment state, NOT in index.json
	dvrCfg      *domain.StreamDVRConfig
	cancel      context.CancelFunc
	done        chan struct{}
}

// Service manages DVR recording sessions.
type Service struct {
	buf     *buffer.Service
	bus     events.Bus
	m       *metrics.Metrics
	recRepo store.RecordingRepository

	mu       sync.Mutex
	sessions map[domain.StreamCode]*recordingSession
}

// NewForTesting builds a Service from pre-constructed deps. Used by unit
// tests that wire fakes / minimal real implementations and skip the DI
// plumbing.
func NewForTesting(buf *buffer.Service, bus events.Bus, m *metrics.Metrics, recRepo store.RecordingRepository) *Service {
	return &Service{
		buf:      buf,
		bus:      bus,
		m:        m,
		recRepo:  recRepo,
		sessions: make(map[domain.StreamCode]*recordingSession),
	}
}

// New creates a Service and registers it with the DI injector.
func New(i do.Injector) (*Service, error) {
	buf := do.MustInvoke[*buffer.Service](i)
	bus := do.MustInvoke[events.Bus](i)
	m := do.MustInvoke[*metrics.Metrics](i)
	recRepo := do.MustInvoke[store.RecordingRepository](i)

	return &Service{
		buf:      buf,
		bus:      bus,
		m:        m,
		recRepo:  recRepo,
		sessions: make(map[domain.StreamCode]*recordingSession),
	}, nil
}

// StartRecording begins (or resumes) recording for the given stream.
func (s *Service) StartRecording(ctx context.Context, streamID domain.StreamCode, mediaBufferID domain.StreamCode, dvrCfg *domain.StreamDVRConfig) (*domain.Recording, error) {
	if dvrCfg == nil || !dvrCfg.Enabled {
		return nil, fmt.Errorf("dvr: not enabled for stream %s", streamID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sessions[streamID]; ok {
		return nil, fmt.Errorf("dvr: stream %s already recording", streamID)
	}

	// Resolve segment directory.
	rootDir := defaultRootDir
	if dvrCfg.StoragePath != "" {
		rootDir = dvrCfg.StoragePath
	}
	segDir, err := resolveSegDir(rootDir, string(streamID))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		return nil, fmt.Errorf("dvr: mkdir %s: %w", segDir, err)
	}

	// Load existing index.json (resume) or create fresh.
	idx, err := loadIndex(segDir)
	if err != nil {
		return nil, fmt.Errorf("dvr: load index: %w", err)
	}
	resuming := idx != nil && idx.SegmentCount > 0
	if idx == nil {
		idx = &domain.DVRIndex{
			StreamCode: streamID,
			StartedAt:  time.Now(),
		}
	}

	// Rebuild in-memory segment list from playlist.m3u8 (needed for retention).
	segments, err := parsePlaylist(segDir)
	if err != nil {
		slog.Warn("dvr: parse playlist failed on resume, starting fresh",
			"stream_code", streamID, "err", err)
		segments = nil
	}

	// Load or create Recording (upsert by stream code = ID).
	recID := domain.RecordingID(streamID)
	rec, err := s.recRepo.FindByID(ctx, recID)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, fmt.Errorf("dvr: load recording: %w", err)
		}
		rec = &domain.Recording{
			ID:         recID,
			StreamCode: streamID,
			StartedAt:  time.Now(),
		}
	}
	rec.Status = domain.RecordingStatusRecording
	rec.StoppedAt = nil
	rec.SegmentDir = segDir

	if err := s.recRepo.Save(ctx, rec); err != nil {
		return nil, fmt.Errorf("dvr: save recording: %w", err)
	}

	segDur := defaultSegmentDuration
	if dvrCfg.SegmentDuration > 0 {
		segDur = time.Duration(dvrCfg.SegmentDuration) * time.Second
	}

	subscribeID := mediaBufferID
	if subscribeID == "" {
		subscribeID = streamID
	}

	workerCtx, cancel := context.WithCancel(ctx)
	sess := &recordingSession{
		recording:   rec,
		streamCode:  rec.StreamCode,
		recordingID: rec.ID,
		index:       idx,
		segments:    segments,
		dvrCfg:      dvrCfg,
		cancel:      cancel,
		done:        make(chan struct{}),
	}
	s.sessions[streamID] = sess

	nextIdx := idx.SegmentCount
	go func() {
		defer close(sess.done)
		s.record(workerCtx, sess, subscribeID, segDir, nextIdx, resuming, segDur)
	}()

	if s.m != nil {
		s.m.DVRRecordingActive.WithLabelValues(string(streamID)).Set(1)
	}

	s.bus.Publish(ctx, domain.Event{
		Type:       domain.EventRecordingStarted,
		StreamCode: streamID,
		Payload:    map[string]any{"recording_id": rec.ID},
	})

	slog.Info("dvr: recording started",
		"stream_code", streamID,
		"next_segment", nextIdx,
		"resuming", resuming,
		"seg_duration", segDur,
	)
	return rec, nil
}

// StopRecording stops the active recording and writes a final VOD playlist.
func (s *Service) StopRecording(ctx context.Context, streamID domain.StreamCode) error {
	s.mu.Lock()
	sess, ok := s.sessions[streamID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("dvr: no active recording for stream %s", streamID)
	}
	sess.cancel()
	delete(s.sessions, streamID)
	s.mu.Unlock()

	// Wait for the recording goroutine to fully drain (final flushSegment
	// write + last recRepo.Save with the in-flight struct) before we mutate
	// sess.recording's StoppedAt/Status. Without this barrier the goroutine's
	// `s.recRepo.Save(ctx, sess.recording)` reads the struct concurrently with
	// the writes below and the race detector trips under retention-pruning
	// loads. Bounded by ctx — a misbehaving goroutine can't pin StopRecording
	// forever.
	select {
	case <-sess.done:
	case <-ctx.Done():
		// ctx cancelled before goroutine drained — proceed anyway; the
		// goroutine will exit on its own thanks to sess.cancel() above.
	}

	now := time.Now()
	sess.recording.StoppedAt = &now
	sess.recording.Status = domain.RecordingStatusStopped

	if err := s.recRepo.Save(ctx, sess.recording); err != nil {
		return fmt.Errorf("dvr: update recording: %w", err)
	}

	if s.m != nil {
		s.m.DVRRecordingActive.WithLabelValues(string(streamID)).Set(0)
	}

	s.bus.Publish(ctx, domain.Event{
		Type:       domain.EventRecordingStopped,
		StreamCode: streamID,
		Payload:    map[string]any{"recording_id": sess.recording.ID},
	})

	slog.Info("dvr: recording stopped",
		"stream_code", streamID,
		"segments", sess.index.SegmentCount,
	)
	return nil
}

// IsRecording reports whether there is an active recording session for streamID.
func (s *Service) IsRecording(streamID domain.StreamCode) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.sessions[streamID]
	return ok
}

// LoadIndex reads index.json for a stream's recording directory.
// Exported for the API info handler.
func (s *Service) LoadIndex(segDir string) (*domain.DVRIndex, error) {
	return loadIndex(segDir)
}

// ParsePlaylist reads playlist.m3u8 and returns the ordered segment metadata.
// Exported for the timeshift handler.
func (s *Service) ParsePlaylist(segDir string) ([]SegmentMeta, error) {
	segs, err := parsePlaylist(segDir)
	if err != nil {
		return nil, err
	}
	out := make([]SegmentMeta, len(segs))
	for i, seg := range segs {
		out[i] = SegmentMeta{
			Index:         seg.index,
			WallTime:      seg.wallTime,
			Duration:      seg.duration,
			Discontinuity: seg.discontinuity,
		}
	}
	return out, nil
}

// SegmentMeta is the exported view of a parsed playlist segment.
type SegmentMeta struct {
	Index         int
	WallTime      time.Time
	Duration      time.Duration
	Discontinuity bool
}

// ---- record loop ----

func (s *Service) record(
	ctx context.Context,
	sess *recordingSession,
	subscribeID domain.StreamCode,
	segDir string,
	startIdx int,
	startWithDiscontinuity bool,
	segDur time.Duration,
) {
	sub, err := s.buf.Subscribe(subscribeID)
	if err != nil {
		slog.Error("dvr: subscribe failed",
			"stream_code", sess.recording.StreamCode, "buffer_id", subscribeID, "err", err)
		return
	}
	defer s.buf.Unsubscribe(subscribeID, sub)

	var (
		segBuf               []byte
		segIdx               = startIdx
		avMux                *tsmux.FromAV
		pendingDiscontinuity = startWithDiscontinuity

		segWallStart  = time.Now()
		segStartPTSms uint64
		segDurMS      = uint64(segDur.Milliseconds())
		hasPTS        bool

		gapStartWall time.Time
		inGap        bool
	)

	gapDur := time.Duration(gapTimeoutFactor) * segDur
	gapTimer := time.NewTimer(gapDur)
	defer gapTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(segBuf) > 0 {
				s.flushSegment(ctx, sess, segIdx, segBuf,
					mediaDur(hasPTS, segWallStart, segStartPTSms, 0),
					segWallStart, pendingDiscontinuity, segDir)
			}
			writePlaylist(sess.segments, segDir, true)
			return

		case <-gapTimer.C:
			if len(segBuf) > 0 {
				s.flushSegment(ctx, sess, segIdx, segBuf,
					mediaDur(hasPTS, segWallStart, segStartPTSms, 0),
					segWallStart, pendingDiscontinuity, segDir)
				segBuf = segBuf[:0]
				segIdx++
			}
			avMux = nil
			hasPTS = false
			pendingDiscontinuity = true
			gapStartWall = time.Now()
			inGap = true
			segWallStart = gapStartWall
			slog.Warn("dvr: stream gap detected",
				"stream_code", sess.recording.StreamCode, "next_segment", segIdx)

		case pkt, ok := <-sub.Recv():
			if !ok {
				return
			}

			// Reset gap timer.
			if !gapTimer.Stop() {
				select {
				case <-gapTimer.C:
				default:
				}
			}
			gapTimer.Reset(gapDur)

			// Record gap when stream resumes.
			if inGap {
				now := time.Now()
				gap := domain.DVRGap{
					From:     gapStartWall,
					To:       now,
					Duration: now.Sub(gapStartWall),
				}
				sess.index.Gaps = append(sess.index.Gaps, gap)
				inGap = false
				segWallStart = now
				slog.Info("dvr: gap recorded",
					"stream_code", sess.recording.StreamCode,
					"from", gap.From.Format(time.RFC3339),
					"to", gap.To.Format(time.RFC3339),
					"duration", gap.Duration.Round(time.Second),
				)
			}

			// Session boundary: flush the in-progress segment (if any)
			// so old-session bytes land in their own segment and the
			// next segment starts cleanly under the new session. Mark
			// the upcoming segment as discontinuous so the recording
			// index records the boundary alongside any gaps.
			//
			// Previously DVR only segmented on gap detection (timer
			// firing when no packets arrive for gapDur). Same-input
			// reconnects that finished within the gap window were
			// silently glued together, hiding source switches in the
			// recording. Reading buffer.Packet.SessionStart makes
			// session breaks first-class — every Normaliser-anchored
			// session transition produces a clean segment boundary.
			if pkt.SessionStart {
				if len(segBuf) > 0 {
					dur := mediaDur(hasPTS, segWallStart, segStartPTSms, 0)
					s.flushSegment(ctx, sess, segIdx, segBuf, dur,
						segWallStart, pendingDiscontinuity, segDir)
					segBuf = segBuf[:0]
					segIdx++
				}
				avMux = nil
				hasPTS = false
				pendingDiscontinuity = true
				segWallStart = time.Now()
			}

			var pktPTSms uint64
			if pkt.AV != nil {
				pktPTSms = pkt.AV.PTSms
			}
			if !hasPTS && pktPTSms > 0 {
				segStartPTSms = pktPTSms
				hasPTS = true
			}

			tsmux.FeedWirePacket(ctx, pkt.TS, pkt.AV, &avMux, func(b []byte) {
				segBuf = append(segBuf, b...)
			})

			shouldCut := (hasPTS && pktPTSms > segStartPTSms && (pktPTSms-segStartPTSms) >= segDurMS) ||
				(!hasPTS && time.Since(segWallStart) >= segDur)

			if shouldCut {
				dur := mediaDur(hasPTS, segWallStart, segStartPTSms, pktPTSms)
				s.flushSegment(ctx, sess, segIdx, segBuf, dur, segWallStart, pendingDiscontinuity, segDir)
				pendingDiscontinuity = false
				segBuf = segBuf[:0]
				segIdx++
				hasPTS = false
				segWallStart = time.Now()
				if pktPTSms > 0 {
					segStartPTSms = pktPTSms
					hasPTS = true
				}
			}
		}
	}
}

// ---- flush ----

func (s *Service) flushSegment(
	ctx context.Context,
	sess *recordingSession,
	segIdx int,
	data []byte,
	dur time.Duration,
	wallTime time.Time,
	discontinuity bool,
	segDir string,
) {
	if len(data) == 0 {
		return
	}

	filename := fmt.Sprintf("%06d.ts", segIdx)
	absPath := filepath.Join(segDir, filename)

	if err := os.WriteFile(absPath, data, 0o644); err != nil {
		slog.Error("dvr: write segment failed", "path", absPath, "err", err)
		if s.m != nil {
			s.m.DVRSegmentWriteErrorsTotal.WithLabelValues(string(sess.recording.StreamCode)).Inc()
		}
		s.bus.Publish(context.WithoutCancel(ctx), domain.Event{
			Type:       domain.EventRecordingFailed,
			StreamCode: sess.recording.StreamCode,
			Payload: map[string]any{
				"recording_id": sess.recording.ID,
				"segment":      filename,
				"error":        err.Error(),
			},
		})
		return
	}

	size := int64(len(data))

	// Update in-memory segment list.
	sess.segments = append(sess.segments, segmentMeta{
		index:         segIdx,
		wallTime:      wallTime,
		duration:      dur,
		size:          size,
		discontinuity: discontinuity,
	})

	// Update index (no segments stored here).
	sess.index.SegmentCount++
	sess.index.TotalSizeBytes += size
	sess.index.LastSegmentAt = wallTime

	s.applyRetention(sess, segDir) //nolint:contextcheck // applyRetention publishes events with intentional Background ctx (segmenter loop, not request-scoped)
	writePlaylist(sess.segments, segDir, false)

	if err := saveIndex(segDir, sess.index); err != nil {
		slog.Warn("dvr: save index failed", "stream_code", sess.recording.StreamCode, "err", err)
	}
	if err := s.recRepo.Save(ctx, sess.recording); err != nil {
		slog.Warn("dvr: save recording failed", "stream_code", sess.recording.StreamCode, "err", err)
	}

	if s.m != nil {
		s.m.DVRSegmentsWrittenTotal.WithLabelValues(string(sess.recording.StreamCode)).Inc()
		s.m.DVRBytesWrittenTotal.WithLabelValues(string(sess.recording.StreamCode)).Add(float64(size))
	}
	s.bus.Publish(context.WithoutCancel(ctx), domain.Event{
		Type:       domain.EventSegmentWritten,
		StreamCode: sess.recording.StreamCode,
		Payload: map[string]any{
			"recording_id":  sess.recording.ID,
			"segment":       filename,
			"duration_sec":  dur.Seconds(),
			"size_bytes":    size,
			"wall_time":     wallTime.Format(time.RFC3339),
			"discontinuity": discontinuity,
		},
	})

	logger.Trace("dvr: segment flushed",
		"stream_code", sess.recording.StreamCode,
		"index", segIdx,
		"size_bytes", size,
		"duration", dur.Round(time.Millisecond),
		"wall_time", wallTime.Format(time.RFC3339),
		"discontinuity", discontinuity,
	)
}

// ---- playlist ----

// writePlaylist writes playlist.m3u8 from the in-memory segment list.
// Includes #EXT-X-PROGRAM-DATE-TIME before first segment and after each DISCONTINUITY.
// ended=true → VOD + #EXT-X-ENDLIST; false → EVENT.
func writePlaylist(segments []segmentMeta, segDir string, ended bool) {
	if len(segments) == 0 {
		return
	}

	maxSec := 1.0
	for _, seg := range segments {
		if d := seg.duration.Seconds(); d > maxSec {
			maxSec = d
		}
	}
	targetDur := int(math.Ceil(maxSec))

	playlistType := "EVENT"
	if ended {
		playlistType = "VOD"
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:3\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", targetDur)
	fmt.Fprintf(&b, "#EXT-X-PLAYLIST-TYPE:%s\n", playlistType)
	b.WriteByte('\n')

	needDateTime := true
	for _, seg := range segments {
		if seg.discontinuity {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
			needDateTime = true
		}
		if needDateTime && !seg.wallTime.IsZero() {
			fmt.Fprintf(&b, "#EXT-X-PROGRAM-DATE-TIME:%s\n",
				seg.wallTime.UTC().Format("2006-01-02T15:04:05.000Z"))
			needDateTime = false
		}
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n", seg.duration.Seconds())
		fmt.Fprintf(&b, "%06d.ts\n", seg.index)
	}

	if ended {
		b.WriteString("#EXT-X-ENDLIST\n")
	}

	playlistPath := filepath.Join(segDir, "playlist.m3u8")
	if err := os.WriteFile(playlistPath, []byte(b.String()), 0o644); err != nil {
		slog.Warn("dvr: write playlist failed", "path", playlistPath, "err", err)
	}
}

// ---- retention ----

func (s *Service) applyRetention(sess *recordingSession, segDir string) {
	dvrCfg := sess.dvrCfg
	if dvrCfg == nil {
		return
	}

	var retentionDur time.Duration
	if dvrCfg.RetentionSec > 0 {
		retentionDur = time.Duration(dvrCfg.RetentionSec) * time.Second
	}
	var maxSizeBytes int64
	if dvrCfg.MaxSizeGB > 0 {
		maxSizeBytes = int64(dvrCfg.MaxSizeGB * 1024 * 1024 * 1024)
	}
	if retentionDur == 0 && maxSizeBytes == 0 {
		return
	}

	for len(sess.segments) > 1 {
		oldest := sess.segments[0]
		expired := retentionDur > 0 && time.Since(oldest.wallTime) > retentionDur
		overSize := maxSizeBytes > 0 && sess.index.TotalSizeBytes > maxSizeBytes

		if !expired && !overSize {
			break
		}

		path := filepath.Join(segDir, fmt.Sprintf("%06d.ts", oldest.index))
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			slog.Warn("dvr: remove old segment failed", "path", path, "err", err)
		}
		sess.index.TotalSizeBytes -= oldest.size
		sess.index.SegmentCount--
		sess.segments = sess.segments[1:]

		// Emit one event per pruned segment so capacity / cost dashboards
		// and downstream archivers can react. Reason classifies the trigger
		// because operators alerting on aggressive retention want to know
		// whether they hit the time floor or the size cap. Bus delivery is
		// async via the worker pool so we don't slow the segmenter loop.
		reason := "size"
		if expired {
			reason = "age"
		}
		// Background context is intentional — applyRetention runs from
		// the long-lived segmenter loop, not a request-scoped path, so
		// no caller cancellation to honour.
		// Read sess.streamCode / sess.recordingID (immutable copies set at
		// session creation) instead of dereferencing sess.recording — the
		// recording goroutine runs concurrently with StopRecording's
		// `sess.recording.StoppedAt = &now` write, and even reading an
		// unrelated field of the same pointer trips the race detector.
		// See recordingSession docstring.
		if s.bus != nil {
			s.bus.Publish(context.Background(), domain.Event{ //nolint:contextcheck // intentional Background
				Type:       domain.EventDVRSegmentPruned,
				StreamCode: sess.streamCode,
				Payload: map[string]any{
					"recording_id":  sess.recordingID,
					"segment_index": oldest.index,
					"size_bytes":    oldest.size,
					"reason":        reason,
				},
			})
		}
		if s.m != nil && s.m.DVRRetentionPrunedBytes != nil {
			s.m.DVRRetentionPrunedBytes.WithLabelValues(string(sess.streamCode), reason).Add(float64(oldest.size))
		}

		// Prune gaps that ended before the new oldest segment's wall time.
		if len(sess.segments) > 0 {
			newOldest := sess.segments[0].wallTime
			kept := sess.index.Gaps[:0]
			for _, g := range sess.index.Gaps {
				if g.To.After(newOldest) {
					kept = append(kept, g)
				}
			}
			sess.index.Gaps = kept
		}
	}
}

// ---- helpers ----

func mediaDur(hasPTS bool, segWallStart time.Time, startPTSms, endPTSms uint64) time.Duration {
	if hasPTS && endPTSms > startPTSms {
		return time.Duration(endPTSms-startPTSms) * time.Millisecond
	}
	return time.Since(segWallStart)
}
