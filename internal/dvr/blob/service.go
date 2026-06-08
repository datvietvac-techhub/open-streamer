package blob

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/samber/do/v2"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

// ProfileSub identifies one rendition lane the blob archive should record. The
// coordinator builds these from buffer.RenditionsForTranscoder.
type ProfileSub struct {
	ID            string            // "pN" = rendition index (dir name)
	Slug          string            // "track_1" (catalog buffer_slug)
	BufferID      domain.StreamCode // buffer to subscribe (RenditionBufferID or stream code)
	Width         int
	Height        int
	BitrateKbps   int
	IsAudioSource bool // exactly one true; this lane records .cmfa
}

// Service records streams into the CMAF blob archive. One recording per stream,
// one lane goroutine per profile; the per-recording mutex serialises catalog
// mutations from the lanes.
type Service struct {
	buf     *buffer.Service
	recRepo store.RecordingRepository

	mu     sync.Mutex
	active map[domain.StreamCode]*recording
}

// New constructs the blob DVR Service and registers it with DI.
func New(i do.Injector) (*Service, error) {
	return &Service{
		buf:     do.MustInvoke[*buffer.Service](i),
		recRepo: do.MustInvoke[store.RecordingRepository](i),
		active:  make(map[domain.StreamCode]*recording),
	}, nil
}

type recording struct {
	streamDir string
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	mu        sync.Mutex // guards cat + originSet
	cat       *Catalog
	originSet bool
}

// IsRecording reports whether a blob recording is active for code.
func (s *Service) IsRecording(code domain.StreamCode) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.active[code]
	return ok
}

// StartRecording begins (or resumes) a blob recording for code across the given
// profiles. It repairs any crash-dirty hours first, then starts one lane per
// profile. Returns a minimal Recording (ID = stream code, SegmentDir = the
// per-stream archive root holding catalog.json).
func (s *Service) StartRecording(ctx context.Context, code domain.StreamCode, profiles []ProfileSub, cfg *domain.StreamDVRConfig) (*domain.Recording, error) {
	if len(profiles) == 0 {
		return nil, fmt.Errorf("blob: start %s: no profiles", code)
	}
	root := domain.DefaultDVRRoot
	if cfg != nil && cfg.StoragePath != "" {
		root = cfg.StoragePath
	}
	streamDir, err := ResolveStreamDir(root, string(code))
	if err != nil {
		return nil, err
	}
	if err := ensureDir(streamDir); err != nil {
		return nil, err
	}
	if err := RepairStream(streamDir); err != nil {
		return nil, err
	}

	s.mu.Lock()
	if _, ok := s.active[code]; ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("blob: already recording %s", code)
	}
	rctx, cancel := context.WithCancel(context.WithoutCancel(ctx)) //nolint:contextcheck // lane outlives the request
	rec := &recording{streamDir: streamDir, cancel: cancel, cat: newCatalog(code, profiles, cfg)}
	s.active[code] = rec
	s.mu.Unlock()

	if err := rec.cat.Save(streamDir); err != nil {
		slog.Warn("blob: initial catalog save failed", "stream_code", code, "err", err)
	}
	// Persist a Recording row (SegmentDir = the archive root) so the media
	// dispatcher resolves the stream dir the same way it does for legacy DVR;
	// HasCatalog(SegmentDir) then routes serving to the blob handler.
	if s.recRepo != nil {
		if err := s.recRepo.Save(ctx, &domain.Recording{
			ID: domain.RecordingID(code), StreamCode: code,
			StartedAt: time.Now().UTC(), SegmentDir: streamDir,
		}); err != nil {
			slog.Warn("blob: recording row save failed", "stream_code", code, "err", err)
		}
	}
	segDur := segDurFromCfg(cfg)
	for _, p := range profiles {
		rec.wg.Add(1)
		go s.runLane(rctx, rec, p, segDur)
	}
	rec.wg.Add(1)
	go s.runRetention(rctx, rec, cfg)
	slog.Info("blob: recording started", "stream_code", code, "profiles", len(profiles), "dir", streamDir)
	return &domain.Recording{ID: domain.RecordingID(code), StreamCode: code, SegmentDir: streamDir}, nil
}

// StopRecording stops the recording for code (idempotent), waiting for every
// lane to seal its in-progress hour before returning.
func (s *Service) StopRecording(_ context.Context, code domain.StreamCode) error {
	s.mu.Lock()
	rec, ok := s.active[code]
	if ok {
		delete(s.active, code)
	}
	s.mu.Unlock()
	if !ok {
		return nil
	}
	rec.cancel()
	rec.wg.Wait()
	rec.mu.Lock()
	err := rec.cat.Save(rec.streamDir)
	rec.mu.Unlock()
	slog.Info("blob: recording stopped", "stream_code", code)
	return err
}

// runLane is the per-profile recorder: subscribe → ingest → cut → write. It
// drives the writer synchronously on this goroutine (v1); the split
// receive/flush topology is a later refinement.
func (s *Service) runLane(ctx context.Context, rec *recording, p ProfileSub, segDur time.Duration) {
	defer rec.wg.Done()
	sub, err := s.buf.Subscribe(p.BufferID)
	if err != nil {
		slog.Warn("blob: lane subscribe failed", "profile", p.ID, "buffer", p.BufferID, "err", err)
		return
	}
	defer s.buf.Unsubscribe(p.BufferID, sub)

	w := newProfileWriter(rec.streamDir, p.ID, p.IsAudioSource, segDur, rec.hourSink(p.ID))
	defer func() { _ = w.Close() }()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	originReported := false
	for {
		select {
		case <-ctx.Done():
			return
		case pkt, ok := <-sub.Recv():
			if !ok {
				return // buffer destroyed
			}
			if err := w.Ingest(pkt, time.Now()); err != nil {
				// A per-packet write error must not kill the recording — log and
				// keep going so a transient fault (or a single bad packet) can't
				// permanently stop the lane.
				slog.Warn("blob: lane ingest error (continuing)", "profile", p.ID, "err", err)
				continue
			}
			if !originReported {
				if _, wallMs, set := w.Origin(); set {
					rec.setOrigin(wallMs)
					originReported = true
				}
			}
		case <-ticker.C:
			if err := w.Tick(time.Now()); err != nil {
				slog.Warn("blob: lane tick error (continuing)", "profile", p.ID, "err", err)
				continue
			}
		}
	}
}

// setOrigin records the wall<->media anchor from the first lane to report it.
func (r *recording) setOrigin(wallMs int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.originSet {
		return
	}
	r.cat.RecordingMediaOriginUnixMs = wallMs
	r.cat.RecordingMediaOriginTicks = 0
	r.originSet = true
	if err := r.cat.Save(r.streamDir); err != nil {
		slog.Warn("blob: catalog origin save failed", "err", err)
	}
}

// hourSink returns the writer's sink callback for profileID — it folds a sealed
// hour into the catalog under the per-recording mutex and persists it.
func (r *recording) hourSink(profileID string) hourRecordSink {
	return func(_ string, hr HourRecord) {
		r.mu.Lock()
		defer r.mu.Unlock()
		pd, ok := r.cat.Profile(profileID)
		if !ok {
			return
		}
		upsertHour(pd, hr)
		extendAvailable(pd, hr)
		if err := r.cat.Save(r.streamDir); err != nil {
			slog.Warn("blob: catalog hour save failed", "profile", profileID, "err", err)
		}
	}
}

// upsertHour replaces an hour record with the same key, else appends it.
func upsertHour(pd *ProfileDesc, hr HourRecord) {
	for i := range pd.Hours {
		if pd.Hours[i].Hour == hr.Hour {
			pd.Hours[i] = hr
			return
		}
	}
	pd.Hours = append(pd.Hours, hr)
}

// extendAvailable grows the profile's last coverage window to include the hour
// (or starts a new window).
func extendAvailable(pd *ProfileDesc, hr HourRecord) {
	if n := len(pd.Available); n > 0 && hr.WallFromMs <= pd.Available[n-1].ToMs+1 {
		if hr.WallToMs > pd.Available[n-1].ToMs {
			pd.Available[n-1].ToMs = hr.WallToMs
		}
		return
	}
	pd.Available = append(pd.Available, MediaWindow{FromMs: hr.WallFromMs, ToMs: hr.WallToMs})
}

// newCatalog builds the initial per-stream catalog from the profile list.
func newCatalog(code domain.StreamCode, profiles []ProfileSub, cfg *domain.StreamDVRConfig) *Catalog {
	cat := &Catalog{
		StreamCode:     string(code),
		Format:         CatalogFormat,
		VideoTimescale: videoTimescale,
	}
	for _, p := range profiles {
		cat.Profiles = append(cat.Profiles, ProfileDesc{
			ID: p.ID, BufferSlug: p.Slug,
			Width: p.Width, Height: p.Height, Bandwidth: p.BitrateKbps * 1000,
		})
		if p.IsAudioSource {
			cat.AudioProfile = p.ID
			cat.BestProfile = p.ID
		}
	}
	if cat.BestProfile == "" && len(profiles) > 0 {
		cat.BestProfile = profiles[len(profiles)-1].ID
	}
	if cfg != nil {
		cat.Retention = RetentionCfg{MaxAgeSec: cfg.RetentionSec, MaxSizeGB: int(cfg.MaxSizeGB)}
	}
	return cat
}

// segDurFromCfg resolves the fragment target duration.
func segDurFromCfg(cfg *domain.StreamDVRConfig) time.Duration {
	if cfg != nil && cfg.SegmentDuration > 0 {
		return time.Duration(cfg.SegmentDuration) * time.Second
	}
	return DefaultSegDur
}
