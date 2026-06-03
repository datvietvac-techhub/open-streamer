package publisher

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/publisher/dash"
)

// serveDASHAdaptive starts one DASH Packager goroutine per ABR
// rendition and a single dash.ABRMaster that maintains the root MPD.
//
// When the stream has no rendition ladder (transcoder not configured),
// falls back to single-rendition serveDASH.
func (s *Service) serveDASHAdaptive(ctx context.Context, stream *domain.Stream) {
	renditions := buffer.RenditionsForTranscoder(stream.Code, stream.Transcoder)
	if len(renditions) == 0 {
		mediaBuf, _ := s.mediaBufferFor(stream.Code)
		s.serveDASH(ctx, mediaBuf)
		return
	}

	cfg := s.cfg.DASH
	rootDir := filepath.Join(cfg.Dir, string(stream.Code))

	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		slog.Error("publisher: DASH ABR setup dir failed",
			"stream_code", stream.Code, "err", err)
		return
	}

	rootManifest := filepath.Join(rootDir, "index.mpd")
	segDur := time.Duration(segSecOrDefault(cfg.LiveSegmentSec)) * time.Second
	window := windowOrDefault(cfg.LiveWindow)

	master := dash.NewABRMaster(rootManifest, string(stream.Code), segDur, window)
	bestIdx := buffer.BestRenditionIndex(renditions)

	slog.Info("publisher: DASH ABR serve started",
		"stream_code", stream.Code, "renditions", len(renditions), "dash_dir", rootDir)

	var wg sync.WaitGroup
	for i, r := range renditions {
		slug := r.Slug
		shardDir := filepath.Join(rootDir, slug)

		if err := os.MkdirAll(shardDir, 0o755); err != nil {
			slog.Error("publisher: DASH ABR setup shard dir failed",
				"stream_code", stream.Code, "slug", slug, "err", err)
			continue
		}

		sub, err := s.buf.Subscribe(r.BufferID)
		if err != nil {
			slog.Error("publisher: DASH ABR subscribe failed",
				"stream_code", stream.Code, "rendition", slug, "err", err)
			continue
		}

		packAudio := i == bestIdx
		bw := r.BandwidthBps()

		packager, err := dash.NewPackager(dash.Config{
			StreamID:          string(stream.Code) + "/" + slug,
			StreamDir:         shardDir,
			ManifestPath:      "", // no per-shard MPD; master writes the root
			SegDur:            segDur,
			Window:            window,
			History:           historyOrDefault(cfg.LiveHistory),
			Ephemeral:         cfg.LiveEphemeral,
			PackAudio:         packAudio,
			ABRMaster:         master,
			ABRSlug:           slug,
			OverrideBandwidth: bw,
		})
		if err != nil {
			slog.Error("publisher: DASH ABR packager init failed",
				"stream_code", stream.Code, "slug", slug, "err", err)
			s.buf.Unsubscribe(r.BufferID, sub)
			continue
		}

		wg.Add(1)
		go func(rSub *buffer.Subscriber, bufferID domain.StreamCode, p *dash.Packager) {
			defer wg.Done()
			defer s.buf.Unsubscribe(bufferID, rSub)
			p.Run(ctx, rSub)
		}(sub, r.BufferID, packager)
	}

	wg.Wait()
	master.Stop()
	slog.Info("publisher: DASH ABR serve stopped", "stream_code", stream.Code)
}
