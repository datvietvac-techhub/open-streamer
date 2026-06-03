package publisher

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/publisher/dash"
)

// serveDASH starts a single-rendition MPEG-DASH publisher for streamID.
// Output is built by the dash package.
func (s *Service) serveDASH(ctx context.Context, streamID domain.StreamCode) {
	cfg := s.cfg.DASH
	streamDir := filepath.Join(cfg.Dir, string(streamID))

	if err := os.MkdirAll(streamDir, 0o755); err != nil {
		slog.Error("publisher: DASH setup dir failed",
			"stream_code", streamID, "dir", streamDir, "err", err)
		return
	}

	sub, err := s.buf.Subscribe(streamID)
	if err != nil {
		slog.Error("publisher: DASH subscribe failed",
			"stream_code", streamID, "err", err)
		return
	}
	defer s.buf.Unsubscribe(streamID, sub)

	packager, err := dash.NewPackager(dash.Config{
		StreamID:     string(streamID),
		StreamDir:    streamDir,
		ManifestPath: filepath.Join(streamDir, "index.mpd"),
		SegDur:       time.Duration(segSecOrDefault(cfg.LiveSegmentSec)) * time.Second,
		Window:       windowOrDefault(cfg.LiveWindow),
		History:      historyOrDefault(cfg.LiveHistory),
		Ephemeral:    cfg.LiveEphemeral,
		PackAudio:    true,
	})
	if err != nil {
		slog.Error("publisher: DASH packager init failed",
			"stream_code", streamID, "err", err)
		return
	}

	slog.Info("publisher: DASH serve started",
		"stream_code", streamID, "dash_dir", streamDir)
	packager.Run(ctx, sub)
	slog.Info("publisher: DASH serve stopped", "stream_code", streamID)
}

// segSecOrDefault returns segSec when positive, otherwise the
// domain-level default. Mirrored across the dash + hls callers.
func segSecOrDefault(segSec int) int {
	if segSec <= 0 {
		return domain.DefaultLiveSegmentSec
	}
	return segSec
}

func windowOrDefault(window int) int {
	if window <= 0 {
		return domain.DefaultLiveWindow
	}
	return window
}

func historyOrDefault(history int) int {
	if history < 0 {
		return domain.DefaultLiveHistory
	}
	return history
}
