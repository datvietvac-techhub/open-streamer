package publisher

// hls_abr.go — ABR HLS publisher.
//
// Topology:
//
//	Buffer (per rendition) ──► hlsSegmenter ──► <streamDir>/<slug>/seg_XXXXXX.ts
//	                                        └──► <streamDir>/<slug>/index.m3u8
//	                                        └──► hlsABRMaster.onShardUpdated()
//	                                                    │ debounced 100 ms
//	                                                    ▼
//	                                        <streamDir>/index.m3u8  (master playlist)
//
// The master playlist lists every ladder rung with EXT-X-STREAM-INF.  Per-rendition
// playlists are written by each shard's hlsSegmenter independently.

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ntt0601zcoder/open-streamer/internal/buffer"
	"github.com/ntt0601zcoder/open-streamer/internal/domain"
)

// hlsABRRep holds metadata about one ladder rung, shared with the master playlist writer.
type hlsABRRep struct {
	slug     string
	bwBps    int
	width    int
	height   int
	hasAudio bool // segments contain an audio track — drives the audio entry in CODECS
	hasData  bool // at least one segment has been flushed
}

// hlsABRMaster coordinates writing the root master HLS playlist.
// Each per-rendition hlsSegmenter calls onShardUpdated; the master debounces
// and rewrites the root index.m3u8 at most every 100 ms.
type hlsABRMaster struct {
	mu       sync.Mutex
	rootPath string // absolute path to the master index.m3u8
	streamID domain.StreamCode
	reps     map[string]*hlsABRRep
	debounce *time.Timer
	// overrides holds metadata set externally (e.g. after a profile bitrate/resolution
	// change). Values here take precedence over what segmenters report via onShardUpdated,
	// so the master playlist reflects the new profile immediately without waiting for the
	// next segment flush.
	overrides sync.Map // map[string]*hlsABRRep (slug → rep override)
}

func newHLSABRMaster(rootPath string, streamID domain.StreamCode) *hlsABRMaster {
	return &hlsABRMaster{
		rootPath: rootPath,
		streamID: streamID,
		reps:     make(map[string]*hlsABRRep),
	}
}

// onShardUpdated is called by each rendition segmenter after every segment flush.
// If SetRepOverride was called for this slug, its values take precedence so the master
// playlist reflects externally-set metadata (e.g. after a profile bitrate change).
//
// hasAudio drives the audio entry in CODECS — without it, browser HLS players
// (hls.js + MSE) addSourceBuffer with a video-only codec string and silently
// drop the audio track even though the .ts segment contains it. Safari's
// native HLS demuxer is more forgiving but still benefits from an accurate
// declaration.
func (m *hlsABRMaster) onShardUpdated(slug string, bwBps, width, height int, hasAudio bool) {
	// Apply external override when present (persists across segment flushes).
	if v, ok := m.overrides.Load(slug); ok {
		ov := v.(*hlsABRRep)
		bwBps = ov.bwBps
		width = ov.width
		height = ov.height
		hasAudio = ov.hasAudio
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	r := m.reps[slug]
	if r == nil {
		r = &hlsABRRep{}
		m.reps[slug] = r
	}
	r.slug = slug
	r.bwBps = bwBps
	r.width = width
	r.height = height
	r.hasAudio = hasAudio
	r.hasData = true

	if m.debounce != nil {
		m.debounce.Stop()
	}
	m.debounce = time.AfterFunc(100*time.Millisecond, m.flushRoot)
}

// SetRepOverride updates the stored metadata for one ABR rendition and immediately
// triggers a debounced master playlist rewrite. Future onShardUpdated calls for the
// same slug will continue using these values, keeping the master playlist accurate
// even before the first new segment arrives from the updated transcoder subprocess.
func (m *hlsABRMaster) SetRepOverride(slug string, bwBps, width, height int, hasAudio bool) {
	ov := &hlsABRRep{slug: slug, bwBps: bwBps, width: width, height: height, hasAudio: hasAudio}
	m.overrides.Store(slug, ov)

	m.mu.Lock()
	if r, ok := m.reps[slug]; ok && r.hasData {
		r.bwBps = bwBps
		r.width = width
		r.height = height
		r.hasAudio = hasAudio
	}
	if m.debounce != nil {
		m.debounce.Stop()
	}
	m.debounce = time.AfterFunc(50*time.Millisecond, m.flushRoot)
	m.mu.Unlock()
}

func (m *hlsABRMaster) flushRoot() {
	m.mu.Lock()
	slugs := make([]string, 0, len(m.reps))
	for s, r := range m.reps {
		if r.hasData {
			slugs = append(slugs, s)
		}
	}
	sort.Strings(slugs)
	repSnap := make([]hlsABRRep, 0, len(slugs))
	for _, s := range slugs {
		repSnap = append(repSnap, *m.reps[s])
	}
	rootPath := m.rootPath
	streamID := m.streamID
	m.mu.Unlock()

	if len(repSnap) == 0 {
		return
	}

	if err := writeHLSMasterPlaylist(rootPath, repSnap); err != nil {
		slog.Warn("publisher: HLS ABR master playlist write failed",
			"stream_code", streamID, "err", err)
	}
}

// writeHLSMasterPlaylist serialises an HLS master playlist and writes it atomically.
func writeHLSMasterPlaylist(path string, reps []hlsABRRep) error {
	var sb strings.Builder
	sb.WriteString("#EXTM3U\n")
	sb.WriteString("#EXT-X-VERSION:3\n")
	sb.WriteString("\n")

	for _, r := range reps {
		codec := hlsCodecString(r.width, r.height, r.hasAudio)
		res := ""
		if r.width > 0 && r.height > 0 {
			res = fmt.Sprintf(",RESOLUTION=%dx%d", r.width, r.height)
		}
		fmt.Fprintf(&sb,
			"#EXT-X-STREAM-INF:BANDWIDTH=%d%s,CODECS=%q\n%s/index.m3u8\n",
			r.bwBps, res, codec, r.slug,
		)
	}

	return writeFileAtomic(path, []byte(sb.String()))
}

// serveHLSAdaptive is the ABR HLS output goroutine.  It spawns one segmenter
// goroutine per ladder rung and waits for all of them to finish.
// ss is optional: when non-nil, the hlsABRMaster reference is stored in ss.hlsMaster
// so that UpdateABRMasterMeta can push metadata updates without restarting the publisher.
func (s *Service) serveHLSAdaptive(ctx context.Context, stream *domain.Stream, ss *streamState) {
	code := stream.Code
	hlsDir := strings.TrimSpace(s.cfg.HLS.Dir)
	if hlsDir == "" {
		slog.Error("publisher: HLS ABR — hls.dir empty", "stream_code", code)
		return
	}

	rends := buffer.RenditionsForTranscoder(code, stream.Transcoder)
	if len(rends) == 0 {
		// Fall back to single-rendition if the transcoder has no ABR ladder.
		mediaBuf := code
		if ss != nil {
			mediaBuf = ss.mediaBuf
		}
		s.serveHLS(ctx, mediaBuf)
		return
	}

	// Audio presence drives the audio entry in the master playlist's CODECS
	// attribute. Inferred from the transcoder config: copy keeps the source
	// audio track, an explicit Codec re-encodes one in. Both produce .ts
	// segments with audio; without this flag the master declares video only
	// and hls.js drops audio in the browser.
	hasAudio := stream.Transcoder.Audio.Copy || stream.Transcoder.Audio.Codec != ""

	streamBase := filepath.Join(hlsDir, string(code))
	if err := os.MkdirAll(streamBase, 0o755); err != nil {
		slog.Error("publisher: HLS ABR setup dir failed",
			"stream_code", code, "dir", streamBase, "err", err)
		return
	}

	rootManifest := filepath.Join(streamBase, "index.m3u8")
	master := newHLSABRMaster(rootManifest, code)

	// Register master in streamState so UpdateABRMasterMeta can reach it.
	if ss != nil {
		ss.mu.Lock()
		ss.hlsMaster = master
		ss.mu.Unlock()
	}

	segSec := s.cfg.HLS.LiveSegmentSec
	win := s.cfg.HLS.LiveWindow
	hist := s.cfg.HLS.LiveHistory
	eph := s.cfg.HLS.LiveEphemeral

	slog.Info("publisher: HLS ABR serve started",
		"stream_code", code,
		"renditions", len(rends),
		"hls_dir", streamBase,
	)

	var wg sync.WaitGroup
	for _, r := range rends {
		wg.Add(1)
		go func(r buffer.RenditionPlayout) {
			defer wg.Done()

			sub, err := s.buf.Subscribe(r.BufferID)
			if err != nil {
				slog.Error("publisher: HLS ABR subscribe failed",
					"stream_code", code, "slug", r.Slug, "err", err)
				return
			}
			defer s.buf.Unsubscribe(r.BufferID, sub)

			shardDir := filepath.Join(streamBase, r.Slug)
			if err := os.MkdirAll(shardDir, 0o755); err != nil {
				slog.Error("publisher: HLS ABR mkdir failed",
					"stream_code", code, "slug", r.Slug, "err", err)
				return
			}

			shardManifest := filepath.Join(shardDir, "index.m3u8")
			opts := &hlsRunOpts{
				abrMaster:   master,
				abrSlug:     r.Slug,
				bwBps:       r.BandwidthBps(),
				width:       r.Width,
				height:      r.Height,
				hasAudio:    hasAudio,
				segCount:    s.hlsSegCounter(code, r.Slug),
				segWriteDur: s.segWriteDurObserver(code, "hls"),
			}
			runHLSSegmenter(ctx, code, sub, shardDir, shardManifest,
				segSec, win, hist, eph, opts)
		}(r)
	}
	wg.Wait()

	// Clear master reference so UpdateABRMasterMeta no longer routes to a stopped master.
	if ss != nil {
		ss.mu.Lock()
		ss.hlsMaster = nil
		ss.mu.Unlock()
	}
}
