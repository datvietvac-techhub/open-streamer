package manager

// tracks.go — per-input media track stats tracker.
//
// Maintains an EWMA-windowed bitrate estimate per AVCodec seen on each input,
// plus best-effort resolution parsing on the first H.264 / H.265 SPS observed.
// All access goes through (*streamState).mu so the read paths in RuntimeStatus
// can grab a consistent snapshot.

import (
	"sync"
	"time"

	"github.com/Eyevinn/mp4ff/avc"
	"github.com/Eyevinn/mp4ff/hevc"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// trackBitrateWindow is the rolling window over which trackStat.byteRate is
// computed. Three seconds smooths the per-second IDR cadence in H.264 streams
// (one big IDR + many P/B frames) without lagging more than a quarter of a
// typical playback session.
const trackBitrateWindow = 3 * time.Second

// trackStat is the per-(input,codec) running counters.
//
// bytesInWindow accumulates payload bytes since the last sample; bitsPerSec
// is recomputed at most once per windowFlushInterval to keep the hot path
// allocation-free.
type trackStat struct {
	codec      domain.AVCodec
	width      int
	height     int
	spsParsed  bool
	bytes      int64     // total bytes in window (reset on flush)
	windowAt   time.Time // start of the current accumulation window
	bitrateBps float64   // last computed bitrate; published verbatim to snapshots
}

// recordBytes adds n bytes to the current window. Caller must hold the
// parent state.mu. Flushes the window into bitrateBps when more than
// trackBitrateWindow has passed since the window started.
func (t *trackStat) recordBytes(n int, now time.Time) {
	if t.windowAt.IsZero() {
		t.windowAt = now
	}
	t.bytes += int64(n)

	elapsed := now.Sub(t.windowAt)
	if elapsed < trackBitrateWindow {
		return
	}
	if elapsed <= 0 {
		return
	}
	bps := float64(t.bytes*8) / elapsed.Seconds()
	// Light EWMA smoothing (α=0.5). Keeps the gauge from twitching on
	// burst-y decoders (e.g. RTMP that delivers 1s worth of frames in a
	// 50ms write) without lagging more than a window.
	if t.bitrateBps == 0 {
		t.bitrateBps = bps
	} else {
		t.bitrateBps = 0.5*t.bitrateBps + 0.5*bps
	}
	t.bytes = 0
	t.windowAt = now
}

// snapshot returns a JSON-safe MediaTrackInfo for the track. Resolution is
// emitted only when the SPS was successfully parsed. Bitrate rounds to the
// nearest kbps; sub-kbps streams report 0 (a true silent track).
func (t *trackStat) snapshot() domain.MediaTrackInfo {
	out := domain.MediaTrackInfo{
		Kind:        codecKind(t.codec),
		Codec:       domain.CodecLabel(t.codec),
		BitrateKbps: int(t.bitrateBps/1000 + 0.5),
	}
	if t.spsParsed {
		out.Width = t.width
		out.Height = t.height
	}
	return out
}

// codecKind classifies AVCodec into the MediaTrackKind enum.
func codecKind(c domain.AVCodec) domain.MediaTrackKind {
	if c.IsAudio() {
		return domain.MediaTrackAudio
	}
	return domain.MediaTrackVideo
}

// inputTrackStats holds one trackStat per codec for a single input.
//
// We key by AVCodec rather than by elementary-stream PID because the gompeg2
// demuxer in use today does not surface PIDs. When a multi-track input
// arrives with two H.264 streams they collapse into a single counter — the
// downstream pipeline already merges them onto one PID anyway (see
// tsmux.FromAV), so keying by codec matches actual rendered output rather
// than nominal input shape. Switching to PID-based keying is straightforward
// once we adopt a demuxer that exposes PIDs.
type inputTrackStats struct {
	mu     sync.Mutex
	tracks map[domain.AVCodec]*trackStat
}

// newInputTrackStats constructs an empty, ready-to-use tracker.
func newInputTrackStats() *inputTrackStats {
	return &inputTrackStats{tracks: make(map[domain.AVCodec]*trackStat, 2)}
}

// observe folds one AVPacket into the per-codec counters. SPS parse is
// attempted only on key frames and only until it succeeds, so the steady-state
// hot path is one map lookup + one int add per packet.
func (s *inputTrackStats) observe(p *domain.AVPacket, now time.Time) {
	if p == nil || p.Codec == domain.AVCodecUnknown {
		return
	}
	s.mu.Lock()
	t, ok := s.tracks[p.Codec]
	if !ok {
		t = &trackStat{codec: p.Codec}
		s.tracks[p.Codec] = t
	}
	t.recordBytes(len(p.Data), now)

	if p.KeyFrame && !t.spsParsed && p.Codec.IsVideo() {
		if w, h, ok := parseVideoResolution(p.Codec, p.Data); ok {
			t.width = w
			t.height = h
			t.spsParsed = true
		}
	}
	s.mu.Unlock()
}

// snapshot returns a sorted list of MediaTrackInfo — video first, then audio,
// stable across calls so the UI doesn't reorder rows on each refresh.
func (s *inputTrackStats) snapshot() []domain.MediaTrackInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tracks) == 0 {
		return nil
	}
	out := make([]domain.MediaTrackInfo, 0, len(s.tracks))
	// Order: video codecs first, then audio. Matches "video first, audio
	// last" UX convention. Each codec is emitted at most once even if the
	// upstream had multiple tracks of the same codec (they collapse into
	// one counter — see inputTrackStats type doc).
	for _, c := range []domain.AVCodec{
		domain.AVCodecH264, domain.AVCodecH265,
		domain.AVCodecMPEG2Video, domain.AVCodecAV1,
		domain.AVCodecAAC, domain.AVCodecMP2, domain.AVCodecMP3,
		domain.AVCodecAC3, domain.AVCodecEAC3,
	} {
		if t, ok := s.tracks[c]; ok {
			out = append(out, t.snapshot())
		}
	}
	return out
}

// totalBitrateKbps returns the sum of all per-codec bitrates as an integer
// kbps value — the aggregate "input bitrate" the runtime envelope publishes.
func (s *inputTrackStats) totalBitrateKbps() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total float64
	for _, t := range s.tracks {
		total += t.bitrateBps
	}
	return int(total/1000 + 0.5)
}

// reset clears all per-track counters. Called on input switch / unregister so
// stale bitrate readings from a dead source don't keep showing on the UI.
func (s *inputTrackStats) reset() {
	s.mu.Lock()
	s.tracks = make(map[domain.AVCodec]*trackStat, 2)
	s.mu.Unlock()
}

// parseVideoResolution returns (width, height, true) on a successful SPS
// parse. AVC and HEVC NALU payloads from the demuxer are bare access units
// — Annex-B start codes have already been stripped — so we can hand the
// bytes straight to the Eyevinn parsers via byte-stream helpers that tolerate
// either form. ok=false signals "not yet decodable, try again on next IDR".
//
//nolint:exhaustive // only video codecs are parseable here; audio falls to default.
func parseVideoResolution(codec domain.AVCodec, frame []byte) (int, int, bool) {
	switch codec {
	case domain.AVCodecH264:
		spss, _ := avc.GetParameterSetsFromByteStream(frame)
		if len(spss) == 0 {
			return 0, 0, false
		}
		sps, err := avc.ParseSPSNALUnit(spss[0], false)
		if err != nil || sps == nil {
			return 0, 0, false
		}
		return int(sps.Width), int(sps.Height), sps.Width > 0 && sps.Height > 0
	case domain.AVCodecH265:
		_, spss, _ := hevc.GetParameterSetsFromByteStream(frame)
		if len(spss) == 0 {
			return 0, 0, false
		}
		sps, err := hevc.ParseSPSNALUnit(spss[0])
		if err != nil || sps == nil {
			return 0, 0, false
		}
		w := int(sps.PicWidthInLumaSamples)
		h := int(sps.PicHeightInLumaSamples)
		return w, h, w > 0 && h > 0
	default:
		return 0, 0, false
	}
}
