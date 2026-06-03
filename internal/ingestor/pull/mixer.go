// Package pull — mixer.go: in-process mixer:// PacketReader.
//
// MixerReader subscribes to TWO upstream stream buffers and forwards an
// interleaved AVPacket stream taking video from the first upstream and audio
// from the second:
//
//	mixer://<video_code>,<audio_code>[?audio_failure=continue]
//
// AV sync is best-effort: each upstream's packet timestamps pass through
// unchanged. For surveillance / camera-with-radio-audio use cases this is
// sufficient; lip-sync-critical scenarios are out of scope for v1.
//
// Failure policy:
//   - Video upstream tear-down ALWAYS aborts the mixer (returns io.EOF /
//     read error → manager triggers failover or marks input degraded).
//   - Audio upstream tear-down aborts by default. With
//     `?audio_failure=continue` the reader unsubscribes from audio and keeps
//     forwarding video-only — operator opt-in for "video must stay up no
//     matter what" semantics.
//
// ABR upstreams ARE supported here: when an upstream has an ABR ladder, the
// reader subscribes to its BEST rendition buffer (highest resolution/bitrate
// rung) and demuxes TS bytes back into AVPackets. This is the path used when
// the downstream stream has its own transcoder; the no-downstream-transcoder
// + ABR-video case is routed through a dedicated coordinator path that
// mirrors the video ladder, never reaching this reader.

package pull

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// MixerReader implements ingestor.PacketReader for `mixer://` URLs.
//
// Each source (video, audio) selects one of two modes at construction:
//   - direct mode: subscribe upstream's main buffer (single-stream upstream;
//     packets arrive as AVPackets). Uses the *Sub fields below.
//   - ABR mode: subscribe upstream's best rendition (TS bytes) and demux
//     via TSDemuxPacketReader. Uses the *Inner / *Chunk fields below.
//
// The two modes can mix freely: e.g. ABR video + single-stream audio is the
// common case for "replace audio of an ABR camera with internet radio".
type MixerReader struct {
	video                domain.StreamCode
	audio                domain.StreamCode
	audioFailureContinue bool
	videoBufID           domain.StreamCode
	audioBufID           domain.StreamCode
	bufSvc               *buffer.Service

	// Direct mode (single-stream upstream).
	videoSub *buffer.Subscriber
	audioSub *buffer.Subscriber // nil after audio tear-down in continue mode

	// ABR mode (TS-demuxed best rendition).
	videoInner *TSDemuxPacketReader
	videoChunk *bufferTSChunkReader
	audioInner *TSDemuxPacketReader
	audioChunk *bufferTSChunkReader
	// Lazy-pumped channels feeding the select in ReadPackets — populated by
	// videoPump / audioPump goroutines that copy from the inner ReadPackets
	// loop into a chan. Needed because TSDemuxPacketReader's ReadPackets is
	// blocking (no select-friendly channel exposed).
	videoChan chan domain.AVPacket
	audioChan chan domain.AVPacket
	pumpCtx   context.Context
	pumpStop  context.CancelFunc
	pumpWg    sync.WaitGroup
	videoEOF  atomic.Bool
	audioEOF  atomic.Bool

	// PTS normalisation is delegated to the ingestor's per-worker Normaliser
	// (internal/timeline), which sees this reader's output and wallclock-
	// anchors each track independently. Mixer-specific V/A pairing (snap
	// onto the burst track when one upstream drains first) is handled by
	// the Normaliser's cross-track snap.

	closed atomic.Bool
}

// NewMixerReader constructs a MixerReader from a mixer:// input. Errors:
//   - URL malformed → propagates protocol error
//   - either upstream missing in lookup → runtime error; coordinator surfaces
//     as input degraded so the manager can switch to a fallback (none allowed
//     in v1, so effectively the stream stops)
//
// Mode per source: ABR upstream → tap best rendition + TS demux; otherwise
// → subscribe main buffer directly. Modes are independent for video vs audio.
func NewMixerReader(input domain.Input, bufSvc *buffer.Service, lookup StreamLookup) (*MixerReader, error) {
	if bufSvc == nil {
		return nil, errors.New("mixer reader: nil buffer service")
	}
	if lookup == nil {
		return nil, errors.New("mixer reader: nil stream lookup")
	}

	video, audio, audioContinue, err := domain.MixerInputSpec(input)
	if err != nil {
		return nil, fmt.Errorf("mixer reader: %w", err)
	}

	upV, ok := lookup(video)
	if !ok {
		return nil, fmt.Errorf("mixer reader: video upstream %q not found", video)
	}
	upA, ok := lookup(audio)
	if !ok {
		return nil, fmt.Errorf("mixer reader: audio upstream %q not found", audio)
	}

	r := &MixerReader{
		video:                video,
		audio:                audio,
		audioFailureContinue: audioContinue,
		videoBufID:           buffer.PlaybackBufferID(video, upV.Transcoder),
		audioBufID:           buffer.PlaybackBufferID(audio, upA.Transcoder),
		bufSvc:               bufSvc,
	}
	// Use TS-demux-over-buffer mode whenever the upstream's playback buffer
	// carries raw TS bytes. After the raw-TS-passthrough refactor that's true
	// for ABR ladders AND for raw-TS sources (UDP / HLS / SRT / File).
	// Direct AV mode is reserved for RTSP / RTMP single-stream upstreams
	// whose main buffer still carries AVPackets.
	//
	// Wallclock pacing on both inner readers: mixer combines two
	// clock-independent upstreams whose playback buffers may burst
	// (HLS-pull chunk arrivals, transcoder FFmpeg warmup flush, file
	// pre-read). Without pacing, V or A bursts forward in PTS at the
	// mixer egress; downstream Normaliser then preserves the burst-
	// elevated PTS as a permanent V/A offset (test_mixer: stream_a
	// transcoder produced 4 s of V in ~100 ms when mixer's TS demuxer
	// finally got its PMT, baking a stable 40+s V-leads-A gap). The
	// TSDemuxPacketReader's per-instance pace() throttles emit by
	// DTS-vs-wallclock — V and A independently align to wallclock at
	// the mixer boundary, source-side bursts absorbed in the inner
	// chunk reader's bounded queue. Mixer is the architecturally
	// correct place to enforce this invariant: it owns the clock-
	// domain boundary between two independent upstreams.
	if streamHasRenditions(upV) || domain.StreamMainBufferIsTS(upV) {
		r.videoChunk = newBufferTSChunkReader(bufSvc, r.videoBufID)
		r.videoInner = NewTSDemuxPacketReader(r.videoChunk, WithRealtimePacing())
	}
	if streamHasRenditions(upA) || domain.StreamMainBufferIsTS(upA) {
		r.audioChunk = newBufferTSChunkReader(bufSvc, r.audioBufID)
		r.audioInner = NewTSDemuxPacketReader(r.audioChunk, WithRealtimePacing())
	}
	return r, nil
}

// Open subscribes to both upstream buffers and (for ABR sources) starts pump
// goroutines that bridge TS-demux output into select-friendly channels. On
// any failure, partial state is rolled back.
func (r *MixerReader) Open(ctx context.Context) error {
	if r.closed.Load() {
		return errors.New("mixer reader: open after close")
	}

	// Pump context outlives the request ctx — request is "open the reader",
	// not "run the pumps forever". Close() cancels this ctx.
	r.pumpCtx, r.pumpStop = context.WithCancel(context.Background())

	if err := r.openVideoSource(ctx); err != nil {
		r.pumpStop()
		return err
	}
	if err := r.openAudioSource(ctx); err != nil {
		r.closeVideoSource()
		r.pumpStop()
		return err
	}
	return nil
}

func (r *MixerReader) openVideoSource(ctx context.Context) error {
	if r.videoInner != nil {
		if err := r.videoInner.Open(ctx); err != nil {
			return fmt.Errorf("mixer reader: subscribe video %q: %w", r.videoBufID, err)
		}
		r.videoChan = make(chan domain.AVPacket, 32)
		r.pumpWg.Add(1)
		//nolint:contextcheck // pumpCtx is intentionally detached; cancelled by Close()
		go r.pumpInner(r.pumpCtx, r.videoInner, r.videoChan, &r.videoEOF, "video")
		return nil
	}
	vsub, err := r.bufSvc.Subscribe(r.videoBufID)
	if err != nil {
		return fmt.Errorf("mixer reader: subscribe video %q: %w", r.videoBufID, err)
	}
	r.videoSub = vsub
	return nil
}

func (r *MixerReader) openAudioSource(ctx context.Context) error {
	if r.audioInner != nil {
		if err := r.audioInner.Open(ctx); err != nil {
			return fmt.Errorf("mixer reader: subscribe audio %q: %w", r.audioBufID, err)
		}
		r.audioChan = make(chan domain.AVPacket, 32)
		r.pumpWg.Add(1)
		//nolint:contextcheck // pumpCtx is intentionally detached; cancelled by Close()
		go r.pumpInner(r.pumpCtx, r.audioInner, r.audioChan, &r.audioEOF, "audio")
		return nil
	}
	asub, err := r.bufSvc.Subscribe(r.audioBufID)
	if err != nil {
		return fmt.Errorf("mixer reader: subscribe audio %q: %w", r.audioBufID, err)
	}
	r.audioSub = asub
	return nil
}

// pumpInner runs ReadPackets in a loop and forwards each packet to ch. On
// error or EOF it sets the eof flag (so ReadPackets can apply policy) and
// closes ch.
func (r *MixerReader) pumpInner(
	ctx context.Context,
	inner *TSDemuxPacketReader,
	ch chan<- domain.AVPacket,
	eofFlag *atomic.Bool,
	label string,
) {
	defer r.pumpWg.Done()
	defer close(ch)
	for {
		batch, err := inner.ReadPackets(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Debug("mixer reader: inner pump done",
					"source", label, "err", err)
			}
			eofFlag.Store(true)
			return
		}
		for _, p := range batch {
			select {
			case <-ctx.Done():
				eofFlag.Store(true)
				return
			case ch <- p:
			}
		}
	}
}

// ReadPackets blocks until one upstream packet arrives or ctx is done.
//
// Selection rules:
//   - video source ends (sub close or pump EOF) → io.EOF (always; downstream
//     stream stops)
//   - audio source ends + audio_failure=down (default) → io.EOF
//   - audio source ends + audio_failure=continue → drop the audio source
//     and keep forwarding video-only on subsequent calls
//   - packets whose codec doesn't match the source channel are dropped
//     (e.g. video upstream emitting an audio AAC packet — we want video only
//     from the video source)
func (r *MixerReader) ReadPackets(ctx context.Context) ([]domain.AVPacket, error) {
	if !r.videoOpen() {
		return nil, errors.New("mixer reader: ReadPackets called before Open")
	}

	videoSubCh := r.videoSubCh()
	videoABRCh := r.videoABRCh()
	audioSubCh := r.audioSubCh()
	audioABRCh := r.audioABRCh()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case pkt, ok := <-videoSubCh:
		if !ok {
			return nil, fmt.Errorf("mixer reader: video upstream %q closed: %w", r.video, io.EOF)
		}
		if pkt.AV == nil || !pkt.AV.Codec.IsVideo() {
			return nil, nil
		}
		return []domain.AVPacket{*pkt.AV}, nil

	case avp, ok := <-videoABRCh:
		if !ok {
			return nil, fmt.Errorf("mixer reader: video upstream %q closed: %w", r.video, io.EOF)
		}
		if !avp.Codec.IsVideo() {
			return nil, nil
		}
		return []domain.AVPacket{avp}, nil

	case pkt, ok := <-audioSubCh:
		if !ok {
			return r.handleAudioClose()
		}
		if pkt.AV == nil || !pkt.AV.Codec.IsAudio() {
			return nil, nil
		}
		return []domain.AVPacket{*pkt.AV}, nil

	case avp, ok := <-audioABRCh:
		if !ok {
			return r.handleAudioClose()
		}
		if !avp.Codec.IsAudio() {
			return nil, nil
		}
		return []domain.AVPacket{avp}, nil
	}
}

// videoOpen reports whether either video mode has been initialised.
func (r *MixerReader) videoOpen() bool { return r.videoSub != nil || r.videoInner != nil }

// videoSubCh returns the direct-mode subscriber channel, or nil if in ABR
// mode (which makes the corresponding select case never fire).
func (r *MixerReader) videoSubCh() <-chan buffer.Packet {
	if r.videoSub == nil {
		return nil
	}
	return r.videoSub.Recv()
}

// videoABRCh returns the ABR-mode pump channel, or nil in direct mode.
func (r *MixerReader) videoABRCh() <-chan domain.AVPacket {
	if r.videoChan == nil {
		return nil
	}
	return r.videoChan
}

func (r *MixerReader) audioSubCh() <-chan buffer.Packet {
	if r.audioSub == nil {
		return nil
	}
	return r.audioSub.Recv()
}

func (r *MixerReader) audioABRCh() <-chan domain.AVPacket {
	if r.audioChan == nil {
		return nil
	}
	return r.audioChan
}

// handleAudioClose applies the audio-failure policy when the audio source
// ends. In continue mode it drops the audio source and tells the caller to
// try again (nil batch); otherwise it returns io.EOF to abort the stream.
func (r *MixerReader) handleAudioClose() ([]domain.AVPacket, error) {
	if !r.audioFailureContinue {
		return nil, fmt.Errorf("mixer reader: audio upstream %q closed: %w", r.audio, io.EOF)
	}
	slog.Warn("mixer reader: audio upstream closed, continuing video-only",
		"audio_upstream", r.audio,
	)
	r.closeAudioSource()
	return nil, nil
}

// closeVideoSource releases video-side resources (direct sub or ABR inner +
// pump). Idempotent — safe to call from Close after a partial Open failure.
func (r *MixerReader) closeVideoSource() {
	if r.videoInner != nil {
		_ = r.videoInner.Close()
		r.videoInner = nil
	}
	if r.videoSub != nil {
		r.bufSvc.Unsubscribe(r.videoBufID, r.videoSub)
		r.videoSub = nil
	}
}

func (r *MixerReader) closeAudioSource() {
	if r.audioInner != nil {
		_ = r.audioInner.Close()
		r.audioInner = nil
		// Pump goroutine exits via inner Close → ReadPackets error → defer
		// close(ch). audioChan = nil here so future ReadPackets ignore it.
		r.audioChan = nil
	}
	if r.audioSub != nil {
		r.bufSvc.Unsubscribe(r.audioBufID, r.audioSub)
		r.audioSub = nil
	}
}

// Close stops all sources + pumps. Idempotent.
func (r *MixerReader) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	if r.pumpStop != nil {
		r.pumpStop()
	}
	r.closeVideoSource()
	r.closeAudioSource()
	r.pumpWg.Wait()
	return nil
}
