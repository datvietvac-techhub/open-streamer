package native

import (
	"errors"
	"fmt"

	"github.com/asticode/go-astiav"
)

// audioReencoder decodes the source audio elementary stream, resamples
// it to the configured output format, and re-encodes it — the path
// taken when AudioConfig.Copy is false (operator wants a fixed
// bitrate / sample rate / channel layout regardless of source).
//
// Lifecycle split mirrors the video side: the ENCODER + resampler +
// FIFO are long-lived (output params are fixed for the stream's
// life), while the DECODER is rebuilt whenever the source's sample
// rate / channel layout changes — which happens on input switches
// between sources of different audio formats (e.g. 44.1 kHz UDP →
// 48 kHz HLS). Rebuilding only the decoder keeps the output AAC
// stream continuous across switches.
//
// Output is ADTS-framed AAC: the libavcodec aac encoder emits raw
// AAC access units, but MPEG-TS PES payloads (what tsmux.FromAV
// writes) carry ADTS-framed AAC, so a 7-byte ADTS header is prepended
// to every encoded frame.
//
// Not safe for concurrent use; the pipeline serialises calls.
type audioReencoder struct {
	cfg AudioConfig

	// Decoder — rebuilt on source-format change.
	decCodec   *astiav.Codec
	dec        *astiav.CodecContext
	decPkt     *astiav.Packet
	decFrame   *astiav.Frame
	srcRate    int
	srcChans   int
	decReady   bool
	decCodecID string

	// Resampler — rebuilt alongside the decoder (its input format
	// tracks the source).
	swr      *astiav.SoftwareResampleContext
	resFrame *astiav.Frame

	// Encoder + FIFO — long-lived.
	encCodec  *astiav.Codec
	enc       *astiav.CodecContext
	encPkt    *astiav.Packet
	encFrame  *astiav.Frame
	fifo      *astiav.AudioFifo
	frameSize int

	outRate  int
	outChans int
	outLayout astiav.ChannelLayout
	outFmt   astiav.SampleFormat

	// PTS bookkeeping: output PTS is derived from a running sample
	// count anchored to the first input frame's PTS so the audio
	// timeline stays coherent with video (both in source-stream ms).
	// fifoHeadPTS is the source-stream PTS (ms) of the oldest sample
	// currently in the FIFO. Re-synced to the input frame's PTS
	// whenever the FIFO is empty (stream start, gaps, post-switch) so
	// the re-encoded output rides the SAME source timeline as video —
	// the pipeline's shared rebaseAudioPTS offset then keeps A/V in
	// sync exactly as it does for the video path. A separate
	// sample-counter clock (the earlier approach) diverged from video
	// by seconds after a switch because audio and video re-anchored at
	// different source PTS.
	fifoHeadPTS  int64
	haveFifoHead bool

	// encInPTS is a monotonic sample-count clock for the encoder's
	// INPUT frames (encoder time base), kept distinct from the
	// ms-domain output PTS so libavcodec sees strictly increasing
	// input timestamps.
	encInPTS int64

	closed bool
}

// newAudioReencoder builds the encoder + resampler + FIFO from cfg.
// The decoder is created lazily on the first frame (its params come
// from the source, not the config). srcCodecName is the libavcodec
// decoder name for the source ("aac", "mp2", "mp3").
func newAudioReencoder(cfg AudioConfig, srcCodecName string) (*audioReencoder, error) {
	outRate := cfg.SampleRate
	if outRate <= 0 {
		outRate = 48000
	}
	outChans := cfg.Channels
	if outChans <= 0 {
		outChans = 2
	}
	layout := astiav.ChannelLayoutStereo
	if outChans == 1 {
		layout = astiav.ChannelLayoutMono
	}

	encName := cfg.Codec
	if encName == "" || encName == "aac" {
		encName = "aac"
	}
	encCodec := astiav.FindEncoderByName(encName)
	if encCodec == nil {
		return nil, fmt.Errorf("native: audio encoder %q not in linked libavcodec", encName)
	}
	enc := astiav.AllocCodecContext(encCodec)
	if enc == nil {
		return nil, errors.New("native: audio AllocCodecContext returned nil")
	}

	a := &audioReencoder{
		cfg:       cfg,
		encCodec:  encCodec,
		enc:       enc,
		outRate:   outRate,
		outChans:  outChans,
		outLayout: layout,
		outFmt:    astiav.SampleFormatFltp, // libavcodec aac wants planar float
	}

	enc.SetSampleRate(outRate)
	enc.SetChannelLayout(layout)
	enc.SetSampleFormat(a.outFmt)
	enc.SetTimeBase(astiav.NewRational(1, outRate))
	if cfg.BitrateK > 0 {
		enc.SetBitRate(int64(cfg.BitrateK) * 1000)
	}
	if err := enc.Open(encCodec, nil); err != nil {
		enc.Free()
		return nil, fmt.Errorf("native: open audio encoder %q: %w", encName, err)
	}

	frameSize := enc.FrameSize()
	if frameSize <= 0 {
		frameSize = 1024 // AAC default
	}
	a.frameSize = frameSize
	a.encPkt = astiav.AllocPacket()
	a.encFrame = astiav.AllocFrame()
	a.resFrame = astiav.AllocFrame()
	a.decPkt = astiav.AllocPacket()
	a.decFrame = astiav.AllocFrame()

	fifo := astiav.AllocAudioFifo(a.outFmt, outChans, frameSize)
	if fifo == nil {
		a.Close()
		return nil, errors.New("native: AllocAudioFifo returned nil")
	}
	a.fifo = fifo

	a.decCodecID = srcCodecName
	return a, nil
}

// Process decodes one source AAC/MP2/MP3 access unit, resamples it,
// and returns any fully-encoded ADTS-AAC OutputFrames. PTS (ms)
// anchors the output sample clock on the first frame.
func (a *audioReencoder) Process(data []byte, ptsMs int64) ([]OutputFrame, error) {
	if a.closed {
		return nil, errors.New("native: audio reencode on closed reencoder")
	}
	if err := a.ensureDecoder(); err != nil {
		return nil, err
	}

	if err := a.decPkt.FromData(data); err != nil {
		return nil, fmt.Errorf("native: audio packet FromData: %w", err)
	}
	if err := a.dec.SendPacket(a.decPkt); err != nil {
		a.decPkt.Unref()
		// A bad audio AU shouldn't kill the stream; skip it.
		return nil, nil //nolint:nilerr // tolerate transient audio corruption
	}
	a.decPkt.Unref()
	return a.drainDecoder(ptsMs)
}

// drainDecoder pulls every PCM frame the decoder is ready to release,
// resamples + encodes each, and returns the encoded ADTS-AAC frames.
// srcPTS is the source PTS of the input access unit being decoded —
// used to (re)seed the FIFO head clock when the FIFO is empty.
func (a *audioReencoder) drainDecoder(srcPTS int64) ([]OutputFrame, error) {
	var out []OutputFrame
	for {
		if err := a.dec.ReceiveFrame(a.decFrame); err != nil {
			if errors.Is(err, astiav.ErrEagain) || errors.Is(err, astiav.ErrEof) {
				return out, nil
			}
			return out, fmt.Errorf("native: audio ReceiveFrame: %w", err)
		}
		if err := a.rebuildResamplerIfNeeded(); err != nil {
			a.decFrame.Unref()
			return out, err
		}
		encoded, err := a.resampleAndEncode(a.decFrame, srcPTS)
		a.decFrame.Unref()
		if err != nil {
			return out, err
		}
		out = append(out, encoded...)
	}
}

// ensureDecoder lazily allocates + opens the source decoder.
func (a *audioReencoder) ensureDecoder() error {
	if a.decReady {
		return nil
	}
	name := a.decCodecID
	if name == "" {
		name = "aac"
	}
	codec := astiav.FindDecoderByName(name)
	if codec == nil {
		return fmt.Errorf("native: audio decoder %q not in linked libavcodec", name)
	}
	cc := astiav.AllocCodecContext(codec)
	if cc == nil {
		return errors.New("native: audio decoder AllocCodecContext nil")
	}
	if err := cc.Open(codec, nil); err != nil {
		cc.Free()
		return fmt.Errorf("native: open audio decoder %q: %w", name, err)
	}
	a.decCodec = codec
	a.dec = cc
	a.decReady = true
	return nil
}

// rebuildResamplerIfNeeded (re)allocates the SwrContext when the
// decoded frame's sample rate or channel count differs from what the
// current resampler was built for — the trigger on an input switch
// between differently-formatted sources.
func (a *audioReencoder) rebuildResamplerIfNeeded() error {
	rate := a.decFrame.SampleRate()
	chans := a.decFrame.ChannelLayout().Channels()
	if a.swr != nil && rate == a.srcRate && chans == a.srcChans {
		return nil
	}
	if a.swr != nil {
		a.swr.Free()
		a.swr = nil
	}
	swr := astiav.AllocSoftwareResampleContext()
	if swr == nil {
		return errors.New("native: AllocSoftwareResampleContext nil")
	}
	a.swr = swr
	a.srcRate = rate
	a.srcChans = chans
	return nil
}

// resampleAndEncode converts one decoded PCM frame to the output
// format, buffers it in the FIFO, and drains full encoder frames.
// srcPTS seeds the FIFO head clock when the FIFO is empty so output
// PTS tracks the source timeline (and re-syncs after switches / gaps).
func (a *audioReencoder) resampleAndEncode(in *astiav.Frame, srcPTS int64) ([]OutputFrame, error) {
	a.resFrame.Unref()
	a.resFrame.SetSampleRate(a.outRate)
	a.resFrame.SetSampleFormat(a.outFmt)
	a.resFrame.SetChannelLayout(a.outLayout)
	if err := a.swr.ConvertFrame(in, a.resFrame); err != nil {
		return nil, fmt.Errorf("native: audio resample: %w", err)
	}
	if a.resFrame.NbSamples() > 0 {
		if _, err := a.fifo.Write(a.resFrame); err != nil {
			return nil, fmt.Errorf("native: audio fifo write: %w", err)
		}
	}
	// Re-anchor the FIFO head to the SOURCE timeline on every input
	// frame: the newest buffered sample corresponds to this frame's
	// source PTS, so the head (oldest) sits fifoSize samples behind it.
	// Recomputing each frame ties output PTS to source PTS and cannot
	// drift — the earlier "advance head by output-sample-count"
	// approach let audio race ahead of video by seconds when the
	// source delivered audio in bursts (file / HLS-pull), which then
	// poisoned the publisher's media-PTS segment-duration math and
	// produced 30-60 s segments.
	a.fifoHeadPTS = srcPTS - int64(a.fifo.Size())*1000/int64(a.outRate)
	a.haveFifoHead = true
	// NOTE: do NOT force-flush the resampler with ConvertFrame(nil)
	// per input frame — nil signals end-of-stream, leaving the SwrContext
	// in a drained state that mishandles the next real input (observed:
	// runaway loop + FIFO inflation that starved the shared dispatch
	// goroutine, dropping video to ~2 fps). The resampler's small
	// internal buffer (a few ms) carries over to the next call on its
	// own; the only nil-flush happens once, in Flush(), at stream stop.
	return a.drainEncoder(false)
}

// drainEncoder pops full frames from the FIFO and encodes them. When
// flush is true, a final short frame is encoded and the encoder is
// flushed.
func (a *audioReencoder) drainEncoder(flush bool) ([]OutputFrame, error) {
	var out []OutputFrame
	for a.fifo.Size() >= a.frameSize || (flush && a.fifo.Size() > 0) {
		a.encFrame.Unref()
		a.encFrame.SetNbSamples(a.frameSize)
		a.encFrame.SetSampleRate(a.outRate)
		a.encFrame.SetSampleFormat(a.outFmt)
		a.encFrame.SetChannelLayout(a.outLayout)
		if err := a.encFrame.AllocBuffer(0); err != nil {
			return out, fmt.Errorf("native: audio frame alloc: %w", err)
		}
		n, err := a.fifo.Read(a.encFrame)
		if err != nil {
			return out, fmt.Errorf("native: audio fifo read: %w", err)
		}
		if n <= 0 {
			break
		}
		a.encFrame.SetNbSamples(n)
		// Encoder input PTS: a plain monotonic sample counter in the
		// encoder's 1/outRate time base, so libavcodec doesn't warn
		// about non-monotonic input. Distinct from the OUTPUT ms PTS.
		a.encFrame.SetPts(a.encInPTS)
		a.encInPTS += int64(n)
		// Output PTS = the source PTS of the oldest FIFO sample;
		// advance the head by the frame's duration. Encoder time base
		// is 1/outRate so we set the frame PTS in sample units for the
		// encoder, but the OUTPUT OutputFrame PTS is the ms-domain
		// fifoHeadPTS — on the same scale as video.
		outPTS := a.fifoHeadPTS
		a.fifoHeadPTS += int64(n) * 1000 / int64(a.outRate)
		pkts, err := a.encodeFrame(a.encFrame, outPTS)
		if err != nil {
			return out, err
		}
		out = append(out, pkts...)
	}
	return out, nil
}

// encodeFrame sends one PCM frame to the encoder and wraps each
// emitted AAC packet as an ADTS-framed audio OutputFrame stamped with
// outPTS (source-timeline ms).
func (a *audioReencoder) encodeFrame(f *astiav.Frame, outPTS int64) ([]OutputFrame, error) {
	if err := a.enc.SendFrame(f); err != nil {
		return nil, fmt.Errorf("native: audio SendFrame: %w", err)
	}
	var out []OutputFrame
	for {
		if err := a.enc.ReceivePacket(a.encPkt); err != nil {
			if errors.Is(err, astiav.ErrEagain) || errors.Is(err, astiav.ErrEof) {
				return out, nil
			}
			return out, fmt.Errorf("native: audio ReceivePacket: %w", err)
		}
		adts := wrapADTS(a.encPkt.Data(), a.outRate, a.outChans)
		out = append(out, OutputFrame{
			Data:        adts,
			Codec:       esCodecAAC,
			PTS:         outPTS,
			DTS:         outPTS,
			TargetIndex: BroadcastTargetIndex,
		})
		a.encPkt.Unref()
	}
}

// Flush drains the FIFO tail + encoder on stream stop.
func (a *audioReencoder) Flush() ([]OutputFrame, error) {
	if a.closed {
		return nil, nil
	}
	out, err := a.drainEncoder(true)
	if err != nil {
		return out, err
	}
	if a.enc != nil {
		if err := a.enc.SendFrame(nil); err == nil {
			for {
				if e := a.enc.ReceivePacket(a.encPkt); e != nil {
					break
				}
				adts := wrapADTS(a.encPkt.Data(), a.outRate, a.outChans)
				// Tail frames continue from the FIFO head clock.
				outPTS := a.fifoHeadPTS
				a.fifoHeadPTS += int64(a.frameSize) * 1000 / int64(a.outRate)
				out = append(out, OutputFrame{
					Data: adts, Codec: esCodecAAC, PTS: outPTS, DTS: outPTS,
					TargetIndex: BroadcastTargetIndex,
				})
				a.encPkt.Unref()
			}
		}
	}
	return out, nil
}

// SwitchInput drops the decoder + resampler so the next frame rebuilds
// them against the new source format. Encoder + FIFO + sample clock
// survive so the output AAC stream stays continuous.
func (a *audioReencoder) SwitchInput() {
	if a.closed {
		return
	}
	if a.dec != nil {
		a.dec.Free()
		a.dec = nil
	}
	a.decReady = false
	if a.swr != nil {
		a.swr.Free()
		a.swr = nil
	}
	a.srcRate, a.srcChans = 0, 0
	// Discard any old-source samples still buffered. Without this they
	// get re-encoded under the new source's PTS anchor, bunching ~5 s
	// of audio into the first post-switch segment — heard as an audio
	// glitch and seen as the player fast-forwarding through that
	// segment. A clean FIFO lets the new source re-anchor from scratch.
	a.flushFIFO()
}

// flushFIFO drains and discards every buffered sample. Used on switch
// to drop the old source's tail so the new source starts clean.
func (a *audioReencoder) flushFIFO() {
	if a.fifo == nil {
		return
	}
	for a.fifo.Size() > 0 {
		a.encFrame.Unref()
		a.encFrame.SetNbSamples(a.frameSize)
		a.encFrame.SetSampleRate(a.outRate)
		a.encFrame.SetSampleFormat(a.outFmt)
		a.encFrame.SetChannelLayout(a.outLayout)
		if err := a.encFrame.AllocBuffer(0); err != nil {
			return
		}
		n, err := a.fifo.Read(a.encFrame)
		if err != nil || n <= 0 {
			return
		}
	}
}

// Close releases every libav resource. Idempotent.
func (a *audioReencoder) Close() {
	if a.closed {
		return
	}
	a.closed = true
	if a.dec != nil {
		a.dec.Free()
		a.dec = nil
	}
	if a.swr != nil {
		a.swr.Free()
		a.swr = nil
	}
	if a.fifo != nil {
		a.fifo.Free()
		a.fifo = nil
	}
	for _, f := range []*astiav.Frame{a.decFrame, a.resFrame, a.encFrame} {
		if f != nil {
			f.Free()
		}
	}
	for _, p := range []*astiav.Packet{a.decPkt, a.encPkt} {
		if p != nil {
			p.Free()
		}
	}
	if a.enc != nil {
		a.enc.Free()
		a.enc = nil
	}
}

// aacSampleRateIndex maps a sample rate to the ADTS sampling-frequency
// index. Returns 15 (escape / explicit) sentinel — but for the rates
// we target (44.1 / 48 kHz) a real index always exists.
func aacSampleRateIndex(rate int) int {
	rates := []int{96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350}
	for i, r := range rates {
		if r == rate {
			return i
		}
	}
	return 4 // default 44100
}

// wrapADTS prepends the 7-byte ADTS header to a raw AAC access unit so
// the result is what MPEG-TS PES payloads carry. AAC-LC (profile 2),
// no CRC. frameLen in the header counts the 7 header bytes + payload.
func wrapADTS(aac []byte, sampleRate, channels int) []byte {
	const headerLen = 7
	frameLen := headerLen + len(aac)
	srIdx := aacSampleRateIndex(sampleRate)
	chCfg := channels
	if chCfg > 7 {
		chCfg = 2
	}

	h := make([]byte, frameLen)
	// Syncword 0xFFF, MPEG-4, Layer 0, protection absent (1).
	h[0] = 0xFF
	h[1] = 0xF1
	// profile (AAC-LC = 1 in the 2-bit field) << 6 | srIdx << 2 | (ch>>2)
	h[2] = byte((1 << 6) | (srIdx << 2) | ((chCfg >> 2) & 0x1))
	// (ch&3)<<6 | frameLen[12:11]
	h[3] = byte(((chCfg & 0x3) << 6) | ((frameLen >> 11) & 0x3))
	h[4] = byte((frameLen >> 3) & 0xFF)
	h[5] = byte(((frameLen & 0x7) << 5) | 0x1F)
	h[6] = 0xFC
	copy(h[headerLen:], aac)
	return h
}
