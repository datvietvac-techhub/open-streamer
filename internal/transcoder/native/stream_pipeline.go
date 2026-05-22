package native

import (
	"errors"
	"fmt"
	"sync"

	"github.com/asticode/go-astiav"
)

// PipelineConfig bundles the three constituent configs for a single
// transcoded rendition's worth of pipeline. P5 will replace this with
// a per-stream PipelineConfig carrying N rendition encoders sharing
// one decoder; P3's job is just to prove the encoder survives a
// decoder swap.
type PipelineConfig struct {
	Encoder EncoderConfig
	Scaler  ScalerConfig
	Decoder DecoderConfig
}

// StreamPipeline composes decoder → scaler → encoder into the single
// data-flow the StreamRunner subprocess will execute per stream. Its
// central design invariant is: SwitchInput tears down and recreates the
// Decoder while the Scaler + Encoder stay alive. That is the property
// the FFmpeg subprocess implementation could not provide and is the
// whole reason for the migration.
//
// Not safe for concurrent use across data and control plane — the
// caller (StreamRunner in P4) drives ProcessPacket on one goroutine
// and signals SwitchInput on the same goroutine via channel selects.
// The mutex here exists only to make SwitchInput observable in tests
// and to back the assertion in production unit-tests that the encoder
// pointer is unchanged across switch.
type StreamPipeline struct {
	cfg PipelineConfig

	// Long-lived across switch.
	encoder *Encoder
	scaler  *Scaler

	// Decoder lifecycle is per-input. Recreated by SwitchInput.
	mu      sync.Mutex
	decoder *Decoder
	closed  bool
}

// NewStreamPipeline constructs all three stages. On error any
// partially-allocated stages are torn down before returning so the
// caller does not need defensive Close calls.
func NewStreamPipeline(cfg PipelineConfig) (*StreamPipeline, error) {
	enc, err := NewEncoder(cfg.Encoder)
	if err != nil {
		return nil, fmt.Errorf("pipeline: encoder: %w", err)
	}
	sc, err := NewScaler(cfg.Scaler)
	if err != nil {
		enc.Close()
		return nil, fmt.Errorf("pipeline: scaler: %w", err)
	}
	dec, err := NewDecoder(cfg.Decoder)
	if err != nil {
		sc.Close()
		enc.Close()
		return nil, fmt.Errorf("pipeline: decoder: %w", err)
	}
	return &StreamPipeline{
		cfg:     cfg,
		encoder: enc,
		scaler:  sc,
		decoder: dec,
	}, nil
}

// ProcessPacket feeds one compressed packet from the active input into
// the decoder → scaler → encoder chain and returns the encoded packets
// ready to ship to the output buffer. Returns (nil, nil) when the
// decoder accepted the packet but produced no output yet (B-frame
// reorder buffering or pipeline warmup).
//
// PTS / DTS are forwarded to the decoder's input packet but the encoder
// derives its own monotonic PTS via NextPTS — that's the simplest way
// to keep the encoder's output timing stable across input switches
// where the source PTS bases differ.
func (p *StreamPipeline) ProcessPacket(data []byte, pts, dts int64) ([][]byte, error) {
	if p.isClosed() {
		return nil, errors.New("native: process on closed pipeline")
	}

	p.mu.Lock()
	dec := p.decoder
	p.mu.Unlock()

	frames, err := dec.Decode(data, pts, dts)
	if err != nil {
		return nil, fmt.Errorf("pipeline: decode: %w", err)
	}
	return p.runFramesThroughEncoder(frames)
}

// SwitchInput swaps the decoder for one matching newCfg. Before the
// swap, the OLD decoder is flushed so any in-flight reordered frames
// still reach the encoder; those flushed frames are encoded under the
// CURRENT encoder PTS sequence so output timing stays monotonic.
//
// The encoder + scaler are NEVER touched here — that's the property
// the whole migration is about. Tests assert encoder pointer identity
// before / after to lock the invariant in.
func (p *StreamPipeline) SwitchInput(newCfg DecoderConfig) ([][]byte, error) {
	if p.isClosed() {
		return nil, errors.New("native: switch on closed pipeline")
	}

	p.mu.Lock()
	oldDec := p.decoder
	p.mu.Unlock()

	// Drain anything the old decoder still has in its B-frame queue
	// so the cutover is clean in encoder PTS space.
	flushFrames, err := oldDec.Flush()
	if err != nil {
		return nil, fmt.Errorf("pipeline: flush old decoder: %w", err)
	}
	out, err := p.runFramesThroughEncoder(flushFrames)
	if err != nil {
		return nil, err
	}

	oldDec.Close()

	newDec, err := NewDecoder(newCfg)
	if err != nil {
		return out, fmt.Errorf("pipeline: alloc new decoder: %w", err)
	}

	p.mu.Lock()
	p.decoder = newDec
	p.cfg.Decoder = newCfg
	p.mu.Unlock()
	return out, nil
}

// Flush drains the full chain (decoder → encoder) on stream stop.
// After Flush the pipeline must be Closed; further Process / Switch
// calls will fail.
func (p *StreamPipeline) Flush() ([][]byte, error) {
	if p.isClosed() {
		return nil, errors.New("native: flush on closed pipeline")
	}

	p.mu.Lock()
	dec := p.decoder
	p.mu.Unlock()

	flushFrames, err := dec.Flush()
	if err != nil {
		return nil, fmt.Errorf("pipeline: flush decoder: %w", err)
	}
	out, err := p.runFramesThroughEncoder(flushFrames)
	if err != nil {
		return out, err
	}
	// Now drain the encoder for any held B-frames.
	tail, err := p.encoder.Flush()
	if err != nil {
		return out, fmt.Errorf("pipeline: flush encoder: %w", err)
	}
	return append(out, tail...), nil
}

// Close releases all three stages. Idempotent.
func (p *StreamPipeline) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	dec := p.decoder
	p.decoder = nil
	p.mu.Unlock()

	if dec != nil {
		dec.Close()
	}
	if p.scaler != nil {
		p.scaler.Close()
	}
	if p.encoder != nil {
		p.encoder.Close()
	}
}

// EncoderPTS returns the next PTS counter value WITHOUT consuming it.
// Test-only inspection used to assert PTS continuity across SwitchInput
// and to verify the encoder was not reallocated.
//
// Production code derives PTS implicitly via Encoder.NextPTS inside
// runFramesThroughEncoder; callers don't need this hook for normal
// operation.
func (p *StreamPipeline) EncoderPTS() int64 {
	if p.isClosed() {
		return -1
	}
	return p.encoder.frameIdx
}

// runFramesThroughEncoder scales + encodes every frame in the slice
// and returns the resulting encoded packets. Frees each input frame
// after use — the caller's decoder yielded them as caller-owned and
// the lifetime ends right here.
func (p *StreamPipeline) runFramesThroughEncoder(frames []*astiav.Frame) ([][]byte, error) {
	var out [][]byte
	for _, f := range frames {
		pkts, err := p.encodeOne(f)
		if err != nil {
			// Free the rest of the slice so we don't leak on early exit.
			f.Free()
			for _, leftover := range frames[len(out)+1:] {
				leftover.Free()
			}
			return out, err
		}
		out = append(out, pkts...)
		f.Free()
	}
	return out, nil
}

// encodeOne scales then encodes a single frame.
func (p *StreamPipeline) encodeOne(f *astiav.Frame) ([][]byte, error) {
	scaled, err := p.scaler.Scale(f)
	if err != nil {
		return nil, fmt.Errorf("pipeline: scale: %w", err)
	}
	scaled.SetPts(p.encoder.NextPTS())
	pkts, err := p.encoder.Encode(scaled)
	scaled.Free()
	if err != nil {
		return nil, fmt.Errorf("pipeline: encode: %w", err)
	}
	return pkts, nil
}

func (p *StreamPipeline) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}
