package native

import (
	"testing"
)

// TestWrapADTS_HeaderFields verifies the 7-byte ADTS header the
// re-encoder prepends to every raw AAC frame. MPEG-TS PES payloads
// carry ADTS-framed AAC, so a malformed header here makes every
// re-encoded segment undecodable. Checks the syncword, the embedded
// frame length, and the sample-rate index for the rates we target.
func TestWrapADTS_HeaderFields(t *testing.T) {
	t.Parallel()
	payload := make([]byte, 100)
	for _, tc := range []struct {
		rate, chans, wantSRIdx int
	}{
		{48000, 2, 3},
		{44100, 2, 4},
		{48000, 1, 3},
	} {
		out := wrapADTS(payload, tc.rate, tc.chans)
		if len(out) != len(payload)+7 {
			t.Fatalf("rate=%d: len=%d want %d", tc.rate, len(out), len(payload)+7)
		}
		// Syncword: 0xFF then 0xF1 (MPEG-4, no CRC).
		if out[0] != 0xFF || out[1] != 0xF1 {
			t.Fatalf("rate=%d: syncword=%02x%02x want FFF1", tc.rate, out[0], out[1])
		}
		// Sample-rate index lives in bits [5:2] of byte 2.
		srIdx := int((out[2] >> 2) & 0x0F)
		if srIdx != tc.wantSRIdx {
			t.Fatalf("rate=%d: srIdx=%d want %d", tc.rate, srIdx, tc.wantSRIdx)
		}
		// Frame length is a 13-bit field spanning bytes 3-5.
		frameLen := (int(out[3]&0x3) << 11) | (int(out[4]) << 3) | (int(out[5]) >> 5)
		if frameLen != len(payload)+7 {
			t.Fatalf("rate=%d: frameLen=%d want %d", tc.rate, frameLen, len(payload)+7)
		}
	}
}

func TestAACSampleRateIndex(t *testing.T) {
	t.Parallel()
	cases := map[int]int{96000: 0, 48000: 3, 44100: 4, 32000: 5, 24000: 6}
	for rate, want := range cases {
		if got := aacSampleRateIndex(rate); got != want {
			t.Fatalf("aacSampleRateIndex(%d)=%d want %d", rate, got, want)
		}
	}
}

// TestNewAudioReencoder_OpensAndCloses confirms the encoder + FIFO
// allocate and free cleanly with the native aac encoder (always
// linked). Skips if the build lacks an aac encoder (it shouldn't).
func TestNewAudioReencoder_OpensAndCloses(t *testing.T) {
	t.Parallel()
	ar, err := newAudioReencoder(AudioConfig{
		Codec: "aac", BitrateK: 128, SampleRate: 48000, Channels: 2,
	}, "aac")
	if err != nil {
		t.Skipf("aac encoder unavailable in linked libavcodec: %v", err)
	}
	if ar.frameSize <= 0 {
		t.Fatalf("frameSize=%d, want >0", ar.frameSize)
	}
	if ar.outRate != 48000 || ar.outChans != 2 {
		t.Fatalf("out params = %dHz/%dch, want 48000/2", ar.outRate, ar.outChans)
	}
	ar.Close()
	ar.Close() // idempotent
}

// TestNewAudioReencoder_DefaultsApplied — zero-value sample rate /
// channels fall back to 48 kHz stereo so an operator who only flips
// copy=false still gets a valid encoder.
func TestNewAudioReencoder_DefaultsApplied(t *testing.T) {
	t.Parallel()
	ar, err := newAudioReencoder(AudioConfig{Codec: "aac"}, "aac")
	if err != nil {
		t.Skipf("aac encoder unavailable: %v", err)
	}
	defer ar.Close()
	if ar.outRate != 48000 || ar.outChans != 2 {
		t.Fatalf("defaults = %dHz/%dch, want 48000/2", ar.outRate, ar.outChans)
	}
}
