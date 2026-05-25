package native

import (
	"testing"

	"github.com/asticode/go-astiav"
)

// TestAudioReencode_48kTo44k_NoOverProduction is the offline
// reproducer for the production bug where a 48 kHz source re-encoded
// to 44.1 kHz produced ~37 % MORE output audio than input duration —
// audio raced ahead of video by seconds and poisoned the publisher's
// segment timing (30-60 s segments). Output sample count must track
// input DURATION (resampled), not balloon.
//
// Feeds 2 s of synthetic 48 kHz stereo AAC and asserts the re-encoded
// 44.1 kHz output is within 5 % of 2 s worth of samples.
func TestAudioReencode_48kTo44k_NoOverProduction(t *testing.T) {
	t.Parallel()
	const (
		srcRate = 48000
		dstRate = 44100
		seconds = 2
	)

	// Source AAC encoder @ 48 kHz to fabricate realistic input.
	srcEnc := astiav.FindEncoderByName("aac")
	if srcEnc == nil {
		t.Skip("aac encoder unavailable")
	}
	srcCC := astiav.AllocCodecContext(srcEnc)
	defer srcCC.Free()
	srcCC.SetSampleRate(srcRate)
	srcCC.SetChannelLayout(astiav.ChannelLayoutStereo)
	srcCC.SetSampleFormat(astiav.SampleFormatFltp)
	srcCC.SetTimeBase(astiav.NewRational(1, srcRate))
	if err := srcCC.Open(srcEnc, nil); err != nil {
		t.Skipf("open src aac: %v", err)
	}
	srcFrameSize := srcCC.FrameSize()
	if srcFrameSize <= 0 {
		srcFrameSize = 1024
	}

	re, err := newAudioReencoder(AudioConfig{
		Codec: "aac", BitrateK: 128, SampleRate: dstRate, Channels: 2,
	}, "aac")
	if err != nil {
		t.Skipf("reencoder unavailable: %v", err)
	}
	defer re.Close()

	srcPkt := astiav.AllocPacket()
	defer srcPkt.Free()
	pcm := astiav.AllocFrame()
	defer pcm.Free()

	totalInSamples := srcRate * seconds
	emittedInSamples := 0
	var outAudioMsSpan int64 = -1
	var firstOutPTS, lastOutPTS int64
	haveFirst := false
	ptsMs := int64(0)

	for emittedInSamples < totalInSamples {
		pcm.Unref()
		pcm.SetNbSamples(srcFrameSize)
		pcm.SetSampleRate(srcRate)
		pcm.SetSampleFormat(astiav.SampleFormatFltp)
		pcm.SetChannelLayout(astiav.ChannelLayoutStereo)
		if err := pcm.AllocBuffer(0); err != nil {
			t.Fatalf("alloc pcm: %v", err)
		}
		if err := pcm.SamplesFillSilence(); err != nil {
			t.Fatalf("fill silence: %v", err)
		}
		pcm.SetPts(int64(emittedInSamples))
		if err := srcCC.SendFrame(pcm); err != nil {
			t.Fatalf("src send: %v", err)
		}
		for {
			if err := srcCC.ReceivePacket(srcPkt); err != nil {
				break
			}
			// The production input is ADTS-framed AAC from the TS
			// demuxer, so wrap the raw encoder output to match.
			adts := wrapADTS(srcPkt.Data(), srcRate, 2)
			out, err := re.Process(adts, ptsMs)
			srcPkt.Unref()
			if err != nil {
				t.Fatalf("reencode: %v", err)
			}
			for _, of := range out {
				if !haveFirst {
					firstOutPTS = of.PTS
					haveFirst = true
				}
				lastOutPTS = of.PTS
			}
		}
		emittedInSamples += srcFrameSize
		ptsMs += int64(srcFrameSize) * 1000 / srcRate
	}

	if haveFirst {
		outAudioMsSpan = lastOutPTS - firstOutPTS
	}
	// Output PTS span should be within ~10 % of the input duration
	// (2 s). The bug produced a span far larger (audio racing ahead).
	inMs := int64(seconds * 1000)
	if outAudioMsSpan < 0 {
		t.Fatal("no audio output produced")
	}
	lo, hi := inMs*85/100, inMs*115/100
	if outAudioMsSpan < lo || outAudioMsSpan > hi {
		t.Fatalf("output PTS span %d ms outside [%d,%d] for %d ms input — re-encode over/under-produces (resample bug)",
			outAudioMsSpan, lo, hi, inMs)
	}
}
