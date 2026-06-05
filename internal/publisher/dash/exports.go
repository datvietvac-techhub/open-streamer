package dash

import "time"

// exports.go exposes a few pure helpers to the DVR blob-archive writer
// (internal/dvr/blob), which reuses the same fragment-duration math, ADTS
// splitting, and wallclock-tick conversion as the live packager so an archived
// fragment is byte-compatible with a live one. These are thin wrappers over the
// unexported implementations — no behaviour change for the live path.

// ComputeVideoSegDurTicks returns a video fragment's duration in 90 kHz ticks
// from its frames and the peeked next-frame PTS (includes the last frame's own
// duration — the documented anti-stutter rule). See computeVideoSegDurTicks.
func ComputeVideoSegDurTicks(frames []VideoFrame, nextPTSms uint64, hasNext bool) uint64 {
	return computeVideoSegDurTicks(frames, nextPTSms, hasNext)
}

// WallclockTicks returns (now − ast) × timescale, clamped to 0 when ast is
// unset. The DVR uses it only for the dynamic-MPD availability-start math.
func WallclockTicks(now, ast time.Time, timescale uint64) uint64 {
	return wallclockTicks(now, ast, timescale)
}

// SplitADTSBundle splits a gomedia-bundled ADTS PES (4–8 access units) into
// individual AudioFrames with per-frame PTS = base + i×1024×1000/sampleRate.
func SplitADTSBundle(data []byte, basePTSms uint64) []AudioFrame {
	return splitADTSBundle(data, basePTSms)
}
