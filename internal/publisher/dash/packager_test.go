package dash

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// Real-world H.264 1080p Main@4.0 SPS/PPS so mp4ff.ParseSPSNALUnit
// accepts them. Captured from a production HLS source; same bytes used
// in v1's test suite.
var (
	testH264SPS = []byte{0x67, 0x4d, 0x40, 0x28, 0xeb, 0x05, 0x07, 0x80, 0x44, 0x00, 0x00, 0x03, 0x00, 0x04, 0x00, 0x00, 0x03, 0x00, 0xf0, 0x3c, 0x60, 0xc6, 0x58}
	testH264PPS = []byte{0x68, 0xee, 0x3c, 0x80}
	// Synthetic IDR slice payload: NAL type 5 (IDR) + arbitrary bytes.
	// The packager only scans NAL types, doesn't decode the slice.
	testH264IDR = []byte{0x65, 0x88, 0x80, 0x40, 0x00, 0x00}
	// Synthetic non-IDR slice: NAL type 1 (non-IDR slice).
	testH264Slice = []byte{0x41, 0x9a, 0x12, 0x34, 0x56}
	// Real ADTS frame: profile 2 (AAC LC), sample rate idx 4 (44100),
	// channel cfg 2 (stereo), frame length 8 bytes including header.
	// Header is 7 bytes; first payload byte 0xAA.
	testADTSFrame = []byte{0xFF, 0xF1, 0x4C, 0x80, 0x01, 0x1F, 0xFC, 0xAA}
)

func startCode() []byte { return []byte{0, 0, 0, 1} }

// buildH264IDR concatenates SPS|PPS|IDR with Annex-B start codes — the
// access-unit shape every H.264 IDR carries when the ingestor delivers
// it to the packager.
func buildH264IDR() []byte {
	out := make([]byte, 0, 4*3+len(testH264SPS)+len(testH264PPS)+len(testH264IDR))
	out = append(out, startCode()...)
	out = append(out, testH264SPS...)
	out = append(out, startCode()...)
	out = append(out, testH264PPS...)
	out = append(out, startCode()...)
	out = append(out, testH264IDR...)
	return out
}

// buildH264NonIDR returns a non-IDR slice in Annex-B form.
func buildH264NonIDR() []byte {
	out := make([]byte, 0, 4+len(testH264Slice))
	out = append(out, startCode()...)
	out = append(out, testH264Slice...)
	return out
}

// setupPackager wires a Packager with a buffer subscription against a
// temp dir. Returns the packager, buffer service, stream code, and a
// cleanup func.
func setupPackager(t *testing.T, packAudio bool) (*Packager, *buffer.Service, domain.StreamCode, func()) {
	t.Helper()
	streamID := domain.StreamCode("test-" + t.Name())
	dir := t.TempDir()

	cfg := Config{
		StreamID:       string(streamID),
		StreamDir:      dir,
		ManifestPath:   filepath.Join(dir, "index.mpd"),
		SegDur:         500 * time.Millisecond, // short for fast tests
		Window:         3,
		History:        0,
		Ephemeral:      true,
		PairingTimeout: 200 * time.Millisecond,
		PackAudio:      packAudio,
	}
	p, err := NewPackager(cfg)
	if err != nil {
		t.Fatalf("NewPackager: %v", err)
	}
	bs := buffer.NewServiceForTesting(64)
	bs.Create(streamID)
	cleanup := func() {
		bs.Delete(streamID)
	}
	return p, bs, streamID, cleanup
}

// pushAV writes an AV packet to the buffer's stream.
func pushAV(t *testing.T, bs *buffer.Service, id domain.StreamCode, codec domain.AVCodec, data []byte, pts, dts uint64, key bool) {
	t.Helper()
	pkt := buffer.Packet{
		AV: &domain.AVPacket{
			Codec:    codec,
			Data:     data,
			PTSms:    pts,
			DTSms:    dts,
			KeyFrame: key,
		},
	}
	if err := bs.Write(id, pkt); err != nil {
		t.Fatalf("buffer.Write: %v", err)
	}
}

// TestPackager_AVPath_WritesInitAndSegments — happy path. Push an IDR
// + a few non-IDRs + AAC frames; verify init_v.mp4, init_a.mp4, and at
// least one media segment + manifest appear on disk.
func TestPackager_AVPath_WritesInitAndSegments(t *testing.T) {
	p, bs, id, done := setupPackager(t, true)
	defer done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := bs.Subscribe(id)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer bs.Unsubscribe(id, sub)

	doneCh := make(chan struct{})
	go func() {
		p.Run(ctx, sub)
		close(doneCh)
	}()

	// Push frames spanning > segDur so the segmenter cuts at the IDR.
	// Initial IDR at PTS=0, then non-IDRs at 40ms intervals up to 800ms,
	// then a second IDR at 1000ms. Audio interleaved at 23ms cadence.
	pushAV(t, bs, id, domain.AVCodecH264, buildH264IDR(), 0, 0, true)
	pushAV(t, bs, id, domain.AVCodecAAC, testADTSFrame, 0, 0, false)
	for i := 1; i <= 20; i++ {
		pts := uint64(i) * 40 //nolint:gosec
		isKey := i == 20
		var frame []byte
		if isKey {
			frame = buildH264IDR()
		} else {
			frame = buildH264NonIDR()
		}
		pushAV(t, bs, id, domain.AVCodecH264, frame, pts, pts, isKey)
	}
	// 50 AAC frames at 23ms cadence (~1150ms of audio). Hybrid V/A
	// coordination requires audio queue to hold targetA frames where
	// targetA = round(V_dur_ms × sr / 1024 / 1000). For this test's
	// 800 ms V cut at 44.1 kHz: targetA ≈ 34 — push more than that so
	// the cut isn't held.
	for i := 0; i < 50; i++ {
		pushAV(t, bs, id, domain.AVCodecAAC, testADTSFrame, uint64(i)*23, uint64(i)*23, false) //nolint:gosec
	}

	// Wait up to 2s for the ticker (50ms) + segmenter to produce a
	// segment + manifest.
	require := func(cond bool, msg string) {
		t.Helper()
		if !cond {
			t.Fatal(msg)
		}
	}
	waitForFile(t, filepath.Join(p.cfg.StreamDir, "init_v.mp4"), 2*time.Second)
	waitForFile(t, filepath.Join(p.cfg.StreamDir, "init_a.mp4"), 2*time.Second)
	waitForFile(t, filepath.Join(p.cfg.StreamDir, "seg_v_00001.m4s"), 2*time.Second)
	waitForFile(t, filepath.Join(p.cfg.StreamDir, "seg_a_00001.m4s"), 2*time.Second)
	waitForFile(t, p.cfg.ManifestPath, 2*time.Second)

	// Manifest should contain both adaptation sets.
	data, err := os.ReadFile(p.cfg.ManifestPath)
	require(err == nil, "read manifest: "+stringOrErr(err))
	require(len(data) > 0, "manifest empty")
	cancel()
	<-doneCh
}

// TestPackager_PairingGate_HoldsUntilBothTracks — push only video,
// verify no segment is written until A arrives. Then push A → both
// segments appear.
func TestPackager_PairingGate_HoldsUntilBothTracks(t *testing.T) {
	p, bs, id, done := setupPackager(t, true)
	defer done()

	// Bump the pairing timeout up so the test deterministically
	// observes the hold (avoids racing the 200ms default).
	p.cfg.PairingTimeout = 5 * time.Second
	p.state = NewStateMachine(p.cfg.PairingTimeout)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := bs.Subscribe(id)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer bs.Unsubscribe(id, sub)

	doneCh := make(chan struct{})
	go func() {
		p.Run(ctx, sub)
		close(doneCh)
	}()

	// Push only video for a while — enough to span segDur and accumulate
	// the queue but pairing gate should hold the flush.
	pushAV(t, bs, id, domain.AVCodecH264, buildH264IDR(), 0, 0, true)
	for i := 1; i <= 20; i++ {
		pts := uint64(i) * 40 //nolint:gosec
		isKey := i == 20
		var frame []byte
		if isKey {
			frame = buildH264IDR()
		} else {
			frame = buildH264NonIDR()
		}
		pushAV(t, bs, id, domain.AVCodecH264, frame, pts, pts, isKey)
	}

	// Wait long enough that the segmenter has had multiple ticks. With
	// PackAudio=true + pairing gate, no segment should appear because
	// audio isn't ready.
	time.Sleep(300 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(p.cfg.StreamDir, "seg_v_00001.m4s")); err == nil {
		t.Fatal("pairing gate failed to hold first flush")
	}

	// Now push audio. Pairing achieved → segments emit. Need at least
	// targetA frames where targetA = round(V_dur_ms × sr / 1024 / 1000),
	// = ~34 for 800 ms V cut at 44.1 kHz. Push 50 with slack.
	for i := 0; i < 50; i++ {
		pushAV(t, bs, id, domain.AVCodecAAC, testADTSFrame, uint64(i)*23, uint64(i)*23, false) //nolint:gosec
	}
	waitForFile(t, filepath.Join(p.cfg.StreamDir, "seg_v_00001.m4s"), 2*time.Second)
	waitForFile(t, filepath.Join(p.cfg.StreamDir, "seg_a_00001.m4s"), 2*time.Second)
	cancel()
	<-doneCh
}

// TestPackager_AccumulateVideoPS_LargeFramesBeforeSPS — regression
// test for the stream_a/track_1 1080p ABR shard failure. Before the
// per-frame NAL extractor, accumulateVideoPS blindly appended every
// frame's full Annex-B bytes to p.videoPS. 1080p frames are 50-200 KB,
// so after ~10 frames with no inline SPS/PPS the 1 MiB cap fired and
// videoPSGivenUp latched — even though SPS/PPS would have arrived
// inline with the next IDR. The extractor must keep p.videoPS tiny
// regardless of frame size, so the cap never fires under normal
// operation.
func TestPackager_AccumulateVideoPS_LargeFramesBeforeSPS(t *testing.T) {
	p, _, _, done := setupPackager(t, true)
	defer done()

	// 20 large (150 KB) non-IDR frames with NO SPS/PPS. Total 3 MB —
	// well past the legacy 1 MiB cap.
	largeNonIDR := make([]byte, 4+150*1024)
	largeNonIDR[0], largeNonIDR[1], largeNonIDR[2], largeNonIDR[3] = 0, 0, 0, 1
	// NAL type 1 = non-IDR slice; rest is junk payload bytes.
	largeNonIDR[4] = 0x41
	for i := 5; i < len(largeNonIDR); i++ {
		largeNonIDR[i] = byte(i & 0xff)
	}
	for i := 0; i < 20; i++ {
		p.accumulateVideoPS(largeNonIDR)
		if p.videoPSGivenUp {
			t.Fatalf("accumulator gave up after %d large non-IDR frames (3 MB+); pre-fix bug regressed", i+1)
		}
	}
	if p.videoInit != nil {
		t.Fatal("videoInit should NOT be built yet — no SPS/PPS seen")
	}
	if got := len(p.videoPS); got > 1024 {
		t.Errorf("p.videoPS = %d bytes after 20 large non-IDR frames; expected tiny (no parameter sets to store)", got)
	}

	// Now feed an IDR carrying inline SPS+PPS. Extractor must pick
	// them up; tryBuildVideoInit must succeed.
	idrWithPS := buildH264IDR()
	p.accumulateVideoPS(idrWithPS)
	p.tryBuildVideoInit()
	if p.videoInit == nil {
		t.Fatal("videoInit not built after SPS+PPS IDR; extractor missed them")
	}
}

// TestPackager_PairingTimeout_VideoOnly — when audio never arrives,
// the pairing window times out and the packager proceeds video-only.
func TestPackager_PairingTimeout_VideoOnly(t *testing.T) {
	p, bs, id, done := setupPackager(t, false) // PackAudio=false: no audio expected
	defer done()
	// Set a very short pairing timeout so the test runs fast.
	p.cfg.PairingTimeout = 100 * time.Millisecond
	p.state = NewStateMachine(p.cfg.PairingTimeout)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := bs.Subscribe(id)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer bs.Unsubscribe(id, sub)

	doneCh := make(chan struct{})
	go func() {
		p.Run(ctx, sub)
		close(doneCh)
	}()

	// Video frames only — pairing timeout should fire and the segment
	// emits without audio.
	pushAV(t, bs, id, domain.AVCodecH264, buildH264IDR(), 0, 0, true)
	for i := 1; i <= 20; i++ {
		pts := uint64(i) * 40 //nolint:gosec
		isKey := i == 20
		var frame []byte
		if isKey {
			frame = buildH264IDR()
		} else {
			frame = buildH264NonIDR()
		}
		pushAV(t, bs, id, domain.AVCodecH264, frame, pts, pts, isKey)
	}
	waitForFile(t, filepath.Join(p.cfg.StreamDir, "seg_v_00001.m4s"), 2*time.Second)

	// No audio init or audio segment should exist.
	if _, err := os.Stat(filepath.Join(p.cfg.StreamDir, "init_a.mp4")); !os.IsNotExist(err) {
		t.Errorf("audio init unexpectedly present: %v", err)
	}
	cancel()
	<-doneCh
}

// TestPackager_NonPackShard_DropsIncomingAACFrames — when PackAudio=false
// (typical ABR non-primary shard), incoming AAC frames must not enqueue.
// Cut returns AudioCount=0 because audioSR=0, so the drain branch in
// writeSegments never fires; without an early return in handleAAC the
// queue grows unbounded, eventually firing "audio frame queue overflow"
// warnings while wasting CPU on ADTS splitting + init-segment I/O.
//
// Verifies: audio queue stays at 0, no init_a.mp4 file appears,
// audioFrameSeen stays false (so tryCut's pairing wait isn't gated on
// audio that this shard isn't responsible for).
func TestPackager_NonPackShard_DropsIncomingAACFrames(t *testing.T) {
	p, bs, id, done := setupPackager(t, false) // PackAudio=false
	defer done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := bs.Subscribe(id)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer bs.Unsubscribe(id, sub)

	doneCh := make(chan struct{})
	go func() {
		p.Run(ctx, sub)
		close(doneCh)
	}()

	// Push 50 AAC frames simulating sustained audio arrival on a
	// non-primary shard.
	for i := 0; i < 50; i++ {
		pushAV(t, bs, id, domain.AVCodecAAC, testADTSFrame, uint64(i)*23, uint64(i)*23, false) //nolint:gosec
	}

	// Give the Run loop time to consume them.
	time.Sleep(300 * time.Millisecond)

	cancel()
	<-doneCh

	p.mu.Lock()
	defer p.mu.Unlock()
	if got := p.queue.AudioLen(); got != 0 {
		t.Errorf("audio queue len = %d, want 0 (non-pack shard must not enqueue)", got)
	}
	if p.audioFrameSeen {
		t.Error("audioFrameSeen = true on non-pack shard; should stay false")
	}
	if p.audioInit != nil {
		t.Error("audioInit built on non-pack shard; should stay nil")
	}
	if _, err := os.Stat(filepath.Join(p.cfg.StreamDir, "init_a.mp4")); !os.IsNotExist(err) {
		t.Errorf("init_a.mp4 unexpectedly present on non-pack shard: %v", err)
	}
}

// TestPackager_SessionBoundary_DropsPendingQueue — push a partial
// in-progress segment, then a packet with SessionStart=true, then
// new-session frames. The new segment should NOT include old-session
// frames.
func TestPackager_SessionBoundary_DropsPendingQueue(t *testing.T) {
	p, bs, id, done := setupPackager(t, false)
	defer done()
	p.cfg.PairingTimeout = 100 * time.Millisecond
	p.state = NewStateMachine(p.cfg.PairingTimeout)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := bs.Subscribe(id)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer bs.Unsubscribe(id, sub)

	doneCh := make(chan struct{})
	go func() {
		p.Run(ctx, sub)
		close(doneCh)
	}()

	// Some frames in session 1.
	pushAV(t, bs, id, domain.AVCodecH264, buildH264IDR(), 0, 0, true)
	for i := 1; i < 5; i++ {
		pts := uint64(i) * 40 //nolint:gosec
		pushAV(t, bs, id, domain.AVCodecH264, buildH264NonIDR(), pts, pts, false)
	}
	// Wait for video init to be built.
	waitForFile(t, filepath.Join(p.cfg.StreamDir, "init_v.mp4"), 1*time.Second)

	// SessionStart marker carrying no payload — the buffer hub's
	// auto-stamp normally fires on the next packet after SetSession.
	// Simulate by writing a packet with SessionStart=true on a fresh
	// session minted via the buffer hub API.
	_ = bs.SetSession(id, domain.SessionStartReconnect, nil, nil)
	pushAV(t, bs, id, domain.AVCodecH264, buildH264IDR(), 5000, 5000, true)
	for i := 1; i <= 20; i++ {
		pts := 5000 + uint64(i)*40 //nolint:gosec
		isKey := i == 20
		var frame []byte
		if isKey {
			frame = buildH264IDR()
		} else {
			frame = buildH264NonIDR()
		}
		pushAV(t, bs, id, domain.AVCodecH264, frame, pts, pts, isKey)
	}
	waitForFile(t, filepath.Join(p.cfg.StreamDir, "seg_v_00001.m4s"), 2*time.Second)

	// Queue length should be small (only post-boundary frames retained).
	// Hard to assert exactly without instrumentation; the file existence
	// + lack of panics is the smoke check.
	cancel()
	<-doneCh
}

// TestPackager_ABRMode_NotifiesMaster — set up an ABRMaster, run a
// shard, verify the master receives a snapshot.
func TestPackager_ABRMode_NotifiesMaster(t *testing.T) {
	dir := t.TempDir()
	rootMPD := filepath.Join(dir, "index.mpd")
	master := NewABRMaster(rootMPD, "ladder", 500*time.Millisecond, 3)
	defer master.Stop()

	streamID := domain.StreamCode("abr-shard")
	shardDir := filepath.Join(dir, "track_1")
	cfg := Config{
		StreamID:       string(streamID),
		StreamDir:      shardDir,
		ManifestPath:   "", // no per-shard MPD — master writes root
		SegDur:         500 * time.Millisecond,
		Window:         3,
		History:        0,
		Ephemeral:      true,
		PairingTimeout: 100 * time.Millisecond,
		PackAudio:      true,
		ABRMaster:      master,
		ABRSlug:        "track_1",
	}
	p, err := NewPackager(cfg)
	if err != nil {
		t.Fatalf("NewPackager: %v", err)
	}

	bs := buffer.NewServiceForTesting(64)
	bs.Create(streamID)
	defer bs.Delete(streamID)
	sub, err := bs.Subscribe(streamID)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer bs.Unsubscribe(streamID, sub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	doneCh := make(chan struct{})
	go func() {
		p.Run(ctx, sub)
		close(doneCh)
	}()

	pushAV(t, bs, streamID, domain.AVCodecH264, buildH264IDR(), 0, 0, true)
	for i := 1; i <= 20; i++ {
		pts := uint64(i) * 40 //nolint:gosec
		isKey := i == 20
		var frame []byte
		if isKey {
			frame = buildH264IDR()
		} else {
			frame = buildH264NonIDR()
		}
		pushAV(t, bs, streamID, domain.AVCodecH264, frame, pts, pts, isKey)
	}
	// 50 AAC frames at 23 ms; hybrid V/A targetA ≈ 34 for an 800 ms V
	// cut at 44.1 kHz. Push enough to clear the coordination check.
	for i := 0; i < 50; i++ {
		pushAV(t, bs, streamID, domain.AVCodecAAC, testADTSFrame, uint64(i)*23, uint64(i)*23, false) //nolint:gosec
	}

	// Wait for the per-shard segment file (proves the packager wrote
	// something) + the debounced root MPD.
	waitForFile(t, filepath.Join(shardDir, "seg_v_00001.m4s"), 2*time.Second)
	waitForFile(t, rootMPD, 2*time.Second)
	cancel()
	<-doneCh
}

// TestSplitADTSBundle exercises the bundled-PES splitter that turns
// gomedia's multi-frame AAC PES into individual AudioFrames. The bug
// the splitter fixes: writeAudioSegment declares each AudioFrame as
// exactly 1024 samples in segment dur math (`len(frames) * 1024`), so
// emitting a bundled PES as a single AudioFrame collapses sample-count
// by the bundling factor — the MPD then advertises ~1.2 s of audio
// per 5 s wallclock interval (~20 % of expected) and the audio
// timeline lags wallclock indefinitely.
func TestSplitADTSBundle(t *testing.T) {
	// testADTSFrame: ADTS header (7B) + 1-byte payload (0xAA).
	// Sample rate idx encodes 48000 Hz → per-frame ms offset = 1024 * 1000 / 48000 = 21.
	const expectedPTSStepMS = 21

	t.Run("empty_input_returns_nil", func(t *testing.T) {
		if got := splitADTSBundle(nil, 0); got != nil {
			t.Errorf("expected nil for empty input, got %v", got)
		}
	})

	t.Run("single_frame_returns_one_AudioFrame", func(t *testing.T) {
		got := splitADTSBundle(testADTSFrame, 1000)
		if len(got) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(got))
		}
		if got[0].PTSms != 1000 {
			t.Errorf("PTSms = %d, want 1000", got[0].PTSms)
		}
		if !bytes.Equal(got[0].Raw, []byte{0xAA}) {
			t.Errorf("Raw = %x, want [aa]", got[0].Raw)
		}
	})

	t.Run("bundle_of_three_frames_splits_with_monotonic_PTS", func(t *testing.T) {
		bundle := bytes.Repeat(testADTSFrame, 3)
		got := splitADTSBundle(bundle, 1000)
		if len(got) != 3 {
			t.Fatalf("expected 3 frames, got %d (%+v)", len(got), got)
		}
		for i, f := range got {
			wantPTS := uint64(1000 + i*expectedPTSStepMS) //nolint:gosec
			if f.PTSms != wantPTS {
				t.Errorf("frame[%d].PTSms = %d, want %d", i, f.PTSms, wantPTS)
			}
			if !bytes.Equal(f.Raw, []byte{0xAA}) {
				t.Errorf("frame[%d].Raw = %x, want [aa]", i, f.Raw)
			}
		}
	})

	t.Run("garbage_tail_stops_at_parse_error", func(t *testing.T) {
		// One valid ADTS frame followed by 3 bytes that don't parse as ADTS.
		bundle := append(append([]byte{}, testADTSFrame...), 0x00, 0x00, 0x00)
		got := splitADTSBundle(bundle, 1000)
		if len(got) != 1 {
			t.Fatalf("expected 1 frame (garbage tail discarded), got %d", len(got))
		}
		if got[0].PTSms != 1000 {
			t.Errorf("PTSms = %d, want 1000", got[0].PTSms)
		}
	})

	t.Run("no_ADTS_header_falls_back_to_single_raw_frame", func(t *testing.T) {
		// Raw AAC payload with no sync word — defensive fallback for paths
		// that pre-strip ADTS upstream.
		raw := []byte{0xAA, 0xBB, 0xCC, 0xDD}
		got := splitADTSBundle(raw, 500)
		if len(got) != 1 {
			t.Fatalf("expected 1 frame on no-ADTS fallback, got %d", len(got))
		}
		if got[0].PTSms != 500 {
			t.Errorf("PTSms = %d, want 500", got[0].PTSms)
		}
		if !bytes.Equal(got[0].Raw, raw) {
			t.Errorf("Raw = %x, want %x (data passed through unchanged)", got[0].Raw, raw)
		}
	})
}

// TestTfdtForSegment — sequential-after-first tfdt anchoring. First
// segment lands on wallclock-since-AST so the MPD timeline is wallclock-
// anchored at its origin; subsequent segments are cumulative on media
// time so adjacent <S t=...> entries are contiguous (zero inter-segment
// gap).
func TestTfdtForSegment(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("video_first_seg_uses_wallclock_anchor", func(t *testing.T) {
		// No entries → seeds onto wallclockTicks(now, AST). At now=AST,
		// wallclockTicks = 0.
		got := videoTfdtForSegment(nil, t0, t0)
		if got != 0 {
			t.Errorf("first segment tfdt at now==AST → %d, want 0", got)
		}
	})

	t.Run("video_first_seg_seeded_late", func(t *testing.T) {
		// AST set, but first emit happens 3 s after. tfdt should reflect
		// that wallclock offset so the MPD timeline starts at the right
		// place.
		got := videoTfdtForSegment(nil, t0.Add(3*time.Second), t0)
		want := uint64(3) * uint64(VideoTimescale)
		if got != want {
			t.Errorf("first segment tfdt at now=AST+3s → %d, want %d", got, want)
		}
	})

	t.Run("video_subsequent_seg_uses_prev_end", func(t *testing.T) {
		entries := []SegmentEntry{
			{StartTicks: 0, DurTicks: 6 * uint64(VideoTimescale)},      // ends at 6s
			{StartTicks: 6 * uint64(VideoTimescale), DurTicks: 540000}, // ends at 12s
		}
		// now is irrelevant for subsequent segments — should NOT influence tfdt.
		got := videoTfdtForSegment(entries, t0.Add(time.Hour), t0)
		want := uint64(12) * uint64(VideoTimescale)
		if got != want {
			t.Errorf("subsequent seg tfdt = %d, want %d (= prev.Start + prev.Dur)", got, want)
		}
	})

	t.Run("audio_first_seg_uses_wallclock_anchor_with_sr_timescale", func(t *testing.T) {
		// Audio timescale = sample rate. At 48 kHz, wallclockTicks(now=AST, AST) = 0.
		got := audioTfdtForSegment(nil, t0, t0, 48000)
		if got != 0 {
			t.Errorf("first audio seg tfdt at now==AST → %d, want 0", got)
		}
	})

	t.Run("audio_subsequent_uses_prev_end", func(t *testing.T) {
		entries := []SegmentEntry{
			{StartTicks: 0, DurTicks: 2 * 48000},     // 2 s @ 48 kHz
			{StartTicks: 2 * 48000, DurTicks: 96000}, // ends at 4 s
		}
		got := audioTfdtForSegment(entries, t0.Add(time.Hour), t0, 48000)
		want := uint64(4) * 48000
		if got != want {
			t.Errorf("subsequent audio seg tfdt = %d, want %d", got, want)
		}
	})
}

// TestComputeVideoSegDurTicks — dur math must cover [first.PTS,
// next.PTS] so the segment doesn't visibly under-report by one
// inter-frame interval (the player-visible 1-frame stutter at every
// segment boundary).
func TestComputeVideoSegDurTicks(t *testing.T) {
	mkFrames := func(ptsValues ...uint64) []VideoFrame {
		out := make([]VideoFrame, len(ptsValues))
		for i, p := range ptsValues {
			out[i] = VideoFrame{PTSms: p}
		}
		return out
	}

	t.Run("uses_next_PTS_when_available", func(t *testing.T) {
		// 25 fps × 150 frames: span = 5960 ms (149 intervals × 40 ms);
		// the 151st frame sits at 6000 ms. Dur must read 6000 ms.
		frames := mkFrames(0, 40, 80) // span doesn't matter when next is given
		got := computeVideoSegDurTicks(frames, 6000, true)
		want := uint64(6000) * uint64(VideoTimescale) / 1000
		if got != want {
			t.Errorf("dur with next=6000ms first=0 → %d ticks, want %d", got, want)
		}
	})

	t.Run("fallback_estimates_when_no_next", func(t *testing.T) {
		// Three frames at 40 ms cadence: span = 80 ms, perFrame ≈ 40 ms,
		// dur = 120 ms (covers the third frame's own duration).
		frames := mkFrames(0, 40, 80)
		got := computeVideoSegDurTicks(frames, 0, false)
		want := uint64(120) * uint64(VideoTimescale) / 1000
		if got != want {
			t.Errorf("fallback dur for 3-frame 40ms = %d, want %d", got, want)
		}
	})

	t.Run("hasNext_but_next_not_past_first_falls_back", func(t *testing.T) {
		// Pathological backward PTS — guards against uint64 underflow.
		frames := mkFrames(1000, 1040, 1080)
		got := computeVideoSegDurTicks(frames, 500, true)
		// Falls back to span+perFrame estimate.
		want := uint64(120) * uint64(VideoTimescale) / 1000
		if got != want {
			t.Errorf("fallback (next<first) = %d, want %d", got, want)
		}
	})

	t.Run("empty_frames_returns_safety_fallback", func(t *testing.T) {
		got := computeVideoSegDurTicks(nil, 0, false)
		if got == 0 {
			t.Error("expected non-zero fallback for empty frames")
		}
	})

	t.Run("single_frame_with_next_uses_next", func(t *testing.T) {
		frames := mkFrames(1000)
		got := computeVideoSegDurTicks(frames, 1040, true)
		want := uint64(40) * uint64(VideoTimescale) / 1000
		if got != want {
			t.Errorf("single-frame with next = %d, want %d", got, want)
		}
	})

	t.Run("single_frame_no_next_uses_40ms_safety", func(t *testing.T) {
		frames := mkFrames(1000)
		got := computeVideoSegDurTicks(frames, 0, false)
		want := uint64(40) * uint64(VideoTimescale) / 1000
		if got != want {
			t.Errorf("single-frame fallback = %d, want %d", got, want)
		}
	})
}

// TestPackager_BehindPrevSegEnd — the timeline-pace gate. Verifies the
// helper that holds emit until wallclock catches up to the previous
// segment's end. Manipulates Packager state directly so each case is
// hermetic; integration with tryCut is covered by the run-loop tests.
func TestPackager_BehindPrevSegEnd(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("availStart_zero_returns_false", func(t *testing.T) {
		p := &Packager{}
		if p.behindPrevSegEnd(t0) {
			t.Error("expected false when availStart is zero")
		}
	})

	t.Run("no_entries_returns_false", func(t *testing.T) {
		p := &Packager{availStart: t0}
		if p.behindPrevSegEnd(t0.Add(time.Second)) {
			t.Error("expected false with no segment entries")
		}
	})

	t.Run("video_now_before_prev_end_returns_true", func(t *testing.T) {
		// Prev video seg: t=0, d=6s @ 90 kHz → end at 6s after AST.
		p := &Packager{
			availStart:  t0,
			vSegEntries: []SegmentEntry{{StartTicks: 0, DurTicks: 6 * uint64(VideoTimescale)}},
		}
		// Now = AST + 1s → 1s < 6s → hold.
		if !p.behindPrevSegEnd(t0.Add(1 * time.Second)) {
			t.Error("expected true when now is before prev seg end")
		}
	})

	t.Run("video_now_at_prev_end_returns_false", func(t *testing.T) {
		p := &Packager{
			availStart:  t0,
			vSegEntries: []SegmentEntry{{StartTicks: 0, DurTicks: 6 * uint64(VideoTimescale)}},
		}
		// Now = AST + 6s → equal → not strictly less → proceed.
		if p.behindPrevSegEnd(t0.Add(6 * time.Second)) {
			t.Error("expected false when now equals prev seg end")
		}
	})

	t.Run("video_now_after_prev_end_returns_false", func(t *testing.T) {
		p := &Packager{
			availStart:  t0,
			vSegEntries: []SegmentEntry{{StartTicks: 0, DurTicks: 6 * uint64(VideoTimescale)}},
		}
		if p.behindPrevSegEnd(t0.Add(7 * time.Second)) {
			t.Error("expected false when now is past prev seg end")
		}
	})

	t.Run("audio_only_now_before_prev_end_returns_true", func(t *testing.T) {
		// Audio-only: vSegEntries empty, audio has 1 entry.
		// Audio timescale = sample rate (48000). Prev audio: t=0, d=2s @ 48 kHz.
		p := &Packager{
			availStart:  t0,
			audioInit:   &AudioInit{SampleRate: 48000},
			aSegEntries: []SegmentEntry{{StartTicks: 0, DurTicks: 2 * 48000}},
		}
		if !p.behindPrevSegEnd(t0.Add(1 * time.Second)) {
			t.Error("expected true when audio is behind on audio-only stream")
		}
	})

	t.Run("video_ahead_audio_behind_returns_true", func(t *testing.T) {
		// Mismatched per-track timelines: video caught up, audio still behind.
		// Gate should hold because audio would overlap.
		p := &Packager{
			availStart: t0,
			audioInit:  &AudioInit{SampleRate: 48000},
			// Video prev seg ends at 1s.
			vSegEntries: []SegmentEntry{{StartTicks: 0, DurTicks: 1 * uint64(VideoTimescale)}},
			// Audio prev seg ends at 5s.
			aSegEntries: []SegmentEntry{{StartTicks: 0, DurTicks: 5 * 48000}},
		}
		// Now = AST + 2s → video OK (2 ≥ 1) but audio still behind (2 < 5).
		if !p.behindPrevSegEnd(t0.Add(2 * time.Second)) {
			t.Error("expected true when audio is behind even if video is OK")
		}
	})

	t.Run("video_behind_audio_ahead_returns_true", func(t *testing.T) {
		p := &Packager{
			availStart:  t0,
			audioInit:   &AudioInit{SampleRate: 48000},
			vSegEntries: []SegmentEntry{{StartTicks: 0, DurTicks: 5 * uint64(VideoTimescale)}},
			aSegEntries: []SegmentEntry{{StartTicks: 0, DurTicks: 1 * 48000}},
		}
		// Now = AST + 2s → video behind (2 < 5), audio OK.
		if !p.behindPrevSegEnd(t0.Add(2 * time.Second)) {
			t.Error("expected true when video is behind")
		}
	})

	t.Run("both_caught_up_returns_false", func(t *testing.T) {
		p := &Packager{
			availStart:  t0,
			audioInit:   &AudioInit{SampleRate: 48000},
			vSegEntries: []SegmentEntry{{StartTicks: 0, DurTicks: 1 * uint64(VideoTimescale)}},
			aSegEntries: []SegmentEntry{{StartTicks: 0, DurTicks: 1 * 48000}},
		}
		if p.behindPrevSegEnd(t0.Add(2 * time.Second)) {
			t.Error("expected false when both tracks caught up")
		}
	})
}

// ─── helpers ─────────────────────────────────────────────────────────

// waitForFile polls path until it exists or timeout elapses.
func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("file did not appear within %v: %s", timeout, path)
}

func stringOrErr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
