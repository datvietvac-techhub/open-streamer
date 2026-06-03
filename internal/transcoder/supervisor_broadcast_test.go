package transcoder

import (
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	pb "github.com/datvietvac-techhub/open-streamer/internal/transcoder/native/proto"
)

// TestSupervisor_WriteOutputPacket_BroadcastFansToAllTargets locks in
// the audio-passthrough contract: an OutputPacket with TargetIndex=-1
// (BroadcastTargetIndex on the subprocess side) is written to EVERY
// rendition buffer. Without that fan-out, only track_1's HLS segments
// would contain audio and track_2 / track_3 players would silently
// play video-only.
func TestSupervisor_WriteOutputPacket_BroadcastFansToAllTargets(t *testing.T) {
	t.Parallel()
	buf := buffer.NewServiceForTesting(8)

	const stream domain.StreamCode = "test_bcast"
	targets := []RenditionTarget{
		{BufferID: stream + "/t0", Profile: Profile{Width: 1920, Height: 1080}},
		{BufferID: stream + "/t1", Profile: Profile{Width: 1280, Height: 720}},
		{BufferID: stream + "/t2", Profile: Profile{Width: 854, Height: 480}},
	}
	subs := make([]*buffer.Subscriber, 0, len(targets))
	for _, tg := range targets {
		buf.Create(tg.BufferID)
		sub, err := buf.Subscribe(tg.BufferID)
		if err != nil {
			t.Fatalf("subscribe %s: %v", tg.BufferID, err)
		}
		defer buf.Unsubscribe(tg.BufferID, sub)
		subs = append(subs, sub)
	}

	sv := &supervisor{
		svc:      &Service{buf: buf},
		streamID: stream,
		targets:  targets,
	}

	payload := []byte{0xFF, 0xF1, 0x4C, 0x80, 0x10, 0xBA, 0x40, 0x00}
	err := sv.writeOutputPacket(&pb.OutputPacket{
		TargetIndex: -1,
		Data:        payload,
		Codec:       pb.Codec_CODEC_AAC,
		PtsMs:       12345,
		DtsMs:       12345,
		Keyframe:    false,
	})
	if err != nil {
		t.Fatalf("writeOutputPacket(broadcast): %v", err)
	}

	for i, sub := range subs {
		select {
		case pkt := <-sub.Recv():
			if pkt.AV == nil {
				t.Fatalf("target %d: AV nil", i)
			}
			if pkt.AV.Codec != domain.AVCodecAAC {
				t.Fatalf("target %d: codec = %v want AAC", i, pkt.AV.Codec)
			}
			if string(pkt.AV.Data) != string(payload) {
				t.Fatalf("target %d: payload mismatch", i)
			}
			if pkt.AV.PTSms != 12345 {
				t.Fatalf("target %d: PTS = %d want 12345", i, pkt.AV.PTSms)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("target %d: timed out waiting for broadcast packet", i)
		}
	}
}

// TestSupervisor_WriteOutputPacket_SessionStartPropagates locks in
// the discontinuity wire contract: an OutputPacket with
// session_start=true on the wire must land on buffer.Packet.SessionStart
// so the publisher's existing onSessionBoundary handler fires and
// emits EXT-X-DISCONTINUITY on the next HLS segment. Without that
// marker reaching the publisher, players keep their MSE init state
// across input switches and corrupt-decode the new source.
func TestSupervisor_WriteOutputPacket_SessionStartPropagates(t *testing.T) {
	t.Parallel()
	buf := buffer.NewServiceForTesting(8)

	const stream domain.StreamCode = "test_sessionstart"
	target := RenditionTarget{BufferID: stream + "/t0", Profile: Profile{Width: 1920, Height: 1080}}
	buf.Create(target.BufferID)
	sub, err := buf.Subscribe(target.BufferID)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer buf.Unsubscribe(target.BufferID, sub)

	sv := &supervisor{
		svc:      &Service{buf: buf},
		streamID: stream,
		targets:  []RenditionTarget{target},
	}

	err = sv.writeOutputPacket(&pb.OutputPacket{
		TargetIndex:  0,
		Data:         []byte{0x00, 0x00, 0x00, 0x01, 0x67},
		Codec:        pb.Codec_CODEC_H264,
		PtsMs:        5000,
		DtsMs:        5000,
		Keyframe:     true,
		SessionStart: true,
	})
	if err != nil {
		t.Fatalf("writeOutputPacket: %v", err)
	}

	select {
	case pkt := <-sub.Recv():
		if !pkt.SessionStart {
			t.Fatal("buffer.Packet.SessionStart not set — publisher will skip EXT-X-DISCONTINUITY")
		}
		if pkt.AV == nil || !pkt.AV.KeyFrame {
			t.Fatalf("expected keyframe AV packet alongside SessionStart, got %+v", pkt)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for SessionStart packet")
	}
}

// TestSupervisor_WriteOutputPacket_SpecificTargetOnlyOneBufferGets
// covers the opposite case — a video frame with TargetIndex=1 must
// land ONLY in target 1's buffer. Catches a regression where the
// fan-out path accidentally fires for video too.
func TestSupervisor_WriteOutputPacket_SpecificTargetOnlyOneBufferGets(t *testing.T) {
	t.Parallel()
	buf := buffer.NewServiceForTesting(8)

	const stream domain.StreamCode = "test_specific"
	targets := []RenditionTarget{
		{BufferID: stream + "/t0", Profile: Profile{Width: 1920, Height: 1080}},
		{BufferID: stream + "/t1", Profile: Profile{Width: 1280, Height: 720}},
	}
	subs := make([]*buffer.Subscriber, 0, len(targets))
	for _, tg := range targets {
		buf.Create(tg.BufferID)
		sub, err := buf.Subscribe(tg.BufferID)
		if err != nil {
			t.Fatalf("subscribe %s: %v", tg.BufferID, err)
		}
		defer buf.Unsubscribe(tg.BufferID, sub)
		subs = append(subs, sub)
	}

	sv := &supervisor{
		svc:      &Service{buf: buf},
		streamID: stream,
		targets:  targets,
	}

	err := sv.writeOutputPacket(&pb.OutputPacket{
		TargetIndex: 1,
		Data:        []byte{0x00, 0x00, 0x00, 0x01, 0x67}, // SPS-ish bytes
		Codec:       pb.Codec_CODEC_H264,
		PtsMs:       2000,
		DtsMs:       2000,
		Keyframe:    true,
	})
	if err != nil {
		t.Fatalf("writeOutputPacket(target 1): %v", err)
	}

	// Target 1 receives the packet.
	select {
	case pkt := <-subs[1].Recv():
		if pkt.AV == nil || !pkt.AV.KeyFrame {
			t.Fatalf("target 1: expected keyframe AV packet, got %+v", pkt)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("target 1: timed out")
	}

	// Target 0 must NOT receive anything.
	select {
	case pkt := <-subs[0].Recv():
		t.Fatalf("target 0: unexpected packet %+v", pkt)
	case <-time.After(100 * time.Millisecond):
		// expected
	}
}
