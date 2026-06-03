package buffer

import (
	"sync"
	"testing"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

const testStream domain.StreamCode = "test"

func TestServiceCreateSubscribeWriteReceive(t *testing.T) {
	s := NewServiceForTesting(8)
	s.Create(testStream)

	sub, err := s.Subscribe(testStream)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer s.Unsubscribe(testStream, sub)

	if err := s.Write(testStream, TSPacket([]byte{0x47, 0x00, 0x01})); err != nil {
		t.Fatalf("write: %v", err)
	}

	select {
	case pkt := <-sub.Recv():
		if len(pkt.TS) != 3 || pkt.TS[0] != 0x47 {
			t.Fatalf("unexpected packet: %v", pkt.TS)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for packet")
	}
}

func TestServiceWriteToUnknownStream(t *testing.T) {
	s := NewServiceForTesting(4)
	if err := s.Write("nope", TSPacket([]byte{1})); err == nil {
		t.Fatal("expected error writing to unknown stream")
	}
}

func TestServiceSubscribeUnknown(t *testing.T) {
	s := NewServiceForTesting(4)
	if _, err := s.Subscribe("nope"); err == nil {
		t.Fatal("expected error subscribing to unknown stream")
	}
}

func TestServiceDropsForSlowConsumer(t *testing.T) {
	s := NewServiceForTesting(2)
	s.Create(testStream)

	sub, err := s.Subscribe(testStream)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer s.Unsubscribe(testStream, sub)

	// Capacity 2, push many — must not block.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			_ = s.Write(testStream, TSPacket([]byte{byte(i)}))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Write blocked — slow consumer must not stall the writer")
	}

	count := 0
drain:
	for {
		select {
		case _, ok := <-sub.Recv():
			if !ok {
				break drain
			}
			count++
		default:
			break drain
		}
	}
	if count == 0 || count > 2 {
		t.Fatalf("expected 1-2 packets, got %d", count)
	}
}

func TestServiceFanOutToMultipleSubscribers(t *testing.T) {
	s := NewServiceForTesting(8)
	s.Create(testStream)

	sub1, _ := s.Subscribe(testStream)
	sub2, _ := s.Subscribe(testStream)
	defer s.Unsubscribe(testStream, sub1)
	defer s.Unsubscribe(testStream, sub2)

	if err := s.Write(testStream, TSPacket([]byte{0xAA})); err != nil {
		t.Fatalf("write: %v", err)
	}

	for _, sub := range []*Subscriber{sub1, sub2} {
		select {
		case pkt := <-sub.Recv():
			if len(pkt.TS) != 1 || pkt.TS[0] != 0xAA {
				t.Fatalf("unexpected: %v", pkt.TS)
			}
		case <-time.After(time.Second):
			t.Fatal("subscriber did not receive packet")
		}
	}
}

func TestServiceSubscribersReceiveIndependentCopies(t *testing.T) {
	s := NewServiceForTesting(8)
	s.Create(testStream)

	sub1, _ := s.Subscribe(testStream)
	sub2, _ := s.Subscribe(testStream)
	defer s.Unsubscribe(testStream, sub1)
	defer s.Unsubscribe(testStream, sub2)

	original := []byte{0x01, 0x02, 0x03}
	_ = s.Write(testStream, TSPacket(original))

	pkt1 := <-sub1.Recv()
	pkt2 := <-sub2.Recv()

	pkt1.TS[0] = 0xFF
	if pkt2.TS[0] != 0x01 {
		t.Fatalf("subscribers shared backing slice: pkt2[0]=%x", pkt2.TS[0])
	}
	if original[0] != 0x01 {
		t.Fatalf("write mutated caller buffer: original[0]=%x", original[0])
	}
}

func TestServiceUnsubscribeClosesChannel(t *testing.T) {
	s := NewServiceForTesting(4)
	s.Create(testStream)

	sub, _ := s.Subscribe(testStream)
	s.Unsubscribe(testStream, sub)

	if _, ok := <-sub.Recv(); ok {
		t.Fatal("channel should be closed after Unsubscribe")
	}
}

func TestServiceDeleteRemovesBuffer(t *testing.T) {
	s := NewServiceForTesting(4)
	s.Create(testStream)
	s.Delete(testStream)

	if err := s.Write(testStream, TSPacket([]byte{1})); err == nil {
		t.Fatal("expected write to fail after Delete")
	}
}

// TestServiceDeleteUnsubscribesConsumers regression-tests the fix for the
// mixer-survives-upstream-restart bug. Delete used to remove the map entry
// while leaving subscriber channels open, which left mixer/copy taps blocked
// on Recv() forever — when the upstream came back the new ringBuffer was a
// different instance, so taps never saw the new packets and the downstream
// stream went silently dead.
//
// Contract: Delete MUST close every subscriber channel before removing the
// map entry, so consumers observe ok=false and can decide to reconnect.
func TestServiceDeleteUnsubscribesConsumers(t *testing.T) {
	s := NewServiceForTesting(4)
	s.Create(testStream)
	sub, err := s.Subscribe(testStream)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	s.Delete(testStream)

	select {
	case _, ok := <-sub.Recv():
		if ok {
			t.Fatal("expected channel close after Delete, got a packet")
		}
	case <-time.After(time.Second):
		t.Fatal("Delete did not close the subscriber channel — mixer/copy taps would block forever on upstream restart")
	}
}

func TestServiceCreateIsIdempotent(t *testing.T) {
	s := NewServiceForTesting(4)
	s.Create(testStream)

	sub, _ := s.Subscribe(testStream)
	defer s.Unsubscribe(testStream, sub)

	s.Create(testStream)
	_ = s.Write(testStream, TSPacket([]byte{0x42}))

	select {
	case pkt := <-sub.Recv():
		if pkt.TS[0] != 0x42 {
			t.Fatalf("unexpected packet: %v", pkt.TS)
		}
	case <-time.After(time.Second):
		t.Fatal("subscription was lost on re-Create")
	}
}

func TestServiceEmptyPacketIsDropped(t *testing.T) {
	s := NewServiceForTesting(4)
	s.Create(testStream)

	sub, _ := s.Subscribe(testStream)
	defer s.Unsubscribe(testStream, sub)

	_ = s.Write(testStream, Packet{})

	select {
	case pkt := <-sub.Recv():
		t.Fatalf("empty packet should not be delivered, got %+v", pkt)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestServiceAVPacketDelivered(t *testing.T) {
	s := NewServiceForTesting(4)
	s.Create(testStream)

	sub, _ := s.Subscribe(testStream)
	defer s.Unsubscribe(testStream, sub)

	src := &domain.AVPacket{Codec: domain.AVCodecH264, Data: []byte{0xAA, 0xBB}, KeyFrame: true}
	_ = s.Write(testStream, Packet{AV: src})

	pkt := <-sub.Recv()
	if pkt.AV == nil || pkt.AV.Codec != domain.AVCodecH264 || !pkt.AV.KeyFrame {
		t.Fatalf("AV packet not preserved: %+v", pkt.AV)
	}

	pkt.AV.Data[0] = 0xFF
	if src.Data[0] != 0xAA {
		t.Fatalf("subscriber mutated source AV.Data")
	}
}

func TestServiceConcurrentWritesNoDeadlock(t *testing.T) {
	s := NewServiceForTesting(64)
	s.Create(testStream)

	sub, _ := s.Subscribe(testStream)
	defer s.Unsubscribe(testStream, sub)

	go func() {
		for range sub.Recv() {
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = s.Write(testStream, TSPacket([]byte{byte(i), byte(j)}))
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent writers deadlocked")
	}
}

func TestRawIngestBufferID(t *testing.T) {
	got := RawIngestBufferID("foo")
	if got != "$raw$foo" {
		t.Fatalf("unexpected raw id: %s", got)
	}
}
