package ingestor

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor/tsnorm"
	"github.com/datvietvac-techhub/open-streamer/internal/timeline"
)

// disabledNormaliser is the no-op Normaliser used by readLoop tests that
// only exercise buffer fan-out (not timestamp anchoring). Pass-through
// matches the existing test expectations on PTS/DTS.
func disabledNormaliser() *timeline.Normaliser {
	return timeline.New(timeline.Config{})
}

// disabledTSNormaliser mirrors disabledNormaliser for the raw-TS path:
// a Normaliser whose underlying timeline.Config is Enabled=false so
// PTS/DTS pass through untouched. The demux/mux roundtrip still runs
// (so writeRawTSChunk exercises its normal code path) but the timeline
// re-anchor does nothing.
func disabledTSNormaliser(ctx context.Context) *tsnorm.Normaliser {
	return tsnorm.New(ctx, timeline.Config{})
}

// ---- mock PacketReader ----

type mockPacketReader struct {
	mu      sync.Mutex
	opens   int
	closes  int
	openErr error
	packets [][]byte // each ReadPackets yields one H264 AU with this payload
	readErr error
}

func (m *mockPacketReader) Open(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.opens++
	return m.openErr
}

func (m *mockPacketReader) ReadPackets(_ context.Context) ([]domain.AVPacket, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.packets) > 0 {
		pkt := m.packets[0]
		m.packets = m.packets[1:]
		return []domain.AVPacket{{
			Codec: domain.AVCodecH264,
			Data:  pkt,
			PTSms: 0,
			DTSms: 0,
		}}, nil
	}
	if m.readErr != nil {
		return nil, m.readErr
	}
	return nil, io.EOF
}

func (m *mockPacketReader) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closes++
	return nil
}

// ---- waitBackoff ----

func TestWaitBackoff_ContextCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	ok := waitBackoff(ctx, 10*time.Second)

	assert.False(t, ok)
	assert.Less(t, time.Since(start), time.Second, "should return immediately on cancelled ctx")
}

func TestWaitBackoff_TimerFires(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	start := time.Now()
	ok := waitBackoff(ctx, 20*time.Millisecond)

	assert.True(t, ok)
	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
}

// ---- readLoop ----

func TestReadLoop_WritesPacketsToBuffer(t *testing.T) {
	t.Parallel()

	streamID := domain.StreamCode("test-stream")
	buf := buffer.NewServiceForTesting(128)
	buf.Create(streamID)

	sub, err := buf.Subscribe(streamID)
	require.NoError(t, err)
	defer buf.Unsubscribe(streamID, sub)

	pkt1 := []byte("pkt-one-188-bytes-padded-to-length-xxxxxxxxxxxxxxxxxxxxxxxx")
	pkt2 := []byte("pkt-two-188-bytes-padded-to-length-xxxxxxxxxxxxxxxxxxxxxxxx")

	r := &mockPacketReader{packets: [][]byte{pkt1, pkt2}}

	errCh := make(chan error, 1)
	go func() {
		errCh <- readLoop(context.Background(), streamID, streamID, domain.Input{}, r, buf, nil, disabledNormaliser(), disabledTSNormaliser(context.Background()))
	}()

	var received [][]byte
	timeout := time.After(2 * time.Second)
	for len(received) < 2 {
		select {
		case p := <-sub.Recv():
			require.NotNil(t, p.AV)
			received = append(received, append([]byte(nil), p.AV.Data...))
		case <-timeout:
			t.Fatal("timed out waiting for packets")
		}
	}

	assert.Equal(t, pkt1, received[0])
	assert.Equal(t, pkt2, received[1])

	err = <-errCh
	assert.ErrorIs(t, err, io.EOF)
}

func TestReadLoop_ContextCancelled(t *testing.T) {
	t.Parallel()

	streamID := domain.StreamCode("stream-ctx")
	buf := buffer.NewServiceForTesting(32)
	buf.Create(streamID)

	ctx, cancel := context.WithCancel(context.Background())

	r := &mockPacketReader{readErr: context.Canceled}

	errCh := make(chan error, 1)
	go func() {
		cancel()
		errCh <- readLoop(ctx, streamID, streamID, domain.Input{}, r, buf, nil, disabledNormaliser(), disabledTSNormaliser(ctx))
	}()

	select {
	case err := <-errCh:
		assert.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("readLoop did not return after context cancel")
	}
}

func TestReadLoop_SkipsEmptyPackets(t *testing.T) {
	t.Parallel()

	streamID := domain.StreamCode("stream-empty")
	buf := buffer.NewServiceForTesting(32)
	buf.Create(streamID)

	sub, err := buf.Subscribe(streamID)
	require.NoError(t, err)
	defer buf.Unsubscribe(streamID, sub)

	realPkt := []byte("real-packet")
	r := &mockPacketReader{
		packets: [][]byte{{}, realPkt},
	}

	done := make(chan error, 1)
	go func() {
		done <- readLoop(context.Background(), streamID, streamID, domain.Input{}, r, buf, nil, disabledNormaliser(), disabledTSNormaliser(context.Background()))
	}()

	select {
	case p := <-sub.Recv():
		require.NotNil(t, p.AV)
		assert.Equal(t, realPkt, p.AV.Data)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
	<-done
}

// ---- runPullWorker ----

func TestRunPullWorker_ReadsAndWritesToBuffer(t *testing.T) {
	t.Parallel()

	streamID := domain.StreamCode("worker-stream")
	buf := buffer.NewServiceForTesting(128)
	buf.Create(streamID)

	sub, err := buf.Subscribe(streamID)
	require.NoError(t, err)
	defer buf.Unsubscribe(streamID, sub)

	pkt := []byte("ts-packet-data")
	r := &mockPacketReader{packets: [][]byte{pkt}}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		runPullWorker(ctx, streamID, streamID, domain.Input{Priority: 0}, r, buf, pullWorkerCallbacks{})
	}()

	select {
	case received := <-sub.Recv():
		require.NotNil(t, received.AV)
		assert.Equal(t, pkt, received.AV.Data)
	case <-time.After(2 * time.Second):
		t.Fatal("no packet received")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit")
	}
}

func TestRunPullWorker_StopsOnContextCancel(t *testing.T) {
	t.Parallel()

	streamID := domain.StreamCode("cancel-stream")
	buf := buffer.NewServiceForTesting(32)
	buf.Create(streamID)

	block := &blockingPacketReader{}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		runPullWorker(ctx, streamID, streamID, domain.Input{Priority: 0}, block, buf, pullWorkerCallbacks{})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop after ctx cancel")
	}
}

type blockingPacketReader struct{}

func (blockingPacketReader) Open(_ context.Context) error { return nil }

func (blockingPacketReader) ReadPackets(ctx context.Context) ([]domain.AVPacket, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (blockingPacketReader) Close() error { return nil }

func TestRunPullWorker_ReconnectsAfterOpenError(t *testing.T) {
	t.Parallel()

	streamID := domain.StreamCode("reconnect-stream")
	buf := buffer.NewServiceForTesting(64)
	buf.Create(streamID)

	sub, err := buf.Subscribe(streamID)
	require.NoError(t, err)
	defer buf.Unsubscribe(streamID, sub)

	pkt := []byte("success-packet")

	var mu sync.Mutex
	callCount := 0

	origReader := &controlledPacketReader{
		openFn: func() error {
			mu.Lock()
			defer mu.Unlock()
			callCount++
			if callCount == 1 {
				return errors.New("transient dial error")
			}
			return nil
		},
		readFn: func() ([]domain.AVPacket, error) {
			return []domain.AVPacket{{
				Codec: domain.AVCodecH264,
				Data:  pkt,
				PTSms: 0,
				DTSms: 0,
			}}, nil
		},
		closeFn: func() error { return nil },
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		runPullWorker(ctx, streamID, streamID, domain.Input{Priority: 0}, origReader, buf, pullWorkerCallbacks{})
	}()

	select {
	case received := <-sub.Recv():
		require.NotNil(t, received.AV)
		assert.Equal(t, pkt, received.AV.Data)
	case <-time.After(4 * time.Second):
		t.Fatal("worker never recovered after transient open error")
	}
}

type controlledPacketReader struct {
	openFn  func() error
	readFn  func() ([]domain.AVPacket, error)
	closeFn func() error
	readMu  sync.Mutex
	readOne bool
}

func (c *controlledPacketReader) Open(_ context.Context) error {
	err := c.openFn()
	c.readMu.Lock()
	if err == nil {
		c.readOne = false
	}
	c.readMu.Unlock()
	return err
}

func (c *controlledPacketReader) ReadPackets(ctx context.Context) ([]domain.AVPacket, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if c.readOne {
		return nil, io.EOF
	}
	c.readOne = true
	return c.readFn()
}

func (c *controlledPacketReader) Close() error {
	if c.closeFn != nil {
		return c.closeFn()
	}
	return nil
}
