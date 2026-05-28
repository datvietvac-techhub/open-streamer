package publisher_test

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/q191201771/lal/pkg/base"
	lalrtmp "github.com/q191201771/lal/pkg/rtmp"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/publisher"
)

func requireFFmpeg(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available:", err)
	}
}

// rtmpTestServer is a minimal RTMP server built on q191201771/lal/pkg/rtmp,
// used solely as the receive sink for our publisher's RTMP push tests.
// LAL drives the handshake + publish-accept; OnReadRtmpAvMsg fires for
// every video / audio frame the publisher pushes, which we count.
//
// Migrated from gomedia/go-rtmp when the rest of the codebase consolidated
// onto LAL (see internal/ingestor/pull/rtmp.go header).
type rtmpTestServer struct {
	port    int
	packets atomic.Int64

	sessionStarted chan struct{}
	startOnce      sync.Once
	secondSession  chan struct{}
	secondOnce     sync.Once

	mu       sync.Mutex
	sessions int32

	server *lalrtmp.Server
}

// pubObserver implements lalrtmp.IPubSessionObserver — counts every AV
// message the publisher delivers.
type pubObserver struct{ srv *rtmpTestServer }

func (o *pubObserver) OnReadRtmpAvMsg(_ base.RtmpMsg) {
	o.srv.packets.Add(1)
}

func newRTMPTestServer(t *testing.T) *rtmpTestServer {
	t.Helper()

	// Discover a free port by binding once and closing — LAL's Listen()
	// owns the listener exclusively, so we can't share the socket. The
	// brief gap between Close and Listen is the standard "free port"
	// pattern; collisions are rare enough to not matter in tests.
	lc := &net.ListenConfig{}
	probe, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := probe.Addr().(*net.TCPAddr).Port
	_ = probe.Close()

	s := &rtmpTestServer{
		port:           port,
		sessionStarted: make(chan struct{}),
		secondSession:  make(chan struct{}),
	}
	s.server = lalrtmp.NewServer(fmt.Sprintf("127.0.0.1:%d", port), s)

	require.NoError(t, s.server.Listen())
	go func() { _ = s.server.RunLoop() }()

	t.Cleanup(func() { s.server.Dispose() })
	return s
}

// ── lalrtmp.IServerObserver ─────────────────────────────────────────────────

func (s *rtmpTestServer) OnRtmpConnect(_ *lalrtmp.ServerSession, _ lalrtmp.ObjectPairArray) {
}

func (s *rtmpTestServer) OnNewRtmpPubSession(session *lalrtmp.ServerSession) error {
	s.mu.Lock()
	s.sessions++
	n := s.sessions
	s.mu.Unlock()

	if n == 1 {
		s.startOnce.Do(func() { close(s.sessionStarted) })
	}
	if n >= 2 {
		s.secondOnce.Do(func() { close(s.secondSession) })
	}

	session.SetPubSessionObserver(&pubObserver{srv: s})
	return nil
}

func (s *rtmpTestServer) OnDelRtmpPubSession(_ *lalrtmp.ServerSession) {}

// Reject sub sessions — this server is push-only.
func (s *rtmpTestServer) OnNewRtmpSubSession(_ *lalrtmp.ServerSession) error {
	return fmt.Errorf("test server does not accept play")
}

func (s *rtmpTestServer) OnDelRtmpSubSession(_ *lalrtmp.ServerSession) {}

func (s *rtmpTestServer) rtmpURL(path string) string {
	return fmt.Sprintf("rtmp://127.0.0.1:%d/live/%s", s.port, path)
}

func (s *rtmpTestServer) waitSession(t *testing.T, ch <-chan struct{}, name string, timeout time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", name)
	}
}

// feedFFmpegTS starts ffmpeg producing TS to stdout and writes raw 188-byte
// TS packets into the buffer. Returns a cancel function.
func feedFFmpegTS(
	t *testing.T,
	streamCode domain.StreamCode,
	buf *buffer.Service,
	markDiscontinuity bool,
) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-re",
		"-f", "lavfi", "-i", "testsrc=size=320x240:rate=25",
		"-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=44100",
		"-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
		"-g", "25", "-pix_fmt", "yuv420p",
		"-c:a", "aac", "-b:a", "64k",
		"-f", "mpegts", "pipe:1",
	)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})

	go func() {
		pktBuf := make([]byte, 188)
		first := markDiscontinuity
		for ctx.Err() == nil {
			if _, err := readFull(stdout, pktBuf); err != nil {
				return
			}
			if first {
				_ = buf.Write(streamCode, buffer.Packet{
					AV: &domain.AVPacket{
						Codec: domain.AVCodecH264,
						Data:  []byte{0},
					},
					// Session-boundary cue migrated from AVPacket.Discontinuity
					// (deleted Phase 5) to buffer.Packet.SessionStart.
					SessionStart: true,
				})
				first = false
			}
			ts := make([]byte, 188)
			copy(ts, pktBuf)
			_ = buf.Write(streamCode, buffer.Packet{TS: ts})
		}
	}()

	return cancel
}

// readFull reads exactly len(buf) bytes from r.
func readFull(r interface{ Read([]byte) (int, error) }, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// TestRTMPPushInputSwitch verifies that the RTMP push session tears down on
// input discontinuity and reconnects with fresh codec probing.
func TestRTMPPushInputSwitch(t *testing.T) {
	requireFFmpeg(t)

	// ── 1. Start RTMP test server (in-process, no Docker) ───────────────────
	srv := newRTMPTestServer(t)
	pushURL := srv.rtmpURL("output")
	t.Logf("RTMP test server on %s", pushURL)

	// ── 2. Create buffer + publisher ────────────────────────────────────────
	streamCode := domain.StreamCode("test_push")
	buf := buffer.NewServiceForTesting(2000)
	buf.Create(streamCode)

	bus := events.New(4, 64)
	pubCfg := config.PublisherConfig{
		HLS:  config.PublisherHLSConfig{Dir: t.TempDir()},
		DASH: config.PublisherDASHConfig{Dir: t.TempDir()},
	}
	pub := publisher.NewServiceForTesting(pubCfg, buf, bus)

	stream := &domain.Stream{
		Code: streamCode,
		Push: []domain.PushDestination{
			{
				URL:             pushURL,
				Enabled:         true,
				TimeoutSec:      10,
				RetryTimeoutSec: 2,
			},
		},
		Protocols: &domain.OutputProtocols{},
	}

	// ── 3. Start push publisher ─────────────────────────────────────────────
	require.NoError(t, pub.Start(context.Background(), stream))
	t.Cleanup(func() { pub.Stop(streamCode) })

	// ── 4. Feed source-1 ────────────────────────────────────────────────────
	t.Log("Starting feed-1...")
	cancelFeed1 := feedFFmpegTS(t, streamCode, buf, false)

	// ── 5. Wait for session-1 ───────────────────────────────────────────────
	srv.waitSession(t, srv.sessionStarted, "session-1", 15*time.Second)
	t.Log("Session-1 established!")

	time.Sleep(3 * time.Second)
	pkts1 := srv.packets.Load()
	t.Logf("Packets received before switch: %d", pkts1)
	assert.Greater(t, pkts1, int64(0), "expected packets before switch")

	// ── 6. Simulate input switch ────────────────────────────────────────────
	t.Log("Simulating input switch...")
	cancelFeed1()

	// Session-boundary marker (migrated from AVPacket.Discontinuity
	// in Phase 5 of the refactor).
	_ = buf.Write(streamCode, buffer.Packet{
		AV: &domain.AVPacket{
			Codec: domain.AVCodecH264,
			Data:  []byte{0},
		},
		SessionStart: true,
	})

	// ── 7. Feed source-2 ────────────────────────────────────────────────────
	t.Log("Starting feed-2...")
	cancelFeed2 := feedFFmpegTS(t, streamCode, buf, true)
	defer cancelFeed2()

	// ── 8. Wait for session-2 (push reconnected) ────────────────────────────
	srv.waitSession(t, srv.secondSession, "session-2", 20*time.Second)
	t.Log("Session-2 established — push reconnected after discontinuity!")

	time.Sleep(3 * time.Second)
	pkts2 := srv.packets.Load()
	t.Logf("Total packets received after switch: %d", pkts2)
	assert.Greater(t, pkts2, pkts1, "expected more packets after switch")

	t.Log("RTMP push survived input switch successfully!")
}
