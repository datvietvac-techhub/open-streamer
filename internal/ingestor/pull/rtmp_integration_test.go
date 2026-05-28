package pull_test

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor/pull"
)

// requireDocker skips the test if Docker is not reachable.
func requireDocker(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "info").Run(); err != nil {
		t.Skip("Docker not available:", err)
	}
}

// requireFFmpeg skips the test if ffmpeg is not on PATH.
func requireFFmpeg(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available:", err)
	}
}

// startMediaMTX runs a mediamtx RTMP relay container and returns host:port.
func startMediaMTX(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "bluenviron/mediamtx:latest",
		ExposedPorts: []string{"1935/tcp"},
		WaitingFor:   wait.ForListeningPort("1935/tcp").WithStartupTimeout(30 * time.Second),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	host, err := ctr.Host(ctx)
	require.NoError(t, err)
	port, err := ctr.MappedPort(ctx, "1935/tcp")
	require.NoError(t, err)
	return host + ":" + strconv.Itoa(port.Int())
}

// TestRTMPReader_PullsFromMediaMTX spins up a mediamtx RTMP relay, publishes a
// synthetic H.264 + AAC test stream with ffmpeg, then verifies the RTMPReader
// connects and produces AVPackets for both video and audio tracks.
func TestRTMPReader_PullsFromMediaMTX(t *testing.T) {
	requireDocker(t)
	requireFFmpeg(t)

	hostPort := startMediaMTX(t)
	publishURL := fmt.Sprintf("rtmp://%s/live/test", hostPort)
	pullURL := publishURL

	// Start ffmpeg as a background publisher using synthetic test sources.
	// -re paces input to real-time so packets trickle over RTMP at 25 fps.
	ffCtx, ffCancel := context.WithCancel(context.Background())
	t.Cleanup(ffCancel)

	ffCmd := exec.CommandContext(ffCtx, "ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-re",
		"-f", "lavfi", "-i", "testsrc=size=320x240:rate=25",
		"-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=44100",
		"-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
		"-g", "25", "-pix_fmt", "yuv420p",
		"-c:a", "aac", "-b:a", "64k",
		"-f", "flv", publishURL,
	)
	require.NoError(t, ffCmd.Start())
	t.Cleanup(func() {
		_ = ffCmd.Process.Kill()
		_ = ffCmd.Wait()
	})

	// mediamtx only accepts play requests once a publisher is actively sending
	// packets. ffmpeg needs a few seconds to dial, send metadata, and begin the
	// stream. Retry Open until it succeeds or a deadline is reached.
	r := pull.NewRTMPReader(domain.Input{
		URL: pullURL,
		Net: domain.InputNetConfig{TimeoutSec: 10},
	})
	openDeadline := time.Now().Add(30 * time.Second)
	var openErr error
	for time.Now().Before(openDeadline) {
		openCtx, openCancel := context.WithTimeout(context.Background(), 5*time.Second)
		openErr = r.Open(openCtx)
		openCancel()
		if openErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, openErr, "RTMPReader.Open never succeeded before deadline")
	defer func() { _ = r.Close() }()

	// Collect packets until we have at least one video AU and one audio frame,
	// or we hit a generous timeout.
	readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer readCancel()

	var (
		haveVideo bool
		haveAudio bool
	)
	for !haveVideo || !haveAudio {
		batch, err := r.ReadPackets(readCtx)
		require.NoError(t, err, "ReadPackets failed before both tracks arrived")
		for _, p := range batch {
			switch p.Codec { //nolint:exhaustive // only checking for the two expected codecs
			case domain.AVCodecH264:
				haveVideo = true
			case domain.AVCodecAAC:
				haveAudio = true
			}
		}
	}
	assert.True(t, haveVideo, "expected at least one H.264 packet")
	assert.True(t, haveAudio, "expected at least one AAC packet")
}
