// open-streamer-transcoder is the per-stream transcoder subprocess.
//
// The main open-streamer process spawns ONE instance per stream that
// needs encoding, hands it a Unix-domain socket path via --socket, and
// talks to it over gRPC. The subprocess owns:
//
//   - exactly one StreamPipeline (decoder → scaler → encoder)
//   - one OS process worth of libavcodec / NVENC state
//   - SIGSEGV blast radius = ONE stream (master respawns)
//
// This binary intentionally does no I/O of its own: no file reads, no
// network sockets except the supervised gRPC socket, no GPU probing.
// All inputs (TS bytes, control messages) arrive via gRPC; all outputs
// (encoded packets, health events, errors) leave via gRPC. That makes
// the lifetime trivially testable from the supervisor side and keeps
// the process small enough to spin up in tens of milliseconds.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/datvietvac-techhub/open-streamer/internal/transcoder/native"
	pb "github.com/datvietvac-techhub/open-streamer/internal/transcoder/native/proto"
	"github.com/datvietvac-techhub/open-streamer/pkg/version"
)

func main() {
	var (
		socketPath  string
		showVersion bool
	)
	flag.StringVar(&socketPath, "socket", "", "Unix-domain socket path to listen on (required)")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.Parse()

	if showVersion {
		info := version.Get()
		fmt.Printf("open-streamer-transcoder %s (%s)\n", info.Version, info.Commit)
		return
	}
	if socketPath == "" {
		fmt.Fprintln(os.Stderr, "open-streamer-transcoder: --socket is required")
		os.Exit(2)
	}

	if err := run(socketPath); err != nil {
		slog.Error("open-streamer-transcoder: exited with error", "err", err)
		os.Exit(1)
	}
}

// run sets up the gRPC server on the supplied Unix socket and blocks
// until SIGINT / SIGTERM or a Serve error. Returns nil on clean exit,
// non-nil on any failure path so main can map it to a non-zero status.
func run(socketPath string) error {
	// Pre-existing socket files would refuse to bind. Supervisor is
	// responsible for picking unique paths per stream, but a previous
	// run that died unhealthily may have left a stale node. Remove
	// then Listen — if the path was actually live, Listen below will
	// fail and surface the conflict.
	_ = os.Remove(socketPath)

	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen unix %s: %w", socketPath, err)
	}
	// Best-effort cleanup on exit. The supervisor may pre-empt with
	// SIGKILL in which case this never runs; that's the supervisor's
	// problem (it owns the socket path's namespace).
	defer func() { _ = os.Remove(socketPath) }()

	srv := grpc.NewServer()
	pb.RegisterTranscoderServer(srv, native.NewServer())

	slog.Info("open-streamer-transcoder: listening", "socket", socketPath, "version", version.Get().Version)

	// Graceful shutdown: catch SIGINT / SIGTERM and stop the gRPC
	// server, which closes the listener and lets every in-flight Run
	// return. The supervisor signals normal stop this way; SIGKILL is
	// the abnormal-exit path that we cannot intercept.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()

	select {
	case sig := <-sigCh:
		slog.Info("open-streamer-transcoder: stopping on signal", "signal", sig.String())
		srv.GracefulStop()
		return nil
	case err := <-errCh:
		// grpc.ErrServerStopped during a clean shutdown is expected.
		if err != nil && err != grpc.ErrServerStopped {
			return fmt.Errorf("grpc serve: %w", err)
		}
		return nil
	}
}
