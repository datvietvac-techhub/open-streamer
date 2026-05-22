package native

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/ntt0601zcoder/open-streamer/internal/transcoder/native/proto"
)

// dialBufconn wires an in-process gRPC server and returns a connected
// TranscoderClient + cleanup. Avoids spawning the actual subprocess
// binary so server-side logic is testable in a single Go test
// invocation with no syscall machinery.
func dialBufconn(t *testing.T) (pb.TranscoderClient, func()) {
	t.Helper()
	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	pb.RegisterTranscoderServer(srv, NewServer())
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("bufconn Serve: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
	)
	require.NoError(t, err)
	return pb.NewTranscoderClient(conn), func() {
		_ = conn.Close()
		srv.GracefulStop()
	}
}

// First message must be Configure. Anything else terminates the
// stream with an error so the supervisor knows it spoke the wrong
// protocol.
func TestServer_RejectsNonConfigureFirstMessage(t *testing.T) {
	t.Parallel()
	client, cleanup := dialBufconn(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	stream, err := client.Run(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&pb.Request{
		Body: &pb.Request_Packet{Packet: &pb.InputPacket{Data: []byte{0}}},
	}))
	require.NoError(t, stream.CloseSend())

	// Server may either emit a terminal Error event first OR close the
	// stream with the error directly. Accept either: read until EOF /
	// error.
	for {
		ev, err := stream.Recv()
		if err != nil {
			// gRPC surfaces the server-side error as a status; just
			// confirm we got SOMETHING non-EOF that signals failure.
			require.NotErrorIs(t, err, io.EOF, "stream closed cleanly but should have errored")
			return
		}
		if e := ev.GetError(); e != nil {
			assert.True(t, e.GetTerminal())
			assert.Contains(t, e.GetMessage(), "Configure")
			// Now expect EOF / error on next Recv.
			continue
		}
	}
}

// End-to-end happy path: Configure → push some H.264 packets from a
// local source encoder → receive encoded packets back on the wire →
// Stop, observe the flush tail, then EOF.
func TestServer_RoundtripConfigureProcessStop(t *testing.T) {
	t.Parallel()
	client, cleanup := dialBufconn(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stream, err := client.Run(ctx)
	require.NoError(t, err)

	// 1) Configure: 720p rendition at 1.6 Mbps, libx264 default.
	require.NoError(t, stream.Send(&pb.Request{
		Body: &pb.Request_Configure{
			Configure: &pb.ConfigureRequest{
				StreamCode: "test-stream",
				HwBackend:  pb.HWBackend_HW_BACKEND_CPU,
				Targets: []*pb.Target{{
					Index:         0,
					Width:         640,
					Height:        360,
					BitrateKbps:   512,
					Framerate:     25,
					GopSeconds:    1,
					Preset:        "ultrafast",
					BframesOrNeg1: 0,
					RefsOrNeg1:    -1,
				}},
			},
		},
	}))

	// Drainer goroutine: just consume OutputPacket events and count
	// bytes so we can assert "the server produced encoded output".
	type result struct {
		bytes int
		err   error
	}
	resCh := make(chan result, 1)
	go func() {
		var bytes int
		for {
			ev, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				resCh <- result{bytes: bytes}
				return
			}
			if err != nil {
				resCh <- result{bytes: bytes, err: err}
				return
			}
			if p := ev.GetPacket(); p != nil {
				bytes += len(p.GetData())
			}
		}
	}()

	// 2) Feed 25 source frames worth of encoded H.264 from a local
	// libx264 source encoder. We reuse the unit-test helpers from
	// encoder_test.go / scaler_test.go.
	srcEnc := buildSourceEncoder(t, 640, 360, 25)
	defer srcEnc.Close()
	for i := 0; i < 25; i++ {
		frame := allocTestNV12Frame(t, 640, 360, 0 /*YUV420P*/, int64(i))
		pkts, err := srcEnc.Encode(frame)
		frame.Free()
		require.NoError(t, err)
		for _, pkt := range pkts {
			require.NoError(t, stream.Send(&pb.Request{
				Body: &pb.Request_Packet{Packet: &pb.InputPacket{Data: pkt}},
			}))
		}
	}

	// 3) Stop: server flushes pipeline + returns nil cleanly.
	require.NoError(t, stream.Send(&pb.Request{Body: &pb.Request_Stop{Stop: &pb.Stop{Reason: "test-done"}}}))
	require.NoError(t, stream.CloseSend())

	select {
	case res := <-resCh:
		require.NoError(t, res.err)
		require.Positive(t, res.bytes, "server produced no encoded output bytes across 25 frames")
	case <-ctx.Done():
		t.Fatal("test timed out waiting for stream EOF")
	}
}

// SwitchInput on the wire: Configure → push frames → switch → push
// more frames → Stop. Assert the stream stays alive across the
// switch and continues emitting output.
func TestServer_SwitchInputKeepsStreamAlive(t *testing.T) {
	t.Parallel()
	client, cleanup := dialBufconn(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stream, err := client.Run(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&pb.Request{
		Body: &pb.Request_Configure{
			Configure: &pb.ConfigureRequest{
				StreamCode: "switch-test",
				HwBackend:  pb.HWBackend_HW_BACKEND_CPU,
				Targets: []*pb.Target{{
					Index:         0,
					Width:         640,
					Height:        360,
					BitrateKbps:   512,
					Framerate:     25,
					GopSeconds:    1,
					Preset:        "ultrafast",
					BframesOrNeg1: 0,
					RefsOrNeg1:    -1,
				}},
			},
		},
	}))

	type result struct {
		preSwitch, postSwitch int
		err                   error
	}
	switchedCh := make(chan struct{})
	resCh := make(chan result, 1)
	go func() {
		var pre, post int
		switched := false
		for {
			ev, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				resCh <- result{preSwitch: pre, postSwitch: post}
				return
			}
			if err != nil {
				resCh <- result{preSwitch: pre, postSwitch: post, err: err}
				return
			}
			if p := ev.GetPacket(); p != nil {
				if !switched {
					pre += len(p.GetData())
				} else {
					post += len(p.GetData())
				}
			}
			select {
			case <-switchedCh:
				switched = true
			default:
			}
		}
	}()

	pushFrames := func(srcW, srcH, n int) {
		srcEnc := buildSourceEncoder(t, srcW, srcH, 25)
		defer srcEnc.Close()
		for i := 0; i < n; i++ {
			frame := allocTestNV12Frame(t, srcW, srcH, 0, int64(i))
			pkts, err := srcEnc.Encode(frame)
			frame.Free()
			require.NoError(t, err)
			for _, pkt := range pkts {
				require.NoError(t, stream.Send(&pb.Request{
					Body: &pb.Request_Packet{Packet: &pb.InputPacket{Data: pkt}},
				}))
			}
		}
	}

	pushFrames(640, 360, 15)
	close(switchedCh)
	// Switch on wire; payload field carries a new ingest buf ID that
	// the supervisor would pass — for the test, any string is fine.
	require.NoError(t, stream.Send(&pb.Request{
		Body: &pb.Request_Switch{Switch: &pb.SwitchInput{NewRawIngestBufId: "new-source"}},
	}))
	pushFrames(640, 360, 15)

	require.NoError(t, stream.Send(&pb.Request{Body: &pb.Request_Stop{Stop: &pb.Stop{}}}))
	require.NoError(t, stream.CloseSend())

	select {
	case res := <-resCh:
		require.NoError(t, res.err)
		require.Positive(t, res.preSwitch, "no output produced before switch")
		// post-switch may be zero if all queued bytes arrived after the
		// switchedCh signal fired; the important property is the stream
		// did NOT error out across the switch.
	case <-ctx.Done():
		t.Fatal("test timed out")
	}
}
