package transcoder

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ntt0601zcoder/open-streamer/internal/buffer"
	"github.com/ntt0601zcoder/open-streamer/internal/domain"
	pb "github.com/ntt0601zcoder/open-streamer/internal/transcoder/native/proto"
)

const (
	// transcoderBinaryName is the per-stream subprocess (see
	// cmd/open-streamer-transcoder). Looked up next to the main
	// binary first, then in $PATH.
	transcoderBinaryName = "open-streamer-transcoder"

	// socketWaitTimeout is how long the supervisor will wait for the
	// subprocess to bind its Unix-domain socket after fork. The
	// subprocess listens almost immediately on a healthy host; 5 s
	// leaves room for slow CI machines without making operator-
	// visible Start latency painful.
	socketWaitTimeout = 5 * time.Second

	// gracefulStopTimeout caps the wait for a SIGINT'd subprocess to
	// exit cleanly. After this elapses we SIGKILL — preventing a
	// hung child from blocking Service.Stop indefinitely.
	gracefulStopTimeout = 3 * time.Second

	// fastCrashThreshold is the runtime under which a crash counts
	// as "fast" (operator likely needs to fix something rather than
	// wait it out). Matches the existing health-degradation logic.
	fastCrashThreshold = 30 * time.Second

	// restartBackoffStart / Max bracket the supervisor's respawn
	// delay after a fast crash. Linear-doubling.
	restartBackoffStart = 1 * time.Second
	restartBackoffMax   = 30 * time.Second
)

// supervisor owns one stream's transcoder subprocess and the gRPC
// stream connected to it. The Service spawns one supervisor per stream
// inside Service.Start; the supervisor's Run goroutine then handles
// the full lifecycle until ctx is cancelled by Service.Stop.
type supervisor struct {
	svc *Service // for callbacks: recordProfileError, fireUnhealthy/Healthy

	streamID    domain.StreamCode
	rawIngestID domain.StreamCode
	targets     []RenditionTarget
	tc          *domain.TranscoderConfig

	// SwitchInputCh carries the next raw-ingest buffer ID when the
	// Manager initiates a switch. The input pump goroutine selects
	// between this channel and sub.Recv. Buffered=1 so SwitchInput
	// never blocks the caller.
	switchCh chan domain.StreamCode

	// done is closed once Run returns so Stop callers can wait for
	// the subprocess to exit before treating the stream as torn down.
	done chan struct{}
}

// newSupervisor builds a supervisor for one stream. Caller is
// responsible for starting Run in a goroutine.
func newSupervisor(
	svc *Service,
	streamID domain.StreamCode,
	rawIngestID domain.StreamCode,
	tc *domain.TranscoderConfig,
	targets []RenditionTarget,
) *supervisor {
	return &supervisor{
		svc:         svc,
		streamID:    streamID,
		rawIngestID: rawIngestID,
		tc:          tc,
		targets:     targets,
		switchCh:    make(chan domain.StreamCode, 1),
		done:        make(chan struct{}),
	}
}

// Run is the long-lived supervisor loop. Spawns the subprocess,
// connects gRPC, pumps data through it. On unexpected subprocess exit
// (not ctx-cancelled), respawns with exponential backoff. Returns
// when ctx is done.
func (sv *supervisor) Run(ctx context.Context) {
	defer close(sv.done)
	backoff := restartBackoffStart
	for ctx.Err() == nil {
		started := time.Now()
		err := sv.runOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		runtime := time.Since(started)
		if err != nil {
			sv.svc.recordProfileError(sv.streamID, 0,
				fmt.Sprintf("native transcoder: %s", err))
			slog.Warn("transcoder: subprocess exited, will respawn",
				"stream_code", sv.streamID,
				"runtime", runtime.String(),
				"backoff", backoff,
				"err", err,
			)
			if runtime < fastCrashThreshold {
				sv.svc.fireUnhealthyIfTransitioned(sv.streamID, 0, err.Error())
			} else {
				// Sustained run before crash → reset backoff so the
				// next crash gets the short delay too.
				backoff = restartBackoffStart
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = nextBackoff(backoff)
			continue
		}
		// Clean exit (ctx-cancelled via Stop). Don't respawn.
		return
	}
}

// runOnce spawns a single subprocess instance and runs the data plane
// until either ctx is cancelled, the subprocess crashes, or the gRPC
// stream errors out. Returns nil on clean shutdown, error otherwise.
func (sv *supervisor) runOnce(ctx context.Context) error {
	binPath, err := sv.svc.resolveBinaryPath()
	if err != nil {
		return fmt.Errorf("resolve binary: %w", err)
	}
	socketPath := pickSocketPath(sv.streamID)
	defer func() { _ = os.Remove(socketPath) }()

	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()

	cmd := exec.CommandContext(subCtx, binPath, "--socket", socketPath)
	cmd.Stderr = newSubprocessLogWriter(sv.streamID, "stderr")
	cmd.Stdout = newSubprocessLogWriter(sv.streamID, "stdout")
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("spawn %s: %w", binPath, err)
	}
	slog.Info("transcoder: subprocess started",
		"stream_code", sv.streamID,
		"binary", binPath,
		"socket", socketPath,
		"pid", cmd.Process.Pid,
	)

	if err := waitForSocket(subCtx, socketPath, socketWaitTimeout); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("wait for socket: %w", err)
	}

	conn, err := grpc.NewClient("unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(_ context.Context, addr string) (net.Conn, error) {
			// addr is "unix://<path>"; strip prefix before dialing.
			path := strings.TrimPrefix(addr, "unix://")
			var d net.Dialer
			return d.DialContext(ctx, "unix", path)
		}),
	)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("grpc dial: %w", err)
	}
	defer func() { _ = conn.Close() }()

	client := pb.NewTranscoderClient(conn)
	stream, err := client.Run(subCtx)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("grpc Run: %w", err)
	}

	if err := stream.Send(sv.buildConfigure()); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("send configure: %w", err)
	}

	sub, err := sv.svc.buf.Subscribe(sv.rawIngestID)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("subscribe raw ingest: %w", err)
	}
	defer sv.svc.buf.Unsubscribe(sv.rawIngestID, sub)

	// Data plane: input pump (raw TS → gRPC) + output drain (gRPC →
	// rendition buffers). First error from either pump terminates
	// the run.
	pumpErrCh := make(chan error, 2)
	go func() { pumpErrCh <- sv.inputPump(subCtx, sub, stream) }()
	go func() { pumpErrCh <- sv.outputDrain(subCtx, stream) }()

	// Watchdog for subprocess exit so a SIGSEGV in libavcodec
	// doesn't deadlock the pumps on stream.Send / Recv.
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	var firstErr error
	select {
	case err := <-pumpErrCh:
		firstErr = err
	case err := <-waitCh:
		firstErr = fmt.Errorf("subprocess died: %w", err)
	case <-subCtx.Done():
		// Stop initiated. Send Stop request best-effort so the
		// subprocess flushes its encoder tail.
		_ = stream.Send(&pb.Request{Body: &pb.Request_Stop{Stop: &pb.Stop{Reason: "service-stop"}}})
		_ = stream.CloseSend()
		firstErr = ctx.Err()
	}
	cancelSub()

	// Drain remaining events + wait for subprocess. SIGKILL after
	// gracefulStopTimeout to bound shutdown.
	stopTimer := time.NewTimer(gracefulStopTimeout)
	defer stopTimer.Stop()
	select {
	case <-waitCh:
	case <-stopTimer.C:
		_ = cmd.Process.Kill()
		<-waitCh
	}

	// If the supervisor was healthy before this exit, signal recovery
	// once the subprocess is gone — coordinator's status engine
	// expects healthy edges on respawn / clean stop alike.
	sv.svc.fireHealthyIfTransitioned(sv.streamID, 0)

	if errors.Is(firstErr, context.Canceled) {
		return nil
	}
	return firstErr
}

// inputPump forwards every packet from the raw-ingest buffer
// subscriber into the gRPC stream as an InputPacket. SessionStart=true
// on a non-first packet triggers a SwitchInput message ahead of the
// data so the subprocess can swap its decoder for the new source.
//
// Returns nil only on ctx cancel; any send error bubbles up so the
// supervisor respawns.
func (sv *supervisor) inputPump(ctx context.Context, sub *buffer.Subscriber, stream pb.Transcoder_RunClient) error {
	firstWritten := false
	for {
		select {
		case <-ctx.Done():
			return nil
		case newRawID := <-sv.switchCh:
			if err := sv.sendSwitch(stream, newRawID); err != nil {
				return err
			}
		case pkt, ok := <-sub.Recv():
			if !ok {
				return errors.New("raw ingest subscriber closed")
			}
			if err := sv.forwardOnePacket(stream, pkt, &firstWritten); err != nil {
				return err
			}
		}
	}
}

// forwardOnePacket handles one buffer.Packet: emits a Switch first
// when the SessionStart marker indicates an upstream session boundary
// on a non-first packet, then forwards the payload as InputPacket.
// Empty payloads are skipped silently — buffer hub may surface raw
// PSI-only chunks or PCR markers that have no encoder relevance.
func (sv *supervisor) forwardOnePacket(stream pb.Transcoder_RunClient, pkt buffer.Packet, firstWritten *bool) error {
	if pkt.SessionStart && *firstWritten {
		if err := sv.sendSwitch(stream, sv.rawIngestID); err != nil {
			return fmt.Errorf("send session-start switch: %w", err)
		}
	}
	data := pkt.TS
	if len(data) == 0 && pkt.AV != nil {
		data = pkt.AV.Data
	}
	if len(data) == 0 {
		return nil
	}
	if err := stream.Send(&pb.Request{
		Body: &pb.Request_Packet{Packet: &pb.InputPacket{
			Data:         data,
			SessionStart: pkt.SessionStart,
			SessionId:    int64(pkt.SessionID),
		}},
	}); err != nil {
		return fmt.Errorf("send packet: %w", err)
	}
	*firstWritten = true
	return nil
}

func (sv *supervisor) sendSwitch(stream pb.Transcoder_RunClient, newRawID domain.StreamCode) error {
	if err := stream.Send(&pb.Request{
		Body: &pb.Request_Switch{Switch: &pb.SwitchInput{NewRawIngestBufId: string(newRawID)}},
	}); err != nil {
		return fmt.Errorf("send switch: %w", err)
	}
	return nil
}

// outputDrain consumes the gRPC Event stream and writes OutputPacket
// payloads into the matching rendition buffer (by TargetIndex). Logs
// Error events at warn level and Health events at debug.
func (sv *supervisor) outputDrain(ctx context.Context, stream pb.Transcoder_RunClient) error {
	for {
		if ctx.Err() != nil {
			return nil //nolint:nilerr // shutdown path; ctx error is the signal, not a fault
		}
		ev, err := stream.Recv()
		if err != nil {
			// io.EOF = subprocess closed the stream cleanly (Stop sent
			// or Run returned nil). ctx.Err() != nil = supervisor
			// initiated shutdown so the connection error is expected.
			// Either case is "clean exit", not a fault.
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return nil //nolint:nilerr // sentinel-style "expected shutdown"
			}
			return fmt.Errorf("recv event: %w", err)
		}
		if err := sv.dispatchEvent(ev); err != nil {
			return err
		}
	}
}

// dispatchEvent routes one gRPC Event to its handler. Returns nil for
// non-fatal events (data writes that succeeded, health pings, non-
// terminal errors) and a wrapped error for fatal cases that should
// tear the run down so the supervisor respawns.
func (sv *supervisor) dispatchEvent(ev *pb.Event) error {
	switch {
	case ev.GetPacket() != nil:
		return sv.writeOutputPacket(ev.GetPacket())
	case ev.GetError() != nil:
		return sv.handleSubprocessError(ev.GetError())
	case ev.GetHealth() != nil:
		// P5 will plumb subprocess-reported per-profile health
		// into recordProfileError; today the supervisor's own
		// crash counter is the only signal.
		slog.Debug("transcoder: subprocess health event",
			"stream_code", sv.streamID,
			"profiles", len(ev.GetHealth().GetProfiles()),
		)
	}
	return nil
}

func (sv *supervisor) writeOutputPacket(pkt *pb.OutputPacket) error {
	avCodec, ok := avCodecFromProto(pkt.GetCodec())
	if !ok {
		slog.Warn("transcoder: output packet with unspecified codec — dropping",
			"stream_code", sv.streamID,
			"target_index", pkt.GetTargetIndex(),
			"bytes", len(pkt.GetData()))
		return nil
	}

	av := &domain.AVPacket{
		Codec:    avCodec,
		Data:     pkt.GetData(),
		PTSms:    uint64(pkt.GetPtsMs()), //nolint:gosec // negative pre-warmup PTS round-trips via two's-complement
		DTSms:    uint64(pkt.GetDtsMs()), //nolint:gosec
		KeyFrame: pkt.GetKeyframe(),
	}
	bufPkt := buffer.Packet{AV: av, SessionStart: pkt.GetSessionStart()}

	idx := pkt.GetTargetIndex()

	// Broadcast (-1) = write to every rendition buffer. Used for the
	// shared audio passthrough so each variant's HLS segment contains
	// both the rendition's transcoded video and the same source AAC,
	// keeping every variant in A/V sync on the same wallclock.
	if idx < 0 {
		for i, target := range sv.targets {
			if err := sv.svc.buf.Write(target.BufferID, bufPkt); err != nil {
				return fmt.Errorf("broadcast buffer write target %d: %w", i, err)
			}
		}
		return nil
	}

	if int(idx) >= len(sv.targets) {
		slog.Warn("transcoder: output packet with unknown target index",
			"stream_code", sv.streamID, "target_index", idx, "n_targets", len(sv.targets))
		return nil
	}
	target := sv.targets[idx]
	if err := sv.svc.buf.Write(target.BufferID, bufPkt); err != nil {
		return fmt.Errorf("buffer write target %d: %w", idx, err)
	}
	return nil
}

// avCodecFromProto narrows the wire-level Codec enum to the
// domain.AVCodec the buffer hub + publisher's tsmux.FromAV expect.
// Returns ok=false on UNSPECIFIED / unknown so the supervisor drops
// the packet with a warning instead of misrouting it.
func avCodecFromProto(c pb.Codec) (domain.AVCodec, bool) {
	switch c {
	case pb.Codec_CODEC_H264:
		return domain.AVCodecH264, true
	case pb.Codec_CODEC_H265:
		return domain.AVCodecH265, true
	case pb.Codec_CODEC_AAC:
		return domain.AVCodecAAC, true
	default:
		return domain.AVCodecUnknown, false
	}
}

func (sv *supervisor) handleSubprocessError(e *pb.Error) error {
	slog.Warn("transcoder: subprocess reported error",
		"stream_code", sv.streamID,
		"target_index", e.GetTargetIndex(),
		"terminal", e.GetTerminal(),
		"msg", e.GetMessage(),
	)
	if e.GetTerminal() {
		return errors.New(e.GetMessage())
	}
	return nil
}

// NotifySwitchInput tells the supervisor to send a Switch message on
// the gRPC stream the next time the input pump wakes. Buffered=1 so
// duplicate calls collapse to the most recent one rather than blocking
// the caller.
func (sv *supervisor) NotifySwitchInput(newRawIngestID domain.StreamCode) {
	select {
	case sv.switchCh <- newRawIngestID:
	default:
		// Channel already has a pending switch. Replace it.
		select {
		case <-sv.switchCh:
		default:
		}
		sv.switchCh <- newRawIngestID
	}
}

// buildConfigure adapts the in-process tc + targets into the wire-level
// ConfigureRequest.
func (sv *supervisor) buildConfigure() *pb.Request {
	cfg := &pb.ConfigureRequest{
		StreamCode:     string(sv.streamID),
		RawIngestBufId: string(sv.rawIngestID),
		HwBackend:      hwBackendFromTC(sv.tc),
		Audio:          audioConfigFromTC(sv.tc),
		Watermark:      watermarkConfigFromTC(sv.tc),
	}
	for i, t := range sv.targets {
		cfg.Targets = append(cfg.Targets, &pb.Target{
			OutputBufId:    string(t.BufferID),
			Index:          int32(i),                //nolint:gosec // i is the rendition slot index, bounded by len(targets)
			Width:          int32(t.Profile.Width),  //nolint:gosec
			Height:         int32(t.Profile.Height), //nolint:gosec
			BitrateKbps:    bitrateK(t.Profile.Bitrate),
			MaxBitrateKbps: int32(t.Profile.MaxBitrate), //nolint:gosec
			Framerate:      t.Profile.Framerate,
			GopSeconds:     int32(t.Profile.KeyframeInterval), //nolint:gosec
			GopFrames:      resolveGopFrames(t.Profile, sv.tc),
			Codec:          t.Profile.Codec,
			Preset:         t.Profile.Preset,
			Profile:        t.Profile.CodecProfile,
			Level:          t.Profile.CodecLevel,
			BframesOrNeg1:  optIntOrNeg1(t.Profile.Bframes),
			RefsOrNeg1:     optIntOrNeg1(t.Profile.Refs),
			Sar:            t.Profile.SAR,
			ResizeMode:     t.Profile.ResizeMode,
		})
	}
	return &pb.Request{Body: &pb.Request_Configure{Configure: cfg}}
}

// resolveGopFrames picks the keyframe interval (in encoder frames) for
// one rendition target. Precedence is per-profile override → global
// frame-count → 0 (encoder default), matching how operators expect to
// set the value: a profile that needs a specific cadence (e.g.
// matching its HLS segment duration) declares it explicitly; the rest
// inherit a server-wide GOP set in the global transcoder section.
//
// Without this resolver every rendition used the encoder's built-in
// default (h264_nvenc: 250 frames ≈ 10s @ 25fps), so HLS segments
// rounded up to that — operators who set global.gop=100 to get 4 s
// segments saw 10s instead.
func resolveGopFrames(p Profile, tc *domain.TranscoderConfig) int32 {
	fr := p.Framerate
	if fr <= 0 && tc != nil && tc.Global.FPS > 0 {
		fr = float64(tc.Global.FPS)
	}
	if p.KeyframeInterval > 0 && fr > 0 {
		return int32(float64(p.KeyframeInterval) * fr) //nolint:gosec // bounded by realistic GOP * fps
	}
	if tc != nil && tc.Global.GOP > 0 {
		return int32(tc.Global.GOP) //nolint:gosec
	}
	return 0
}

// ─── helpers ────────────────────────────────────────────────────────────

// pickSocketPath generates a per-stream socket path with a random nonce
// so two transcoder lifetimes for the same stream code (vintage A,
// crash, supervisor respawns vintage B) cannot collide on the same
// socket inode.
func pickSocketPath(streamID domain.StreamCode) string {
	nonce := make([]byte, 4)
	_, _ = rand.Read(nonce)
	safe := strings.ReplaceAll(string(streamID), "/", "_")
	return filepath.Join(os.TempDir(), fmt.Sprintf("ost-%s-%s.sock", safe, hex.EncodeToString(nonce)))
}

// waitForSocket polls until the socket file appears or timeout fires.
// Subprocess listens almost immediately on a healthy host so the poll
// interval is short.
func waitForSocket(ctx context.Context, path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("socket %s not bound within %s", path, timeout)
}

// nextBackoff doubles the current backoff up to a cap.
func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > restartBackoffMax {
		d = restartBackoffMax
	}
	return d
}

// hwBackendFromTC maps the per-stream HW preference into the wire enum.
func hwBackendFromTC(tc *domain.TranscoderConfig) pb.HWBackend {
	if tc == nil {
		return pb.HWBackend_HW_BACKEND_UNSPECIFIED
	}
	switch tc.Global.HW {
	case domain.HWAccelNVENC:
		return pb.HWBackend_HW_BACKEND_NVENC
	case domain.HWAccelVAAPI:
		return pb.HWBackend_HW_BACKEND_VAAPI
	case domain.HWAccelVideoToolbox:
		return pb.HWBackend_HW_BACKEND_VIDEOTOOLBOX
	case domain.HWAccelQSV:
		return pb.HWBackend_HW_BACKEND_QSV
	case domain.HWAccelNone:
		return pb.HWBackend_HW_BACKEND_CPU
	default:
		return pb.HWBackend_HW_BACKEND_UNSPECIFIED
	}
}

func audioConfigFromTC(tc *domain.TranscoderConfig) *pb.AudioConfig {
	if tc == nil {
		return nil
	}
	return &pb.AudioConfig{
		Copy:       tc.Audio.Copy,
		Codec:      string(tc.Audio.Codec),
		BitrateK:   int32(tc.Audio.Bitrate),    //nolint:gosec
		SampleRate: int32(tc.Audio.SampleRate), //nolint:gosec
		Channels:   int32(tc.Audio.Channels),   //nolint:gosec
		Normalize:  tc.Audio.Normalize,
	}
}

func watermarkConfigFromTC(tc *domain.TranscoderConfig) *pb.WatermarkConfig {
	if tc == nil || tc.Watermark == nil || !tc.Watermark.Enabled {
		return nil
	}
	wm := tc.Watermark
	return &pb.WatermarkConfig{
		Enabled:   wm.Enabled,
		Type:      string(wm.Type),
		Text:      wm.Text,
		AssetPath: wm.ImagePath,
		Position:  string(wm.Position),
		OffsetX:   int32(wm.OffsetX), //nolint:gosec
		OffsetY:   int32(wm.OffsetY), //nolint:gosec
		Opacity:   wm.Opacity,
		FontSize:  int32(wm.FontSize), //nolint:gosec
		FontColor: wm.FontColor,
		FontFile:  wm.FontFile,
		Resize:    wm.Resize,
	}
}

// bitrateK extracts the integer kbps from a "<n>k" string. Falls back
// to 0 when the format isn't recognised — encoder treats 0 as default.
func bitrateK(s string) int32 {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, "k")
	s = strings.TrimSuffix(s, "K")
	if s == "" {
		return 0
	}
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil {
		return 0
	}
	return int32(n) //nolint:gosec
}

func optIntOrNeg1(p *int) int32 {
	if p == nil {
		return -1
	}
	return int32(*p) //nolint:gosec
}

// subprocessLogWriter pipes the subprocess's stdout / stderr into
// slog with a stream-code tag so multi-stream installs stay
// readable. One per stream + stream (stdout / stderr).
type subprocessLogWriter struct {
	streamID domain.StreamCode
	stream   string
}

func newSubprocessLogWriter(streamID domain.StreamCode, stream string) *subprocessLogWriter {
	return &subprocessLogWriter{streamID: streamID, stream: stream}
}

func (w *subprocessLogWriter) Write(p []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		if line == "" {
			continue
		}
		slog.Info("transcoder: subprocess log",
			"stream_code", w.streamID,
			"src", w.stream,
			"msg", line,
		)
	}
	return len(p), nil
}
