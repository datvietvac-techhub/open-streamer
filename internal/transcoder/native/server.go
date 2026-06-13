package native

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	pb "github.com/datvietvac-techhub/open-streamer/internal/transcoder/native/proto"
)

// Server is the gRPC service handler that the per-stream transcoder
// subprocess registers. One Run stream per stream lifetime — the
// supervisor in the main open-streamer process opens it once on
// Service.Start and closes it on Stop. Inside that stream we
// instantiate exactly ONE StreamPipeline.
//
// Server is intentionally minimal: no per-process state, no
// connection multiplexing. The subprocess is single-tenant (one
// stream per subprocess) so all the lifecycle state lives inside
// the Run method's local pipeline.
type Server struct {
	pb.UnimplementedTranscoderServer
}

// NewServer returns a ready-to-register gRPC handler. Kept as a
// constructor (instead of zero-value &Server{}) so future server-
// wide fields (metrics, logger) can be wired without changing call
// sites.
func NewServer() *Server {
	return &Server{}
}

// Run is the bidi entry point. Lifecycle:
//
//  1. Read the first message — MUST be Configure, anything else is a
//     protocol error and we terminate the stream.
//  2. Build a StreamPipeline from the Configure body.
//  3. Loop reading further Request messages:
//     - Packet  → pipeline.ProcessPacket → emit each output packet
//     as an Event_Packet on the response stream.
//     - Switch  → pipeline.SwitchInput   → emit any flushed packets
//     from the old decoder under the encoder's continuing PTS.
//     - Stop    → flush pipeline, emit tail, return nil.
//  4. If the client closes the stream (io.EOF on Recv), perform the
//     same flush-then-return as Stop so we don't leak frames.
//
// Errors from the pipeline are sent back as a terminal Event_Error
// and the stream returns the original error so gRPC surfaces it to
// the client too.
func (s *Server) Run(stream pb.Transcoder_RunServer) error {
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("native server: first recv: %w", err)
	}
	cfg := first.GetConfigure()
	if cfg == nil {
		return errors.New("native server: first message must be Configure")
	}

	pipeline, err := NewStreamPipeline(pipelineConfigFromProto(cfg))
	if err != nil {
		_ = stream.Send(terminalError(fmt.Sprintf("pipeline init: %s", err)))
		return fmt.Errorf("native server: pipeline init: %w", err)
	}
	defer pipeline.Close()

	slog.Info("native server: stream started",
		"stream_code", cfg.GetStreamCode(),
		"targets", len(cfg.GetTargets()),
		"hw_backend", cfg.GetHwBackend().String(),
	)

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// Client half-closed. Flush + exit normally so the encoder
			// tail makes it back over the wire.
			return s.flushAndSend(stream, pipeline)
		}
		if err != nil {
			return fmt.Errorf("native server: recv: %w", err)
		}

		if err := s.dispatch(stream, pipeline, req); err != nil {
			_ = stream.Send(terminalError(err.Error()))
			return err
		}
		// Stop body ends the stream cleanly after flushing.
		if req.GetStop() != nil {
			return nil
		}
	}
}

// dispatch routes one Request to the pipeline and sends back any
// resulting events. Returns the first error from pipeline or send;
// caller wraps it in a terminal Error event.
func (s *Server) dispatch(stream pb.Transcoder_RunServer, p *StreamPipeline, req *pb.Request) error {
	switch {
	case req.GetPacket() != nil:
		pkt := req.GetPacket()
		// AV-path packets carry codec + ms PTS/DTS so audio is routed to
		// the audio path (not fed to the H.264 decoder) and the video
		// timeline tracks the source instead of collapsing to a frame
		// counter. Raw-TS chunks carry CODEC_UNSPECIFIED / 0 and recover
		// in-band PES timing inside the TS-input path.
		out, err := p.ProcessPacket(pkt.GetData(), codecFromProto(pkt.GetCodec()), pkt.GetPtsMs(), pkt.GetDtsMs())
		if err != nil {
			return fmt.Errorf("process packet: %w", err)
		}
		return s.sendOutputs(stream, out)
	case req.GetSwitch() != nil:
		// Reuse the stream's configured decoder backend (NVDEC stays
		// NVDEC) — only the input source changes across a switch, never
		// the hardware decode path.
		flushed, err := p.SwitchInput(DecoderConfig{
			Codec: p.DecoderCodec(),
		})
		if err != nil {
			return fmt.Errorf("switch input: %w", err)
		}
		return s.sendOutputs(stream, flushed)
	case req.GetStop() != nil:
		return s.flushAndSend(stream, p)
	case req.GetConfigure() != nil:
		// Reconfigure mid-stream is not supported in P4 — the
		// pipeline's encoder dims / framerate are fixed at construct
		// time. A future phase may forward a subset of Configure
		// fields (watermark hot-reload, bitrate ladder swap) to the
		// pipeline; for now reject so callers don't think it worked.
		return errors.New("reconfigure after init is not supported in this phase")
	default:
		return errors.New("unknown Request body variant")
	}
}

func (s *Server) flushAndSend(stream pb.Transcoder_RunServer, p *StreamPipeline) error {
	tail, err := p.Flush()
	if err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return s.sendOutputs(stream, tail)
}

// sendOutputs wraps each pipeline OutputFrame in an Event_Packet and
// sends it on the gRPC stream. TargetIndex on the frame routes the
// packet to one rendition buffer (0..N-1) or to ALL of them
// (BroadcastTargetIndex == -1, used by the shared audio passthrough);
// the supervisor enforces the broadcast semantic on the receive side.
//
// Codec / PTS / DTS / keyframe are populated so the supervisor can
// build a properly-shaped domain.AVPacket without inspecting bytes —
// the AV-path write to the buffer hub then drives publisher's
// tsmux.FromAV with correct codec routing and keyframe alignment.
func (s *Server) sendOutputs(stream pb.Transcoder_RunServer, outs []OutputFrame) error {
	for _, o := range outs {
		if err := stream.Send(&pb.Event{
			Body: &pb.Event_Packet{
				Packet: &pb.OutputPacket{
					TargetIndex:  o.TargetIndex,
					Data:         o.Data,
					Codec:        protoCodec(o.Codec),
					PtsMs:        o.PTS,
					DtsMs:        o.DTS,
					Keyframe:     o.Keyframe,
					SessionStart: o.SessionStart,
				},
			},
		}); err != nil {
			return fmt.Errorf("stream send: %w", err)
		}
	}
	return nil
}

// protoCodec maps the pipeline's local codec enum to the wire enum.
// esCodecUnknown becomes CODEC_UNSPECIFIED so the supervisor surfaces
// a clear error instead of misrouting bytes.
func protoCodec(c esFrameCodec) pb.Codec {
	switch c { //nolint:exhaustive // default maps esCodecUnknown (and any unmapped codec) to UNSPECIFIED
	case esCodecH264:
		return pb.Codec_CODEC_H264
	case esCodecH265:
		return pb.Codec_CODEC_H265
	case esCodecAAC:
		return pb.Codec_CODEC_AAC
	default:
		return pb.Codec_CODEC_UNSPECIFIED
	}
}

// codecFromProto maps the wire Codec enum onto the pipeline's local
// esFrameCodec — the inverse of protoCodec — so the AV-path ProcessPacket
// can route audio vs video. Unknown/unspecified falls through to the
// video branch (unchanged legacy behaviour).
func codecFromProto(c pb.Codec) esFrameCodec {
	switch c { //nolint:exhaustive // default maps CODEC_UNSPECIFIED (and any unmapped codec) to esCodecUnknown
	case pb.Codec_CODEC_H264:
		return esCodecH264
	case pb.Codec_CODEC_H265:
		return esCodecH265
	case pb.Codec_CODEC_AAC:
		return esCodecAAC
	default:
		return esCodecUnknown
	}
}

// terminalError builds an Event_Error with terminal=true so the
// supervisor knows the subprocess is about to exit.
func terminalError(msg string) *pb.Event {
	return &pb.Event{
		Body: &pb.Event_Error{
			Error: &pb.Error{
				Message:     msg,
				TargetIndex: -1,
				Terminal:    true,
			},
		},
	}
}

// pipelineConfigFromProto adapts the wire-level ConfigureRequest into
// the in-process PipelineConfig the StreamPipeline understands.
//
// Every Target becomes one RenditionConfig (its own encoder + scaler
// pair). All renditions share the single decoder declared by Decoder.
// Targets are expected to be ordered by Target.index — the pipeline
// emits OutputFrame.TargetIndex matching the slice position, and the
// supervisor uses that to write into the corresponding rendition
// buffer.
func pipelineConfigFromProto(c *pb.ConfigureRequest) PipelineConfig {
	cfg := PipelineConfig{
		Decoder: DecoderConfig{Codec: decoderCodecForBackend(c.GetHwBackend())},
		// Audio.Copy=true (or a nil proto) keeps the cheap passthrough;
		// Copy=false routes AAC through decode → resample → re-encode.
		Audio: AudioConfigFromProto(c.GetAudio()),
	}
	wm := watermarkConfigFromProto(c.GetWatermark())
	for _, t := range c.GetTargets() {
		fr := framerateOrDefault(t.GetFramerate())
		// Prefer the supervisor-resolved gop_frames; only fall back to
		// the legacy gop_seconds for sources still on the old wire.
		gop := int(t.GetGopFrames())
		if gop == 0 {
			gop = int(t.GetGopSeconds()) * fr
		}
		enc := EncoderConfig{
			Codec:       encoderCodecForBackend(c.GetHwBackend(), t.GetCodec()),
			Width:       int(t.GetWidth()),
			Height:      int(t.GetHeight()),
			Framerate:   fr,
			BitrateKbps: int(t.GetBitrateKbps()),
			MaxBitrate:  int(t.GetMaxBitrateKbps()),
			GOPSize:     gop,
			MaxBFrames:  bframesOrDefault(t.GetBframesOrNeg1()),
			Options:     map[string]string{},
		}
		if t.GetPreset() != "" {
			enc.Options["preset"] = t.GetPreset()
		}
		if t.GetProfile() != "" {
			enc.Options["profile"] = t.GetProfile()
		}
		if t.GetLevel() != "" {
			enc.Options["level"] = t.GetLevel()
		}
		// NVENC ignores frame.PictType=I by default — the pipeline's
		// forceIDR mechanism (set after SwitchInput so post-switch
		// segments start with a clean IDR) only works when this opt
		// is on. libx264 honours the hint unconditionally and
		// ignores unknown options, so setting it for every backend
		// is safe.
		if isNVENCEncoder(enc.Codec) {
			enc.Options["forced-idr"] = "1"
		}
		cfg.Renditions = append(cfg.Renditions, RenditionConfig{
			Encoder: enc,
			Scaler: ScalerConfig{
				DstWidth:    int(t.GetWidth()),
				DstHeight:   int(t.GetHeight()),
				DstPixelFmt: 0, // PixelFormatYuv420P; filled by NewScaler default.
			},
			Watermark: wm,
		})
	}
	return cfg
}

// watermarkConfigFromProto narrows the wire-level WatermarkConfig
// into the pipeline's local shape. A nil / disabled proto value
// returns the zero WatermarkConfig (Enabled=false), which the
// pipeline reads as "no overlay" and skips the filter graph
// allocation for that rendition.
func watermarkConfigFromProto(w *pb.WatermarkConfig) WatermarkConfig {
	if w == nil || !w.GetEnabled() {
		return WatermarkConfig{}
	}
	return WatermarkConfig{
		Enabled:   true,
		Type:      w.GetType(),
		Text:      w.GetText(),
		AssetPath: w.GetAssetPath(),
		Position:  w.GetPosition(),
		OffsetX:   int(w.GetOffsetX()),
		OffsetY:   int(w.GetOffsetY()),
		Opacity:   w.GetOpacity(),
		FontSize:  int(w.GetFontSize()),
		FontColor: w.GetFontColor(),
		FontFile:  w.GetFontFile(),
	}
}

// AudioConfigFromProto narrows the proto AudioConfig to the
// PipelineConfig's audio struct. Exposed for use by future
// audio pipeline wiring; not yet consumed by StreamPipeline.
func AudioConfigFromProto(a *pb.AudioConfig) AudioConfig {
	if a == nil {
		// No audio config on the wire → default to passthrough, never
		// the more expensive (and lossy) re-encode path.
		return AudioConfig{Copy: true}
	}
	return AudioConfig{
		Copy:       a.GetCopy(),
		Codec:      a.GetCodec(),
		BitrateK:   int(a.GetBitrateK()),
		SampleRate: int(a.GetSampleRate()),
		Channels:   int(a.GetChannels()),
		Normalize:  a.GetNormalize(),
	}
}

// isNVENCEncoder reports whether the encoder name is one of NVIDIA's
// NVENC variants. Used to attach NVENC-specific options (forced-idr)
// that other encoders ignore harmlessly.
func isNVENCEncoder(name string) bool {
	switch name {
	case "h264_nvenc", "hevc_nvenc", "av1_nvenc":
		return true
	}
	return false
}

// encoderCodecForBackend resolves the per-backend libavcodec encoder
// name. P4 falls back to libx264 when explicit codec is empty (matches
// EncoderConfig defaults). P5 will fold per-backend H.265 + AV1 in.
func encoderCodecForBackend(hw pb.HWBackend, explicit string) string {
	if explicit != "" {
		return explicit
	}
	switch hw {
	case pb.HWBackend_HW_BACKEND_NVENC:
		return "h264_nvenc"
	case pb.HWBackend_HW_BACKEND_VAAPI:
		return "h264_vaapi"
	case pb.HWBackend_HW_BACKEND_VIDEOTOOLBOX:
		return "h264_videotoolbox"
	case pb.HWBackend_HW_BACKEND_QSV:
		return "h264_qsv"
	case pb.HWBackend_HW_BACKEND_CPU, pb.HWBackend_HW_BACKEND_UNSPECIFIED:
		fallthrough
	default:
		return "libx264"
	}
}

// decoderCodecForBackend picks the video DECODER name for the backend.
// NVENC hosts get NVDEC (cuvid) so decode runs on the GPU's dedicated
// decoder engine instead of the CPU — otherwise the CPU software-decodes
// every stream while the GPU's NVDEC sits idle. Other backends keep the
// universal CPU decoder (cuvid is NVIDIA-only).
//
// The startup decoder assumes H.264 (the dominant source codec). HEVC
// sources are handled at runtime: StreamPipeline.ensureVideoDecoder
// rebuilds the decoder to hevc / hevc_cuvid on the first video frame whose
// codec differs from the active decoder, so no codec needs to be known
// here before the first packet arrives.
func decoderCodecForBackend(hw pb.HWBackend) string {
	if hw == pb.HWBackend_HW_BACKEND_NVENC {
		return "h264_cuvid"
	}
	return "h264"
}

func framerateOrDefault(f float64) int {
	if f <= 0 {
		return 25
	}
	return int(f)
}

func bframesOrDefault(v int32) int {
	if v < 0 {
		return -1 // encoder default per EncoderConfig contract
	}
	return int(v)
}
