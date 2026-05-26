// Package native is the in-process libavcodec transcoding pipeline that
// runs inside the per-stream open-streamer-transcoder subprocess.
//
// Topology:
//
//	transcoder.Service.Start (in the main open-streamer process)
//	  └─ spawns one open-streamer-transcoder subprocess per stream and
//	     talks to it over a bidirectional gRPC stream on a Unix socket.
//
//	Inside the subprocess, native.StreamPipeline runs:
//	  • one decoder (h264_cuvid / h264 / hevc / …) — ephemeral, rebuilt on
//	    every input switch, sized to the source;
//	  • one scaler per rendition (sws_scale on CPU, scale_cuda on GPU) —
//	    rebuilt when the source dimensions / SAR / fps change;
//	  • one encoder per rendition (h264_nvenc / libx264 / vaapi / qsv /
//	    videotoolbox) — opened ONCE and never closed across input switches.
//
//	The single decode fans out to every rendition. On an input switch only
//	the decoder (and, when the source dimensions change, the scaler graph)
//	is rebuilt; the encoders survive, so SPS/PPS stay stable and the output
//	PTS is rebased onto one continuous clock — downstream segments stay
//	continuous with no playlist freeze.
package native

import "errors"

// ErrNotImplemented signals an operation this package does not support.
var ErrNotImplemented = errors.New("transcoder/native: not implemented")
