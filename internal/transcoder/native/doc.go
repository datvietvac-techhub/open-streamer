// Package native is the in-process libavcodec pipeline that replaces the
// FFmpeg subprocess transcoder.
//
// Architecture (filled out across P1–P6 of the native-transcoder migration):
//
//	┌── open-streamer (main, in-process orchestrator) ──────────────────┐
//	│   transcoder.Service.Start                                        │
//	│     ↓ spawns / talks to                                           │
//	│   open-streamer-transcoder subprocess (1 OS process / stream)     │
//	│     via gRPC stream over Unix-domain socket                       │
//	│                                                                   │
//	│   ┌── open-streamer-transcoder (1 process / stream) ──────────┐   │
//	│   │  native.StreamPipeline                                     │   │
//	│   │    encoder = h264_nvenc (or libx264 / vaapi / qsv / VT)    │   │
//	│   │    open ONCE per stream lifetime; never closed across      │   │
//	│   │    input switches                                          │   │
//	│   │                                                            │   │
//	│   │    on each input cycle:                                    │   │
//	│   │      decoder = h264_cuvid / h264 / hevc / …                │   │
//	│   │      scaler  = sws_scale / scale_npp / hwupload+scale_cuda │   │
//	│   │      both rebuilt freely when input dim / SAR / fps change │   │
//	│   │                                                            │   │
//	│   │  Frame holder absorbs the gap between OldDecoder.Free and  │   │
//	│   │  NewDecoder.first frame by repeating the last YUV frame    │   │
//	│   │  at the encoder's fps tick — keeps NVENC fed, downstream   │   │
//	│   │  segments continuous, no playlist freeze on input switch.  │   │
//	│   └────────────────────────────────────────────────────────────┘   │
//	└───────────────────────────────────────────────────────────────────┘
//
// P0 SCOPE: declarations only. No live code paths yet. Service.Start in
// the parent package returns ErrNotImplemented; consumers see the empty
// stubs here so the build graph stays honest about what's not wired.
package native

import "errors"

// ErrNotImplemented signals that a stub in this package has not been
// filled in yet. P1–P6 will replace each return with a real path.
var ErrNotImplemented = errors.New("transcoder/native: not implemented in current phase")
