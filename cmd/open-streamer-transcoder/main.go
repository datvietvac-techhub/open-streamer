// open-streamer-transcoder is the per-stream transcoder subprocess. The
// main open-streamer binary spawns ONE instance of this binary per
// stream that needs encoding, talks to it over a Unix-domain socket with
// gRPC, and reaps it on stream stop.
//
// Lifetime model — one OS process per stream — gives us crash isolation
// for free: a SIGSEGV from libavcodec / NVENC takes down ONE stream
// (which the parent supervisor respawns) instead of every transcoded
// stream on the box. Mirrors Flussonic's coder-streamer-<name> per-
// stream BEAM node.
//
// P0 SCOPE: stub. Prints version and exits. P4 wires the gRPC server
// + native.Runner.
package main

import (
	"fmt"
	"os"

	"github.com/ntt0601zcoder/open-streamer/pkg/version"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		info := version.Get()
		fmt.Printf("open-streamer-transcoder %s (%s)\n", info.Version, info.Commit)
		return
	}
	fmt.Fprintln(os.Stderr, "open-streamer-transcoder: native pipeline not yet wired (P0 stub)")
	fmt.Fprintln(os.Stderr, "  see internal/transcoder/native/doc.go for migration plan")
	os.Exit(2)
}
