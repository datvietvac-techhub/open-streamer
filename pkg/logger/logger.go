// Package logger initialises the application-wide slog.Logger and the
// LAL-internal nazalog level so a single config knob covers both.
// One logger is created at startup and passed via DI.
// Never call slog.SetDefault in tests — use the injected logger instead.
package logger

import (
	"context"
	"io"
	"log/slog"
	"os"

	"github.com/q191201771/naza/pkg/nazalog"

	"github.com/datvietvac-techhub/open-streamer/config"
)

// LevelTrace is a custom slog level below Debug for very high-frequency
// logs (per-segment flushes, per-RTP packet, etc.) that would otherwise
// drown out useful Debug output. Set log.level to "trace" to enable.
const LevelTrace = slog.Level(-8)

// Trace emits a log record at LevelTrace via the default logger. Use
// this for hot-path events (e.g. HLS / DASH / DVR segment flushed) so
// they only show when the operator opts in via log.level=trace.
//
// Background context is intentional: Trace is fire-and-forget, used in
// hot paths where threading a real context would force every caller (and
// every caller's caller) to grow a ctx parameter just for a debug print.
//
//nolint:contextcheck // hot-path log helper; ctx threading would cascade through entire call graph for no observability gain.
func Trace(msg string, args ...any) {
	slog.Default().Log(context.Background(), LevelTrace, msg, args...)
}

// Apply installs cfg as the current process-wide logging configuration:
// it sets slog.SetDefault to a logger built from cfg AND re-initialises
// LAL's nazalog level via lalLevel(cfg). Calling this on every config
// change (startup + runtime hot-reload) keeps both logging stacks in
// lock-step — without the nazalog half, hot-reloading log.level via the
// /config API only affects slog and lal continues emitting whatever
// level was set at boot, which is the most common "I lowered my log
// level but lal's INFO chatter still floods journalctl" footgun.
func Apply(cfg config.LogConfig) {
	slog.SetDefault(New(cfg))
	// Init mutates the underlying global *logger struct in place; both
	// nazalog package-level functions and any captured GetGlobalLogger
	// pointers (e.g. lal's pkg/rtmp.Log) see the new level immediately.
	_ = nazalog.Init(func(o *nazalog.Option) {
		o.Level = lalLevel(cfg)
	})
}

// lalLevel maps our slog level vocabulary to LAL's nazalog level. The
// mapping is asymmetric: only operator-requested verbose levels (trace
// or debug) unlock LAL's Debug output. Anything else collapses to LAL's
// Error level because LAL warns aggressively on harmless protocol
// quirks ("read user control message, ignore", "< R Acknowledgement.
// ignore. sequence number=…") and would otherwise drown out the rest
// of journalctl. LAL's Errorf path covers genuine protocol failures
// so no real signal is lost by suppressing warn.
func lalLevel(cfg config.LogConfig) nazalog.Level {
	switch cfg.Level {
	case "trace", "debug":
		return nazalog.LevelDebug
	default:
		return nazalog.LevelError
	}
}

// New creates a slog.Logger from the given LogConfig.
// Format "json" produces structured JSON; anything else produces human-readable text.
//
// Recognised levels: trace (most verbose), debug, info (default), warn,
// error. The "trace" level enables per-segment / per-packet logs that
// debug deliberately omits.
func New(cfg config.LogConfig) *slog.Logger {
	var level slog.Level
	switch cfg.Level {
	case "trace":
		level = LevelTrace
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: replaceLevelName,
	}

	var handler slog.Handler
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	return slog.New(handler)
}

// NewWithWriter creates a logger that writes to the provided writer (useful for tests).
func NewWithWriter(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: replaceLevelName,
	}))
}

// replaceLevelName renames LevelTrace from slog's default "DEBUG-4" to
// "TRACE" in the rendered output. slog only knows Debug/Info/Warn/Error
// names by default — for any custom level it falls back to the stringified
// offset, which is unreadable in logs.
func replaceLevelName(_ []string, a slog.Attr) slog.Attr {
	if a.Key != slog.LevelKey {
		return a
	}
	if lvl, ok := a.Value.Any().(slog.Level); ok && lvl == LevelTrace {
		a.Value = slog.StringValue("TRACE")
	}
	return a
}
