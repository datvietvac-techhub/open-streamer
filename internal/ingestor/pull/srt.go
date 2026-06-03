package pull

// srt.go — SRT pull ingestor (caller mode).
//
// Dials a remote SRT endpoint and reads raw MPEG-TS. SRT is a transport
// protocol — no codec conversion is needed; bytes are forwarded directly to
// the TSDemuxPacketReader pipeline.
//
// URL format (all standard SRT query parameters are honoured):
//
//	srt://host:port
//	srt://host:port?streamid=live/stream&latency=200&passphrase=secret
//	srt://host:port?conntimeo=5000&rcvlatency=120&peerlatency=120
//
// Any parameters in domain.Input.Params are merged into the URL query string
// before parsing, so callers can pass SRT options without embedding them in
// the URL (useful when the URL is user-supplied and opaque).
//
// Context cancellation: gosrt's SetReadDeadline is a no-op, so we start a
// watcher goroutine that calls conn.Close() when ctx is cancelled, which
// unblocks a blocking conn.Read().

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"sync"
	"time"

	srt "github.com/datarhei/gosrt"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// srtReadChunk matches the SRT live-streaming convention: one datagram carries
// exactly 7 MPEG-TS packets (7 × 188 = 1316 bytes ≤ typical SRT payload size).
const srtReadChunk = 188 * 7

// SRTReader dials an SRT server in caller mode and emits raw MPEG-TS chunks.
// It implements TSChunkReader and is intended to wrap with NewTSDemuxPacketReader.
type SRTReader struct {
	input     domain.Input
	conn      srt.Conn
	buf       []byte
	stopWatch context.CancelFunc // cancels the ctx-watcher goroutine on Close
	watchWG   sync.WaitGroup     // tracks the watchContext goroutine so Close can wait for it before mutating r.conn
}

// NewSRTReader constructs an SRTReader without opening a connection.
func NewSRTReader(input domain.Input) *SRTReader {
	return &SRTReader{
		input: input,
		buf:   make([]byte, srtReadChunk),
	}
}

// Open dials the remote SRT endpoint in caller mode.
// All SRT options present in the URL query string or in input.Params are
// applied to the connection config via Config.UnmarshalURL.
func (r *SRTReader) Open(ctx context.Context) error {
	cfg, host, err := r.buildConfig()
	if err != nil {
		return fmt.Errorf("srt reader: config %q: %w", r.input.URL, err)
	}

	conn, err := srt.Dial("srt", host, cfg)
	if err != nil {
		return fmt.Errorf("srt reader: dial %q: %w", host, err)
	}

	r.conn = conn

	slog.Info("srt reader: connected",
		"url", r.input.URL,
		"host", host,
		"stream_id", conn.StreamId(),
		"latency_ms", cfg.ReceiverLatency.Milliseconds(),
	)

	// SRT does not honour SetReadDeadline; close the conn from a watcher
	// goroutine when the caller's context is cancelled.
	watchCtx, stopWatch := context.WithCancel(context.Background())
	r.stopWatch = stopWatch
	r.watchWG.Add(1)
	go r.watchContext(ctx, watchCtx)

	return nil
}

// watchContext closes the SRT connection when the caller's ctx is cancelled.
// It exits immediately when stopWatch() is called (e.g. from Close).
func (r *SRTReader) watchContext(callerCtx, watchCtx context.Context) {
	defer r.watchWG.Done()
	select {
	case <-callerCtx.Done():
		if r.conn != nil {
			_ = r.conn.Close()
		}
	case <-watchCtx.Done():
		// Close() was called explicitly; nothing more to do.
	}
}

// Read returns the next raw MPEG-TS chunk from the SRT stream.
// Returns (nil, io.EOF) when the remote endpoint closes the connection.
func (r *SRTReader) Read(ctx context.Context) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	n, err := r.conn.Read(r.buf)
	if n > 0 {
		out := make([]byte, n)
		copy(out, r.buf[:n])
		return out, nil
	}

	switch {
	case err == nil:
		return nil, nil // transient zero-byte read; caller will retry

	case errors.Is(err, srt.ErrClientClosed), errors.Is(err, io.EOF):
		if ctx.Err() != nil {
			return nil, ctx.Err() // closed because of context cancellation
		}
		return nil, io.EOF

	default:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("srt reader: read: %w", err)
	}
}

// Close stops the context watcher and closes the SRT socket.
// Safe to call before Open or more than once.
//
// Wait for the watcher goroutine to exit BEFORE nilling out r.conn — the
// watcher may be inside `if r.conn != nil { r.conn.Close() }` when its
// callerCtx fires; mutating r.conn without that wait would race the read.
func (r *SRTReader) Close() error {
	if r.stopWatch != nil {
		r.stopWatch()
		r.stopWatch = nil
	}
	r.watchWG.Wait()
	if r.conn != nil {
		err := r.conn.Close()
		r.conn = nil
		return err
	}
	return nil
}

// ─── Config helpers ──────────────────────────────────────────────────────────

// buildConfig constructs the gosrt Config from the input URL and Params.
// Returns the resolved host address alongside the config.
func (r *SRTReader) buildConfig() (srt.Config, string, error) {
	rawURL := r.mergedURL()

	cfg := srt.DefaultConfig()
	host, err := cfg.UnmarshalURL(rawURL)
	if err != nil {
		return srt.Config{}, "", err
	}

	// Explicit connect timeout from InputNetConfig overrides any ?conntimeo= in the URL.
	if r.input.Net.TimeoutSec > 0 {
		cfg.ConnectionTimeout = time.Duration(r.input.Net.TimeoutSec) * time.Second
	}

	return cfg, host, nil
}

// mergedURL returns the input URL with any input.Params merged into the query
// string (Params values override URL-embedded values for the same key).
func (r *SRTReader) mergedURL() string {
	if len(r.input.Params) == 0 {
		return r.input.URL
	}
	u, err := url.Parse(r.input.URL)
	if err != nil {
		return r.input.URL
	}
	q := u.Query()
	for k, v := range r.input.Params {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()
	return u.String()
}
