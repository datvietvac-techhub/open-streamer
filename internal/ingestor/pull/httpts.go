package pull

// httpts.go — HTTP MPEG-TS pull (low-latency relay client).
//
// Counterpart to publisher/serve_mpegts.go: a single chunked HTTP GET that
// stays open and emits raw TS bytes as they arrive. Compared with the HLS
// pull reader (poll playlist → fetch segments) this avoids the segment
// boundary, the playlist round-trip, and the segment download buffering —
// end-to-end latency drops from 4–10 s to ~50–200 ms over LAN.
//
// Lifecycle:
//
//	Open(ctx) → http.Get with cancellable context, response body kept open
//	Read(ctx) → blocks on body.Read into a fixed-size scratch buffer
//	Close()   → body.Close → in-flight Read unblocks with io.EOF
//
// Reconnection is the responsibility of runPullWorker (the standard
// reconnect-with-backoff loop): when the upstream closes the body or the
// network drops, Read returns an error, the worker calls Close, then Open
// again on its next backoff cycle.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/pkg/version"
)

// httpTSReadBufSize is the per-Read scratch buffer. 32 KiB is a sweet spot:
// large enough to amortise syscall overhead at high bitrates (>50 Mbps),
// small enough that a single Read returns within a few ms at low-bitrate
// idle so the demux/segmenter never starves on partial data.
const httpTSReadBufSize = 32 * 1024

// httpTSDefaultDialTimeout caps the initial HTTP connect + headers exchange.
// The body itself has no timeout — it must stay open as long as the source
// is live. 15s matches DefaultHLSPlaylistTimeoutSec for symmetry.
const httpTSDefaultDialTimeout = 15 * time.Second

// HTTPTSReader pulls a continuous MPEG-TS stream over chunked HTTP.
type HTTPTSReader struct {
	input domain.Input

	client *http.Client
	resp   *http.Response // nil before Open / after Close
}

// NewHTTPTSReader constructs a reader for the given input. Connection is
// deferred to Open so the constructor never blocks.
func NewHTTPTSReader(input domain.Input) *HTTPTSReader {
	transport := &http.Transport{
		// Keep-alives off: every Open is a fresh stream so pooled
		// connections don't help and may surface stale TCP state.
		DisableKeepAlives: true,
		// Force the response to be streamed rather than buffered, so
		// our Read sees bytes as soon as they arrive on the wire.
		DisableCompression:    true,
		ResponseHeaderTimeout: timeoutOr(input.Net.TimeoutSec, httpTSDefaultDialTimeout),
	}
	return &HTTPTSReader{
		input: input,
		client: &http.Client{
			// Per-request dial / TLS / headers timeout. Body is unbounded.
			Timeout:   0,
			Transport: transport,
		},
	}
}

// Open issues the GET request and stores the response. The response body
// must be closed via Close().
func (r *HTTPTSReader) Open(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.input.URL, nil)
	if err != nil {
		return fmt.Errorf("httpts: build request %q: %w", r.input.URL, err)
	}
	req.Header.Set("User-Agent", version.UserAgent())
	for k, v := range r.input.Headers {
		req.Header.Set(k, v)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("httpts: GET %q: %w", r.input.URL, err)
	}
	if resp.StatusCode/100 != 2 {
		_ = resp.Body.Close()
		// Reuse hls.go's HTTP error envelope so the worker's
		// shouldFailoverImmediately heuristic catches 401/403/404/etc.
		return &httpStatusErr{url: r.input.URL, code: resp.StatusCode}
	}
	r.resp = resp
	return nil
}

// Read blocks for the next chunk of bytes from the response body. Returns
// io.EOF when the upstream closes cleanly.
func (r *HTTPTSReader) Read(ctx context.Context) ([]byte, error) {
	if r.resp == nil {
		return nil, io.ErrUnexpectedEOF
	}
	// Honour caller cancellation immediately, even if Body.Read is currently
	// blocked. Body was created with the Open ctx; if a different ctx is
	// passed here we can't propagate to it, but we can short-circuit ourselves.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	buf := make([]byte, httpTSReadBufSize)
	n, err := r.resp.Body.Read(buf)
	if n > 0 {
		return buf[:n], nil
	}
	if err == nil {
		// 0-byte read with no error — defensive guard, treat as transient.
		return nil, nil
	}
	return nil, err
}

// Close releases the response body. Safe to call before Open or multiple
// times.
func (r *HTTPTSReader) Close() error {
	if r.resp == nil {
		return nil
	}
	err := r.resp.Body.Close()
	r.resp = nil
	return err
}

// timeoutOr returns d when sec > 0, otherwise fallback. Centralised so
// other readers can mirror the convention without re-implementing it.
func timeoutOr(sec int, fallback time.Duration) time.Duration {
	if sec > 0 {
		return time.Duration(sec) * time.Second
	}
	return fallback
}

// Compile-time interface check.
var _ TSChunkReader = (*HTTPTSReader)(nil)
