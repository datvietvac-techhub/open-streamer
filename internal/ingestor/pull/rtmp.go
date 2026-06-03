package pull

// rtmp.go — RTMP pull ingestor using q191201771/lal/pkg/rtmp.
//
// Migrated from yapingcat/gomedia/go-rtmp after that library exposed a
// process-killing panic on AMF type bytes outside the AMF0/AMF3 spec
// range (production servers send vendor-specific
// extensions in connect responses). lal's rtmp parser tolerates those
// gracefully and the rest of our codebase already trusts lal for the
// outbound RTMP push path — consolidating on a single library removes
// dependency duplication.
//
// Frame format work this reader does on top of lal:
//
//   - Video sequence headers (AVC / HEVC, both classic and Enhanced RTMP):
//     parse out SPS / PPS / VPS, build an Annex-B prefix, store it. On
//     every IDR NALU we prepend the prefix so downstream decoders can
//     initialise from a single frame without external codec config.
//   - Video NALUs: AVCC length-prefix (4-byte BE) → Annex-B start codes.
//   - Audio AAC sequence headers: parse AudioSpecificConfig, store an
//     AscContext for ADTS generation.
//   - Audio AAC raw frames: prepend a 7-byte ADTS header.
//
// Lifecycle: NewRTMPReader → Open (TCP dial via lal + RTMP handshake +
// play negotiation, blocks until first AV frame is imminent) → ReadPackets
// loop → Close. Open errors out cleanly without leaking goroutines on
// dial / handshake / play-start failures. Close is idempotent.

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/q191201771/lal/pkg/base"
	"github.com/q191201771/lal/pkg/rtmp"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// rtmpChanSize is the buffered AVPacket channel depth between the lal
// session goroutine and ReadPackets. 16384 packets ≈ several seconds at
// the highest bitrates we expect — gives ReadPackets ample slack across
// GC pauses while bounding memory at ~16 MB worst-case.
const rtmpChanSize = 16384

// rtmpDefaultDialTimeout caps the TCP dial + RTMP handshake + play-start
// negotiation. After Open returns the connection runs unbounded; this
// only guards initial setup. Mirrored into lal as PullTimeoutMs.
const rtmpDefaultDialTimeout = 10 * time.Second

// RTMPReader pulls a remote RTMP play stream and emits domain.AVPacket.
//
// Owns one lal PullSession plus a goroutine that waits on its WaitChan
// to detect server disconnect. The OnReadRtmpAvMsg callback runs on
// lal's internal read goroutine; we convert each RtmpMsg into AVPacket(s)
// and push to the buffered pkts channel that ReadPackets drains.
type RTMPReader struct {
	input domain.Input

	mu      sync.Mutex
	session *rtmp.PullSession
	pkts    chan domain.AVPacket
	closing atomic.Bool
	closeWg sync.WaitGroup

	// converter holds the per-stream codec configuration state and turns
	// each `base.RtmpMsg` from lal into zero or more domain.AVPackets.
	// Shared with the push server (push/rtmp_server.go) — see
	// rtmp_msg_converter.go for the conversion details.
	converter *RTMPMsgConverter
}

// NewRTMPReader constructs a reader for input without opening any
// connections. URL parse / dial / handshake happens in Open.
func NewRTMPReader(input domain.Input) *RTMPReader {
	return &RTMPReader{input: input}
}

// Open dials the upstream, completes the RTMP handshake, sends the play
// command, and blocks until lal confirms play-start (or the configured
// timeout fires). After this returns nil, ReadPackets is ready to drain
// frames.
func (r *RTMPReader) Open(ctx context.Context) error {
	dialTimeout := rtmpDefaultDialTimeout
	if r.input.Net.TimeoutSec > 0 {
		dialTimeout = time.Duration(r.input.Net.TimeoutSec) * time.Second
	}

	pkts := make(chan domain.AVPacket, rtmpChanSize)

	session := rtmp.NewPullSession(func(opt *rtmp.PullSessionOption) {
		opt.PullTimeoutMs = int(dialTimeout / time.Millisecond)
		// ReadAvTimeoutMs=0 (no per-read AV timeout). Stream-level liveness
		// is enforced by the manager's input_packet_timeout_sec rather than
		// at the lal layer; conflating the two would race on packet bursts.
		opt.ReadAvTimeoutMs = 0
		opt.HandshakeComplexFlag = true
		// ReuseReadMessageBufferFlag=false: lal otherwise re-uses one buffer
		// across callbacks. We need to keep frame bytes alive past the
		// callback (they end up in AVPackets on the buffer hub), so opt out
		// of buffer reuse to avoid corruption.
		opt.ReuseReadMessageBufferFlag = false
		// rtmps:// URLs land on this path; lal applies TLS automatically
		// when Pull(url) sees scheme=rtmps. Custom CA bundles need a
		// non-nil TlsConfig — we accept the system-default for now.
		opt.TlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	})

	session.WithOnReadRtmpAvMsg(func(msg base.RtmpMsg) {
		r.handleRtmpMsg(msg, pkts)
	})

	r.mu.Lock()
	r.session = session
	r.pkts = pkts
	r.converter = NewRTMPMsgConverter()
	r.closing.Store(false)
	r.mu.Unlock()

	// lal Pull blocks until either play-start succeeds or an error.
	// Run with our context so caller cancellation aborts the dial.
	pullErr := make(chan error, 1)
	go func() {
		pullErr <- session.Pull(r.input.URL)
	}()

	select {
	case err := <-pullErr:
		if err != nil {
			r.abortOpen()
			return fmt.Errorf("rtmp reader: pull %q: %w", r.input.URL, err)
		}
	case <-ctx.Done():
		r.abortOpen()
		return ctx.Err()
	case <-time.After(dialTimeout + 2*time.Second):
		r.abortOpen()
		return fmt.Errorf("rtmp reader: pull-start timeout after %s", dialTimeout)
	}

	// Spawn a watcher that closes the pkts channel when lal's session
	// terminates (server disconnect / network error). ReadPackets then
	// returns io.EOF so the worker reconnects via the standard backoff.
	r.closeWg.Add(1)
	go r.watchSessionEnd(session, pkts)

	return nil
}

// watchSessionEnd waits for lal's session WaitChan (server-driven exit)
// then closes pkts, unblocking ReadPackets with io.EOF.
func (r *RTMPReader) watchSessionEnd(session *rtmp.PullSession, pkts chan<- domain.AVPacket) {
	defer r.closeWg.Done()
	if err := <-session.WaitChan(); err != nil {
		// Session-end errors are routine here — server disconnect or our
		// own Dispose during Close. Logged at Debug because the worker's
		// reconnect-with-backoff handles the recovery without operator
		// involvement.
		slog.Debug("rtmp reader: session ended", "url", r.input.URL, "err", err)
	}
	r.mu.Lock()
	if !r.closing.Load() {
		r.closing.Store(true)
		close(pkts)
	}
	r.mu.Unlock()
}

// abortOpen tears down a partial Open. Idempotent.
func (r *RTMPReader) abortOpen() {
	r.mu.Lock()
	session := r.session
	r.session = nil
	r.mu.Unlock()
	if session != nil {
		_ = session.Dispose()
	}
}

// ReadPackets blocks until at least one AVPacket is available, then drains
// up to 256 more so the caller's per-call overhead amortises across a
// burst. Returns (nil, io.EOF) on stream end and (nil, ctx.Err()) on
// caller cancellation.
func (r *RTMPReader) ReadPackets(ctx context.Context) ([]domain.AVPacket, error) {
	r.mu.Lock()
	ch := r.pkts
	r.mu.Unlock()
	if ch == nil {
		return nil, io.EOF
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case p, ok := <-ch:
		if !ok {
			return nil, io.EOF
		}
		batch := []domain.AVPacket{p}
		for len(batch) < 256 {
			select {
			case p2, ok2 := <-ch:
				if !ok2 {
					return batch, nil
				}
				batch = append(batch, p2)
			default:
				return batch, nil
			}
		}
		return batch, nil
	}
}

// Close tears down the RTMP session and waits for the watcher goroutine
// to exit. Idempotent.
func (r *RTMPReader) Close() error {
	r.mu.Lock()
	session := r.session
	r.session = nil
	pkts := r.pkts
	r.pkts = nil
	closing := r.closing.Swap(true)
	r.mu.Unlock()

	if session != nil {
		_ = session.Dispose()
	}
	r.closeWg.Wait()

	if !closing && pkts != nil {
		// Watcher's deferred close didn't fire (rare race, typically Close
		// before any frame). Close defensively.
		safeClosePktChan(pkts)
	}
	return nil
}

// handleRtmpMsg is invoked on lal's read goroutine for every RTMP AV
// message. Delegates the codec-specific work to RTMPMsgConverter and
// emits the resulting AVPackets onto pkts.
func (r *RTMPReader) handleRtmpMsg(msg base.RtmpMsg, pkts chan<- domain.AVPacket) {
	if r.closing.Load() {
		return
	}
	for _, p := range r.converter.Convert(msg) {
		emitRTMPPacket(pkts, &r.closing, p)
	}
}

// emitRTMPPacket sends p onto pkts unless the reader is closing. Drops
// silently on closed channel to keep lal's read goroutine from panicking.
func emitRTMPPacket(pkts chan<- domain.AVPacket, closing *atomic.Bool, p domain.AVPacket) {
	if closing.Load() {
		return
	}
	defer func() { _ = recover() }() // guard against close race
	select {
	case pkts <- p:
	default:
		// Channel full — drop to keep lal's read goroutine unblocked.
		// The buffer hub fan-out has the same drop policy by design.
	}
}

// safeClosePktChan closes the channel if it isn't already closed. Used
// by Close as a defensive double-close guard for a rare race with the
// session-end watcher's deferred close.
func safeClosePktChan(ch chan domain.AVPacket) {
	defer func() { _ = recover() }()
	close(ch)
}
