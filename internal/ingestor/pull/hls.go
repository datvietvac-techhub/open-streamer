package pull

// hls.go — HLS pull ingestor.
//
// Downloads and parses M3U8 playlists without any third-party parser library.
// Supports live (sliding window), VOD (#EXT-X-ENDLIST), and adaptive master
// playlists — the highest-bandwidth variant is selected automatically.
//
// Data flow:
//
//	Open() → poll goroutine → fetchPlaylistWithRetry → fetchSegmentWithRetry
//	                        ↓ one hlsResult per segment
//	Read() ← out channel (buffered; closed when the source is exhausted)
//
// Reliability contract:
//   - Transient segment failures: logged and skipped; streaming continues.
//   - Persistent playlist failures: terminal error forwarded to Read callers.
//   - VOD (#EXT-X-ENDLIST): io.EOF delivered after all segments are consumed.
//   - Deduplication by media-sequence number: O(1) state, no map growth.

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/pkg/version"
)

const (
	hlsDefaultPlaylistTimeout = time.Duration(domain.DefaultHLSPlaylistTimeoutSec) * time.Second
	hlsDefaultSegmentTimeout  = time.Duration(domain.DefaultHLSSegmentTimeoutSec) * time.Second
	hlsMinPollInterval        = 500 * time.Millisecond
	hlsMaxPlaylistRetries     = 5
	hlsMaxSegmentRetries      = 2
	hlsRetryBase              = 500 * time.Millisecond
)

// M3U8 tag constants — used in both the parser and the media-tag detector.
const (
	m3u8TagMediaSeq   = "#EXT-X-MEDIA-SEQUENCE:"
	m3u8TagTargetDur  = "#EXT-X-TARGETDURATION:"
	m3u8TagEndList    = "#EXT-X-ENDLIST"
	m3u8TagDisconuity = "#EXT-X-DISCONTINUITY"
	m3u8TagExtInf     = "#EXTINF:"
	m3u8TagStreamInf  = "#EXT-X-STREAM-INF:"
)

// ─── HTTP error type ─────────────────────────────────────────────────────────

// httpStatusErr captures a non-2xx HTTP response.
// Permanent errors (4xx client errors that cannot be resolved by retrying)
// short-circuit the retry loop so that failover is triggered immediately.
type httpStatusErr struct {
	url  string
	code int
}

func (e *httpStatusErr) Error() string {
	return fmt.Sprintf("hls: GET %q: HTTP %d", e.url, e.code)
}

// isPermanent returns true for codes that signal the resource is permanently
// unavailable and will never succeed on retry (auth failure, not-found, gone).
func (e *httpStatusErr) isPermanent() bool {
	switch e.code {
	case http.StatusUnauthorized, // 401
		http.StatusForbidden, // 403
		http.StatusNotFound,  // 404
		http.StatusGone:      // 410
		return true
	}
	return false
}

// isPermanentTransportError reports whether a non-HTTP-status error from the
// playlist or segment fetch is fundamentally unrecoverable by retrying. TLS
// certificate failures (untrusted CA, hostname mismatch, expired cert) and
// DNS hostname-not-found are configuration / source-side problems that won't
// fix themselves in a few hundred milliseconds — burning 5 retries on them
// just delays the failover to the backup input.
func isPermanentTransportError(err error) bool {
	if err == nil {
		return false
	}
	// errors.As-style detection on standard library types.
	var unkAuth x509.UnknownAuthorityError
	if errors.As(err, &unkAuth) {
		return true
	}
	var hostErr x509.HostnameError
	if errors.As(err, &hostErr) {
		return true
	}
	var certInvalid x509.CertificateInvalidError
	if errors.As(err, &certInvalid) {
		return true
	}
	// Fallback: substring match catches other tls/x509 wrappers + dns.
	// Net/http often wraps these in url.Error or *tls.PermanentError without
	// re-exporting a type that errors.As can match cleanly.
	msg := err.Error()
	if strings.Contains(msg, "x509:") ||
		strings.Contains(msg, "tls:") ||
		strings.Contains(msg, "no such host") {
		return true
	}
	return false
}

// ─── Result channel type ────────────────────────────────────────────────────

type hlsResult struct {
	data []byte
	err  error // non-nil only on terminal condition (io.EOF or fatal error)
}

// ─── M3U8 types ─────────────────────────────────────────────────────────────

type hlsMediaPlaylist struct {
	seqBase        uint64
	targetDuration float64
	ended          bool
	segments       []hlsSegment
}

type hlsSegment struct {
	uri           string
	seq           uint64
	duration      float64
	discontinuity bool
}

type hlsVariant struct {
	uri       string
	bandwidth uint32
}

// ─── HLSReader ───────────────────────────────────────────────────────────────

// HLSReader polls an HLS playlist and emits MPEG-TS segment bytes in order.
// It implements TSChunkReader and is intended to wrap with NewTSDemuxPacketReader.
//
// Expected URL format: http[s]://host/path/to/playlist.m3u8
// Master playlists are transparently resolved to the highest-bandwidth variant.
//
// Open/Close may be called multiple times (runPullWorker reconnects on error).
// Each Open() creates a fresh output channel so that the previous poll goroutine's
// deferred close does not affect the new one.
type HLSReader struct {
	input  domain.Input
	maxBuf int

	// plClient: short-timeout client for playlist GETs.
	// segClient: long-timeout client for segment body reads.
	plClient  *http.Client
	segClient *http.Client

	out    chan hlsResult
	cancel context.CancelFunc
	once   sync.Once
}

// NewHLSReader constructs an HLSReader without opening the connection.
func NewHLSReader(input domain.Input, cfg config.IngestorConfig) *HLSReader {
	maxBuf := cfg.HLSMaxSegmentBuffer
	if maxBuf <= 0 {
		maxBuf = domain.DefaultHLSMaxSegmentBuffer
	}
	return &HLSReader{
		input:  input,
		maxBuf: maxBuf,
	}
}

// Open starts the background polling goroutine.
// Returns immediately; segments arrive asynchronously via Read.
// Safe to call again after Close — each call creates a fresh output channel.
func (r *HLSReader) Open(ctx context.Context) error {
	plTimeout := time.Duration(r.input.Net.TimeoutSec) * time.Second
	if plTimeout == 0 {
		plTimeout = hlsDefaultPlaylistTimeout
	}
	segTimeout := plTimeout * 4
	if segTimeout < hlsDefaultSegmentTimeout {
		segTimeout = hlsDefaultSegmentTimeout
	}

	// Build a shared transport so playlist + segment clients pick up the
	// same TLS policy. Default uses Go's secure defaults; the operator can
	// opt out via input.net.insecure_tls = true (e.g. self-signed source on
	// a trusted private network — broadcast CDNs often serve expired certs).
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if r.input.Net.InsecureTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // operator-opted-in for sources on trusted networks
		slog.Warn("hls reader: TLS verification disabled per input.net.insecure_tls",
			"url", r.input.URL)
	}
	r.plClient = &http.Client{Timeout: plTimeout, Transport: transport}
	r.segClient = &http.Client{Timeout: segTimeout, Transport: transport}

	// Fresh channel and once per Open() call so the previous poll goroutine's
	// deferred close(out) cannot race with this new goroutine's close.
	out := make(chan hlsResult, r.maxBuf)
	r.out = out
	r.once = sync.Once{}

	pollCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	go r.poll(pollCtx, r.input.URL, out)
	return nil
}

// Read blocks until the next MPEG-TS segment chunk is available.
// Returns (nil, io.EOF) when a VOD stream is fully consumed or the source ends.
func (r *HLSReader) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res, ok := <-r.out:
		if !ok {
			return nil, io.EOF
		}
		return res.data, res.err
	}
}

// Close stops the poll goroutine. Safe to call before Open or more than once.
func (r *HLSReader) Close() error {
	r.once.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
	})
	return nil
}

// ─── Poll loop ───────────────────────────────────────────────────────────────

func (r *HLSReader) poll(ctx context.Context, rawURL string, out chan hlsResult) {
	defer close(out)

	mediaURL := rawURL
	var started bool
	var lastSeq uint64

	for {
		if ctx.Err() != nil {
			return
		}

		pl, err := r.fetchPlaylistWithRetry(ctx, &mediaURL)
		if err != nil {
			r.sendResult(ctx, out, hlsResult{err: err})
			return
		}

		delivered := r.deliverNewSegments(ctx, pl, &started, &lastSeq, out)
		if !delivered {
			return // context cancelled inside deliverNewSegments
		}

		if pl.ended {
			r.sendResult(ctx, out, hlsResult{err: io.EOF})
			return
		}

		if !r.waitForNextPoll(ctx, pl.targetDuration) {
			return
		}
	}
}

// deliverNewSegments downloads and queues segments not yet delivered.
// Returns false when the context was cancelled.
func (r *HLSReader) deliverNewSegments(
	ctx context.Context,
	pl *hlsMediaPlaylist,
	started *bool,
	lastSeq *uint64,
	out chan hlsResult,
) bool {
	for _, seg := range pl.segments {
		if *started && seg.seq <= *lastSeq {
			continue
		}
		data, err := r.fetchSegmentWithRetry(ctx, seg.uri)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			slog.Warn("hls: segment fetch failed, skipping",
				"url", seg.uri, "err", err)
			continue
		}
		if !r.sendResult(ctx, out, hlsResult{data: data}) {
			return false
		}
		*lastSeq = seg.seq
		*started = true
	}
	return true
}

// sendResult sends a result to out, returning false if ctx is cancelled.
func (r *HLSReader) sendResult(ctx context.Context, out chan hlsResult, res hlsResult) bool {
	select {
	case out <- res:
		return true
	case <-ctx.Done():
		return false
	}
}

// waitForNextPoll waits half a target duration before the next playlist poll.
// Returns false when the context is cancelled.
func (r *HLSReader) waitForNextPoll(ctx context.Context, targetDuration float64) bool {
	if targetDuration <= 0 {
		targetDuration = 2
	}
	wait := time.Duration(float64(time.Second) * targetDuration / 2)
	if wait < hlsMinPollInterval {
		wait = hlsMinPollInterval
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// ─── Playlist fetching ───────────────────────────────────────────────────────

// fetchPlaylistWithRetry fetches and parses the playlist, resolving a master
// playlist to its highest-bandwidth variant. Retries transient HTTP errors.
// The mediaURL pointer is updated when a master → media redirect occurs.
func (r *HLSReader) fetchPlaylistWithRetry(ctx context.Context, mediaURL *string) (*hlsMediaPlaylist, error) {
	var lastErr error
	for attempt := 0; attempt <= hlsMaxPlaylistRetries; attempt++ {
		if attempt > 0 {
			if !r.retryWait(ctx, attempt-1) {
				return nil, ctx.Err()
			}
		}
		pl, err := r.resolveAndFetch(ctx, mediaURL)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// Permanent HTTP errors (401, 403, 404, 410) will not recover on retry;
			// fail immediately so the caller can trigger failover without delay.
			var he *httpStatusErr
			if errors.As(err, &he) && he.isPermanent() {
				return nil, err
			}
			// TLS / x509 / DNS errors are configuration-side and won't fix
			// themselves — short-circuit so the caller fails over instead of
			// burning the full retry budget on doomed attempts.
			if isPermanentTransportError(err) {
				return nil, err
			}
			lastErr = err
			slog.Warn("hls: playlist fetch failed, retrying",
				"url", *mediaURL, "attempt", attempt+1, "err", err)
			continue
		}
		return pl, nil
	}
	return nil, fmt.Errorf("hls: playlist %q: %w (after %d retries)", *mediaURL, lastErr, hlsMaxPlaylistRetries)
}

// resolveAndFetch fetches the URL; if it is a master playlist, selects the
// best variant, updates *mediaURL, and fetches again (one level of redirection).
func (r *HLSReader) resolveAndFetch(ctx context.Context, mediaURL *string) (*hlsMediaPlaylist, error) {
	for {
		result, err := r.fetchPlaylistOnce(ctx, *mediaURL)
		if err != nil {
			return nil, err
		}
		if !result.isMaster() {
			return result.media, nil
		}
		best := pickBestVariant(result.variants)
		if best == "" {
			return nil, fmt.Errorf("hls: master playlist has no usable variants")
		}
		*mediaURL = best
	}
}

type fetchResult struct {
	media    *hlsMediaPlaylist
	variants []hlsVariant
}

func (f *fetchResult) isMaster() bool { return f.media == nil }

// fetchPlaylistOnce performs a single HTTP GET and parses the M3U8 body.
func (r *HLSReader) fetchPlaylistOnce(ctx context.Context, playlistURL string) (*fetchResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, playlistURL, nil)
	if err != nil {
		return nil, fmt.Errorf("hls: build request: %w", err)
	}
	req.Header.Set("User-Agent", version.UserAgent())
	for k, v := range r.input.Headers {
		req.Header.Set(k, v)
	}

	resp, err := r.plClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hls: GET %q: %w", playlistURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, &httpStatusErr{url: playlistURL, code: resp.StatusCode}
	}

	base, _ := url.Parse(playlistURL)
	return parseM3U8(resp.Body, base)
}

// ─── Segment fetching ────────────────────────────────────────────────────────

// Response-body size caps. Without them, fetchSegmentOnce's io.ReadAll and the
// playlist parser allocate whatever the upstream sends — a malicious,
// compromised, or (via SSRF / a 3xx redirect, see S-4) attacker-chosen host can
// return a multi-GB body and OOM-kill the shared process, taking every stream on
// the host down (A-7). The only prior bound was the 60 s segment timeout
// (~7.5 GB at 1 Gbps). Package vars so ops can tune them and tests can shrink
// them.
var (
	hlsMaxSegmentBytes  int64 = 64 << 20 // 64 MiB — generous for any real segment
	hlsMaxPlaylistBytes int64 = 8 << 20  // 8 MiB — a huge live/VOD playlist is still well under this
)

// errBodyTooLarge is returned when a response body exceeds its size cap. For a
// segment it is treated as a failed fetch (retried, then skipped) rather than
// fatal, so one bad segment doesn't kill the stream.
var errBodyTooLarge = errors.New("hls: response body exceeds size cap")

// readCapped reads from r up to max bytes. It reads at most max+1 bytes via an
// io.LimitReader, so an over-limit body is rejected WITHOUT allocating the whole
// thing (the OOM the cap exists to prevent); a body of exactly max is allowed.
func readCapped(r io.Reader, maxBytes int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, errBodyTooLarge
	}
	return data, nil
}

// fetchSegmentWithRetry downloads a segment, retrying transient failures.
func (r *HLSReader) fetchSegmentWithRetry(ctx context.Context, segURL string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= hlsMaxSegmentRetries; attempt++ {
		if attempt > 0 {
			if !r.retryWait(ctx, attempt-1) {
				return nil, ctx.Err()
			}
		}
		data, err := r.fetchSegmentOnce(ctx, segURL)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
			continue
		}
		return data, nil
	}
	return nil, lastErr
}

func (r *HLSReader) fetchSegmentOnce(ctx context.Context, segURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, segURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", version.UserAgent())
	for k, v := range r.input.Headers {
		req.Header.Set(k, v)
	}
	resp, err := r.segClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hls: GET segment %q: %w", segURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, &httpStatusErr{url: segURL, code: resp.StatusCode}
	}
	return readCapped(resp.Body, hlsMaxSegmentBytes)
}

// retryWait waits for exponential backoff (hlsRetryBase × 2^attempt).
// Returns false when the context is cancelled.
func (r *HLSReader) retryWait(ctx context.Context, attempt int) bool {
	delay := hlsRetryBase * (1 << attempt)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// ─── M3U8 parser ────────────────────────────────────────────────────────────

// parseM3U8 reads an M3U8 playlist from r and returns either a media playlist
// or a list of master variants. Base is used to resolve relative URIs.
func parseM3U8(body io.Reader, base *url.URL) (*fetchResult, error) {
	var (
		segments    []hlsSegment
		variants    []hlsVariant
		seqBase     uint64
		targetDur   float64
		ended       bool
		isMediaPL   bool
		pendingDur  float64
		pendingDisc bool
	)

	// Cap the playlist body so a malicious/redirected host can't stream an
	// unbounded M3U8 into the scanner (A-7). 8 MiB transitively bounds the
	// parsed segment/variant count too (~50 B/line → well under 200k lines).
	scanner := bufio.NewScanner(io.LimitReader(body, hlsMaxPlaylistBytes))
	scanner.Buffer(make([]byte, 256*1024), 256*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line == "#EXTM3U" {
			continue
		}
		isMediaPL = isMediaPL || isMediaTag(line)
		if err := parseLine(line, base, scanner, &parseState{
			seqBase:     &seqBase,
			targetDur:   &targetDur,
			ended:       &ended,
			pendingDur:  &pendingDur,
			pendingDisc: &pendingDisc,
			segments:    &segments,
			variants:    &variants,
		}); err != nil {
			return nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("hls: m3u8 scan: %w", err)
	}

	if !isMediaPL && len(variants) > 0 {
		return &fetchResult{variants: variants}, nil
	}

	return &fetchResult{media: &hlsMediaPlaylist{
		seqBase:        seqBase,
		targetDuration: targetDur,
		ended:          ended,
		segments:       segments,
	}}, nil
}

type parseState struct {
	seqBase     *uint64
	targetDur   *float64
	ended       *bool
	pendingDur  *float64
	pendingDisc *bool
	segments    *[]hlsSegment
	variants    *[]hlsVariant
}

// parseLine processes one M3U8 tag or URI line, updating parseState.
//
//nolint:unparam // error return reserved for future use
func parseLine(line string, base *url.URL, scanner *bufio.Scanner, st *parseState) error {
	switch {
	case strings.HasPrefix(line, m3u8TagMediaSeq):
		n, _ := strconv.ParseUint(strings.TrimPrefix(line, m3u8TagMediaSeq), 10, 64)
		*st.seqBase = n

	case strings.HasPrefix(line, m3u8TagTargetDur):
		d, _ := strconv.ParseFloat(strings.TrimPrefix(line, m3u8TagTargetDur), 64)
		*st.targetDur = d

	case line == m3u8TagEndList:
		*st.ended = true

	case line == m3u8TagDisconuity:
		*st.pendingDisc = true

	case strings.HasPrefix(line, m3u8TagExtInf):
		*st.pendingDur = parseINFDuration(strings.TrimPrefix(line, m3u8TagExtInf))

	case strings.HasPrefix(line, m3u8TagStreamInf):
		bw := parseBandwidthAttr(strings.TrimPrefix(line, m3u8TagStreamInf))
		if scanner.Scan() {
			uri := strings.TrimSpace(scanner.Text())
			if uri != "" && !strings.HasPrefix(uri, "#") {
				*st.variants = append(*st.variants, hlsVariant{
					uri:       resolveHLSURL(base, uri),
					bandwidth: bw,
				})
			}
		}

	case !strings.HasPrefix(line, "#") && *st.pendingDur > 0:
		seqNo := *st.seqBase + uint64(len(*st.segments))
		*st.segments = append(*st.segments, hlsSegment{
			uri:           resolveHLSURL(base, line),
			seq:           seqNo,
			duration:      *st.pendingDur,
			discontinuity: *st.pendingDisc,
		})
		*st.pendingDur = 0
		*st.pendingDisc = false
	}
	return nil
}

// isMediaTag returns true if the line signals a media (not master) playlist.
func isMediaTag(line string) bool {
	return strings.HasPrefix(line, m3u8TagTargetDur) ||
		strings.HasPrefix(line, m3u8TagMediaSeq) ||
		strings.HasPrefix(line, m3u8TagExtInf) ||
		line == m3u8TagEndList
}

// parseINFDuration extracts the float duration from the #EXTINF value string.
// Format: "duration,title" or "duration".
func parseINFDuration(val string) float64 {
	if idx := strings.IndexByte(val, ','); idx >= 0 {
		val = val[:idx]
	}
	d, _ := strconv.ParseFloat(strings.TrimSpace(val), 64)
	return d
}

// parseBandwidthAttr extracts the BANDWIDTH= integer from an attribute list.
func parseBandwidthAttr(attrs string) uint32 {
	for _, part := range strings.Split(attrs, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(strings.ToUpper(part), "BANDWIDTH=") {
			val := strings.SplitN(part, "=", 2)
			if len(val) == 2 {
				n, _ := strconv.ParseUint(strings.TrimSpace(val[1]), 10, 32)
				return uint32(n)
			}
		}
	}
	return 0
}

// pickBestVariant returns the URI of the variant with the highest bandwidth.
func pickBestVariant(variants []hlsVariant) string {
	var best string
	var bestBW uint32
	for _, v := range variants {
		if v.bandwidth >= bestBW {
			bestBW = v.bandwidth
			best = v.uri
		}
	}
	return best
}

// resolveHLSURL resolves ref against base, returning an absolute URL string.
func resolveHLSURL(base *url.URL, ref string) string {
	if base == nil {
		return ref
	}
	if strings.HasPrefix(ref, "http://") || strings.HasPrefix(ref, "https://") {
		return ref
	}
	refURL, err := url.Parse(ref)
	if err != nil {
		return ref
	}
	return base.ResolveReference(refURL).String()
}
