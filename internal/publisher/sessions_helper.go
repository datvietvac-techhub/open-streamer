package publisher

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	srt "github.com/datarhei/gosrt"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/sessions"
)

// touchThrottle caps how often the per-frame data-write path bumps the
// session's UpdatedAt timestamp via Closer.Touch. The idle reaper closes
// sessions whose UpdatedAt is older than `sessions.idle_timeout_sec`
// (default 30s), so refreshing every ~5s leaves a comfortable safety
// margin while keeping the tracker's mutex out of the per-frame hot path
// (RTMP / SRT routinely emit hundreds of frames per second).
const touchThrottle = 5 * time.Second

// playSession is the publisher-side adapter around sessions.Tracker. It hides
// the nil-tracker case (feature disabled) and exposes a per-session bytes
// counter the protocol-specific write loops increment after every successful
// outbound write. Close runs once and credits the final byte total to the
// session record before emitting EventSessionClosed.
type playSession struct {
	closer     sessions.Closer
	streamCode domain.StreamCode // set on creation; used by RTSP touch loop to filter
	bytes      atomic.Int64
	lastTouch  atomic.Int64 // unix-nano of last Closer.Touch — drives throttling
	disable    bool
}

// noopPlaySession is what we return when the tracker is unconfigured. add /
// close are cheap atomic ops; the path is hot enough (every TS chunk for
// SRT, every NAL for RTSP) that we don't want a nil check in the caller.
func noopPlaySession() *playSession { return &playSession{disable: true} }

func (p *playSession) add(n int64) {
	if p.disable || n <= 0 {
		return
	}
	p.bytes.Add(n)
	p.touch()
}

// touch refreshes the session's UpdatedAt via Closer.Touch and flushes
// any pending byte delta to the tracker, throttled so the underlying
// tracker mutex is not contended on every frame write. Connection-bound
// protocols (RTMP / SRT / RTSP / MPEGTS) MUST call this from their
// write path — without it (a) the idle reaper closes the session after
// `sessions.idle_timeout_sec` (default 30s) even when streaming is
// healthy, and (b) the API would show 0 bytes mid-stream because the
// per-frame add() only updates this side's atomic counter, not the
// tracker record.
//
// HLS / DASH don't use this path: their session records are refreshed by
// the HTTP middleware on every segment GET (see sessions.HTTPMiddleware).
func (p *playSession) touch() {
	if p.disable || p.closer == nil {
		return
	}
	now := time.Now().UnixNano()
	last := p.lastTouch.Load()
	if now-last < int64(touchThrottle) {
		return
	}
	// CompareAndSwap so concurrent writers (RTMP video + audio frames on
	// adjacent goroutines) elect a single Touch caller per throttle window
	// — without it both fire and the tracker mutex sees double the load.
	// The elected caller atomically swaps out the pending byte counter so
	// concurrent add()s after the swap accumulate into the next window
	// without being lost or double-credited.
	if p.lastTouch.CompareAndSwap(last, now) {
		delta := p.bytes.Swap(0)
		p.closer.Touch(delta)
	}
}

// close finalises the session, crediting any bytes that have not yet been
// flushed by touch(). Bytes is a Swap(0) so concurrent close + touch races
// (touch publishes V, close publishes 0; or vice-versa) leave a correct
// total in the tracker record.
func (p *playSession) close() {
	if p.disable || p.closer == nil {
		return
	}
	p.closer.Close(domain.SessionCloseClient, p.bytes.Swap(0))
}

// openSRTSession opens a tracker session for one SRT play subscriber. Streamid
// can carry a `?token=…` for operator-issued auth tokens — extracted via
// sessions.TokenFromQuery so the session is correctly tagged named_by="token".
func openSRTSession(ctx context.Context, t sessions.Tracker, code domain.StreamCode, conn srt.Conn) *playSession {
	if t == nil {
		return noopPlaySession()
	}
	streamID := conn.StreamId()
	token := ""
	if i := strings.IndexByte(streamID, '?'); i >= 0 {
		token = sessions.TokenFromQuery(streamID[i+1:])
	}
	_, c := t.OpenConn(ctx, sessions.ConnHit{
		StreamCode: code,
		Protocol:   domain.SessionProtoSRT,
		RemoteAddr: conn.RemoteAddr().String(),
		Token:      token,
	})
	return &playSession{closer: c, streamCode: code}
}

// openRTMPSession opens a tracker session for an external RTMP play client.
// The remote addr / flashVer come from the RTMP server's per-conn metadata
// captured at OnPlay handshake time.
func openRTMPSession(ctx context.Context, t sessions.Tracker, code domain.StreamCode, remoteAddr, flashVer string) *playSession {
	if t == nil {
		return noopPlaySession()
	}
	_, c := t.OpenConn(ctx, sessions.ConnHit{
		StreamCode: code,
		Protocol:   domain.SessionProtoRTMP,
		RemoteAddr: remoteAddr,
		UserAgent:  flashVer,
	})
	return &playSession{closer: c, streamCode: code}
}

// openRTSPSession opens a tracker session for one RTSP client (one ServerSession
// from gortsplib). UserAgent is the RTSP "User-Agent" request header.
func openRTSPSession(ctx context.Context, t sessions.Tracker, code domain.StreamCode, remote net.Addr, userAgent string) *playSession {
	if t == nil {
		return noopPlaySession()
	}
	_, c := t.OpenConn(ctx, sessions.ConnHit{
		StreamCode: code,
		Protocol:   domain.SessionProtoRTSP,
		RemoteAddr: remote.String(),
		UserAgent:  userAgent,
	})
	return &playSession{closer: c, streamCode: code}
}

// openMPEGTSSession opens a tracker session for one HTTP MPEG-TS pull
// client. The transport is HTTP but the connection is long-lived (one GET
// streams forever), so we use OpenConn (UUID-based, like RTMP/SRT) rather
// than the per-segment fingerprint path that HLS / DASH use — there is no
// concept of "same viewer across segment requests" here, every disconnect
// is a real session boundary.
//
// Token is sniffed from the request query so operator-issued auth tokens
// surface as named_by="token" in the dashboard, matching what HLS / DASH
// do for the same query parameter.
func openMPEGTSSession(ctx context.Context, t sessions.Tracker, code domain.StreamCode, r *http.Request) *playSession {
	if t == nil {
		return noopPlaySession()
	}
	token := strings.TrimSpace(r.URL.Query().Get("token"))
	_, c := t.OpenConn(ctx, sessions.ConnHit{
		StreamCode: code,
		Protocol:   domain.SessionProtoMPEGTS,
		RemoteAddr: r.RemoteAddr,
		UserAgent:  r.Header.Get("User-Agent"),
		Token:      token,
		Secure:     r.TLS != nil,
	})
	return &playSession{closer: c, streamCode: code}
}
