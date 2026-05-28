package pull

// udp.go — UDP MPEG-TS ingestor (pull mode, listen socket).
//
// Listens on a UDP port, reads raw MPEG-TS datagrams, and forwards them to
// TSDemuxPacketReader. A background pump goroutine reads from the socket and
// sends validated datagrams to a buffered channel; Read() never spawns a
// goroutine per call, giving fast, cheap context cancellation.
//
// # Multicast support
//
// When the bind address is an IPv4 or IPv6 multicast group, the socket
// automatically issues an IGMP / MLD join using golang.org/x/net/ipv4 and
// golang.org/x/net/ipv6.  Source-Specific Multicast (SSM, RFC 4607) is also
// supported for IPv4 via ?source=<src-ip>.
//
// # RTP stripping
//
// Many broadcast encoders wrap MPEG-TS inside RTP (RFC 2250). If a datagram
// does NOT start with the MPEG-TS sync byte (0x47) but its first byte looks
// like an RTP version-2 header, the 12-byte (+ optional CSRC/extension) RTP
// header is stripped automatically. Set ?rtp=0 in the URL to disable.
//
// # URL formats
//
//	udp://0.0.0.0:5000                     — unicast, bind all interfaces
//	udp://239.1.1.1:5000                   — IPv4 multicast, any source
//	udp://[ff02::1]:5000                   — IPv6 multicast
//	udp://eth0@239.1.1.1:5000              — multicast on eth0 (FFmpeg syntax)
//	udp://239.1.1.1:5000?iface=eth0        — multicast on specific interface
//	udp://239.1.1.1:5000?source=10.0.0.1   — IPv4 source-specific multicast
//	udp://host:5000?rtp=0                  — disable RTP auto-stripping
//	udp://host:5000?pkt_size=65536         — override max datagram buffer size
//	udp://host:5000?fifo_size=50000000     — OS recv buffer (50 MB); aliases: buffer_size, recv_buffer_size
//
// # Lifecycle for worker reconnection
//
// Open → (Read loop) → Close → Open → … is explicitly supported; each Open()
// creates a fresh socket and pump goroutine; Close() tears them down and waits.

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

const (
	udpDefaultPktSize = 188 * 7                // 1316 bytes — typical MPEG-TS datagram
	udpMaxPktSize     = 65536                  // maximum useful UDP payload
	udpDefaultChanBuf = 1024                   // datagrams buffered between pump and Read; ~88 ms at 120 Mbps with 1316-byte datagrams. Tunable via ?chan_buf=
	udpDefaultOSBuf   = 16 * 1024 * 1024       // 16 MiB OS receive buffer; tunable via ?fifo_size= / ?buffer_size= / ?recv_buffer_size=. Kernel may cap below this — operator must raise net.core.rmem_max for the request to take full effect.
	udpPollDeadline   = 400 * time.Millisecond // deadline for done-check polling
)

// udpOpts holds parsed URL query parameters for UDPReader.
type udpOpts struct {
	pktSize int    // max UDP read buffer size
	osBuf   int    // OS receive buffer size (SetReadBuffer); aliased from fifo_size / buffer_size / recv_buffer_size
	chanBuf int    // datagrams buffered between pump and Read goroutines
	iface   string // network interface name for multicast join
	source  string // SSM source address (empty = any-source multicast)
	rtpAuto bool   // true = auto-detect and strip RTP headers
}

// UDPReader listens on a UDP port and emits raw MPEG-TS chunks.
// It implements TSChunkReader and is intended to be wrapped with
// NewTSDemuxPacketReader.
type UDPReader struct {
	input domain.Input
	opts  udpOpts

	conn   *net.UDPConn
	chunks chan []byte
	done   chan struct{}
	wg     sync.WaitGroup

	// pumpErr holds the first non-timeout error from the pump goroutine.
	// It is written by pump() before close(r.chunks) and read by Read() after
	// the channel close — the channel-close happens-before ordering makes this
	// access race-free without a mutex.
	// Reset to nil on each Open() call so reconnection cycles start clean.
	pumpErr error
}

// NewUDPReader constructs a UDPReader for the given input without opening a
// connection.
func NewUDPReader(input domain.Input) *UDPReader {
	return &UDPReader{
		input: input,
		opts:  parseUDPOpts(input.URL),
	}
}

// Open binds the UDP socket, optionally joins a multicast group, and starts the
// background pump goroutine.
//
// Each Open() call creates a fresh socket so the reader can be reused after
// Close() by the ingestor reconnection loop.
func (r *UDPReader) Open(_ context.Context) error {
	r.pumpErr = nil // reset from any previous lifecycle
	u, err := url.Parse(r.input.URL)
	if err != nil {
		return fmt.Errorf("udp: parse url %q: %w", r.input.URL, err)
	}

	addr, err := net.ResolveUDPAddr("udp", u.Host)
	if err != nil {
		return fmt.Errorf("udp: resolve %q: %w", u.Host, err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("udp: listen %q: %w", u.Host, err)
	}
	// On Linux this prefers SO_RCVBUFFORCE (bypasses net.core.rmem_max when
	// the process has CAP_NET_ADMIN — root does) and falls back to plain
	// SO_RCVBUF otherwise. Matches production media servers that
	// don't require operators to raise rmem_max for high-bitrate multicast.
	// Non-Linux builds use plain SetReadBuffer.
	setUDPRecvBuffer(conn, r.opts.osBuf)

	// Restrict multicast delivery to groups this socket actually joined.
	// Go's net.ListenUDP rewrites a multicast bind to 0.0.0.0; on Linux,
	// wildcard sockets default to IP_MULTICAST_ALL=1, which delivers EVERY
	// multicast packet joined by ANY socket on the host (e.g. when another
	// media server is co-located on the same box and has joined other
	// groups). Disable it so kernel filtering matches our explicit IGMP
	// memberships. No-op on non-Linux. See udp_join_*.go for details.
	disableIPMulticastAll(conn)

	if err := r.joinMulticast(conn, addr); err != nil {
		_ = conn.Close()
		return fmt.Errorf("udp: multicast join %s: %w", addr.IP, err)
	}

	r.conn = conn
	r.chunks = make(chan []byte, r.opts.chanBuf)
	r.done = make(chan struct{})
	r.wg.Add(1)
	go r.pump()
	return nil
}

// Read returns the next validated MPEG-TS datagram (with any RTP header stripped).
//
// When the pump goroutine exits due to a real network error, Read returns that
// error (not io.EOF) so the upstream pumpChunks goroutine can store it in
// TSDemuxPacketReader.readErr and the worker will reconnect instead of triggering
// permanent failover.
//
// Returns (nil, io.EOF) only when Close() is called cleanly without a pump error.
// Returns (nil, ctx.Err()) when the caller's context is cancelled.
func (r *UDPReader) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case chunk, ok := <-r.chunks:
		if !ok {
			// The pump goroutine exited.  If it recorded a real error (network
			// failure, unexpected OS error) propagate it so the caller can
			// distinguish this from a clean shutdown or natural stream end.
			if r.pumpErr != nil {
				return nil, r.pumpErr
			}
			return nil, io.EOF
		}
		return chunk, nil
	}
}

// Close stops the pump goroutine, closes the UDP socket, and waits for clean
// shutdown. Safe to call before Open or multiple times.
//
// Lifetime ordering matters under -race: pump() reads r.conn for SetReadDeadline
// and ReadFromUDP, so r.conn = nil must NOT happen until wg.Wait() has returned.
// The same applies to r.done / r.chunks — pump may still be in flight when
// Close starts. We close the socket (which unblocks pump's syscall), wait
// for pump to exit, and only then null out the shared fields.
func (r *UDPReader) Close() error {
	if r.done != nil {
		select {
		case <-r.done:
		default:
			close(r.done)
		}
	}
	var err error
	if r.conn != nil {
		err = r.conn.Close()
	}
	r.wg.Wait()
	// Pump has exited — safe to drop references now.
	r.conn = nil
	r.done = nil
	r.chunks = nil
	return err
}

// LocalAddr returns the local address the socket is bound to.
// Returns nil if the socket has not been opened yet.
func (r *UDPReader) LocalAddr() net.Addr {
	if r.conn == nil {
		return nil
	}
	return r.conn.LocalAddr()
}

// ─── Internal pump goroutine ────────────────────────────────────────────────

// pump is the background goroutine that reads UDP datagrams from the socket
// and sends them to r.chunks.
//
// A short read deadline is set each iteration so the goroutine can check
// r.done without blocking indefinitely — no extra goroutine is spawned.
//
// Slow consumers: if r.chunks is full a datagram is silently dropped rather
// than blocking the pump (which would stall the OS receive buffer).
func (r *UDPReader) pump() {
	defer r.wg.Done()
	defer close(r.chunks)

	buf := make([]byte, r.opts.pktSize)
	for {
		// Fast-path: check for shutdown before blocking on the socket.
		select {
		case <-r.done:
			return
		default:
		}

		_ = r.conn.SetReadDeadline(time.Now().Add(udpPollDeadline))
		n, _, err := r.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // deadline expired; loop back and check r.done
			}
			// Real network error (connection reset, socket closed by Close(), …).
			// Store it so Read() can return it instead of a generic io.EOF, giving
			// the ingestor worker a chance to reconnect rather than trigger failover.
			r.pumpErr = err
			return
		}
		if n == 0 {
			continue
		}

		pkt := r.processPacket(buf[:n])
		if pkt == nil {
			continue // not a recognisable MPEG-TS payload
		}

		cp := make([]byte, len(pkt))
		copy(cp, pkt)

		select {
		case r.chunks <- cp:
		case <-r.done:
			return
		default:
			// Drop: consumer is too slow. Packet loss is preferable to
			// stalling the pump and growing the OS receive buffer backlog.
		}
	}
}

// processPacket returns the MPEG-TS payload from a UDP datagram, or nil if the
// datagram should be discarded.
//
//   - Datagrams starting with 0x47 are raw MPEG-TS and returned as-is.
//   - With rtpAuto=true, datagrams that look like RTP version-2 packets
//     have their header stripped; the remainder must start with 0x47.
//   - Everything else is silently discarded.
func (r *UDPReader) processPacket(pkt []byte) []byte {
	if len(pkt) == 0 {
		return nil
	}
	if pkt[0] == 0x47 {
		return pkt // already raw MPEG-TS
	}
	if r.opts.rtpAuto {
		return stripRTPHeader(pkt) // returns nil if not valid RTP/MPEG-TS
	}
	return nil
}

// stripRTPHeader strips the RTP header from pkt and returns the MPEG-TS payload.
// Returns nil if pkt does not look like an RTP/MPEG-TS datagram.
//
// Header layout (RFC 3550):
//
//	0         1         2         3
//	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|V=2|P|X|  CC   |M|     PT      |       Sequence Number         |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                           Timestamp                           |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|           Synchronization Source (SSRC) identifier            |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|              Contributing Source (CSRC) identifiers …         |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	| Optional extension header …                                   |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	| MPEG-TS payload (0x47 …)                                      |
func stripRTPHeader(pkt []byte) []byte {
	if len(pkt) < 12 {
		return nil
	}
	// Version field must be 2 (top 2 bits of first byte = 0b10).
	if pkt[0]&0xC0 != 0x80 {
		return nil
	}

	// Fixed header: 12 bytes.
	// CSRC list: CC (lower 4 bits of byte 0) × 4 bytes.
	hdrLen := 12 + int(pkt[0]&0x0F)*4

	// Optional header extension (bit 4 of byte 0 = X bit).
	if pkt[0]&0x10 != 0 {
		if len(pkt) < hdrLen+4 {
			return nil
		}
		// Extension length field is the number of 32-bit words that follow
		// the 4-byte extension header.
		extWords := int(pkt[hdrLen+2])<<8 | int(pkt[hdrLen+3])
		hdrLen += 4 + extWords*4
	}

	if hdrLen >= len(pkt) {
		return nil
	}
	payload := pkt[hdrLen:]
	if payload[0] != 0x47 {
		return nil // payload is not MPEG-TS
	}
	return payload
}

// ─── Multicast join ─────────────────────────────────────────────────────────

// joinMulticast joins the multicast group encoded in addr when addr.IP is a
// multicast address.  No-op for unicast addresses.
//
// IPv4 ASM with an explicit interface routes through joinIPv4ASMByIfaceName
// — that helper avoids `net.InterfaceByName` (which itself enumerates
// interfaces via netlink and fails on hosts with IPv6 disabled via sysctl)
// and the x/net/ipv4 join path. Other paths (SSM, IPv6, no-iface) continue
// to use the standard library.
func (r *UDPReader) joinMulticast(conn *net.UDPConn, addr *net.UDPAddr) error {
	if !addr.IP.IsMulticast() {
		return nil
	}

	if addr.IP.To4() != nil {
		if r.opts.source == "" && r.opts.iface != "" {
			// Plain ASM with explicit interface — fast path that bypasses
			// netlink entirely on Linux.
			return joinIPv4ASMByIfaceName(conn, addr.IP, r.opts.iface)
		}
		// SSM, or no interface specified, or non-Linux — go through stdlib.
		var iface *net.Interface
		if r.opts.iface != "" {
			var err error
			iface, err = net.InterfaceByName(r.opts.iface)
			if err != nil {
				return fmt.Errorf("interface %q: %w", r.opts.iface, err)
			}
		}
		return r.joinIPv4(conn, addr, iface)
	}

	var iface *net.Interface
	if r.opts.iface != "" {
		var err error
		iface, err = net.InterfaceByName(r.opts.iface)
		if err != nil {
			return fmt.Errorf("interface %q: %w", r.opts.iface, err)
		}
	}
	return r.joinIPv6(conn, addr, iface)
}

// joinIPv4 handles paths NOT covered by the joinMulticast fast-path:
//   - SSM (?source=): always routed through golang.org/x/net/ipv4.
//   - Plain ASM with no interface: nil iface → x/net/ipv4 doesn't touch netlink.
//
// The "ASM + explicit iface" combination is handled directly in joinMulticast
// via joinIPv4ASMByIfaceName so it never reaches this function.
func (r *UDPReader) joinIPv4(conn *net.UDPConn, group *net.UDPAddr, iface *net.Interface) error {
	pc := ipv4.NewPacketConn(conn)
	if r.opts.source != "" {
		src := net.ParseIP(r.opts.source)
		if src == nil {
			return fmt.Errorf("invalid SSM source address %q", r.opts.source)
		}
		return pc.JoinSourceSpecificGroup(iface, &net.UDPAddr{IP: group.IP}, &net.UDPAddr{IP: src})
	}
	return pc.JoinGroup(iface, &net.UDPAddr{IP: group.IP})
}

// joinIPv6 issues an MLD join for the given group on conn.
func (r *UDPReader) joinIPv6(conn *net.UDPConn, group *net.UDPAddr, iface *net.Interface) error {
	return ipv6.NewPacketConn(conn).JoinGroup(iface, &net.UDPAddr{IP: group.IP})
}

// ─── URL option parsing ──────────────────────────────────────────────────────

// parseUDPOpts extracts recognised query parameters from rawURL.
// Unknown parameters are silently ignored.
//
// FFmpeg-compatible aliases:
//   - userinfo `iface@host:port` is parsed as the multicast interface name
//     (overridden by explicit ?iface=)
//   - ?fifo_size=, ?buffer_size=, ?recv_buffer_size= all set the OS recv
//     buffer (SetReadBuffer); operator-side net.core.rmem_max may cap.
func parseUDPOpts(rawURL string) udpOpts {
	opts := udpOpts{
		pktSize: udpDefaultPktSize,
		osBuf:   udpDefaultOSBuf,
		chanBuf: udpDefaultChanBuf,
		rtpAuto: true,
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return opts
	}
	// FFmpeg syntax: udp://<iface>@<host>:<port>. Use as default; explicit
	// ?iface= still wins.
	if u.User != nil {
		if name := u.User.Username(); name != "" {
			opts.iface = name
		}
	}
	q := u.Query()
	if v := q.Get("iface"); v != "" {
		opts.iface = v
	}
	if v := q.Get("source"); v != "" {
		opts.source = v
	}
	if n, ok := positiveInt(q.Get("pkt_size")); ok && n <= udpMaxPktSize {
		opts.pktSize = n
	}
	if v := q.Get("rtp"); v == "0" || v == "false" {
		opts.rtpAuto = false
	}
	for _, k := range []string{"fifo_size", "buffer_size", "recv_buffer_size"} {
		if n, ok := positiveInt(q.Get(k)); ok {
			opts.osBuf = n
			break
		}
	}
	if n, ok := positiveInt(q.Get("chan_buf")); ok {
		opts.chanBuf = n
	}
	return opts
}

// positiveInt parses s as a strictly-positive int. Empty / unparseable /
// non-positive values return ok=false so the caller keeps its default.
func positiveInt(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}
