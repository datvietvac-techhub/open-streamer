package pull_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor/pull"
)

func TestUDPReader_ReceivesPackets(t *testing.T) {
	t.Parallel()

	// Bind the reader on an OS-assigned loopback port.
	r := pull.NewUDPReader(domain.Input{URL: "udp://127.0.0.1:0"})
	require.NoError(t, r.Open(context.Background()))
	defer func() { _ = r.Close() }()

	// Find out what port was actually bound.
	addr := r.LocalAddr()
	require.NotNil(t, addr, "UDPReader must expose LocalAddr for tests")

	// Payload must start with 0x47 (MPEG-TS sync byte) to pass processPacket.
	payload := append([]byte{0x47}, []byte("ts-packet-12345")...)
	go func() {
		time.Sleep(20 * time.Millisecond)
		d := net.Dialer{}
		conn, err := d.DialContext(context.Background(), "udp", addr.String())
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = conn.Write(payload)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	got, err := r.Read(ctx)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func TestUDPReader_ContextCancelled(t *testing.T) {
	t.Parallel()

	r := pull.NewUDPReader(domain.Input{URL: "udp://127.0.0.1:0"})
	require.NoError(t, r.Open(context.Background()))
	defer func() { _ = r.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := r.Read(ctx)
	require.Error(t, err)
}

func TestUDPReader_BadAddress_OpenFails(t *testing.T) {
	t.Parallel()

	// Port 1 is typically reserved and should fail to bind on most systems.
	r := pull.NewUDPReader(domain.Input{URL: "udp://256.256.256.256:5000"})
	err := r.Open(context.Background())
	require.Error(t, err)
}

func TestUDPReader_Close_Idempotent(t *testing.T) {
	t.Parallel()

	r := pull.NewUDPReader(domain.Input{URL: "udp://127.0.0.1:0"})
	require.NoError(t, r.Open(context.Background()))
	assert.NoError(t, r.Close())
	assert.NoError(t, r.Close()) // second close must not panic
}
