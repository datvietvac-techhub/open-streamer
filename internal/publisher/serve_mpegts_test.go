package publisher_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/publisher"
)

// End-to-end through the HTTP handler: client GETs /<code>/mpegts, receives
// raw TS bytes that were written into the buffer hub.
func TestHandleMPEGTS_StreamsBufferContent(t *testing.T) {
	t.Parallel()

	streamCode := domain.StreamCode("ch_test")
	buf := buffer.NewServiceForTesting(64)
	buf.Create(streamCode)

	bus := events.New(4, 64)
	pubCfg := config.PublisherConfig{
		HLS:  config.PublisherHLSConfig{Dir: t.TempDir()},
		DASH: config.PublisherDASHConfig{Dir: t.TempDir()},
	}
	pub := publisher.NewServiceForTesting(pubCfg, buf, bus)

	stream := &domain.Stream{
		Code:      streamCode,
		Protocols: &domain.OutputProtocols{MPEGTS: true},
	}
	require.NoError(t, pub.Start(context.Background(), stream))
	t.Cleanup(func() { pub.Stop(streamCode) })

	// Mount the handler on a chi router so {code} URL param is bound the
	// same way as in production.
	r := chi.NewRouter()
	r.Get("/{code}/mpegts", pub.HandleMPEGTS())
	srv := httptest.NewServer(r)
	defer srv.Close()

	// Producer: push a few raw TS chunks into the buffer hub.
	chunks := [][]byte{
		makeTSPayload(0xAA),
		makeTSPayload(0xBB),
		makeTSPayload(0xCC),
	}
	go func() {
		// Small delay so the client connection is established before the
		// first chunk is written; otherwise the writer's `select default:`
		// may drop early packets and the test becomes flaky.
		time.Sleep(50 * time.Millisecond)
		for _, c := range chunks {
			_ = buf.Write(streamCode, buffer.Packet{TS: c})
		}
	}()

	resp := mustGet(t, srv.URL+"/ch_test/mpegts")
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "video/mp2t", resp.Header.Get("Content-Type"))

	// Read until we have all three packets. Run the read in a goroutine
	// so the test can apply a wall-clock deadline; the handler keeps the
	// body open indefinitely, so a plain io.ReadFull would block forever.
	gotCh := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 0, 3*188)
		scratch := make([]byte, 4096)
		for len(buf) < 3*188 {
			n, err := resp.Body.Read(scratch)
			if n > 0 {
				buf = append(buf, scratch[:n]...)
			}
			if err != nil {
				break
			}
		}
		gotCh <- buf
	}()

	var got []byte
	select {
	case got = <-gotCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for TS bytes from handler")
	}
	require.GreaterOrEqual(t, len(got), 3*188, "should have received three TS packets")
	assert.Equal(t, byte(0xAA), got[4])
	assert.Equal(t, byte(0xBB), got[188+4])
	assert.Equal(t, byte(0xCC), got[2*188+4])
}

// Stream not running → 404. Catches misconfigured clients without leaking
// internal state about which streams exist.
func TestHandleMPEGTS_404WhenStreamNotRunning(t *testing.T) {
	t.Parallel()

	buf := buffer.NewServiceForTesting(64)
	bus := events.New(4, 64)
	pub := publisher.NewServiceForTesting(config.PublisherConfig{
		HLS:  config.PublisherHLSConfig{Dir: t.TempDir()},
		DASH: config.PublisherDASHConfig{Dir: t.TempDir()},
	}, buf, bus)

	r := chi.NewRouter()
	r.Get("/{code}/mpegts", pub.HandleMPEGTS())
	srv := httptest.NewServer(r)
	defer srv.Close()

	resp := mustGet(t, srv.URL+"/missing/mpegts")
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// Stream is running but MPEGTS opted out → 404. Same status code as the
// missing-stream case so external observers can't probe which streams have
// the protocol disabled vs which don't exist.
func TestHandleMPEGTS_404WhenProtocolDisabled(t *testing.T) {
	t.Parallel()

	streamCode := domain.StreamCode("ch_disabled")
	buf := buffer.NewServiceForTesting(64)
	buf.Create(streamCode)
	bus := events.New(4, 64)
	pub := publisher.NewServiceForTesting(config.PublisherConfig{
		HLS:  config.PublisherHLSConfig{Dir: t.TempDir()},
		DASH: config.PublisherDASHConfig{Dir: t.TempDir()},
	}, buf, bus)

	require.NoError(t, pub.Start(context.Background(), &domain.Stream{
		Code:      streamCode,
		Protocols: &domain.OutputProtocols{MPEGTS: false}, // explicitly off
	}))
	t.Cleanup(func() { pub.Stop(streamCode) })

	r := chi.NewRouter()
	r.Get("/{code}/mpegts", pub.HandleMPEGTS())
	srv := httptest.NewServer(r)
	defer srv.Close()

	resp := mustGet(t, srv.URL+"/ch_disabled/mpegts")
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// Client disconnect must not leak the subscriber on the server side. We
// can't directly observe the unsubscribe, but we can verify that a
// subsequent Stop succeeds (which would deadlock if the subscriber were
// still wedged on a full channel).
func TestHandleMPEGTS_ClientDisconnectReleasesSubscriber(t *testing.T) {
	t.Parallel()

	streamCode := domain.StreamCode("ch_disconnect")
	buf := buffer.NewServiceForTesting(64)
	buf.Create(streamCode)
	bus := events.New(4, 64)
	pub := publisher.NewServiceForTesting(config.PublisherConfig{
		HLS:  config.PublisherHLSConfig{Dir: t.TempDir()},
		DASH: config.PublisherDASHConfig{Dir: t.TempDir()},
	}, buf, bus)

	require.NoError(t, pub.Start(context.Background(), &domain.Stream{
		Code:      streamCode,
		Protocols: &domain.OutputProtocols{MPEGTS: true},
	}))

	r := chi.NewRouter()
	r.Get("/{code}/mpegts", pub.HandleMPEGTS())
	srv := httptest.NewServer(r)
	defer srv.Close()

	// Open then immediately close the client connection.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/ch_disconnect/mpegts", nil)
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	cancel()

	// Stop must succeed without hanging.
	done := make(chan struct{})
	go func() {
		pub.Stop(streamCode)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop deadlocked — subscriber was leaked")
	}
}

// TestHandleMPEGTS_PlaybackCapRejectsAndReleases verifies A-1 end-to-end: with
// a per-stream cap of 1, a second concurrent connection is rejected with 503,
// and once the first connection closes the freed slot lets a new one through
// (the deferred release in the handler ran).
func TestHandleMPEGTS_PlaybackCapRejectsAndReleases(t *testing.T) {
	t.Parallel()

	streamCode := domain.StreamCode("ch_cap")
	buf := buffer.NewServiceForTesting(64)
	buf.Create(streamCode)
	bus := events.New(4, 64)
	pub := publisher.NewServiceForTesting(config.PublisherConfig{
		HLS:                      config.PublisherHLSConfig{Dir: t.TempDir()},
		DASH:                     config.PublisherDASHConfig{Dir: t.TempDir()},
		MaxPlaybackConnPerStream: 1,
	}, buf, bus)
	require.NoError(t, pub.Start(context.Background(), &domain.Stream{
		Code:      streamCode,
		Protocols: &domain.OutputProtocols{MPEGTS: true},
	}))
	t.Cleanup(func() { pub.Stop(streamCode) })

	r := chi.NewRouter()
	r.Get("/{code}/mpegts", pub.HandleMPEGTS())
	srv := httptest.NewServer(r)
	defer srv.Close()
	url := srv.URL + "/ch_cap/mpegts"

	// conn1 occupies the only slot (acquire happens before the 200 header).
	ctx1, cancel1 := context.WithCancel(context.Background())
	req1, _ := http.NewRequestWithContext(ctx1, http.MethodGet, url, nil)
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp1.StatusCode)

	// conn2 is over the per-stream cap → 503, no slot held.
	resp2 := mustGet(t, url)
	assert.Equal(t, http.StatusServiceUnavailable, resp2.StatusCode)
	_ = resp2.Body.Close()

	// Release conn1; the deferred release frees the slot.
	cancel1()
	_, _ = io.Copy(io.Discard, resp1.Body)
	_ = resp1.Body.Close()

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel() // release the probe's own slot before the next tick
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		ok := resp.StatusCode == http.StatusOK
		_ = resp.Body.Close()
		return ok
	}, 2*time.Second, 50*time.Millisecond, "slot must free after conn1 closes")
}

// makeTSPayload returns one 188-byte TS-shaped packet whose 5th byte is the
// given marker. Used so the test can assert on packet contents flowing end-
// to-end without bringing in a full TS builder.
func makeTSPayload(marker byte) []byte {
	pkt := make([]byte, 188)
	pkt[0] = 0x47
	pkt[1] = 0x40 // PUSI=1, PID=0 (placeholder)
	pkt[2] = 0x00
	pkt[3] = 0x10 // AFC=01, CC=0
	pkt[4] = marker
	for i := 5; i < 188; i++ {
		pkt[i] = 0xCD
	}
	return pkt
}

// mustGet performs a context-aware GET so the lint rule (noctx) is satisfied
// and fails the test on transport errors. Caller still owns the response
// body and must Close it.
func mustGet(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}
