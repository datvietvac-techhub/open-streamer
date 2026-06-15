package pull

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// newTestHTTPTSReader builds a reader for the httptest servers (127.0.0.1)
// used below. Ingest no longer carries an SSRF dial guard, so any target is
// reachable.
func newTestHTTPTSReader(in domain.Input) *HTTPTSReader {
	return NewHTTPTSReader(in)
}

// HTTPTSReader streams whatever the server sends, in order, with no
// reframing. The test serves three TS-like packets back-to-back and verifies
// that successive Read calls return them concatenated (the reader doesn't
// re-frame at TS boundaries — that's the segmenter's job downstream).
func TestHTTPTSReader_StreamsBodyBytes(t *testing.T) {
	t.Parallel()

	pkts := [][]byte{
		buildESPacket(0x100, 0xAA),
		buildESPacket(0x100, 0xBB),
		buildESPacket(0x101, 0xCC),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for _, p := range pkts {
			_, _ = w.Write(p)
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer srv.Close()

	r := newTestHTTPTSReader(domain.Input{URL: srv.URL + "/streams/test/mpegts"})
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	// Drain bytes until we have at least the three packets' worth (= 3*188).
	got := make([]byte, 0, 3*188)
	deadline := time.Now().Add(2 * time.Second)
	for len(got) < 3*188 && time.Now().Before(deadline) {
		chunk, err := r.Read(context.Background())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, chunk...)
	}
	require.GreaterOrEqual(t, len(got), 3*188, "should have received three TS packets")

	// Marker bytes preserved at the right offsets.
	assert.Equal(t, byte(0xAA), got[4])
	assert.Equal(t, byte(0xBB), got[188+4])
	assert.Equal(t, byte(0xCC), got[2*188+4])
}

// Non-2xx response surfaces as an error from Open so the worker can decide
// whether to retry (5xx) or fail over (4xx) via shouldFailoverImmediately.
func TestHTTPTSReader_OpenReturnsHTTPStatusError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.NotFound(w, nil)
	}))
	defer srv.Close()

	r := newTestHTTPTSReader(domain.Input{URL: srv.URL + "/streams/missing/mpegts"})
	err := r.Open(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "404", "error must surface the HTTP status code")
}

// Close before Open is a no-op (for symmetry with the worker's deferred
// Close() pattern).
func TestHTTPTSReader_CloseWithoutOpen(t *testing.T) {
	t.Parallel()
	r := newTestHTTPTSReader(domain.Input{URL: "http://example.invalid/"})
	assert.NoError(t, r.Close())
}

// Headers configured on the input are forwarded with the request — used for
// auth tokens / custom routing headers when relaying through a CDN.
func TestHTTPTSReader_ForwardsCustomHeaders(t *testing.T) {
	t.Parallel()
	gotAuth := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotAuth <- req.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := newTestHTTPTSReader(domain.Input{
		URL:     srv.URL + "/streams/test/mpegts",
		Headers: map[string]string{"Authorization": "Bearer abc123"},
	})
	require.NoError(t, r.Open(context.Background()))
	defer r.Close()

	select {
	case auth := <-gotAuth:
		assert.Equal(t, "Bearer abc123", auth)
	case <-time.After(time.Second):
		t.Fatal("server never received the Authorization header")
	}
}

// Read after Close returns the natural body-closed error rather than
// panicking. Useful when Close races a pending Read goroutine.
func TestHTTPTSReader_ReadAfterCloseDoesNotPanic(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Hold the connection open briefly so Read has something to block on.
		w.WriteHeader(http.StatusOK)
		time.Sleep(50 * time.Millisecond)
	}))
	defer srv.Close()

	r := newTestHTTPTSReader(domain.Input{URL: srv.URL + "/streams/test/mpegts"})
	require.NoError(t, r.Open(context.Background()))
	require.NoError(t, r.Close())

	_, err := r.Read(context.Background())
	require.Error(t, err, "Read after Close must return an error")
}
