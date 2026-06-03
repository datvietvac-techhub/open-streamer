package pull_test

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/ingestor/pull"
)

// ReadCloser is a minimal interface matching pull readers for test helpers.
type ReadCloser interface {
	Read(ctx context.Context) ([]byte, error)
	Close() error
}

func TestFileReader_ReadsFile(t *testing.T) {
	t.Parallel()

	want := []byte("hello open-streamer file content")
	f := writeTempFile(t, want)

	r := pull.NewFileReader(f, false)
	require.NoError(t, r.Open(context.Background()))
	defer func() { _ = r.Close() }()

	got := readAll(t, r)
	assert.Equal(t, want, got)
}

func TestFileReader_Open_NotFound(t *testing.T) {
	t.Parallel()

	r := pull.NewFileReader("/nonexistent/path/file.ts", false)
	err := r.Open(context.Background())
	require.Error(t, err)
}

func TestFileReader_Close_IdempotentBeforeOpen(t *testing.T) {
	t.Parallel()

	r := pull.NewFileReader("/tmp/doesnt-matter", false)
	assert.NoError(t, r.Close())
}

func TestFileReader_ReturnsEOFAtEnd(t *testing.T) {
	t.Parallel()

	f := writeTempFile(t, []byte("data"))
	r := pull.NewFileReader(f, false)
	require.NoError(t, r.Open(context.Background()))
	defer func() { _ = r.Close() }()

	// Drain all data.
	for {
		_, err := r.Read(context.Background())
		if err == io.EOF {
			return
		}
		require.NoError(t, err)
	}
}

// ---- helpers ----

func writeTempFile(t *testing.T, data []byte) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "filetest-*.ts")
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

// readAll drains a Reader until io.EOF, returning concatenated bytes.
func readAll(t *testing.T, r ReadCloser) []byte {
	t.Helper()
	var out []byte
	for {
		chunk, err := r.Read(context.Background())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		out = append(out, chunk...)
	}
	return out
}
