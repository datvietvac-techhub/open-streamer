package pull

// buffer_ts_chunk_test.go covers the buffer-hub TSChunkReader adapter
// used by CopyReader (and the planned MixerReader) for ABR upstream
// rendition demuxing.

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

const tsBufID domain.StreamCode = "abr-rendition-1"

func TestBufferTSChunkReader_OpenSubscribesToBuffer(t *testing.T) {
	t.Parallel()
	buf := buffer.NewServiceForTesting(8)
	buf.Create(tsBufID)
	r := newBufferTSChunkReader(buf, tsBufID)

	require.NoError(t, r.Open(context.Background()))
	require.NotNil(t, r.sub)
}

func TestBufferTSChunkReader_OpenAfterCloseFails(t *testing.T) {
	t.Parallel()
	buf := buffer.NewServiceForTesting(8)
	buf.Create(tsBufID)
	r := newBufferTSChunkReader(buf, tsBufID)
	require.NoError(t, r.Close()) // close before open
	err := r.Open(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open after close")
}

func TestBufferTSChunkReader_OpenUnknownBufferReturnsError(t *testing.T) {
	t.Parallel()
	buf := buffer.NewServiceForTesting(8)
	r := newBufferTSChunkReader(buf, "not-created")
	err := r.Open(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "subscribe")
}

func TestBufferTSChunkReader_ReadBeforeOpenFails(t *testing.T) {
	t.Parallel()
	buf := buffer.NewServiceForTesting(8)
	r := newBufferTSChunkReader(buf, tsBufID)
	_, err := r.Read(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read before open")
}

func TestBufferTSChunkReader_ReadSurfacesTSPayload(t *testing.T) {
	t.Parallel()
	buf := buffer.NewServiceForTesting(8)
	buf.Create(tsBufID)
	r := newBufferTSChunkReader(buf, tsBufID)
	require.NoError(t, r.Open(context.Background()))
	defer func() { _ = r.Close() }()

	go func() {
		// Small delay so the read blocks first.
		time.Sleep(10 * time.Millisecond)
		_ = buf.Write(tsBufID, buffer.Packet{TS: []byte{0x47, 0x40, 0x11}})
	}()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	got, err := r.Read(ctx)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x47, 0x40, 0x11}, got)
}

func TestBufferTSChunkReader_ReadSkipsAVOnlyPackets(t *testing.T) {
	t.Parallel()
	buf := buffer.NewServiceForTesting(8)
	buf.Create(tsBufID)
	r := newBufferTSChunkReader(buf, tsBufID)
	require.NoError(t, r.Open(context.Background()))
	defer func() { _ = r.Close() }()

	go func() {
		// First write is AV-only (no TS) — Read must SKIP and wait.
		_ = buf.Write(tsBufID, buffer.Packet{AV: &domain.AVPacket{}})
		time.Sleep(10 * time.Millisecond)
		_ = buf.Write(tsBufID, buffer.Packet{TS: []byte{0x47, 0x42, 0x12}})
	}()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	got, err := r.Read(ctx)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x47, 0x42, 0x12}, got, "AV-only packet should be skipped")
}

func TestBufferTSChunkReader_ReadHonoursContextCancellation(t *testing.T) {
	t.Parallel()
	buf := buffer.NewServiceForTesting(8)
	buf.Create(tsBufID)
	r := newBufferTSChunkReader(buf, tsBufID)
	require.NoError(t, r.Open(context.Background()))
	defer func() { _ = r.Close() }()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := r.Read(ctx)
	assert.True(t, errors.Is(err, context.Canceled))
}

func TestBufferTSChunkReader_ReadReturnsEOFWhenBufferDestroyed(t *testing.T) {
	t.Parallel()
	buf := buffer.NewServiceForTesting(8)
	buf.Create(tsBufID)
	r := newBufferTSChunkReader(buf, tsBufID)
	require.NoError(t, r.Open(context.Background()))

	go func() {
		time.Sleep(10 * time.Millisecond)
		// Tearing down the buffer closes every subscriber channel.
		buf.Delete(tsBufID)
	}()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := r.Read(ctx)
	assert.ErrorIs(t, err, io.EOF)
}

func TestBufferTSChunkReader_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	buf := buffer.NewServiceForTesting(8)
	buf.Create(tsBufID)
	r := newBufferTSChunkReader(buf, tsBufID)
	require.NoError(t, r.Open(context.Background()))

	require.NoError(t, r.Close())
	require.NoError(t, r.Close(), "second close must succeed")
}

// NewBufferTSDemuxReader composes the chunk reader with the TS demuxer.
// Verify the type's smoke-test constructor doesn't panic.
func TestNewBufferTSDemuxReader_Constructs(t *testing.T) {
	t.Parallel()
	buf := buffer.NewServiceForTesting(8)
	r := NewBufferTSDemuxReader(buf, tsBufID)
	require.NotNil(t, r)
}
