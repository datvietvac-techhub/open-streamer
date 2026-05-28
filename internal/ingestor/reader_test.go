package ingestor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/ingestor"
	"github.com/datvietvac-techhub/open-streamer/internal/vod"
)

var (
	testCfg = config.IngestorConfig{
		HLSMaxSegmentBuffer: 8,
	}
	// testVODs is a registry pre-loaded with one mount so file:// tests can
	// exercise the resolver path without poking the actual filesystem.
	testVODs = func() *vod.Registry {
		r := vod.NewRegistry()
		r.Sync([]*domain.VODMount{{Name: "tmp", Storage: "/tmp"}})
		return r
	}()
)

func TestNewPacketReader_PushListenURL_ReturnsError(t *testing.T) {
	t.Parallel()

	pushURLs := []string{
		"rtmp://0.0.0.0:1935/live/key",
		"rtmp://127.0.0.1:1935/live/key",
		"rtmp://[::]:1935/live/key",
		"srt://0.0.0.0:9999?streamid=key",
	}

	for _, u := range pushURLs {
		t.Run(u, func(t *testing.T) {
			t.Parallel()
			_, err := ingestor.NewPacketReader(domain.Input{URL: u}, testCfg, testVODs, nil, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "push-listen")
		})
	}
}

func TestNewPacketReader_UnknownScheme_ReturnsError(t *testing.T) {
	t.Parallel()

	unknownURLs := []string{
		"ftp://server/file",
		"ws://server/live",
		"",
		"relative/path",
	}

	for _, u := range unknownURLs {
		t.Run(u, func(t *testing.T) {
			t.Parallel()
			_, err := ingestor.NewPacketReader(domain.Input{URL: u}, testCfg, testVODs, nil, nil)
			require.Error(t, err)
		})
	}
}

func TestNewPacketReader_ValidPullURLs_ReturnsPacketReader(t *testing.T) {
	t.Parallel()

	// These URLs trigger reader construction only — no actual network I/O.
	// file:// URLs must reference a registered VOD mount; bare and empty-host
	// file:/// paths are intentionally not in this list because they are now rejected.
	validURLs := []string{
		"udp://239.1.1.1:5000",
		"http://cdn.example.com/playlist.m3u8",
		"https://cdn.example.com/playlist.m3u8",
		"srt://relay.example.com:9999",
		"rtmp://server.example.com/live/key",
		"rtsp://camera.local:554/stream",
		"file://tmp/source.ts",
	}

	for _, u := range validURLs {
		t.Run(u, func(t *testing.T) {
			t.Parallel()
			r, err := ingestor.NewPacketReader(domain.Input{URL: u}, testCfg, testVODs, nil, nil)
			require.NoError(t, err)
			assert.NotNil(t, r)
		})
	}
}

func TestNewPacketReader_FileURLRejectedWithoutMount(t *testing.T) {
	t.Parallel()
	emptyVODs := vod.NewRegistry()

	cases := []string{
		"file:///etc/passwd",      // empty host
		"file://unknown/clip.mp4", // mount not registered
	}
	for _, u := range cases {
		t.Run(u, func(t *testing.T) {
			t.Parallel()
			_, err := ingestor.NewPacketReader(domain.Input{URL: u}, testCfg, emptyVODs, nil, nil)
			require.Error(t, err)
		})
	}
}

func TestNewPacketReader_FileURLNeedsResolver(t *testing.T) {
	t.Parallel()
	_, err := ingestor.NewPacketReader(domain.Input{URL: "file://tmp/x.ts"}, testCfg, nil, nil, nil)
	require.Error(t, err)
}
