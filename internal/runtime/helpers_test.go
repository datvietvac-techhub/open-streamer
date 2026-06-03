package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// Pure helpers — no goroutines, no service deps. Any regression here is a
// dispatch bug that would silently break listener lifecycle, so they're
// worth covering exhaustively.

func TestRTMPListenerEnabled(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   *config.ListenersConfig
		want bool
	}{
		{name: "nil → false", in: nil, want: false},
		{name: "disabled → false", in: &config.ListenersConfig{
			RTMP: config.RTMPListenerConfig{Enabled: false, Port: 1935},
		}, want: false},
		{name: "enabled but port 0 → false", in: &config.ListenersConfig{
			RTMP: config.RTMPListenerConfig{Enabled: true, Port: 0},
		}, want: false},
		{name: "enabled + port → true", in: &config.ListenersConfig{
			RTMP: config.RTMPListenerConfig{Enabled: true, Port: 1935},
		}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, rtmpListenerEnabled(tc.in))
		})
	}
}

func TestRTSPListenerEnabled(t *testing.T) {
	t.Parallel()
	assert.False(t, rtspListenerEnabled(nil))
	assert.False(t, rtspListenerEnabled(&config.ListenersConfig{
		RTSP: config.RTSPListenerConfig{Enabled: false, Port: 554},
	}))
	assert.False(t, rtspListenerEnabled(&config.ListenersConfig{
		RTSP: config.RTSPListenerConfig{Enabled: true, Port: 0},
	}))
	assert.True(t, rtspListenerEnabled(&config.ListenersConfig{
		RTSP: config.RTSPListenerConfig{Enabled: true, Port: 554},
	}))
}

func TestSRTListenerEnabled(t *testing.T) {
	t.Parallel()
	assert.False(t, srtListenerEnabled(nil))
	assert.False(t, srtListenerEnabled(&config.ListenersConfig{
		SRT: config.SRTListenerConfig{Enabled: false, Port: 9999},
	}))
	assert.False(t, srtListenerEnabled(&config.ListenersConfig{
		SRT: config.SRTListenerConfig{Enabled: true, Port: 0},
	}))
	assert.True(t, srtListenerEnabled(&config.ListenersConfig{
		SRT: config.SRTListenerConfig{Enabled: true, Port: 9999},
	}))
}

// rtmpListenerOf / rtspListenerOf / srtListenerOf return the concrete
// sub-config for diff comparison. They must return nil when the parent
// is nil (so configChanged's nil branch fires) and a non-aliased pointer
// when present (so callers can mutate without leaking back).
func TestListenerOf(t *testing.T) {
	t.Parallel()
	assert.Nil(t, rtmpListenerOf(nil))
	assert.Nil(t, rtspListenerOf(nil))
	assert.Nil(t, srtListenerOf(nil))

	full := &config.ListenersConfig{
		RTMP: config.RTMPListenerConfig{Enabled: true, Port: 1935},
		RTSP: config.RTSPListenerConfig{Enabled: true, Port: 554},
		SRT:  config.SRTListenerConfig{Enabled: true, Port: 9999},
	}
	assert.Equal(t, full.RTMP, *rtmpListenerOf(full))
	assert.Equal(t, full.RTSP, *rtspListenerOf(full))
	assert.Equal(t, full.SRT, *srtListenerOf(full))
}

// configChanged is the diff primitive for every section. It must report
// "changed" only when values genuinely differ, treating nil↔nil as no-op
// and nil↔non-nil as a transition (otherwise services flap on first
// boot when a section is added without other content).
func TestConfigChanged(t *testing.T) {
	t.Parallel()
	type sample struct{ A int }

	assert.False(t, configChanged(nil, nil), "nil↔nil → unchanged")
	assert.True(t, configChanged(nil, &sample{A: 1}), "nil → set → changed")
	assert.True(t, configChanged(&sample{A: 1}, nil), "set → nil → changed")
	assert.False(t, configChanged(&sample{A: 1}, &sample{A: 1}), "equal value → unchanged")
	assert.True(t, configChanged(&sample{A: 1}, &sample{A: 2}), "different value → changed")
}

// Constructor + simple getter. New must populate every field; CurrentConfig
// must return whatever was most recently committed via the internal pointer.
// We don't drive BootstrapWith here because it fans out to every service
// — that's covered separately with a real DI-wired Manager.
func TestNewAndCurrentConfig(t *testing.T) {
	t.Parallel()
	m := New(t.Context(), Deps{})
	assert.NotNil(t, m)
	assert.NotNil(t, m.services)
	assert.Nil(t, m.CurrentConfig(), "no config set yet → nil")

	cfg := &domain.GlobalConfig{}
	m.mu.Lock()
	m.current = cfg
	m.mu.Unlock()
	assert.Same(t, cfg, m.CurrentConfig(), "CurrentConfig returns the stored pointer")
}
