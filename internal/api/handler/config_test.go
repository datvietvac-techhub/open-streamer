package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

const (
	configPath      = "/config"
	configStatusFmt = "status=%d"
)

type fakeRuntimeManager struct {
	cfg      *domain.GlobalConfig
	applyErr error
	applied  *domain.GlobalConfig
}

func (f *fakeRuntimeManager) CurrentConfig() *domain.GlobalConfig { return f.cfg }
func (f *fakeRuntimeManager) Apply(_ context.Context, c *domain.GlobalConfig) error {
	if f.applyErr != nil {
		return f.applyErr
	}
	f.applied = c
	f.cfg = c
	return nil
}

func TestPortsFromConfigEmpty(t *testing.T) {
	p := portsFromConfig(&domain.GlobalConfig{})
	if p.HTTPAddr != "" || p.RTSPPort != 0 || p.RTMPPort != 0 || p.SRTPort != 0 {
		t.Errorf("empty config should yield zero values, got %+v", p)
	}
}

func TestPortsFromConfigPopulated(t *testing.T) {
	gcfg := &domain.GlobalConfig{
		Server: &config.ServerConfig{HTTPAddr: ":8080"},
		Listeners: &config.ListenersConfig{
			RTSP: config.RTSPListenerConfig{Enabled: true, Port: 18554},
			RTMP: config.RTMPListenerConfig{Enabled: true, Port: 1936},
			SRT:  config.SRTListenerConfig{Enabled: true, Port: 10000},
		},
	}
	p := portsFromConfig(gcfg)
	if p.HTTPAddr != ":8080" || p.RTSPPort != 18554 || p.RTMPPort != 1936 || p.SRTPort != 10000 {
		t.Errorf("ports not propagated: %+v", p)
	}
}

func TestPortsFromConfigPartial(t *testing.T) {
	gcfg := &domain.GlobalConfig{
		Server: &config.ServerConfig{HTTPAddr: ":80"},
	}
	p := portsFromConfig(gcfg)
	if p.HTTPAddr != ":80" || p.RTSPPort != 0 {
		t.Errorf("partial config wrong: %+v", p)
	}
}

func TestGetConfigShape(t *testing.T) {
	rtm := &fakeRuntimeManager{cfg: &domain.GlobalConfig{
		Server: &config.ServerConfig{HTTPAddr: ":9000"},
	}}
	h := &ConfigHandler{rtm: rtm}

	w := httptest.NewRecorder()
	h.GetConfig(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, configPath, nil))
	if w.Code != http.StatusOK {
		t.Fatalf(configStatusFmt, w.Code)
	}
	var got configResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("body not JSON: %v", err)
	}
	if got.GlobalConfig == nil || got.GlobalConfig.Server.HTTPAddr != ":9000" {
		t.Error("global config not echoed back")
	}
	if len(got.VideoCodecs) == 0 || len(got.AudioCodecs) == 0 || len(got.OutputProtocols) == 0 {
		t.Error("static enum lists must be populated")
	}
	if got.Ports.HTTPAddr != ":9000" {
		t.Errorf("ports not derived from config: %+v", got.Ports)
	}
}

func TestUpdateConfigMergesAndApplies(t *testing.T) {
	rtm := &fakeRuntimeManager{cfg: &domain.GlobalConfig{
		Server: &config.ServerConfig{HTTPAddr: ":8080"},
	}}
	h := &ConfigHandler{rtm: rtm}

	// Body changes only the HTTP addr.
	body, _ := json.Marshal(map[string]any{
		"server": map[string]any{"http_addr": ":9999"},
	})
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, configPath, bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.UpdateConfig(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if rtm.applied == nil || rtm.applied.Server.HTTPAddr != ":9999" {
		t.Errorf("apply not called with merged config: %+v", rtm.applied)
	}
}

func TestUpdateConfigInvalidJSON(t *testing.T) {
	rtm := &fakeRuntimeManager{cfg: &domain.GlobalConfig{}}
	h := &ConfigHandler{rtm: rtm}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, configPath, bytes.NewReader([]byte("{bad")))
	w := httptest.NewRecorder()
	h.UpdateConfig(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf(configStatusFmt, w.Code)
	}
}

func TestUpdateConfigApplyError(t *testing.T) {
	rtm := &fakeRuntimeManager{
		cfg:      &domain.GlobalConfig{},
		applyErr: errors.New("apply boom"),
	}
	h := &ConfigHandler{rtm: rtm}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, configPath, bytes.NewReader([]byte("{}")))
	w := httptest.NewRecorder()
	h.UpdateConfig(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf(configStatusFmt, w.Code)
	}
}

func TestSetRuntimeManager(t *testing.T) {
	h := &ConfigHandler{}
	rtm := &fakeRuntimeManager{}
	h.SetRuntimeManager(rtm)
	if h.rtm != rtm {
		t.Error("SetRuntimeManager did not assign")
	}
}
