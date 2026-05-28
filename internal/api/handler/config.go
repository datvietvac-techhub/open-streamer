package handler

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/datvietvac-techhub/open-streamer/internal/autopublish"
	"github.com/datvietvac-techhub/open-streamer/internal/coordinator"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/hwdetect"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
	"github.com/datvietvac-techhub/open-streamer/internal/vod"
	"github.com/datvietvac-techhub/open-streamer/pkg/version"
	"github.com/samber/do/v2"
)

// RuntimeConfigManager is the subset of runtime.Manager that the config handler needs.
type RuntimeConfigManager interface {
	CurrentConfig() *domain.GlobalConfig
	Apply(ctx context.Context, newCfg *domain.GlobalConfig) error
}

// streamLifecycle is the subset of *coordinator.Coordinator needed to apply
// stream changes from the full-config YAML editor. Defined as an interface so
// the diff/sync flow can be unit-tested without spinning up the real pipeline.
type streamLifecycle interface {
	Start(ctx context.Context, stream *domain.Stream) error
	Stop(ctx context.Context, code domain.StreamCode)
	Update(ctx context.Context, old, new *domain.Stream) error
	IsRunning(code domain.StreamCode) bool
}

// publisherPorts exposes listener port configuration so the UI can build output URLs.
// All RTMP/RTSP/SRT traffic shares one port per protocol with both ingest and play.
type publisherPorts struct {
	HTTPAddr string `json:"http_addr"` // e.g. ":8080"
	RTSPPort int    `json:"rtsp_port"` // e.g. 554; 0 = disabled
	RTMPPort int    `json:"rtmp_port"` // e.g. 1935; 0 = disabled
	SRTPort  int    `json:"srt_port"`  // e.g. 9999; 0 = disabled
}

// configResponse is the payload returned by GET /config.
type configResponse struct {
	Version            version.Info               `json:"version"`
	HWAccels           []domain.HWAccel           `json:"hw_accels"`
	VideoCodecs        []domain.VideoCodec        `json:"video_codecs"`
	AudioCodecs        []domain.AudioCodec        `json:"audio_codecs"`
	OutputProtocols    []string                   `json:"output_protocols"`
	StreamStatuses     []domain.StreamStatus      `json:"stream_statuses"`
	WatermarkTypes     []domain.WatermarkType     `json:"watermark_types"`
	WatermarkPositions []domain.WatermarkPosition `json:"watermark_positions"`
	Ports              publisherPorts             `json:"ports"`
	GlobalConfig       *domain.GlobalConfig       `json:"global_config"`
}

// ConfigHandler serves the GET/POST /config and GET/PUT /config/yaml endpoints.
//
// The /config/yaml endpoints expose every editable resource — GlobalConfig,
// streams, and hooks — as a single YAML document, so the frontend editor can
// round-trip the entire system configuration in one screen.
type ConfigHandler struct {
	rtm          RuntimeConfigManager
	streamRepo   store.StreamRepository
	templateRepo store.TemplateRepository
	hookRepo     store.HookRepository
	vodRepo      store.VODMountRepository
	vods         *vod.Registry
	coord        streamLifecycle
	autopublish  *autopublish.Service
	bus          events.Bus
}

// NewConfigHandler creates a ConfigHandler from the DI injector.
// The RuntimeConfigManager is injected later via SetRuntimeManager because it
// depends on services that themselves depend on this handler (circular DI).
func NewConfigHandler(i do.Injector) (*ConfigHandler, error) {
	h := &ConfigHandler{
		streamRepo:   do.MustInvoke[store.StreamRepository](i),
		templateRepo: do.MustInvoke[store.TemplateRepository](i),
		hookRepo:     do.MustInvoke[store.HookRepository](i),
		vodRepo:      do.MustInvoke[store.VODMountRepository](i),
		vods:         do.MustInvoke[*vod.Registry](i),
		coord:        do.MustInvoke[*coordinator.Coordinator](i),
		bus:          do.MustInvoke[events.Bus](i),
	}
	// autopublish is optional in tests; tolerate absence.
	if ap, err := do.Invoke[*autopublish.Service](i); err == nil {
		h.autopublish = ap
	}
	return h, nil
}

// SetRuntimeManager injects the RuntimeManager after construction (breaks the circular DI dependency).
func (h *ConfigHandler) SetRuntimeManager(m RuntimeConfigManager) {
	h.rtm = m
}

func portsFromConfig(gcfg *domain.GlobalConfig) publisherPorts {
	var p publisherPorts
	if gcfg.Server != nil {
		p.HTTPAddr = gcfg.Server.HTTPAddr
	}
	if gcfg.Listeners != nil {
		if gcfg.Listeners.RTSP.Enabled {
			p.RTSPPort = gcfg.Listeners.RTSP.Port
		}
		if gcfg.Listeners.RTMP.Enabled {
			p.RTMPPort = gcfg.Listeners.RTMP.Port
		}
		if gcfg.Listeners.SRT.Enabled {
			p.SRTPort = gcfg.Listeners.SRT.Port
		}
	}
	return p
}

// GetConfig returns static enum values, host-detected hardware capabilities,
// current GlobalConfig, and derived publisher ports.
//
// @Summary     Get server configuration.
// @Description Returns available hardware accelerators (OS-detected), static enum lists, publisher listener ports, and the current runtime GlobalConfig.
// @Tags        system
// @Produce     json
// @Success     200 {object} apidocs.ConfigData
// @Router      /config [get].
func (h *ConfigHandler) GetConfig(w http.ResponseWriter, _ *http.Request) {
	gcfg := h.rtm.CurrentConfig()
	resp := configResponse{
		Version:  version.Get(),
		HWAccels: hwdetect.Available(),
		VideoCodecs: []domain.VideoCodec{
			domain.VideoCodecH264,
			domain.VideoCodecH265,
			domain.VideoCodecAV1,
			domain.VideoCodecMPEG2,
			domain.VideoCodecCopy,
		},
		AudioCodecs: []domain.AudioCodec{
			domain.AudioCodecAAC,
			domain.AudioCodecMP2,
			domain.AudioCodecMP3,
			domain.AudioCodecAC3,
			domain.AudioCodecEAC3,
			domain.AudioCodecCopy,
		},
		OutputProtocols: []string{"hls", "dash", "rtmp", "rtsp", "srt"},
		StreamStatuses: []domain.StreamStatus{
			domain.StatusIdle,
			domain.StatusActive,
			domain.StatusDegraded,
			domain.StatusStopped,
		},
		WatermarkTypes: []domain.WatermarkType{
			domain.WatermarkTypeText,
			domain.WatermarkTypeImage,
		},
		WatermarkPositions: []domain.WatermarkPosition{
			domain.WatermarkTopLeft,
			domain.WatermarkTopRight,
			domain.WatermarkBottomLeft,
			domain.WatermarkBottomRight,
			domain.WatermarkCenter,
		},
		Ports:        portsFromConfig(gcfg),
		GlobalConfig: gcfg,
	}
	writeJSON(w, http.StatusOK, resp)
}

// UpdateConfig partially updates the runtime GlobalConfig, persists it to the store,
// and hot-reloads services (start/stop) to match the new configuration.
//
// Only the fields present in the request body are updated; omitted fields keep
// their current values.  Sending a section as JSON null disables that service.
//
// @Summary     Partially update server configuration.
// @Description Merges the request body into the current GlobalConfig and applies it — services are started or stopped to match.
// @Tags        system
// @Accept      json
// @Produce     json
// @Param       body body domain.GlobalConfig true "Partial configuration to merge"
// @Success     200 {object} apidocs.ConfigUpdateResponse
// @Failure     400 {object} apidocs.ErrorResponse
// @Failure     500 {object} apidocs.ErrorResponse
// @Router      /config [post].
func (h *ConfigHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	// Deep-copy the current config so json.Decode merges on top of it.
	current := h.rtm.CurrentConfig()
	raw, err := json.Marshal(current)
	if err != nil {
		serverError(w, r, "MARSHAL_FAILED", "marshal current config", err)
		return
	}
	var merged domain.GlobalConfig
	if err := json.Unmarshal(raw, &merged); err != nil {
		serverError(w, r, "COPY_FAILED", "copy current config", err)
		return
	}

	// Unmarshal the request body on top: present fields overwrite, omitted fields stay.
	if err := json.NewDecoder(r.Body).Decode(&merged); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_BODY", "invalid JSON: "+err.Error())
		return
	}

	// FFmpeg path validation removed in P0 of the native transcoder
	// migration — see internal/transcoder/native. Until in-process libav
	// capability probe lands, save-time accepts any transcoder config.
	if vErr := h.validateTranscoderPath(r.Context(), &merged); vErr != nil {
		writeError(w, http.StatusBadRequest, vErr.code, vErr.message)
		return
	}

	if err := h.rtm.Apply(r.Context(), &merged); err != nil {
		serverError(w, r, "APPLY_FAILED", "apply config", err)
		return
	}

	// Audit hook for config drift — operators monitoring infrastructure
	// inventory want to know when GlobalConfig changes (which downstream
	// services were hot-restarted, which ports moved, …). Payload kept small
	// (no full config dump) since the consumer can call GET /config to fetch
	// the new state if needed; a fat payload here would only bloat hook
	// queues for every config save. Nil-safe so tests that build the handler
	// without DI don't NPE.
	if h.bus != nil {
		h.bus.Publish(r.Context(), domain.Event{
			Type:    domain.EventConfigChanged,
			Payload: map[string]any{"source": "post_config"},
		})
	}

	// Return the freshly applied config.
	gcfg := h.rtm.CurrentConfig()
	writeJSON(w, http.StatusOK, map[string]any{
		"global_config": gcfg,
		"ports":         portsFromConfig(gcfg),
	})
}
