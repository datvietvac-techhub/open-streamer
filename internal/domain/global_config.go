package domain

import "github.com/datvietvac-techhub/open-streamer/config"

// GlobalConfig holds all runtime configuration that is persisted in the store
// (as opposed to config.StorageConfig which is bootstrap-only from config.yaml/env).
//
// Pointer fields: nil means the section is not configured and the corresponding
// service should not start.  This allows users to enable/disable entire subsystems
// by adding or removing config sections via the API.
type GlobalConfig struct {
	Server     *config.ServerConfig     `json:"server,omitempty" yaml:"server,omitempty"`
	Listeners  *config.ListenersConfig  `json:"listeners,omitempty" yaml:"listeners,omitempty"`
	Ingestor   *config.IngestorConfig   `json:"ingestor,omitempty" yaml:"ingestor,omitempty"`
	Buffer     *config.BufferConfig     `json:"buffer,omitempty" yaml:"buffer,omitempty"`
	Publisher  *config.PublisherConfig  `json:"publisher,omitempty" yaml:"publisher,omitempty"`
	Manager    *config.ManagerConfig    `json:"manager,omitempty" yaml:"manager,omitempty"`
	Hooks      *config.HooksConfig      `json:"hooks,omitempty" yaml:"hooks,omitempty"`
	Sessions   *config.SessionsConfig   `json:"sessions,omitempty" yaml:"sessions,omitempty"`
	Watermarks *config.WatermarksConfig `json:"watermarks,omitempty" yaml:"watermarks,omitempty"`
	Log        *config.LogConfig        `json:"log,omitempty" yaml:"log,omitempty"`
}
