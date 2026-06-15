// Package config holds the root configuration struct and Viper-based loading.
package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// ManagerConfig controls Stream Manager failover and input health checks.
type ManagerConfig struct {
	// InputPacketTimeoutSec is the maximum gap without a successful read on the
	// active input before it is marked failed. Pull protocols that deliver in
	// bursts (e.g. HLS: one segment per Read) need this at least as large as the
	// typical interval between reads (segment duration + playlist poll), or a
	// healthy primary will be falsely failed over to a lower priority.
	InputPacketTimeoutSec int `mapstructure:"input_packet_timeout_sec" json:"input_packet_timeout_sec" yaml:"input_packet_timeout_sec"`
}

// ServerConfig holds HTTP/gRPC server settings.
type ServerConfig struct {
	HTTPAddr string     `mapstructure:"http_addr" json:"http_addr" yaml:"http_addr"`
	CORS     CORSConfig `mapstructure:"cors" json:"cors" yaml:"cors"`
	// PprofAddr enables Go's net/http/pprof + runtime introspection
	// endpoints on a SEPARATE listener. Default empty = disabled.
	// Recommend "127.0.0.1:6060" so the listener stays loopback-only —
	// pprof exposes goroutine stacks and heap layouts that should never
	// be reachable from the public network. Reach it from a remote box
	// via SSH tunnel: `ssh -L 6060:127.0.0.1:6060 host`.
	//
	// Endpoints exposed when set:
	//   /debug/pprof/heap       — heap snapshot (live objects)
	//   /debug/pprof/allocs     — cumulative alloc profile
	//   /debug/pprof/goroutine  — goroutine stack dump (?debug=2 for text)
	//   /debug/pprof/profile    — 30s CPU profile
	//   /debug/pprof/block      — blocking profile (needs SetBlockProfileRate)
	//   /debug/pprof/mutex      — mutex contention (needs SetMutexProfileFraction)
	PprofAddr string `mapstructure:"pprof_addr" json:"pprof_addr,omitempty" yaml:"pprof_addr,omitempty"`
}

// CORSConfig controls Cross-Origin Resource Sharing for the HTTP API and
// static media routes mounted on the same listener.
type CORSConfig struct {
	// Enabled turns CORS middleware on for the HTTP listener.
	Enabled bool `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	// AllowedOrigins lists values for Access-Control-Allow-Origin. Use "*"
	// for any origin (cannot be used together with AllowCredentials).
	AllowedOrigins []string `mapstructure:"allowed_origins" json:"allowed_origins" yaml:"allowed_origins"`
	// AllowedMethods lists Access-Control-Allow-Methods; empty uses a REST default set.
	AllowedMethods []string `mapstructure:"allowed_methods" json:"allowed_methods,omitempty" yaml:"allowed_methods,omitempty"`
	// AllowedHeaders lists Access-Control-Allow-Headers; empty uses a common API default set.
	AllowedHeaders []string `mapstructure:"allowed_headers" json:"allowed_headers,omitempty" yaml:"allowed_headers,omitempty"`
	// ExposedHeaders lists Access-Control-Expose-Headers.
	ExposedHeaders []string `mapstructure:"exposed_headers" json:"exposed_headers,omitempty" yaml:"exposed_headers,omitempty"`
	// AllowCredentials sets Access-Control-Allow-Credentials. Must be false
	// when AllowedOrigins contains "*".
	AllowCredentials bool `mapstructure:"allow_credentials" json:"allow_credentials" yaml:"allow_credentials"`
	// MaxAge is the preflight cache duration in seconds (Access-Control-Max-Age).
	MaxAge int `mapstructure:"max_age" json:"max_age" yaml:"max_age"`
}

// StorageConfig selects the storage backend and its connection details.
type StorageConfig struct {
	// Driver selects the backend: "json" | "yaml"
	Driver string `mapstructure:"driver"`

	// JSON backend
	JSONDir string `mapstructure:"json_dir"`

	// YAML backend
	YAMLDir string `mapstructure:"yaml_dir"`
}

// IngestorConfig controls server-level ingestion infrastructure.
// Per-input settings (timeouts, S3 region, SRT latency, etc.) are configured
// on each Input via the API and stored in the data storage.
//
// Network listeners (RTMP, SRT, RTSP) are configured in the top-level
// ListenersConfig and shared with the publisher, so the same port serves both
// push (ingest) and pull (play) traffic.
type IngestorConfig struct {
	// HLSMaxSegmentBuffer caps the number of pre-fetched HLS segments held in memory.
	// This is a server-wide memory guard, not a per-stream policy.
	HLSMaxSegmentBuffer int `mapstructure:"hls_max_segment_buffer" json:"hls_max_segment_buffer" yaml:"hls_max_segment_buffer"` // default 8
}

// BufferConfig controls the in-memory ring buffer.
type BufferConfig struct {
	// Capacity is the number of MPEG-TS packets per stream buffer.
	Capacity int `mapstructure:"capacity" json:"capacity" yaml:"capacity"`
}

// PublisherConfig controls filesystem-based output delivery (HLS, DASH).
// Live network listeners (RTSP, RTMP, SRT) are configured in ListenersConfig
// because the same port serves both ingest and playback.
type PublisherConfig struct {
	HLS  PublisherHLSConfig  `mapstructure:"hls" json:"hls" yaml:"hls"`
	DASH PublisherDASHConfig `mapstructure:"dash" json:"dash" yaml:"dash"`

	// MaxPlaybackConnPerStream / MaxPlaybackConnTotal cap concurrent RTSP /
	// RTMP / SRT / HTTP-MPEGTS playback connections (per stream and globally)
	// to bound the heavyweight per-connection state a flood can allocate (A-1).
	// Unset (0) applies a protective default; a negative value means unlimited.
	// HLS/DASH viewers go over stateless HTTP and are not counted here.
	MaxPlaybackConnPerStream int `mapstructure:"max_playback_conn_per_stream" json:"max_playback_conn_per_stream" yaml:"max_playback_conn_per_stream"`
	MaxPlaybackConnTotal     int `mapstructure:"max_playback_conn_total" json:"max_playback_conn_total" yaml:"max_playback_conn_total"`
}

// PublisherHLSConfig is filesystem + live packaging for Apple HLS (m3u8 + segments).
type PublisherHLSConfig struct {
	Dir string `mapstructure:"dir" json:"dir" yaml:"dir"`
	// LiveEphemeral enables bounded retention (sliding manifest, delete old segments).
	LiveEphemeral bool `mapstructure:"live_ephemeral" json:"live_ephemeral" yaml:"live_ephemeral"`
	// LiveSegmentSec is segment duration in seconds.
	LiveSegmentSec int `mapstructure:"live_segment_sec" json:"live_segment_sec" yaml:"live_segment_sec"`
	// LiveWindow is the sliding window size (segments) in the playlist.
	LiveWindow int `mapstructure:"live_window" json:"live_window" yaml:"live_window"`
	// LiveHistory is extra segments kept on disk after they leave the manifest.
	LiveHistory int `mapstructure:"live_history" json:"live_history" yaml:"live_history"`
}

// PublisherDASHConfig is filesystem + live packaging for MPEG-DASH (dynamic MPD + ISO BMFF init/media .m4s).
// Dir must be set and must not match PublisherHLSConfig.Dir (separate subscribers and segment files).
type PublisherDASHConfig struct {
	Dir string `mapstructure:"dir" json:"dir" yaml:"dir"`
	// Live* mirror HLS packaging semantics for the DASH muxer.
	LiveEphemeral  bool `mapstructure:"live_ephemeral" json:"live_ephemeral" yaml:"live_ephemeral"`
	LiveSegmentSec int  `mapstructure:"live_segment_sec" json:"live_segment_sec" yaml:"live_segment_sec"`
	LiveWindow     int  `mapstructure:"live_window" json:"live_window" yaml:"live_window"`
	LiveHistory    int  `mapstructure:"live_history" json:"live_history" yaml:"live_history"`
}

// ListenersConfig groups all live network listeners.
//
// Each protocol uses ONE port that serves both directions of traffic:
//   - RTMP: encoders push, players pull, on the same TCP port (default 1935).
//   - RTSP: clients pull live streams (default 554).
//   - SRT:  encoders push or clients pull on the same UDP port (default 9999),
//     dispatched by the SRT streamid mode flag.
//
// Setting Enabled=false (or leaving the section out entirely) disables that
// protocol's listener for both ingest and playback.
type ListenersConfig struct {
	RTMP RTMPListenerConfig `mapstructure:"rtmp" json:"rtmp" yaml:"rtmp"`
	RTSP RTSPListenerConfig `mapstructure:"rtsp" json:"rtsp" yaml:"rtsp"`
	SRT  SRTListenerConfig  `mapstructure:"srt"  json:"srt"  yaml:"srt"`
}

// RTMPListenerConfig is the shared RTMP listener used by both ingest and play.
// Encoders publish to rtmp://host:port/<key>; players pull from
// rtmp://host:port/<app>/<key>.
type RTMPListenerConfig struct {
	Enabled    bool   `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	ListenHost string `mapstructure:"listen_host" json:"listen_host" yaml:"listen_host"`
	Port       int    `mapstructure:"port" json:"port" yaml:"port"` // default 1935
}

// RTSPListenerConfig is the RTSP listener (publisher-side; ingest is pull-only).
// Clients use rtsp://host:port/live/<stream_code>.
type RTSPListenerConfig struct {
	Enabled    bool   `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	ListenHost string `mapstructure:"listen_host" json:"listen_host" yaml:"listen_host"`
	Port       int    `mapstructure:"port" json:"port" yaml:"port"` // default 554
	// Transport is "tcp" (default) or "udp" for the RTSP muxer.
	Transport string `mapstructure:"transport" json:"transport" yaml:"transport"`
}

// SRTListenerConfig is the shared SRT listener.
// Players set streamid=live/<stream_code>; publish ingest is dispatched by the
// streamid mode flag (mode=publish vs mode=request).
type SRTListenerConfig struct {
	Enabled    bool   `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	ListenHost string `mapstructure:"listen_host" json:"listen_host" yaml:"listen_host"`
	Port       int    `mapstructure:"port" json:"port" yaml:"port"` // default 9999
	// LatencyMS is the SRT latency in milliseconds applied to the listener.
	LatencyMS int `mapstructure:"latency_ms" json:"latency_ms" yaml:"latency_ms"`
}

// WatermarksConfig controls the on-disk library of uploadable watermark
// images. Operators upload PNG/JPG/GIF logos via /watermarks; the service
// stores them in Dir alongside JSON sidecar metadata.
type WatermarksConfig struct {
	// Dir is the assets directory. Must be writable by the open-streamer
	// process. Defaults to "./watermarks" when empty (matches the layout
	// install scripts use under /var/lib/open-streamer/watermarks).
	Dir string `mapstructure:"dir" json:"dir" yaml:"dir"`
}

// SessionsConfig controls the play-sessions tracker.
//
// Sessions are kept in memory only; restart loses every active record.
// Idle timeout governs when an HLS/DASH viewer (no TCP-level "left" signal)
// is considered gone. MaxLifetime caps the longest a single session can
// stay open — useful as a safety valve against a viewer behind a misbehaving
// proxy that keeps replaying segments forever.
type SessionsConfig struct {
	// Enabled toggles the entire feature. When false, the tracker no-ops and
	// the API endpoints return empty.
	Enabled bool `mapstructure:"enabled" json:"enabled" yaml:"enabled"`

	// IdleTimeoutSec is how long a session may go without activity before the
	// reaper closes it. Default 30 s when <= 0.
	IdleTimeoutSec int `mapstructure:"idle_timeout_sec" json:"idle_timeout_sec" yaml:"idle_timeout_sec"`

	// MaxLifetimeSec, when > 0, hard-closes any session older than this even
	// if it's still seeing activity. 0 disables the cap.
	MaxLifetimeSec int `mapstructure:"max_lifetime_sec" json:"max_lifetime_sec" yaml:"max_lifetime_sec"`

	// GeoIPDBPath, when non-empty, points to a MaxMind .mmdb file
	// (GeoLite2-Country / GeoLite2-City; commercial GeoIP2 also works).
	// The wiring layer opens it once at startup and uses it to fill
	// PlaySession.Country with the ISO 3166-1 alpha-2 code. When empty
	// (default) or when opening fails, sessions degrade silently to
	// Country="" via NullGeoIP — no impact on session tracking itself.
	//
	// Hot-reloading the file on disk requires a server restart; the
	// .mmdb is mmap'd for the lifetime of the process.
	GeoIPDBPath string `mapstructure:"geoip_db_path" json:"geoip_db_path,omitempty" yaml:"geoip_db_path,omitempty"`
}

// HooksConfig controls server-wide hook dispatcher defaults. Per-hook
// settings (target, retries, timeout, secret, batch sizing, event filter)
// are configured on each Hook via the API.
type HooksConfig struct {
	// WorkerCount sizes the events.Bus worker pool that fans events out
	// to subscribers. Each worker invokes the registered handler for an
	// incoming event; with the batched HTTP delivery, the hook handler
	// just enqueues into a per-hook batcher (~µs) so this number rarely
	// needs tuning. 1-4 covers nearly every workload. 0 = use 4.
	WorkerCount int `mapstructure:"worker_count" json:"worker_count" yaml:"worker_count"`

	// BatchMaxItems is the global default for HTTP hook batch size.
	// Per-hook BatchMaxItems overrides this; the code default
	// (DefaultHookBatchMaxItems) wins only when both are 0.
	BatchMaxItems int `mapstructure:"batch_max_items" json:"batch_max_items,omitempty" yaml:"batch_max_items,omitempty"`

	// BatchFlushIntervalSec is the global default for the per-hook flush
	// timer in seconds. Per-hook overrides win; code default is
	// DefaultHookBatchFlushIntervalSec.
	BatchFlushIntervalSec int `mapstructure:"batch_flush_interval_sec" json:"batch_flush_interval_sec,omitempty" yaml:"batch_flush_interval_sec,omitempty"`

	// BatchMaxQueueItems is the global default for the per-hook in-memory
	// queue cap. Per-hook overrides win; code default is
	// DefaultHookBatchMaxQueueItems.
	BatchMaxQueueItems int `mapstructure:"batch_max_queue_items" json:"batch_max_queue_items,omitempty" yaml:"batch_max_queue_items,omitempty"`
}

// AuthConfig holds authentication settings. Today it covers only the
// control-plane HTTP API (admin); media/playback auth is a separate plane.
type AuthConfig struct {
	API APIAuthConfig `mapstructure:"api" json:"api" yaml:"api"`
}

// APIAuthConfig configures HTTP Basic auth on the management API. When
// Enabled is false (default) the API is open, preserving prior behaviour.
// Users are part of the global config (managed via the config API by a
// `write` user). On boot, if Enabled is true and Users is empty, an `admin`
// user with the `write` role is seeded from OPEN_STREAMER_API_ADMIN_PASSWORD
// when that env var is set — otherwise every request is rejected until users
// are defined (fail-closed).
type APIAuthConfig struct {
	Enabled bool      `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	Users   []APIUser `mapstructure:"users" json:"users,omitempty" yaml:"users,omitempty"`
}

// APIUser is one management-API account. Role is "read" (GET/HEAD/OPTIONS only)
// or "write" (all methods). PasswordHash is a bcrypt hash — never store a
// plaintext password here (generate one with `open-streamer hashpw`).
type APIUser struct {
	Username     string `mapstructure:"username" json:"username" yaml:"username"`
	PasswordHash string `mapstructure:"password_hash" json:"password_hash" yaml:"password_hash"`
	Role         string `mapstructure:"role" json:"role" yaml:"role"`
}

// LogConfig controls structured logging output.
type LogConfig struct {
	Level  string `mapstructure:"level" json:"level" yaml:"level"`    // trace | debug | info | warn | error
	Format string `mapstructure:"format" json:"format" yaml:"format"` // text | json
}

// LoadStorage reads only the StorageConfig from environment variables and an optional
// config file. All other config sections are managed by GlobalConfig in the store.
func LoadStorage() (StorageConfig, error) {
	v := viper.New()

	v.SetEnvPrefix("OPEN_STREAMER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetDefault("storage.driver", "json")
	v.SetDefault("storage.json_dir", "./test_data")
	v.SetDefault("storage.yaml_dir", "./test_data")

	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("/etc/open-streamer")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return StorageConfig{}, fmt.Errorf("config: read: %w", err)
		}
	}

	var wrapper struct {
		Storage StorageConfig `mapstructure:"storage"`
	}
	if err := v.Unmarshal(&wrapper); err != nil {
		return StorageConfig{}, fmt.Errorf("config: unmarshal: %w", err)
	}

	return wrapper.Storage, nil
}
