package blob

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// CatalogFormat is the value stamped into Catalog.Format for this archive.
const CatalogFormat = "cmaf-blob-v1"

// catalogFileName is the per-stream catalog file under <root>/<streamCode>/.
const catalogFileName = "catalog.json"

// MediaWindow is a wall-time interval (UTC ms) a profile actually covers, so a
// timeshift window touching a profile that started/stopped mid-recording
// resolves to its real coverage instead of 404-ing.
type MediaWindow struct {
	FromMs int64 `json:"from_ms"`
	ToMs   int64 `json:"to_ms"`
}

// HourRecord summarises one hour of one profile, used for window selection and
// retention without opening every .ranges file.
type HourRecord struct {
	Hour          string `json:"hour"` // "YYYY/MM/DD/HH"
	WallFromMs    int64  `json:"wall_from_ms"`
	WallToMs      int64  `json:"wall_to_ms"`
	MediaFromV    uint64 `json:"media_from_ticks_v"`
	MediaToV      uint64 `json:"media_to_ticks_v"`
	Sealed        bool   `json:"sealed"`
	Discontinuity bool   `json:"discontinuity"`
	FragCountV    int    `json:"frag_count_v"`
	FragCountA    int    `json:"frag_count_a,omitempty"`
	SizeBytes     int64  `json:"size_bytes"`
}

// ProfileDesc describes one recorded rendition (pN dir). Only the audio_profile
// has non-zero audio fields + .cmfa hours.
type ProfileDesc struct {
	ID             string        `json:"id"`          // "pN" = rendition index
	BufferSlug     string        `json:"buffer_slug"` // "track_1"; detects ladder reorder
	Codec          string        `json:"codec"`
	Width          int           `json:"width"`
	Height         int           `json:"height"`
	Bandwidth      int           `json:"bandwidth"`
	AudioCodec     string        `json:"audio_codec,omitempty"`
	AudioTimescale int           `json:"audio_timescale,omitempty"`
	AudioBandwidth int           `json:"audio_bandwidth,omitempty"`
	Available      []MediaWindow `json:"available"`
	Hours          []HourRecord  `json:"hours"`
}

// Gap is a recorded discontinuity in coverage (packet drop, source restart).
type Gap struct {
	FromMs int64  `json:"from_ms"`
	ToMs   int64  `json:"to_ms"`
	Reason string `json:"reason"`
}

// RetentionCfg mirrors the recording's retention knobs into the catalog so the
// central pass is self-contained.
type RetentionCfg struct {
	MaxAgeSec   int `json:"max_age_sec,omitempty"`
	MaxSizeGB   int `json:"max_size_gb,omitempty"`
	MaxAgeHours int `json:"max_age_hours,omitempty"`
}

// Catalog is the per-stream archive catalog (catalog.json). It is the single
// wall<->media anchor (RecordingMediaOrigin*) plus per-profile coverage. Only
// the per-stream Service owner goroutine mutates and Saves it.
type Catalog struct {
	StreamCode                 string        `json:"stream_code"`
	Format                     string        `json:"format"`
	StartedAt                  string        `json:"started_at"`
	VideoTimescale             uint32        `json:"video_timescale"`
	RecordingMediaOriginTicks  uint64        `json:"recording_media_origin_ticks"`
	RecordingMediaOriginUnixMs int64         `json:"recording_media_origin_unix_ms"`
	AudioProfile               string        `json:"audio_profile"`
	BestProfile                string        `json:"best_profile"`
	Profiles                   []ProfileDesc `json:"profiles"`
	Gaps                       []Gap         `json:"gaps,omitempty"`
	Retention                  RetentionCfg  `json:"retention"`
}

// CatalogSummary aggregates a catalog's coverage for status reporting: the
// overall available wall-time range (across all profiles), total bytes on disk,
// and counts. HasData is false when no profile has any recorded coverage yet.
type CatalogSummary struct {
	FromMs    int64
	ToMs      int64
	TotalSize int64
	Hours     int
	Profiles  int
	HasData   bool
}

// Summary computes the catalog's coverage summary. The range spans the earliest
// Available.FromMs to the latest Available.ToMs over every profile; total size
// sums every hour's recorded bytes.
func (c *Catalog) Summary() CatalogSummary {
	s := CatalogSummary{Profiles: len(c.Profiles)}
	for i := range c.Profiles {
		pd := &c.Profiles[i]
		for _, w := range pd.Available {
			if !s.HasData || w.FromMs < s.FromMs {
				s.FromMs = w.FromMs
			}
			if !s.HasData || w.ToMs > s.ToMs {
				s.ToMs = w.ToMs
			}
			s.HasData = true
		}
		for _, hr := range pd.Hours {
			s.TotalSize += hr.SizeBytes
			s.Hours++
		}
	}
	return s
}

// Profile returns the descriptor for id and whether it exists.
func (c *Catalog) Profile(id string) (*ProfileDesc, bool) {
	for i := range c.Profiles {
		if c.Profiles[i].ID == id {
			return &c.Profiles[i], true
		}
	}
	return nil, false
}

// WallToMediaTicks maps a UTC-ms instant to absolute video MediaTicks using the
// persisted recording origin. Used to translate a timeshift `from=` into the
// authoritative media clock for binary search.
func (c *Catalog) WallToMediaTicks(wallMs int64) uint64 {
	if c.VideoTimescale == 0 {
		return c.RecordingMediaOriginTicks
	}
	deltaMs := wallMs - c.RecordingMediaOriginUnixMs
	delta := deltaMs * int64(c.VideoTimescale) / 1000
	t := int64(c.RecordingMediaOriginTicks) + delta //nolint:gosec // origin fits int64 for any real recording
	if t < 0 {
		return 0
	}
	return uint64(t)
}

// MediaTicksToWall maps absolute video MediaTicks back to UTC ms.
func (c *Catalog) MediaTicksToWall(ticks uint64) int64 {
	if c.VideoTimescale == 0 {
		return c.RecordingMediaOriginUnixMs
	}
	delta := (int64(ticks) - int64(c.RecordingMediaOriginTicks)) * 1000 / int64(c.VideoTimescale) //nolint:gosec // bounded
	return c.RecordingMediaOriginUnixMs + delta
}

// Save atomically writes the catalog (tmp + rename), mirroring the legacy
// saveIndex pattern. Must be called only by the per-stream Service owner.
func (c *Catalog) Save(streamDir string) error {
	if c.Format == "" {
		c.Format = CatalogFormat
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("blob: marshal catalog: %w", err)
	}
	final := filepath.Join(streamDir, catalogFileName)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("blob: write catalog tmp: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("blob: rename catalog: %w", err)
	}
	return nil
}

// LoadCatalog reads <streamDir>/catalog.json. ok is false (nil error) when the
// catalog does not exist yet — the caller treats that as "no blob recording".
func LoadCatalog(streamDir string) (cat *Catalog, ok bool, err error) {
	data, err := os.ReadFile(filepath.Join(streamDir, catalogFileName))
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("blob: read catalog: %w", err)
	}
	var c Catalog
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, false, fmt.Errorf("blob: unmarshal catalog: %w", err)
	}
	return &c, true, nil
}

// HasCatalog reports whether a blob-archive catalog exists for the stream dir —
// the dispatcher's discriminator between blob and legacy .ts recordings.
func HasCatalog(streamDir string) bool {
	_, err := os.Stat(filepath.Join(streamDir, catalogFileName))
	return err == nil
}
