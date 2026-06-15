package domain

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// HookID is the unique identifier for a registered hook.
type HookID string

// HookType is the delivery mechanism for a hook.
type HookType string

// HookType values name supported hook transports.
const (
	HookTypeHTTP HookType = "http"
	// HookTypeFile appends each event as a single JSON line to the path
	// in Hook.Target. Useful for downstream log shippers (Filebeat,
	// Vector, Promtail) and for ops-friendly local audit trails without
	// running an HTTP receiver.
	HookTypeFile HookType = "file"
)

// StreamCodeFilter defines include/exclude rules for stream code matching.
// Only and Except are mutually exclusive; Only takes precedence when both are set.
type StreamCodeFilter struct {
	// Only delivers events only for streams in this list.
	Only []StreamCode `json:"only,omitempty" yaml:"only,omitempty"`
	// Except delivers events for all streams except those in this list.
	Except []StreamCode `json:"except,omitempty" yaml:"except,omitempty"`
}

// Matches reports whether the given stream code passes the filter.
func (f *StreamCodeFilter) Matches(code StreamCode) bool {
	if f == nil {
		return true
	}
	if len(f.Only) > 0 {
		for _, c := range f.Only {
			if c == code {
				return true
			}
		}
		return false
	}
	for _, c := range f.Except {
		if c == code {
			return false
		}
	}
	return true
}

// Hook is a registered external integration that receives domain events.
type Hook struct {
	ID     HookID   `json:"id" yaml:"id"`
	Name   string   `json:"name" yaml:"name"`
	Type   HookType `json:"type" yaml:"type"`
	Target string   `json:"target" yaml:"target"` // HTTP(S) URL or absolute file path
	Secret string   `json:"secret" yaml:"secret"` // HMAC-SHA256 signing secret (HTTP only)

	// EventTypes filters which events trigger delivery. Empty = all events.
	EventTypes []EventType `json:"event_types,omitempty" yaml:"event_types,omitempty"`

	// StreamCodes filters delivery by stream code.
	// Only and Except are mutually exclusive; Only takes precedence when both are set.
	// Omitting the field (nil) means all streams are included.
	StreamCodes *StreamCodeFilter `json:"stream_codes,omitempty" yaml:"stream_codes,omitempty"`

	// Metadata holds user-defined key-value pairs merged into every event payload
	// delivered by this hook. Useful for tagging events with custom context
	// (e.g. environment, tenant ID, region) without modifying the server config.
	Metadata map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`

	Enabled bool `json:"enabled" yaml:"enabled"`

	// MaxRetries is the number of delivery attempts before giving up.
	// 0 means use the server default (3). For HTTP hooks this caps retries
	// inside a single batch flush; events that still fail are re-queued
	// for the next flush regardless of MaxRetries.
	MaxRetries int `json:"max_retries" yaml:"max_retries"`

	// TimeoutSec is the per-attempt delivery timeout in seconds.
	// 0 means use the server default (10s).
	TimeoutSec int `json:"timeout_sec" yaml:"timeout_sec"`

	// BatchMaxItems caps the number of events bundled into one HTTP POST
	// body. 0 = use HooksConfig.BatchMaxItems, then DefaultHookBatchMaxItems.
	// Ignored for File hooks (they always write one event per line).
	BatchMaxItems int `json:"batch_max_items,omitempty" yaml:"batch_max_items,omitempty"`

	// BatchFlushIntervalSec is the maximum time a batch may sit before being
	// flushed even when below BatchMaxItems. 0 = use HooksConfig default,
	// then DefaultHookBatchFlushIntervalSec.
	BatchFlushIntervalSec int `json:"batch_flush_interval_sec,omitempty" yaml:"batch_flush_interval_sec,omitempty"`

	// BatchMaxQueueItems caps the per-hook in-memory queue (pending +
	// re-queued failures). When exceeded, the OLDEST events are dropped
	// with a warning log so the queue never grows unbounded against an
	// unreachable target. 0 = use HooksConfig default, then
	// DefaultHookBatchMaxQueueItems.
	BatchMaxQueueItems int `json:"batch_max_queue_items,omitempty" yaml:"batch_max_queue_items,omitempty"`
}

// Validate checks the hook's type and target are well-formed (S-2). For a file
// hook — which writes to the host filesystem — an absolute path alone is not
// safe, so when allowedFileRoot is non-empty the target is confined inside it
// (rejecting `..` escape). allowedFileRoot == "" keeps the legacy "absolute
// path only" behaviour for backward compatibility. Shared by the REST handler,
// the YAML config path, and the file delivery sink so every entry point that
// can reach the filesystem applies the same rule.
func (h *Hook) Validate(allowedFileRoot string) error {
	target := strings.TrimSpace(h.Target)
	if target == "" {
		return errors.New("hook target is required")
	}
	switch h.Type {
	case HookTypeHTTP:
		if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
			return errors.New("http hook target must be an http(s):// URL")
		}
	case HookTypeFile:
		if !filepath.IsAbs(target) {
			return errors.New("file hook target must be an absolute path")
		}
		if allowedFileRoot != "" {
			root := filepath.Clean(allowedFileRoot)
			if clean := filepath.Clean(target); !pathInside(clean, root) {
				return fmt.Errorf("file hook target %q must be inside the configured hooks.file_root_dir %q", target, root)
			}
		}
	default:
		return fmt.Errorf("unsupported hook type %q (use http|file)", h.Type)
	}
	return nil
}
