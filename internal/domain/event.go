// Package domain defines core types shared across Open Streamer modules.
package domain

import "time"

// EventType identifies the kind of domain event that occurred.
type EventType string

// EventType values are emitted on the event bus for stream lifecycle, inputs, recordings, and segments.
const (
	// Stream lifecycle — published by coordinator and API handler.
	EventStreamCreated EventType = "stream.created"
	EventStreamUpdated EventType = "stream.updated" // PUT /streams/{code} on existing record
	EventStreamStarted EventType = "stream.started"
	EventStreamStopped EventType = "stream.stopped"
	EventStreamDeleted EventType = "stream.deleted"

	// Input health — published by ingestor worker and stream manager.
	EventInputConnected    EventType = "input.connected"    // source connected successfully
	EventInputReconnecting EventType = "input.reconnecting" // transient error, retrying
	EventInputDegraded     EventType = "input.degraded"     // error detected by manager
	EventInputFailed       EventType = "input.failed"       // worker exited / non-retriable
	EventInputFailover     EventType = "input.failover"     // switched to a different input
	EventInputRecovered    EventType = "input.recovered"    // previously degraded input is healthy again (auto or via failback probe)

	// DVR recordings — published by dvr.Service.
	EventRecordingStarted EventType = "recording.started"
	EventRecordingStopped EventType = "recording.stopped"
	EventRecordingFailed  EventType = "recording.failed"
	EventSegmentWritten   EventType = "segment.written"
	EventDVRSegmentPruned EventType = "dvr.segment_pruned" // retention loop deleted an aged-out segment

	// Transcoder — published by transcoder.Service.
	EventTranscoderStarted EventType = "transcoder.started"
	EventTranscoderStopped EventType = "transcoder.stopped"
	EventTranscoderError   EventType = "transcoder.error"

	// Push out (RTMP/RTMPS) — published by publisher push_rtmp.go on each
	// state transition. Lets operators alert on per-destination CDN drops
	// without scraping runtime status.
	EventPushStarted      EventType = "push.started"      // dialing target, pre-handshake
	EventPushActive       EventType = "push.active"       // handshake ok; media flowing
	EventPushReconnecting EventType = "push.reconnecting" // write fail or input discontinuity, retrying
	EventPushFailed       EventType = "push.failed"       // dest.Limit exhausted or terminal error

	// Server-config — published by config handler when GlobalConfig changes.
	EventConfigChanged EventType = "config.changed"

	// Watermark assets — published by watermarks service on REST CRUD.
	EventWatermarkAssetCreated EventType = "watermark.asset_created"
	EventWatermarkAssetDeleted EventType = "watermark.asset_deleted"

	// Hooks — published by hooks service on REST CRUD (meta-events; useful
	// for audit logs and inventory sync).
	EventHookCreated EventType = "hook.created"
	EventHookUpdated EventType = "hook.updated"
	EventHookDeleted EventType = "hook.deleted"

	// Templates — published by template handler on REST CRUD. Update fires
	// after every dependent stream's pipeline has been hot-reloaded so
	// downstream consumers can assume the new resolved config is live.
	EventTemplateCreated EventType = "template.created"
	EventTemplateUpdated EventType = "template.updated"
	EventTemplateDeleted EventType = "template.deleted"

	// Policies — published by the policy handler on REST CRUD of a media-auth
	// Policy. The authorizer's compiled rule set is hot-swapped before the
	// event fires, so consumers can assume the new rules are already live.
	EventPolicyCreated EventType = "policy.created"
	EventPolicyUpdated EventType = "policy.updated"
	EventPolicyDeleted EventType = "policy.deleted"

	// Runtime streams — published by the autopublish service when a
	// template-prefix match materialises (or expires) a stream that does
	// NOT exist in the persisted config. Useful for audit logs and the
	// runtime-stream observability dashboard.
	EventStreamRuntimeCreated EventType = "stream.runtime_created"
	EventStreamRuntimeExpired EventType = "stream.runtime_expired"
)

// Event is an immutable fact describing a domain state change.
type Event struct {
	ID         string         `json:"id"` // UUID for idempotent delivery
	Type       EventType      `json:"type"`
	StreamCode StreamCode     `json:"stream_code"`
	OccurredAt time.Time      `json:"occurred_at"`
	Payload    map[string]any `json:"payload,omitempty"` // event-specific fields
}
