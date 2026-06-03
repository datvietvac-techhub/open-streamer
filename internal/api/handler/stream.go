// Package handler contains HTTP request handlers for the API server.
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/datvietvac-techhub/open-streamer/internal/autopublish"
	"github.com/datvietvac-techhub/open-streamer/internal/coordinator"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/manager"
	"github.com/datvietvac-techhub/open-streamer/internal/publisher"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
	"github.com/datvietvac-techhub/open-streamer/internal/transcoder"
	"github.com/go-chi/chi/v5"
	"github.com/samber/do/v2"
)

// streamCoordinator captures every coordinator method StreamHandler uses.
// Defined as an interface so tests can inject a tiny stub instead of
// wiring the full coordinator dependency graph (buffer + manager + tc +
// pub + dvr + bus + metrics — see internal/coordinator/coordinator.go).
// *coordinator.Coordinator satisfies this implicitly.
type streamCoordinator interface {
	StreamStatus(code domain.StreamCode) domain.StreamStatus
	StreamStartedAt(code domain.StreamCode) (time.Time, bool)
	IsRunning(code domain.StreamCode) bool
	Start(ctx context.Context, stream *domain.Stream) error
	Stop(ctx context.Context, code domain.StreamCode)
	Update(ctx context.Context, old, new *domain.Stream) error
	ABRCopyRuntimeStatus(code domain.StreamCode) (manager.RuntimeStatus, bool)
	ABRMixerRuntimeStatus(code domain.StreamCode) (manager.RuntimeStatus, bool)
}

// streamManagerDep / streamTranscoderDep / streamPublisherDep narrow
// each upstream service to just the read-only RuntimeStatus / SwitchInput
// surface the handler needs. Same rationale as streamCoordinator —
// keeps the test harness from having to instantiate a full ingestor +
// FFmpeg pool just to call a List handler.
type streamManagerDep interface {
	RuntimeStatus(code domain.StreamCode) (manager.RuntimeStatus, bool)
	SwitchInput(code domain.StreamCode, priority int) error
}

type streamTranscoderDep interface {
	RuntimeStatus(code domain.StreamCode) (transcoder.RuntimeStatus, bool)
}

type streamPublisherDep interface {
	RuntimeStatus(code domain.StreamCode) (publisher.RuntimeStatus, bool)
}

// StreamHandler handles stream lifecycle REST endpoints.
type StreamHandler struct {
	streamRepo   store.StreamRepository
	templateRepo store.TemplateRepository
	coordinator  streamCoordinator
	manager      streamManagerDep
	transcoder   streamTranscoderDep
	publisher    streamPublisherDep
	autopublish  runtimeStreamLister
	bus          events.Bus
}

// runtimeStreamLister narrows the autopublish service to the methods
// the stream API needs: enumerate runtime streams (for list responses
// + source tagging), look up a single runtime entry (so Get / Restart
// resolve runtime codes instead of 404-ing through the on-disk repo),
// and StopRuntime for the operator-initiated teardown path.
// *autopublish.Service satisfies this implicitly.
type runtimeStreamLister interface {
	ListRuntime() []autopublish.RuntimeEntry
	IsRuntime(code domain.StreamCode) bool
	Lookup(code domain.StreamCode) (autopublish.RuntimeEntry, bool)
	StopRuntime(ctx context.Context, code domain.StreamCode) bool
}

// StreamSource tags whether a Stream record originated from the config
// store or from auto-publish (a template-prefix match that materialised
// a runtime stream on the fly). Surfaced on every list / get response
// so clients can filter or label correctly.
type StreamSource string

// StreamSource values.
const (
	StreamSourceConfig  StreamSource = "config"
	StreamSourceRuntime StreamSource = "runtime"
)

// streamResponse is the API representation of a stream.
// Persisted config from domain.Stream is embedded at top level; everything
// runtime-computed (status, pipeline activity, input health, transcoder state)
// lives under a single `runtime` envelope so clients have one root for live data.
type streamResponse struct {
	*domain.Stream
	Source  StreamSource          `json:"source"`
	Runtime manager.RuntimeStatus `json:"runtime"`
}

func (h *StreamHandler) withStatus(s *domain.Stream) streamResponse {
	rt, registered := h.manager.RuntimeStatus(s.Code)
	rt.Status = h.coordinator.StreamStatus(s.Code)
	rt.PipelineActive = registered

	// ABR-copy / ABR-mixer streams bypass the manager entirely (their
	// pipeline is N in-process taps, not an ingest worker), so the manager
	// has no record to report. Substitute the coordinator's synthetic
	// snapshot so the UI can show input status / activity instead of
	// "UNKNOWN".
	if !registered {
		if abrRT, ok := h.coordinator.ABRCopyRuntimeStatus(s.Code); ok {
			abrRT.Status = rt.Status
			rt = abrRT
		} else if mxRT, ok := h.coordinator.ABRMixerRuntimeStatus(s.Code); ok {
			mxRT.Status = rt.Status
			rt = mxRT
		}
	}

	if rt.PipelineActive {
		// Nest transcoder + push runtime under the same envelope; keeps them
		// from colliding with the persisted Stream.Transcoder / Stream.Push
		// config fields on the same json tags.
		if tc, ok := h.transcoder.RuntimeStatus(s.Code); ok {
			rt.Transcoder = &tc
		}
		if pr, ok := h.publisher.RuntimeStatus(s.Code); ok {
			rt.Publisher = &pr
		}
	}
	if rt.Inputs == nil {
		// json marshals a nil slice as `null`; emit `[]` so the contract is
		// stable for clients regardless of pipeline state.
		rt.Inputs = []manager.InputHealthSnapshot{}
	}
	if startedAt, ok := h.coordinator.StreamStartedAt(s.Code); ok {
		// Stamp the wallclock the pipeline went live AND the elapsed
		// seconds at response time. Computing UptimeSec server-side
		// shields the frontend from browser-vs-server clock drift —
		// some operator dashboards run on machines hours off from NTP.
		t := startedAt
		rt.StartedAt = &t
		rt.UptimeSec = int64(time.Since(startedAt) / time.Second)
	}
	rt.Media = buildMediaSummary(s, rt.Inputs, rt.ActiveInputPriority)
	source := StreamSourceConfig
	if h.autopublish != nil && h.autopublish.IsRuntime(s.Code) {
		source = StreamSourceRuntime
	}
	return streamResponse{Stream: s, Source: source, Runtime: rt}
}

// buildMediaSummary derives the runtime media envelope (UI panel data) from
// the stream config + observed input snapshots. Output tracks come from the
// persisted transcoder config; when the stream isn't transcoding (Transcoder
// == nil), output mirrors input — that's the actual on-wire shape because
// the publisher just re-muxes the active input's elementary streams.
func buildMediaSummary(
	s *domain.Stream,
	inputs []manager.InputHealthSnapshot,
	activePriority int,
) *manager.MediaSummary {
	out := &manager.MediaSummary{}

	// Input tracks come from the currently-active input. Falls back to the
	// first available snapshot when the active priority isn't yet set
	// (StatusIdle, just after Register).
	var active *manager.InputHealthSnapshot
	for i := range inputs {
		if inputs[i].InputPriority == activePriority {
			active = &inputs[i]
			break
		}
	}
	if active == nil && len(inputs) > 0 {
		active = &inputs[0]
	}
	if active != nil {
		out.Inputs = active.Tracks
		out.InputBitrateKbps = active.BitrateKbps
	}

	// Output tracks come from the persisted transcoder config. No transcode
	// → output is whatever the publisher re-muxes, which is exactly the
	// active input's tracks (single video PID + single audio PID — see
	// tsmux.FromAV semantics).
	if s.Transcoder != nil {
		out.Outputs = domain.OutputTracks(s.Transcoder)
		out.OutputBitrateKbps = domain.AggregateBitrateKbps(out.Outputs)
	} else {
		out.Outputs = out.Inputs
		out.OutputBitrateKbps = out.InputBitrateKbps
	}
	return out
}

// NewStreamHandler creates a StreamHandler and registers it with the DI injector.
func NewStreamHandler(i do.Injector) (*StreamHandler, error) {
	h := &StreamHandler{
		streamRepo:   do.MustInvoke[store.StreamRepository](i),
		templateRepo: do.MustInvoke[store.TemplateRepository](i),
		coordinator:  do.MustInvoke[*coordinator.Coordinator](i),
		manager:      do.MustInvoke[*manager.Service](i),
		transcoder:   do.MustInvoke[*transcoder.Service](i),
		publisher:    do.MustInvoke[*publisher.Service](i),
		bus:          do.MustInvoke[events.Bus](i),
	}
	if ap, err := do.Invoke[*autopublish.Service](i); err == nil {
		h.autopublish = ap
	}
	return h, nil
}

// resolveStream merges a stream with the config of its referenced template
// (if any). The coordinator always operates on the resolved view: the raw
// stream that lives in the repo carries only the per-stream overrides, the
// template fills in everything left at its zero value. Returns the stream
// untouched when there is no template reference, the template is missing
// from the repo, or any error occurs — the caller can keep operating on
// the raw config and the worst case is that the operator sees a "template
// not found" log line.
func (h *StreamHandler) resolveStream(ctx context.Context, s *domain.Stream) *domain.Stream {
	if s == nil || s.Template == nil || h.templateRepo == nil {
		return s
	}
	tpl, err := h.templateRepo.FindByCode(ctx, *s.Template)
	if err != nil {
		return s
	}
	return domain.ResolveStream(s, tpl)
}

// List streams; optional ?status=idle|active|degraded|stopped.
// @Summary List streams
// @Tags streams
// @Produce json
// @Param status query string false "Filter by status"
// @Success 200 {object} apidocs.StreamList
// @Failure 400 {object} apidocs.ErrorBody
// @Failure 500 {object} apidocs.ErrorBody
// @Router /streams [get].
func (h *StreamHandler) List(w http.ResponseWriter, r *http.Request) {
	var statusFilter *domain.StreamStatus
	if q := r.URL.Query().Get("status"); q != "" {
		st := domain.StreamStatus(q)
		switch st {
		case domain.StatusIdle, domain.StatusActive, domain.StatusDegraded, domain.StatusStopped:
			statusFilter = &st
		default:
			writeError(w, http.StatusBadRequest, "INVALID_QUERY", "unknown status filter")
			return
		}
	}

	streams, err := h.streamRepo.List(r.Context(), store.StreamFilter{})
	if err != nil {
		serverError(w, r, "LIST_FAILED", "list streams", err)
		return
	}
	streams = append(streams, h.listRuntimeStreams()...)

	resp := make([]streamResponse, 0, len(streams))
	for _, s := range streams {
		sr := h.withStatus(s)
		if statusFilter != nil && sr.Runtime.Status != *statusFilter {
			continue
		}
		resp = append(resp, sr)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": resp, "total": len(resp)})
}

// listRuntimeStreams returns one *domain.Stream stub per live runtime
// stream. The stub carries only the per-stream identity (code + template
// reference) — symmetric with the response shape used for config streams,
// which the on-disk repo also serves raw (the template merge is reserved
// for the coordinator-facing path). Clients that want the effective
// config fetch the referenced template separately.
func (h *StreamHandler) listRuntimeStreams() []*domain.Stream {
	if h.autopublish == nil {
		return nil
	}
	entries := h.autopublish.ListRuntime()
	if len(entries) == 0 {
		return nil
	}
	out := make([]*domain.Stream, 0, len(entries))
	for _, e := range entries {
		tplCode := e.TemplateCode
		out = append(out, &domain.Stream{Code: e.Code, Template: &tplCode})
	}
	return out
}

// Get returns one stream by code.
// @Summary Get stream
// @Tags streams
// @Produce json
// @Param code path string true "Stream code (a-zA-Z0-9_)"
// @Success 200 {object} apidocs.StreamData
// @Failure 404 {object} apidocs.ErrorBody
// @Failure 500 {object} apidocs.ErrorBody
// @Router /streams/{code} [get].
func (h *StreamHandler) Get(w http.ResponseWriter, r *http.Request) {
	code := domain.StreamCode(chi.URLParam(r, "code"))
	stream, err := h.streamRepo.FindByCode(r.Context(), code)
	if err != nil {
		// Fall back to the autopublish registry: a runtime stream
		// materialised by a template-prefix match never lands on
		// disk, but it should still resolve through GET so the UI
		// can render the same shape as the list response.
		if h.autopublish != nil {
			if e, ok := h.autopublish.Lookup(code); ok {
				tpl := e.TemplateCode
				stub := &domain.Stream{Code: e.Code, Template: &tpl}
				writeJSON(w, http.StatusOK, map[string]any{"data": h.withStatus(stub)})
				return
			}
		}
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": h.withStatus(stream)})
}

// Put creates a new stream or partially updates an existing one.
// Only fields present in the request body are applied; omitted fields keep their current values.
// @Summary Create or partial-update stream
// @Tags streams
// @Accept json
// @Produce json
// @Param code path string true "Stream code"
// @Param body body apidocs.StreamPutRequest true "Partial or full stream document"
// @Success 200 {object} apidocs.StreamData
// @Success 201 {object} apidocs.StreamData
// @Failure 400 {object} apidocs.ErrorBody
// @Failure 500 {object} apidocs.ErrorBody
// @Router /streams/{code} [post].
func (h *StreamHandler) Put(w http.ResponseWriter, r *http.Request) {
	code := domain.StreamCode(chi.URLParam(r, "code"))
	cur, exists, err := h.loadCurrentStream(r, code)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}

	// Reject Put on a runtime-only code. Without this guard the handler
	// would treat the code as "freshly created" (because the store has
	// no record), save a parallel config record, then call
	// coordinator.Start — which is a no-op because the runtime pipeline
	// is already running. Result: two streamResponse entries for the
	// same code (one source=config, one source=runtime), config from
	// the body never takes effect, and the reconciler eventually
	// re-Starts with the config record only after the idle reaper
	// removes the runtime entry. The intended workflow is to update
	// the TEMPLATE (`POST /templates/<code>`) to change shared config
	// for every dependent runtime stream, or DELETE the runtime stream
	// first if the operator truly wants to register a per-stream
	// config record under the same code.
	if !exists && h.autopublish != nil {
		if e, ok := h.autopublish.Lookup(code); ok {
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":         "RUNTIME_STREAM_NOT_EDITABLE",
				"message":       "this code is currently an auto-published runtime stream; update the referenced template to change shared config, or DELETE the runtime stream first to register a config stream under the same code",
				"code":          string(code),
				"template_code": string(e.TemplateCode),
			})
			return
		}
	}

	body, validationErr := decodeStreamBody(r, code, cur, exists)
	if validationErr != nil {
		writeError(w, http.StatusBadRequest, validationErr.code, validationErr.message)
		return
	}

	if vErr := h.validateCopyConfig(r, body); vErr != nil {
		writeError(w, http.StatusBadRequest, vErr.code, vErr.message)
		return
	}

	if vErr := h.validateMixerConfig(r, body); vErr != nil {
		writeError(w, http.StatusBadRequest, vErr.code, vErr.message)
		return
	}

	wasRunning := exists && h.coordinator.IsRunning(code)
	// nowEnabled: existing stream toggled disabled→enabled.
	// freshlyCreated: brand-new stream that should run from the moment it's
	// saved. Without this branch, POST /streams would persist the record
	// but never call Start — the stream would stay in StatusStopped until
	// the reconciler picked it up on the next tick.
	nowEnabled := exists && cur.Disabled && !body.Disabled
	freshlyCreated := !exists && !body.Disabled && len(body.Inputs) > 0

	// Reject a save that points at a non-existent template — silently
	// persisting it would leave the runtime resolution unable to fill
	// in the inherited fields and produce a confusing partial config.
	// The nil-check on templateRepo lets the harness skip this gate in
	// tests that build the handler without the template wiring.
	if body.Template != nil && h.templateRepo != nil {
		if _, err := h.templateRepo.FindByCode(r.Context(), *body.Template); err != nil {
			writeError(w, http.StatusBadRequest, "TEMPLATE_NOT_FOUND",
				"referenced template does not exist: "+string(*body.Template))
			return
		}
	}

	// Save first so the pipeline continues with the old config if persistence fails.
	if err := h.streamRepo.Save(r.Context(), body); err != nil {
		serverError(w, r, "SAVE_FAILED", "save stream", err)
		return
	}

	// Detach from request cancellation: the coordinator spawns goroutines whose
	// lifetime is the stream pipeline, not the HTTP request. Without this, when
	// the response is written and r.Context() is cancelled, every spawned worker
	// (HLS/DASH/RTMP/transcoder/manager) is killed instantly. WithoutCancel keeps
	// request-scoped values (logging tags) but drops the cancel signal.
	pipelineCtx := context.WithoutCancel(r.Context())
	// Resolve through the template before handing to the coordinator so
	// inherited fields are visible to the pipeline diff / start path. The
	// raw stream stays on disk; only the in-memory view that drives the
	// runtime is merged.
	resolvedCur := h.resolveStream(pipelineCtx, cur)
	resolvedBody := h.resolveStream(pipelineCtx, body)
	switch {
	case wasRunning:
		if err := h.coordinator.Update(pipelineCtx, resolvedCur, resolvedBody); err != nil {
			serverError(w, r, "UPDATE_FAILED", "update stream pipeline", err)
			return
		}
	case nowEnabled, freshlyCreated:
		if err := h.coordinator.Start(pipelineCtx, resolvedBody); err != nil {
			serverError(w, r, "START_FAILED", "start stream pipeline", err)
			return
		}
	}

	if !exists {
		h.bus.Publish(r.Context(), domain.Event{
			Type:       domain.EventStreamCreated,
			StreamCode: body.Code,
		})
		writeJSON(w, http.StatusCreated, map[string]any{"data": *body})
		return
	}
	// Existing record was overwritten with new config — emit so audit/inventory
	// hooks can sync without polling. Payload carries the diff trigger flags
	// the coordinator already computed so consumers know whether the pipeline
	// was just persisted, freshly enabled, or restarted with topology changes.
	h.bus.Publish(r.Context(), domain.Event{
		Type:       domain.EventStreamUpdated,
		StreamCode: body.Code,
		Payload: map[string]any{
			"was_running": wasRunning,
			"now_enabled": nowEnabled,
			"disabled":    body.Disabled,
		},
	})
	writeJSON(w, http.StatusOK, map[string]any{"data": *body})
}

type putValidationError struct {
	code    string
	message string
}

// validateCopyConfig is the API-handler wrapper that loads existing streams
// from the repo and delegates to validateCopyConfigOn. Split this way so the
// pure validation logic can be unit-tested without a real or fake repo.
func (h *StreamHandler) validateCopyConfig(r *http.Request, proposed *domain.Stream) *putValidationError {
	all, err := h.streamRepo.List(r.Context(), store.StreamFilter{})
	if err != nil {
		return &putValidationError{code: "LIST_FAILED", message: "list streams for copy validation: " + err.Error()}
	}
	return validateCopyConfigOn(proposed, all)
}

// validateCopyConfigOn runs the copy:// safety checks against an in-memory
// snapshot of the existing streams (any source — repo list, fixture, …).
// `existing` is the world BEFORE the write; `proposed` substitutes in for
// any same-code entry to form the world AFTER.
//
// Order of checks (each gives a more specific error than the next when both
// would fire on the same input):
//
//  1. Per-input URL grammar — catch malformed `copy://` before anything
//     else interprets it.
//  2. Shape constraints — local transcoder vs ABR upstream, fallback rules.
//  3. Cycle detection across the merged graph.
func validateCopyConfigOn(proposed *domain.Stream, existing []*domain.Stream) *putValidationError {
	// (1) URL grammar.
	for i, in := range proposed.Inputs {
		if !domain.IsCopyInput(in) {
			continue
		}
		if _, err := domain.CopyInputTarget(in); err != nil {
			return &putValidationError{
				code:    "INVALID_COPY_URL",
				message: fmt.Sprintf("inputs[%d]: %s", i, err.Error()),
			}
		}
	}

	// Build merged view: existing repo streams with proposed substituted
	// for the same code.
	merged := make([]*domain.Stream, 0, len(existing)+1)
	replaced := false
	for _, s := range existing {
		if s == nil {
			continue
		}
		if s.Code == proposed.Code {
			merged = append(merged, proposed)
			replaced = true
			continue
		}
		merged = append(merged, s)
	}
	if !replaced {
		merged = append(merged, proposed)
	}

	lookup := func(c domain.StreamCode) (*domain.Stream, bool) {
		for _, s := range merged {
			if s.Code == c {
				return s, true
			}
		}
		return nil, false
	}

	// (2) Shape constraints. (Cycle detection across the copy:// graph was
	// removed: shape validation already rejects the most common bad case
	// — self-copy — and operators are trusted not to author multi-stream
	// fan-out loops; misconfig surfaces as runtime degradation, not fan-out.)
	if err := domain.ValidateCopyShape(proposed, lookup); err != nil {
		return &putValidationError{code: "INVALID_COPY_SHAPE", message: err.Error()}
	}

	return nil
}

// validateMixerConfig is the API-handler wrapper that loads existing streams
// from the repo and delegates to validateMixerConfigOn. Split this way so the
// pure validation logic can be unit-tested without a real or fake repo.
func (h *StreamHandler) validateMixerConfig(r *http.Request, proposed *domain.Stream) *putValidationError {
	all, err := h.streamRepo.List(r.Context(), store.StreamFilter{})
	if err != nil {
		return &putValidationError{code: "LIST_FAILED", message: "list streams for mixer validation: " + err.Error()}
	}
	return validateMixerConfigOn(proposed, all)
}

// validateMixerConfigOn runs the mixer:// safety checks against an in-memory
// snapshot of the existing streams.
//
// Order of checks:
//
//  1. Per-input URL grammar — catch malformed `mixer://` early.
//  2. Shape constraints — sole-input rule, ABR-upstream rule, self-mix,
//     local-transcoder forbidden.
func validateMixerConfigOn(proposed *domain.Stream, existing []*domain.Stream) *putValidationError {
	// (1) URL grammar.
	for i, in := range proposed.Inputs {
		if !domain.IsMixerInput(in) {
			continue
		}
		if _, _, _, err := domain.MixerInputSpec(in); err != nil {
			return &putValidationError{
				code:    "INVALID_MIXER_URL",
				message: fmt.Sprintf("inputs[%d]: %s", i, err.Error()),
			}
		}
	}

	// Build merged view: existing repo streams with proposed substituted
	// for the same code.
	merged := make([]*domain.Stream, 0, len(existing)+1)
	replaced := false
	for _, s := range existing {
		if s == nil {
			continue
		}
		if s.Code == proposed.Code {
			merged = append(merged, proposed)
			replaced = true
			continue
		}
		merged = append(merged, s)
	}
	if !replaced {
		merged = append(merged, proposed)
	}

	lookup := func(c domain.StreamCode) (*domain.Stream, bool) {
		for _, s := range merged {
			if s.Code == c {
				return s, true
			}
		}
		return nil, false
	}

	// (2) Shape constraints.
	if err := domain.ValidateMixerShape(proposed, lookup); err != nil {
		return &putValidationError{code: "INVALID_MIXER_SHAPE", message: err.Error()}
	}

	return nil
}

func (h *StreamHandler) loadCurrentStream(
	r *http.Request,
	code domain.StreamCode,
) (*domain.Stream, bool, error) {
	cur, err := h.streamRepo.FindByCode(r.Context(), code)
	if err == nil {
		return cur, true, nil
	}
	if errors.Is(err, store.ErrNotFound) {
		return nil, false, nil
	}
	return nil, false, err
}

func decodeStreamBody(
	r *http.Request,
	code domain.StreamCode,
	cur *domain.Stream,
	exists bool,
) (*domain.Stream, *putValidationError) {
	// Start from the existing stream so omitted fields keep their current values.
	// For new streams, start from zero value.
	//
	// Deep-clone via JSON round-trip when merging onto an existing stream:
	// `base = *cur` is a shallow copy, so pointer fields (Transcoder, DVR, Push
	// items) would alias `cur`. json.Decode on `base` would then mutate `cur`
	// in-place, and ComputeDiff(cur, body) downstream would see identical
	// pointers and report no change — silently swallowing the update.
	var base domain.Stream
	if exists {
		data, err := json.Marshal(cur)
		if err != nil {
			return nil, &putValidationError{code: "INVALID_BODY", message: "clone existing stream: " + err.Error()}
		}
		if err := json.Unmarshal(data, &base); err != nil {
			return nil, &putValidationError{code: "INVALID_BODY", message: "clone existing stream: " + err.Error()}
		}
	}

	// Unmarshal onto base — only fields present in the JSON are overwritten.
	if err := json.NewDecoder(r.Body).Decode(&base); err != nil {
		return nil, &putValidationError{code: "INVALID_BODY", message: err.Error()}
	}

	base.Code = code
	if err := domain.ValidateStreamCode(string(base.Code)); err != nil {
		return nil, &putValidationError{code: "INVALID_CODE", message: err.Error()}
	}
	if err := base.ValidateInputPriorities(); err != nil {
		return nil, &putValidationError{code: "INVALID_INPUT_PRIORITY", message: err.Error()}
	}
	if err := base.ValidateUniqueInputs(); err != nil {
		return nil, &putValidationError{code: "DUPLICATE_INPUT", message: err.Error()}
	}
	if err := base.Watermark.Validate(); err != nil {
		return nil, &putValidationError{code: "INVALID_WATERMARK", message: err.Error()}
	}
	// Transcoder.Mode validation removed alongside the Mode field in the
	// native-transcoder migration — there is only one topology now.
	return &base, nil
}

// Delete removes a stream and stops its pipeline.
// @Summary Delete stream
// @Tags streams
// @Param code path string true "Stream code"
// @Success 204 "No Content"
// @Failure 500 {object} apidocs.ErrorBody
// @Router /streams/{code} [delete].
func (h *StreamHandler) Delete(w http.ResponseWriter, r *http.Request) {
	code := domain.StreamCode(chi.URLParam(r, "code"))
	pipelineCtx := context.WithoutCancel(r.Context())
	// Runtime streams are torn down through autopublish so the entry,
	// observer goroutine, and pipeline come down atomically — without
	// this the entry stays in the registry until the observer notices
	// the buffer-hub close, leaving a small window where a fresh push
	// would short-circuit on the stale entry. Config-stream deletes
	// just go through coordinator.Stop + streamRepo.Delete.
	if h.autopublish != nil && h.autopublish.StopRuntime(pipelineCtx, code) {
		h.bus.Publish(r.Context(), domain.Event{
			Type:       domain.EventStreamDeleted,
			StreamCode: code,
		})
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// Detach from request cancellation so teardown completes even if the client disconnects.
	h.coordinator.Stop(pipelineCtx, code)
	if err := h.streamRepo.Delete(r.Context(), code); err != nil {
		serverError(w, r, "DELETE_FAILED", "delete stream", err)
		return
	}
	h.bus.Publish(r.Context(), domain.Event{
		Type:       domain.EventStreamDeleted,
		StreamCode: code,
	})
	w.WriteHeader(http.StatusNoContent)
}

// Restart stops the running pipeline (if any), waits for full cleanup, then starts
// a fresh pipeline. This guarantees all goroutines have exited and on-disk segments
// have been removed before the new pipeline begins.
// @Summary Restart stream pipeline
// @Tags streams
// @Produce json
// @Param code path string true "Stream code"
// @Success 200 {object} apidocs.StreamActionData
// @Failure 400 {object} apidocs.ErrorBody
// @Failure 404 {object} apidocs.ErrorBody
// @Failure 500 {object} apidocs.ErrorBody
// @Router /streams/{code}/restart [post].
func (h *StreamHandler) Restart(w http.ResponseWriter, r *http.Request) {
	code := domain.StreamCode(chi.URLParam(r, "code"))
	stream, err := h.streamRepo.FindByCode(r.Context(), code)
	if err != nil {
		// Runtime stream restart — the encoder owns the lifecycle, so
		// "restart" means: stop the runtime entry now (without waiting
		// for the 30 s idle reaper) and let the encoder's reconnect
		// drive re-materialisation. We do NOT re-Start from here:
		// runtime streams have no persistent record to fetch a fresh
		// config from, and starting one without the autopublish path
		// would leak a pipeline whose entry the matcher doesn't own.
		if h.autopublish != nil && h.autopublish.StopRuntime(context.WithoutCancel(r.Context()), code) {
			writeJSON(w, http.StatusOK, map[string]any{"data": map[string]string{
				"status": "stopped",
				"source": "runtime",
				"hint":   "runtime stream torn down; re-materialises on the next push that matches a template prefix",
			}})
			return
		}
		writeStoreError(w, r, err)
		return
	}
	if stream.Disabled {
		writeError(w, http.StatusBadRequest, "STREAM_DISABLED", "stream is disabled; clear disabled flag before starting")
		return
	}

	// Stop is blocking: waits for all goroutines to finish and cleans up on-disk
	// segments before returning. Safe to call even when the pipeline is not running.
	// Detach from request cancellation — the new pipeline outlives this request, and
	// teardown must complete even if the client disconnects mid-restart.
	pipelineCtx := context.WithoutCancel(r.Context())
	h.coordinator.Stop(pipelineCtx, code)

	if err := h.coordinator.Start(pipelineCtx, h.resolveStream(pipelineCtx, stream)); err != nil {
		serverError(w, r, "START_FAILED", "restart stream pipeline", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]string{"status": "started"}})
}

// SwitchInput forces the active ingest source to the given input priority at runtime.
// The switch is temporary — it reverts automatically when the selected input degrades permanently.
// Switching to a different priority replaces any previous manual override.
//
// @Summary     Manual input switch
// @Tags        streams
// @Produce     json
// @Param       code  path  string                    true  "Stream code"
// @Param       body  body  apidocs.InputSwitchRequest true  "Target input priority"
// @Success     200   {object} apidocs.StreamActionData
// @Failure     400   {object} apidocs.ErrorBody
// @Failure     404   {object} apidocs.ErrorBody
// @Failure     500   {object} apidocs.ErrorBody
// @Router      /streams/{code}/switch [post].
func (h *StreamHandler) SwitchInput(w http.ResponseWriter, r *http.Request) {
	code := domain.StreamCode(chi.URLParam(r, "code"))

	var body struct {
		Priority int `json:"priority"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_BODY", err.Error())
		return
	}

	if err := h.manager.SwitchInput(code, body.Priority); err != nil {
		writeError(w, http.StatusBadRequest, "SWITCH_FAILED", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]string{"status": "switched"}})
}
