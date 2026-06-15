package handler

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

const (
	maxYAMLBodyBytes = 4 << 20 // 4 MiB — generous enough for many streams + hooks, blocks accidental huge uploads
	yamlContentType  = "application/yaml"
	msgMustBeGEZero  = "must be >= 0"
)

// fullConfig is the unified schema exposed to the frontend editor.
// It bundles every editable resource so the user can manage the whole system
// from a single YAML document — change a port, add a stream, register a hook,
// all in one round-trip.
//
// Order of declaration matches apply order on PUT: GlobalConfig first (ports
// the publisher will bind), then Policies (so streams/templates referencing
// them resolve), then Templates, then Hooks, then VOD, then Streams.
type fullConfig struct {
	GlobalConfig *domain.GlobalConfig `yaml:"global_config"`
	Policies     []*domain.Policy     `yaml:"policies"`
	Templates    []*domain.Template   `yaml:"templates"`
	Streams      []*domain.Stream     `yaml:"streams"`
	Hooks        []*domain.Hook       `yaml:"hooks"`
	VOD          []*domain.VODMount   `yaml:"vod"`
}

// fieldError describes a single validation failure.
// It is JSON-serialised so the editor can highlight the offending key.
type fieldError struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

// GetConfigYAML returns the entire system state — GlobalConfig, all streams,
// all hooks — as one YAML document. The frontend editor uses this to populate
// its initial buffer.
//
// @Summary     Get full system configuration as YAML
// @Description Returns global_config, streams, and hooks bundled in a single YAML document. Pair with PUT /config/yaml to round-trip an editor.
// @Tags        system
// @Produce     application/yaml
// @Success     200 {string} string "YAML document"
// @Failure     500 {object} map[string]any
// @Router      /config/yaml [get].
func (h *ConfigHandler) GetConfigYAML(w http.ResponseWriter, r *http.Request) {
	streams, err := h.streamRepo.List(r.Context(), store.StreamFilter{})
	if err != nil {
		serverError(w, r, "LIST_STREAMS_FAILED", "list streams for yaml export", err)
		return
	}
	var templates []*domain.Template
	if h.templateRepo != nil {
		var err error
		templates, err = h.templateRepo.List(r.Context())
		if err != nil {
			serverError(w, r, "LIST_TEMPLATES_FAILED", "list templates for yaml export", err)
			return
		}
	}
	var policies []*domain.Policy
	if h.policyRepo != nil {
		var err error
		policies, err = h.policyRepo.List(r.Context())
		if err != nil {
			serverError(w, r, "LIST_POLICIES_FAILED", "list policies for yaml export", err)
			return
		}
	}
	hooks, err := h.hookRepo.List(r.Context())
	if err != nil {
		serverError(w, r, "LIST_HOOKS_FAILED", "list hooks for yaml export", err)
		return
	}
	vodMounts, err := h.vodRepo.List(r.Context())
	if err != nil {
		serverError(w, r, "LIST_VOD_FAILED", "list vod mounts for yaml export", err)
		return
	}

	full := fullConfig{
		GlobalConfig: h.rtm.CurrentConfig(),
		Policies:     policies,
		Templates:    templates,
		Streams:      streams,
		Hooks:        hooks,
		VOD:          vodMounts,
	}
	out, err := yaml.Marshal(full)
	if err != nil {
		serverError(w, r, "MARSHAL_FAILED", "marshal full config to yaml", err)
		return
	}
	w.Header().Set("Content-Type", yamlContentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

// ReplaceConfigYAML replaces the entire system configuration with the YAML body.
// Performs a full replace, not a merge:
//   - GlobalConfig sections omitted from the body are treated as absent and the
//     corresponding service is stopped.
//   - Streams omitted from the body are stopped and deleted.
//   - Hooks omitted from the body are deleted.
//
// All field errors are collected and returned together so the editor can
// highlight every issue in one round-trip.
//
// @Summary     Replace full system configuration from YAML
// @Description Replaces global_config, streams, and hooks with the request body. Validation errors are returned as a list. On success, the runtime manager + coordinator diff against the previous state and start/stop/restart services accordingly.
// @Tags        system
// @Accept      application/yaml
// @Produce     json
// @Param       body body string true "Full system configuration YAML document"
// @Success     200 {object} map[string]any
// @Failure     400 {object} map[string]any
// @Failure     422 {object} map[string]any
// @Failure     500 {object} map[string]any
// @Router      /config/yaml [put].
func (h *ConfigHandler) ReplaceConfigYAML(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxYAMLBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "BODY_TOO_LARGE",
			fmt.Sprintf("request body exceeds %d bytes", maxYAMLBodyBytes))
		return
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		writeError(w, http.StatusBadRequest, "EMPTY_BODY", "YAML body is empty")
		return
	}

	// Strict decode — unknown fields are an error so typos don't silently disappear.
	var newCfg fullConfig
	dec := yaml.NewDecoder(strings.NewReader(string(body)))
	dec.KnownFields(true)
	if err := dec.Decode(&newCfg); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_YAML", err.Error())
		return
	}

	// Validate everything up-front; nothing is applied until every section passes.
	if errs := validateFullConfig(&newCfg); len(errs) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"error": map[string]any{
				"code":    "VALIDATION_FAILED",
				"message": fmt.Sprintf("%d field(s) failed validation", len(errs)),
				"fields":  errs,
			},
		})
		return
	}

	// Apply order matters:
	//   1. GlobalConfig — bringing services up/down may affect what streams and
	//      hooks can run; do it first so subsequent stream starts see the right
	//      publisher ports, hooks worker pool, etc.
	//   2. Hooks — pure data; the hooks service reads from the repo on each event.
	//   3. Streams — data + lifecycle; depends on the publisher being on the
	//      ports defined in step 1.
	gcfg := newCfg.GlobalConfig
	if gcfg == nil {
		gcfg = &domain.GlobalConfig{}
	}
	if err := h.rtm.Apply(r.Context(), gcfg); err != nil {
		serverError(w, r, "APPLY_GLOBAL_FAILED", "apply global config from yaml", err)
		return
	}

	// Policies land BEFORE templates and streams: both can reference a policy
	// via PlaybackPolicy, so the policy set must exist (and the authorizer must
	// be reloaded) before those references go live.
	if err := h.applyPolicies(r.Context(), newCfg.Policies, newCfg.Streams, newCfg.Templates); err != nil {
		serverError(w, r, "APPLY_POLICIES_FAILED", "apply policies from yaml", err)
		return
	}

	// Templates land BEFORE streams: stream.Template references must be able
	// to resolve when applyStreams calls coord.Start/Update. The autopublish
	// matcher gets a final refresh after the desired template set is
	// persisted so prefix routing sees the new shape on the next push.
	if err := h.applyTemplates(r.Context(), newCfg.Templates, newCfg.Streams); err != nil {
		serverError(w, r, "APPLY_TEMPLATES_FAILED", "apply templates from yaml", err)
		return
	}

	if err := h.applyHooks(r.Context(), newCfg.Hooks); err != nil {
		serverError(w, r, "APPLY_HOOKS_FAILED", "apply hooks from yaml", err)
		return
	}

	if err := h.applyVOD(r.Context(), newCfg.VOD); err != nil {
		serverError(w, r, "APPLY_VOD_FAILED", "apply vod mounts from yaml", err)
		return
	}

	// Detach from request cancellation: applyStreams calls coord.Start/Stop/Update
	// which spawn pipeline goroutines whose lifetime is the stream, not this request.
	// Without this, the writes complete but every freshly-started worker is killed
	// the moment the response returns. WithoutCancel preserves request-scoped values.
	if err := h.applyStreams(context.WithoutCancel(r.Context()), newCfg.Streams); err != nil {
		serverError(w, r, "APPLY_STREAMS_FAILED", "apply streams from yaml", err)
		return
	}

	current := h.rtm.CurrentConfig()
	streamsAfter, _ := h.streamRepo.List(r.Context(), store.StreamFilter{})
	var templatesAfter []*domain.Template
	if h.templateRepo != nil {
		templatesAfter, _ = h.templateRepo.List(r.Context())
	}
	var policiesAfter []*domain.Policy
	if h.policyRepo != nil {
		policiesAfter, _ = h.policyRepo.List(r.Context())
	}
	hooksAfter, _ := h.hookRepo.List(r.Context())
	vodAfter, _ := h.vodRepo.List(r.Context())

	// Bulk YAML replace fans out into many smaller mutations (streams + hooks
	// + vod + global config) but operators usually want ONE event for the
	// whole transaction — easier to correlate in audit dashboards than 50
	// individual stream.updated/hook.updated entries. The fine-grained events
	// are still emitted by their respective Apply paths. Nil-safe so tests
	// building the handler without DI don't NPE.
	if h.bus != nil {
		h.bus.Publish(r.Context(), domain.Event{
			Type:    domain.EventConfigChanged,
			Payload: map[string]any{"source": "put_config_yaml"},
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"global_config": current,
		"policies":      policiesAfter,
		"templates":     templatesAfter,
		"streams":       streamsAfter,
		"hooks":         hooksAfter,
		"vod":           vodAfter,
		"ports":         portsFromConfig(current),
	})
}

// applyTemplates diffs the desired template list against the store. Same
// upsert-then-delete shape as applyHooks, with two extra rules:
//
//   - A template only present in the store is deleted ONLY when no stream
//     in `streamsAfter` references it. Otherwise the bulk-replace would
//     orphan the reference and the next coord.Start would fail validation.
//     Surface this as a structured error so the editor can highlight which
//     streams still need detaching.
//   - On success the autopublish prefix matcher is refreshed so the new
//     template set's prefixes route encoder pushes correctly.
//
// The caller passes the desired stream list so the foreign-key check runs
// against post-replace state, not pre-replace.
func (h *ConfigHandler) applyTemplates(
	ctx context.Context,
	desired []*domain.Template,
	streamsAfter []*domain.Stream,
) error {
	// Nil templateRepo means the handler was constructed without DI (unit
	// tests building a minimal ConfigHandler). Skip the apply entirely so
	// existing tests that don't care about templates still pass.
	if h.templateRepo == nil {
		return nil
	}
	existing, err := h.templateRepo.List(ctx)
	if err != nil {
		return fmt.Errorf("list templates: %w", err)
	}

	desiredByCode := make(map[domain.TemplateCode]*domain.Template, len(desired))
	for _, tpl := range desired {
		if tpl == nil {
			continue
		}
		desiredByCode[tpl.Code] = tpl
	}

	// Build "what would the future stream set reference?" using the request's
	// streams (not the store's current set) — operators usually delete a
	// template + the streams that reference it in the same edit, and the
	// store doesn't reflect those stream deletes until applyStreams runs.
	refsAfter := make(map[domain.TemplateCode]map[domain.StreamCode]struct{})
	for _, s := range streamsAfter {
		if s == nil || s.Template == nil {
			continue
		}
		set, ok := refsAfter[*s.Template]
		if !ok {
			set = make(map[domain.StreamCode]struct{})
			refsAfter[*s.Template] = set
		}
		set[s.Code] = struct{}{}
	}

	for _, old := range existing {
		if old == nil {
			continue
		}
		if _, keep := desiredByCode[old.Code]; keep {
			continue
		}
		if blockers := refsAfter[old.Code]; len(blockers) > 0 {
			codes := make([]string, 0, len(blockers))
			for c := range blockers {
				codes = append(codes, string(c))
			}
			return fmt.Errorf("template %q is referenced by stream(s) %v — detach them in the same edit",
				old.Code, codes)
		}
		if err := h.templateRepo.Delete(ctx, old.Code); err != nil {
			return fmt.Errorf("delete template %q: %w", old.Code, err)
		}
	}

	for _, tpl := range desired {
		if tpl == nil {
			continue
		}
		if err := h.templateRepo.Save(ctx, tpl); err != nil {
			return fmt.Errorf("save template %q: %w", tpl.Code, err)
		}
	}

	// Rebuild the autopublish prefix snapshot so subsequent encoder pushes
	// see the new template set. Nil-safe — tests construct the handler
	// without the autopublish service.
	if h.autopublish != nil {
		if err := h.autopublish.RefreshTemplates(ctx); err != nil {
			return fmt.Errorf("refresh autopublish: %w", err)
		}
	}
	return nil
}

// applyPolicies diffs the desired media-auth policy list against the store and
// upserts/deletes accordingly, then hot-reloads the authorizer's compiled set.
//
// A policy only present in the store is deleted ONLY when no stream or template
// in the desired document still references it (via PlaybackPolicy) — otherwise
// the bulk-replace would leave a dangling reference that fails closed at
// playback. The desired stream/template lists are passed so the foreign-key
// check runs against post-replace state, not the pre-replace store.
func (h *ConfigHandler) applyPolicies(
	ctx context.Context,
	desired []*domain.Policy,
	streamsAfter []*domain.Stream,
	templatesAfter []*domain.Template,
) error {
	// Nil policyRepo means the handler was built without DI (minimal unit
	// tests). Skip entirely so those tests still pass.
	if h.policyRepo == nil {
		return nil
	}
	existing, err := h.policyRepo.List(ctx)
	if err != nil {
		return fmt.Errorf("list policies: %w", err)
	}

	desiredByCode := make(map[domain.PolicyCode]*domain.Policy, len(desired))
	for _, p := range desired {
		if p != nil {
			desiredByCode[p.Code] = p
		}
	}

	// "What would reference each policy after the replace?" — from the desired
	// streams AND templates, not the current store.
	refs := make(map[domain.PolicyCode][]string)
	for _, s := range streamsAfter {
		if s != nil && s.PlaybackPolicy != "" {
			refs[s.PlaybackPolicy] = append(refs[s.PlaybackPolicy], "stream "+string(s.Code))
		}
	}
	for _, t := range templatesAfter {
		if t != nil && t.PlaybackPolicy != "" {
			refs[t.PlaybackPolicy] = append(refs[t.PlaybackPolicy], "template "+string(t.Code))
		}
	}

	for _, old := range existing {
		if old == nil {
			continue
		}
		if _, keep := desiredByCode[old.Code]; keep {
			continue
		}
		if blockers := refs[old.Code]; len(blockers) > 0 {
			return fmt.Errorf("policy %q is referenced by %v — detach them in the same edit", old.Code, blockers)
		}
		if err := h.policyRepo.Delete(ctx, old.Code); err != nil {
			return fmt.Errorf("delete policy %q: %w", old.Code, err)
		}
	}

	for _, p := range desired {
		if p == nil {
			continue
		}
		if err := h.policyRepo.Save(ctx, p); err != nil {
			return fmt.Errorf("save policy %q: %w", p.Code, err)
		}
	}

	// Hot-reload the authorizer's compiled set so the new policies take effect
	// without a restart. Nil-safe — tests build the handler without it.
	if h.authz != nil {
		ps, err := h.policyRepo.List(ctx)
		if err != nil {
			return fmt.Errorf("reload policies: %w", err)
		}
		h.authz.SetPolicies(ps)
	}
	return nil
}

// applyHooks diffs the desired hook list against what is in the store and
// performs the necessary save/delete operations.
//   - Hooks present in desired but missing from store → Save
//   - Hooks present in store but missing from desired → Delete
//   - Hooks present in both → Save (overwrite is cheap and idempotent)
func (h *ConfigHandler) applyHooks(ctx context.Context, desired []*domain.Hook) error {
	existing, err := h.hookRepo.List(ctx)
	if err != nil {
		return fmt.Errorf("list hooks: %w", err)
	}

	desiredByID := make(map[domain.HookID]*domain.Hook, len(desired))
	for _, hk := range desired {
		if hk == nil {
			continue
		}
		desiredByID[hk.ID] = hk
	}

	for _, old := range existing {
		if old == nil {
			continue
		}
		if _, keep := desiredByID[old.ID]; !keep {
			if err := h.hookRepo.Delete(ctx, old.ID); err != nil {
				return fmt.Errorf("delete hook %q: %w", old.ID, err)
			}
		}
	}

	for _, hk := range desired {
		if hk == nil {
			continue
		}
		if err := h.hookRepo.Save(ctx, hk); err != nil {
			return fmt.Errorf("save hook %q: %w", hk.ID, err)
		}
	}
	return nil
}

// applyVOD diffs the desired VOD mount list against the store and performs
// save/delete to converge. After the store is updated the in-memory registry
// is resynced so the next ingest start sees the new layout.
func (h *ConfigHandler) applyVOD(ctx context.Context, desired []*domain.VODMount) error {
	existing, err := h.vodRepo.List(ctx)
	if err != nil {
		return fmt.Errorf("list vod mounts: %w", err)
	}

	desiredByName := make(map[domain.VODName]*domain.VODMount, len(desired))
	for _, m := range desired {
		if m == nil {
			continue
		}
		desiredByName[m.Name] = m
	}

	for _, old := range existing {
		if old == nil {
			continue
		}
		if _, keep := desiredByName[old.Name]; !keep {
			if err := h.vodRepo.Delete(ctx, old.Name); err != nil {
				return fmt.Errorf("delete vod mount %q: %w", old.Name, err)
			}
		}
	}

	for _, m := range desired {
		if m == nil {
			continue
		}
		if err := h.vodRepo.Save(ctx, m); err != nil {
			return fmt.Errorf("save vod mount %q: %w", m.Name, err)
		}
	}

	mounts, err := h.vodRepo.List(ctx)
	if err != nil {
		return fmt.Errorf("reload vod mounts: %w", err)
	}
	h.vods.Sync(mounts)
	return nil
}

// applyStreams diffs the desired stream set against the store and the
// coordinator's running pipelines, and reconciles each stream individually:
//   - Removed (in store, missing from desired) → Stop pipeline + Delete
//   - Added   (missing from store)             → Save + Start (if not Disabled and has Inputs)
//   - Updated (in both)                        → Save, then Update (if running)
//     or Start (if was disabled/stopped and now enabled)
func (h *ConfigHandler) applyStreams(ctx context.Context, desired []*domain.Stream) error {
	existing, err := h.streamRepo.List(ctx, store.StreamFilter{})
	if err != nil {
		return fmt.Errorf("list streams: %w", err)
	}
	existingByCode := indexStreams(existing)
	desiredByCode := indexStreams(desired)

	if err := h.removeStreams(ctx, existingByCode, desiredByCode); err != nil {
		return err
	}
	return h.upsertStreams(ctx, existingByCode, desiredByCode)
}

func indexStreams(in []*domain.Stream) map[domain.StreamCode]*domain.Stream {
	out := make(map[domain.StreamCode]*domain.Stream, len(in))
	for _, s := range in {
		if s != nil {
			out[s.Code] = s
		}
	}
	return out
}

func (h *ConfigHandler) removeStreams(
	ctx context.Context,
	existing, desired map[domain.StreamCode]*domain.Stream,
) error {
	for code := range existing {
		if _, keep := desired[code]; keep {
			continue
		}
		h.coord.Stop(ctx, code)
		if err := h.streamRepo.Delete(ctx, code); err != nil {
			return fmt.Errorf("delete stream %q: %w", code, err)
		}
	}
	return nil
}

func (h *ConfigHandler) upsertStreams(
	ctx context.Context,
	existing, desired map[domain.StreamCode]*domain.Stream,
) error {
	for code, want := range desired {
		old, exists := existing[code]
		if err := h.streamRepo.Save(ctx, want); err != nil {
			return fmt.Errorf("save stream %q: %w", code, err)
		}
		if err := h.reconcileStreamLifecycle(ctx, old, want, exists); err != nil {
			return err
		}
	}
	return nil
}

func (h *ConfigHandler) reconcileStreamLifecycle(
	ctx context.Context,
	old, want *domain.Stream,
	exists bool,
) error {
	// Resolve through the template before the coord call so the diff
	// engine and pipeline start path see the inherited config. Without
	// this, an edit like "set protocols all to false" never visibly
	// changes anything because: the raw `old` has Protocols=nil (was
	// inheriting), the raw `want` has Protocols=&{all false}, and the
	// publisher's protocol diff reads zero-from-nil on both sides → no
	// stop is triggered and the previously-resolved HLS/DASH workers
	// keep running. StreamHandler.Put already resolves at the same
	// boundary; the YAML editor's apply path was the outlier.
	resolvedOld := h.resolveStream(ctx, old)
	resolvedWant := h.resolveStream(ctx, want)
	runnable := !resolvedWant.Disabled && len(resolvedWant.Inputs) > 0
	switch {
	case exists && h.coord.IsRunning(resolvedWant.Code):
		if err := h.coord.Update(ctx, resolvedOld, resolvedWant); err != nil {
			return fmt.Errorf("update stream %q: %w", resolvedWant.Code, err)
		}
	case runnable:
		if err := h.coord.Start(ctx, resolvedWant); err != nil {
			return fmt.Errorf("start stream %q: %w", resolvedWant.Code, err)
		}
	}
	return nil
}

// resolveStream merges a stream with its referenced template's config.
// Falls back to the raw stream when the template repo isn't wired (unit
// tests) or the lookup fails — same defensive shape StreamHandler.Put
// uses so a missing template doesn't crash the apply flow.
func (h *ConfigHandler) resolveStream(ctx context.Context, s *domain.Stream) *domain.Stream {
	if s == nil || s.Template == nil || h.templateRepo == nil {
		return s
	}
	tpl, err := h.templateRepo.FindByCode(ctx, *s.Template)
	if err != nil {
		return s
	}
	return domain.ResolveStream(s, tpl)
}

// validateFullConfig collects validation failures across every section of the
// document. Returning the full list (rather than first-fail) lets the editor
// surface every issue at once instead of forcing the user through many
// round-trips.
func validateFullConfig(c *fullConfig) []fieldError {
	if c == nil {
		return []fieldError{{Path: "", Message: "config is nil"}}
	}
	var errs []fieldError
	if c.GlobalConfig != nil {
		errs = append(errs, validateGlobalConfig(c.GlobalConfig)...)
	}
	errs = append(errs, validatePolicies(c.Policies)...)
	errs = append(errs, validateStreams(c.Streams)...)
	// File hooks are confined to hooks.file_root_dir (S-2); validate against the
	// root declared in THIS document's global_config so a single YAML round-trip
	// is self-consistent.
	hookFileRoot := ""
	if c.GlobalConfig != nil && c.GlobalConfig.Hooks != nil {
		hookFileRoot = c.GlobalConfig.Hooks.FileRootDir
	}
	errs = append(errs, validateHooks(c.Hooks, hookFileRoot)...)
	errs = append(errs, validateVOD(c.VOD)...)
	return errs
}

// validatePolicies checks each media-auth policy individually (via
// domain.Policy.Validate) plus enforces unique codes across the document.
func validatePolicies(policies []*domain.Policy) []fieldError {
	errs := make([]fieldError, 0, len(policies))
	seen := make(map[domain.PolicyCode]int, len(policies))
	for i, p := range policies {
		base := fmt.Sprintf("policies[%d]", i)
		if p == nil {
			errs = append(errs, fieldError{Path: base, Message: "policy entry is null"})
			continue
		}
		if err := p.Validate(); err != nil {
			errs = append(errs, fieldError{Path: base, Message: err.Error()})
			continue
		}
		if prev, dup := seen[p.Code]; dup {
			errs = append(errs, fieldError{
				Path:    base + ".code",
				Message: fmt.Sprintf("duplicate policy code %q (also at index %d)", p.Code, prev),
			})
			continue
		}
		seen[p.Code] = i
	}
	return errs
}

// validateVOD checks each mount individually plus enforces unique names.
// Storage paths must be absolute; the registry skips invalid entries silently
// at sync time, so we surface them up-front during YAML validation instead.
func validateVOD(mounts []*domain.VODMount) []fieldError {
	errs := make([]fieldError, 0, len(mounts))
	seen := make(map[domain.VODName]int, len(mounts))
	for i, m := range mounts {
		base := fmt.Sprintf("vod[%d]", i)
		if m == nil {
			errs = append(errs, fieldError{Path: base, Message: "vod mount entry is null"})
			continue
		}
		if err := domain.ValidateVODName(string(m.Name)); err != nil {
			errs = append(errs, fieldError{Path: base + ".name", Message: err.Error()})
		} else if prev, dup := seen[m.Name]; dup {
			errs = append(errs, fieldError{
				Path:    base + ".name",
				Message: fmt.Sprintf("duplicate vod mount name %q (also at index %d)", m.Name, prev),
			})
		} else {
			seen[m.Name] = i
		}
		if err := m.ValidateStorage(); err != nil {
			errs = append(errs, fieldError{Path: base + ".storage", Message: err.Error()})
		}
	}
	return errs
}

// validateGlobalConfig performs structural checks on the GlobalConfig section.
func validateGlobalConfig(c *domain.GlobalConfig) []fieldError {
	var errs []fieldError
	if c == nil {
		return []fieldError{{Path: "global_config", Message: "config is nil"}}
	}

	if c.Server != nil {
		errs = append(errs, validateServer(c.Server)...)
	}
	if c.Buffer != nil {
		if c.Buffer.Capacity <= 0 {
			errs = append(errs, fieldError{Path: "global_config.buffer.capacity", Message: "must be > 0"})
		}
	}
	if c.Publisher != nil {
		errs = append(errs, validatePublisher(c.Publisher)...)
	}
	if c.Hooks != nil {
		if c.Hooks.WorkerCount <= 0 {
			errs = append(errs, fieldError{Path: "global_config.hooks.worker_count", Message: "must be > 0"})
		}
	}
	if c.Log != nil {
		errs = append(errs, validateLog(c.Log)...)
	}
	if c.Listeners != nil {
		errs = append(errs, validateListeners(c.Listeners)...)
	}
	return errs
}

func validateServer(s *config.ServerConfig) []fieldError {
	var errs []fieldError
	if strings.TrimSpace(s.HTTPAddr) == "" {
		errs = append(errs, fieldError{Path: "global_config.server.http_addr", Message: "required (e.g. \":8080\")"})
	} else if _, _, err := net.SplitHostPort(s.HTTPAddr); err != nil {
		errs = append(errs, fieldError{Path: "global_config.server.http_addr", Message: "invalid host:port: " + err.Error()})
	}
	if s.CORS.AllowCredentials {
		for _, o := range s.CORS.AllowedOrigins {
			if o == "*" {
				errs = append(errs, fieldError{
					Path:    "global_config.server.cors.allow_credentials",
					Message: "must be false when allowed_origins contains \"*\"",
				})
				break
			}
		}
	}
	return errs
}

func validatePublisher(p *config.PublisherConfig) []fieldError {
	var errs []fieldError
	if p.HLS.LiveSegmentSec < 0 {
		errs = append(errs, fieldError{Path: "global_config.publisher.hls.live_segment_sec", Message: msgMustBeGEZero})
	}
	if p.DASH.LiveSegmentSec < 0 {
		errs = append(errs, fieldError{Path: "global_config.publisher.dash.live_segment_sec", Message: msgMustBeGEZero})
	}
	if p.HLS.Dir != "" && p.DASH.Dir != "" && p.HLS.Dir == p.DASH.Dir {
		errs = append(errs, fieldError{
			Path:    "global_config.publisher.dash.dir",
			Message: "must differ from publisher.hls.dir",
		})
	}
	return errs
}

// validateListeners checks the shared network-listener config that drives both
// ingest and play traffic on a single port per protocol.
func validateListeners(l *config.ListenersConfig) []fieldError {
	const portRequired = "required when listener is enabled"
	var errs []fieldError
	if l.RTMP.Enabled {
		errs = append(errs, validatePort("global_config.listeners.rtmp.port", l.RTMP.Port)...)
		if l.RTMP.Port == 0 {
			errs = append(errs, fieldError{Path: "global_config.listeners.rtmp.port", Message: portRequired})
		}
	}
	if l.RTSP.Enabled {
		errs = append(errs, validatePort("global_config.listeners.rtsp.port", l.RTSP.Port)...)
		if l.RTSP.Port == 0 {
			errs = append(errs, fieldError{Path: "global_config.listeners.rtsp.port", Message: portRequired})
		}
	}
	if l.SRT.Enabled {
		errs = append(errs, validatePort("global_config.listeners.srt.port", l.SRT.Port)...)
		if l.SRT.Port == 0 {
			errs = append(errs, fieldError{Path: "global_config.listeners.srt.port", Message: portRequired})
		}
	}
	if l.SRT.LatencyMS < 0 {
		errs = append(errs, fieldError{Path: "global_config.listeners.srt.latency_ms", Message: msgMustBeGEZero})
	}
	return errs
}

func validateLog(l *config.LogConfig) []fieldError {
	var errs []fieldError
	if l.Level != "" {
		switch strings.ToLower(l.Level) {
		case "trace", "debug", "info", "warn", "error":
		default:
			errs = append(errs, fieldError{Path: "global_config.log.level", Message: "must be trace|debug|info|warn|error"})
		}
	}
	if l.Format != "" {
		switch strings.ToLower(l.Format) {
		case "text", "json":
		default:
			errs = append(errs, fieldError{Path: "global_config.log.format", Message: "must be text|json"})
		}
	}
	return errs
}

// validateStreams checks each stream individually plus enforces that codes are
// unique across the whole list (the store can't tell us which one is the dup).
func validateStreams(streams []*domain.Stream) []fieldError {
	var errs []fieldError
	seen := make(map[domain.StreamCode]int, len(streams))
	for i, s := range streams {
		if s == nil {
			errs = append(errs, fieldError{
				Path:    fmt.Sprintf("streams[%d]", i),
				Message: "stream entry is null",
			})
			continue
		}
		base := fmt.Sprintf("streams[%d]", i)
		if err := domain.ValidateStreamCode(string(s.Code)); err != nil {
			errs = append(errs, fieldError{Path: base + ".code", Message: err.Error()})
		} else if prev, dup := seen[s.Code]; dup {
			errs = append(errs, fieldError{
				Path:    base + ".code",
				Message: fmt.Sprintf("duplicate stream code %q (also at index %d)", s.Code, prev),
			})
		} else {
			seen[s.Code] = i
		}
		if err := s.ValidateInputPriorities(); err != nil {
			errs = append(errs, fieldError{Path: base + ".inputs", Message: err.Error()})
		}
		if err := s.ValidateUniqueInputs(); err != nil {
			errs = append(errs, fieldError{Path: base + ".inputs", Message: err.Error()})
		}
	}
	return errs
}

// validateHooks checks each hook individually plus enforces unique IDs across
// the list. Type/target combinations are sanity-checked because a malformed
// hook would silently swallow events at runtime.
func validateHooks(hooks []*domain.Hook, fileRoot ...string) []fieldError {
	root := ""
	if len(fileRoot) > 0 {
		root = fileRoot[0]
	}
	errs := make([]fieldError, 0, len(hooks))
	seen := make(map[domain.HookID]int, len(hooks))
	for i, hk := range hooks {
		errs = append(errs, validateHook(hk, i, seen, root)...)
	}
	return errs
}

func validateHook(hk *domain.Hook, i int, seen map[domain.HookID]int, fileRoot string) []fieldError {
	base := fmt.Sprintf("hooks[%d]", i)
	if hk == nil {
		return []fieldError{{Path: base, Message: "hook entry is null"}}
	}
	var errs []fieldError
	errs = append(errs, validateHookID(hk, i, base, seen)...)
	if err := hk.Validate(fileRoot); err != nil {
		errs = append(errs, fieldError{Path: base + ".target", Message: err.Error()})
	}
	if hk.MaxRetries < 0 {
		errs = append(errs, fieldError{Path: base + ".max_retries", Message: msgMustBeGEZero})
	}
	if hk.TimeoutSec < 0 {
		errs = append(errs, fieldError{Path: base + ".timeout_sec", Message: msgMustBeGEZero})
	}
	if hk.StreamCodes != nil && len(hk.StreamCodes.Only) > 0 && len(hk.StreamCodes.Except) > 0 {
		errs = append(errs, fieldError{
			Path:    base + ".stream_codes",
			Message: "only and except are mutually exclusive",
		})
	}
	return errs
}

func validateHookID(hk *domain.Hook, i int, base string, seen map[domain.HookID]int) []fieldError {
	if strings.TrimSpace(string(hk.ID)) == "" {
		return []fieldError{{Path: base + ".id", Message: "required"}}
	}
	if prev, dup := seen[hk.ID]; dup {
		return []fieldError{{
			Path:    base + ".id",
			Message: fmt.Sprintf("duplicate hook id %q (also at index %d)", hk.ID, prev),
		}}
	}
	seen[hk.ID] = i
	return nil
}

// validatePort allows 0 (disabled) or a real TCP/UDP port number.
func validatePort(path string, port int) []fieldError {
	if port == 0 || (port >= 1 && port <= 65535) {
		return nil
	}
	return []fieldError{{Path: path, Message: "must be 0 (disabled) or 1-65535"}}
}
