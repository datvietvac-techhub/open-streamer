package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/datvietvac-techhub/open-streamer/internal/autopublish"
	"github.com/datvietvac-techhub/open-streamer/internal/coordinator"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/samber/do/v2"
)

// TemplateHandler handles template CRUD endpoints. On Save it also hot-reloads
// every stream that inherits from the template by recomputing the resolved
// stream config (template merged with stream-level overrides) and calling
// coordinator.Update for each running dependent. After every Save / Delete
// it asks the autopublish service to rebuild its in-memory prefix matcher
// so the new prefix list takes effect on the next push attempt.
type TemplateHandler struct {
	templateRepo store.TemplateRepository
	streamRepo   store.StreamRepository
	coordinator  streamCoordinator
	autopublish  *autopublish.Service
	bus          events.Bus
}

// NewTemplateHandler creates a TemplateHandler and registers it with DI.
func NewTemplateHandler(i do.Injector) (*TemplateHandler, error) {
	return &TemplateHandler{
		templateRepo: do.MustInvoke[store.TemplateRepository](i),
		streamRepo:   do.MustInvoke[store.StreamRepository](i),
		coordinator:  do.MustInvoke[*coordinator.Coordinator](i),
		autopublish:  do.MustInvoke[*autopublish.Service](i),
		bus:          do.MustInvoke[events.Bus](i),
	}, nil
}

// refreshAutoPublish rebuilds the autopublish service's matcher snapshot
// after a template was saved or deleted. Nil-safe so tests that construct
// the handler without a wired autopublish service still work; failures
// are logged at WARN — the matcher will catch up on the next save and
// runtime streams are not data-critical.
func (h *TemplateHandler) refreshAutoPublish(ctx context.Context) {
	if h.autopublish == nil {
		return
	}
	if err := h.autopublish.RefreshTemplates(ctx); err != nil {
		slog.Warn("template handler: autopublish refresh failed", "err", err)
	}
}

// publishTemplateEvent emits a template lifecycle meta-event. Nil-safe so
// tests can construct the handler without a bus.
func (h *TemplateHandler) publishTemplateEvent(r *http.Request, typ domain.EventType, code domain.TemplateCode) {
	if h.bus == nil || code == "" {
		return
	}
	h.bus.Publish(r.Context(), domain.Event{
		Type:    typ,
		Payload: map[string]any{"template_code": string(code)},
	})
}

// List templates.
// @Summary List templates
// @Tags templates
// @Produce json
// @Success 200 {object} map[string]any
// @Failure 500 {object} apidocs.ErrorBody
// @Router /templates [get].
func (h *TemplateHandler) List(w http.ResponseWriter, r *http.Request) {
	tpls, err := h.templateRepo.List(r.Context())
	if err != nil {
		serverError(w, r, "LIST_FAILED", "list templates", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": tpls, "total": len(tpls)})
}

// Get returns one template by code.
// @Summary Get template
// @Tags templates
// @Produce json
// @Param code path string true "Template code"
// @Success 200 {object} map[string]any
// @Failure 404 {object} apidocs.ErrorBody
// @Router /templates/{code} [get].
func (h *TemplateHandler) Get(w http.ResponseWriter, r *http.Request) {
	code := domain.TemplateCode(chi.URLParam(r, "code"))
	if err := domain.ValidateTemplateCode(string(code)); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_TEMPLATE_CODE", err.Error())
		return
	}
	tpl, err := h.templateRepo.FindByCode(r.Context(), code)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": tpl})
}

// Put creates or replaces a template, then hot-reloads every running stream
// that inherits from it. The body's code field is ignored — the URL param
// is authoritative — so an operator can't rename a template by editing the
// body (which would silently orphan dependent streams).
// @Summary Create or update template
// @Tags templates
// @Accept json
// @Produce json
// @Param code path string true "Template code"
// @Param body body domain.Template true "Template configuration"
// @Success 200 {object} map[string]any
// @Success 201 {object} map[string]any
// @Failure 400 {object} apidocs.ErrorBody
// @Failure 500 {object} apidocs.ErrorBody
// @Router /templates/{code} [post].
func (h *TemplateHandler) Put(w http.ResponseWriter, r *http.Request) {
	code := domain.TemplateCode(chi.URLParam(r, "code"))
	if err := domain.ValidateTemplateCode(string(code)); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_TEMPLATE_CODE", err.Error())
		return
	}

	var body domain.Template
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_BODY", err.Error())
		return
	}
	body.Code = code // URL param wins so the persisted record can't drift

	if err := domain.ValidatePrefixes(body.Prefixes); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_PREFIX", err.Error())
		return
	}
	if conflict, err := h.findConflictingPrefix(r.Context(), code, body.Prefixes); err != nil {
		serverError(w, r, "LIST_FAILED", "scan templates for prefix conflicts", err)
		return
	} else if conflict != nil {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":            "PREFIX_OVERLAP",
			"message":          conflict.message,
			"conflicting_with": string(conflict.otherTemplate),
			"prefix":           conflict.ourPrefix,
			"overlaps":         conflict.otherPrefix,
		})
		return
	}

	prior, _ := h.templateRepo.FindByCode(r.Context(), code)
	created := prior == nil

	if err := h.templateRepo.Save(r.Context(), &body); err != nil {
		serverError(w, r, "SAVE_FAILED", "save template", err)
		return
	}

	// Hot-reload dependents. The pipeline context is detached from the HTTP
	// request because stream pipelines outlive the request — see the same
	// pattern in StreamHandler.Put.
	pipelineCtx := context.WithoutCancel(r.Context())
	if err := h.reloadStreams(pipelineCtx, prior, &body); err != nil {
		serverError(w, r, "RELOAD_FAILED", "reload dependent streams", err)
		return
	}

	h.refreshAutoPublish(pipelineCtx)

	eventType := domain.EventTemplateUpdated
	status := http.StatusOK
	if created {
		eventType = domain.EventTemplateCreated
		status = http.StatusCreated
	}
	h.publishTemplateEvent(r, eventType, body.Code)
	writeJSON(w, status, map[string]any{"data": body})
}

// Delete removes a template. Returns 409 Conflict when any stream still
// references it — operators must detach the dependent streams first
// (PUT the stream with Template:null) before the template can be removed.
// @Summary Delete template
// @Tags templates
// @Produce json
// @Param code path string true "Template code"
// @Success 204 "No Content"
// @Failure 404 {object} apidocs.ErrorBody
// @Failure 409 {object} apidocs.ErrorBody
// @Failure 500 {object} apidocs.ErrorBody
// @Router /templates/{code} [delete].
func (h *TemplateHandler) Delete(w http.ResponseWriter, r *http.Request) {
	code := domain.TemplateCode(chi.URLParam(r, "code"))
	if err := domain.ValidateTemplateCode(string(code)); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_TEMPLATE_CODE", err.Error())
		return
	}

	if _, err := h.templateRepo.FindByCode(r.Context(), code); err != nil {
		writeStoreError(w, r, err)
		return
	}

	refs, err := h.findReferencingStreams(r.Context(), code)
	if err != nil {
		serverError(w, r, "LIST_FAILED", "scan streams for template references", err)
		return
	}
	if len(refs) > 0 {
		codes := make([]string, 0, len(refs))
		for _, s := range refs {
			codes = append(codes, string(s.Code))
		}
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":   "TEMPLATE_IN_USE",
			"message": "template is referenced by one or more streams",
			"streams": codes,
		})
		return
	}

	if err := h.templateRepo.Delete(r.Context(), code); err != nil {
		serverError(w, r, "DELETE_FAILED", "delete template", err)
		return
	}
	h.refreshAutoPublish(r.Context())
	h.publishTemplateEvent(r, domain.EventTemplateDeleted, code)
	w.WriteHeader(http.StatusNoContent)
}

// reloadStreams walks every stream referencing the template and asks the
// coordinator to hot-reload the resolved config. Streams that are not
// currently running are skipped — the next bootstrap / start picks up the
// new template automatically. Errors per-stream are aggregated; one bad
// stream does not block the rest from reloading.
func (h *TemplateHandler) reloadStreams(ctx context.Context, prior, next *domain.Template) error {
	if next == nil {
		return errors.New("template handler: next template is nil")
	}
	streams, err := h.streamRepo.List(ctx, store.StreamFilter{})
	if err != nil {
		return err
	}
	var errs []error
	for _, s := range streams {
		if s == nil || s.Template == nil || *s.Template != next.Code {
			continue
		}
		if !h.coordinator.IsRunning(s.Code) {
			continue
		}
		oldResolved := domain.ResolveStream(s, prior)
		newResolved := domain.ResolveStream(s, next)
		if err := h.coordinator.Update(ctx, oldResolved, newResolved); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// findReferencingStreams returns every stream whose Template field points at
// code. Linear scan is fine for the dataset sizes Open-Streamer targets;
// callers index by code in memory if hot-path lookups ever become needed.
func (h *TemplateHandler) findReferencingStreams(ctx context.Context, code domain.TemplateCode) ([]*domain.Stream, error) {
	all, err := h.streamRepo.List(ctx, store.StreamFilter{})
	if err != nil {
		return nil, err
	}
	var refs []*domain.Stream
	for _, s := range all {
		if s != nil && s.Template != nil && *s.Template == code {
			refs = append(refs, s)
		}
	}
	return refs, nil
}

// prefixConflict describes an overlap between two prefixes that belong to
// different templates. Surfaced in the API response so the operator can
// see exactly which other template owns the conflicting prefix.
type prefixConflict struct {
	otherTemplate domain.TemplateCode
	ourPrefix     string
	otherPrefix   string
	message       string
}

// findConflictingPrefix scans every OTHER template and returns the first
// (ours, theirs) pair where the prefixes overlap. self is excluded from
// the scan so an update to an existing template doesn't conflict with
// its own previous prefixes. Returns nil when there are no conflicts.
func (h *TemplateHandler) findConflictingPrefix(
	ctx context.Context,
	self domain.TemplateCode,
	prefixes []string,
) (*prefixConflict, error) {
	if len(prefixes) == 0 {
		return nil, nil
	}
	all, err := h.templateRepo.List(ctx)
	if err != nil {
		return nil, err
	}
	for _, other := range all {
		if other == nil || other.Code == self {
			continue
		}
		for _, ours := range prefixes {
			for _, theirs := range other.Prefixes {
				if !domain.PrefixesOverlap(ours, theirs) {
					continue
				}
				return &prefixConflict{
					otherTemplate: other.Code,
					ourPrefix:     ours,
					otherPrefix:   theirs,
					message: "prefix " + ours + " overlaps with " + theirs +
						" owned by template " + string(other.Code),
				}, nil
			}
		}
	}
	return nil, nil
}
