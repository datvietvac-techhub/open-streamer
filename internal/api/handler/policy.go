package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/events"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/samber/do/v2"
)

// policyAuthorizer is the subset of *mediaauth.Authorizer the handler needs to
// hot-swap the compiled policy set after a CRUD change. Defined here (not
// imported) so the handler stays decoupled from the mediaauth package and is
// trivially mockable in tests.
type policyAuthorizer interface {
	SetPolicies([]*domain.Policy)
}

// PolicyHandler handles media-auth Policy CRUD endpoints. After every Save /
// Delete it reloads the authorizer's compiled rule set so changes take effect
// immediately, without restarting any stream. Delete refuses (409) while any
// stream or template still references the policy.
type PolicyHandler struct {
	policyRepo   store.PolicyRepository
	streamRepo   store.StreamRepository
	templateRepo store.TemplateRepository
	authz        policyAuthorizer
	bus          events.Bus
}

// NewPolicyHandler creates a PolicyHandler and registers it with DI. The
// authorizer is wired separately via SetAuthorizer because it is built after
// the publisher in the post-DI wiring step (see cmd/server.wireMediaAuth).
func NewPolicyHandler(i do.Injector) (*PolicyHandler, error) {
	return &PolicyHandler{
		policyRepo:   do.MustInvoke[store.PolicyRepository](i),
		streamRepo:   do.MustInvoke[store.StreamRepository](i),
		templateRepo: do.MustInvoke[store.TemplateRepository](i),
		bus:          do.MustInvoke[events.Bus](i),
	}, nil
}

// SetAuthorizer wires the media-auth authorizer so CRUD changes hot-reload its
// compiled policy set. Nil-safe — tests may leave it unset.
func (h *PolicyHandler) SetAuthorizer(a policyAuthorizer) { h.authz = a }

// reloadAuthorizer rebuilds the authorizer's compiled policy set from the store.
// Nil-safe; failures are logged at WARN — the on-disk state is already the
// source of truth and the next change retries the swap.
func (h *PolicyHandler) reloadAuthorizer(ctx context.Context) {
	if h.authz == nil {
		return
	}
	ps, err := h.policyRepo.List(ctx)
	if err != nil {
		slog.Warn("policy handler: authorizer reload failed", "err", err)
		return
	}
	h.authz.SetPolicies(ps)
}

func (h *PolicyHandler) publishPolicyEvent(r *http.Request, typ domain.EventType, code domain.PolicyCode) {
	if h.bus == nil || code == "" {
		return
	}
	h.bus.Publish(r.Context(), domain.Event{
		Type:    typ,
		Payload: map[string]any{"policy_code": string(code)},
	})
}

// List policies.
// @Summary List media-auth policies
// @Tags policies
// @Produce json
// @Success 200 {object} map[string]any
// @Failure 500 {object} apidocs.ErrorBody
// @Router /policies [get].
func (h *PolicyHandler) List(w http.ResponseWriter, r *http.Request) {
	ps, err := h.policyRepo.List(r.Context())
	if err != nil {
		serverError(w, r, "LIST_FAILED", "list policies", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": ps, "total": len(ps)})
}

// Get returns one policy by code.
// @Summary Get media-auth policy
// @Tags policies
// @Produce json
// @Param code path string true "Policy code"
// @Success 200 {object} map[string]any
// @Failure 404 {object} apidocs.ErrorBody
// @Router /policies/{code} [get].
func (h *PolicyHandler) Get(w http.ResponseWriter, r *http.Request) {
	code := domain.PolicyCode(chi.URLParam(r, "code"))
	if err := domain.ValidatePolicyCode(string(code)); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_POLICY_CODE", err.Error())
		return
	}
	p, err := h.policyRepo.FindByCode(r.Context(), code)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": p})
}

// Put creates or replaces a policy, then hot-reloads the authorizer. The body's
// code field is ignored — the URL param is authoritative — so an operator can't
// rename a policy by editing the body (which would orphan dependent streams).
// @Summary Create or update media-auth policy
// @Tags policies
// @Accept json
// @Produce json
// @Param code path string true "Policy code"
// @Param body body domain.Policy true "Policy configuration"
// @Success 200 {object} map[string]any
// @Success 201 {object} map[string]any
// @Failure 400 {object} apidocs.ErrorBody
// @Failure 500 {object} apidocs.ErrorBody
// @Router /policies/{code} [post].
func (h *PolicyHandler) Put(w http.ResponseWriter, r *http.Request) {
	code := domain.PolicyCode(chi.URLParam(r, "code"))
	if err := domain.ValidatePolicyCode(string(code)); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_POLICY_CODE", err.Error())
		return
	}

	var body domain.Policy
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_BODY", err.Error())
		return
	}
	body.Code = code // URL param wins so the persisted record can't drift
	if err := body.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_POLICY", err.Error())
		return
	}

	prior, _ := h.policyRepo.FindByCode(r.Context(), code)
	created := prior == nil

	if err := h.policyRepo.Save(r.Context(), &body); err != nil {
		serverError(w, r, "SAVE_FAILED", "save policy", err)
		return
	}
	h.reloadAuthorizer(r.Context())

	eventType := domain.EventPolicyUpdated
	status := http.StatusOK
	if created {
		eventType = domain.EventPolicyCreated
		status = http.StatusCreated
	}
	h.publishPolicyEvent(r, eventType, body.Code)
	writeJSON(w, status, map[string]any{"data": body})
}

// Delete removes a policy. Returns 409 Conflict when any stream or template
// still references it — operators must detach the dependents first.
// @Summary Delete media-auth policy
// @Tags policies
// @Produce json
// @Param code path string true "Policy code"
// @Success 204 "No Content"
// @Failure 404 {object} apidocs.ErrorBody
// @Failure 409 {object} apidocs.ErrorBody
// @Failure 500 {object} apidocs.ErrorBody
// @Router /policies/{code} [delete].
func (h *PolicyHandler) Delete(w http.ResponseWriter, r *http.Request) {
	code := domain.PolicyCode(chi.URLParam(r, "code"))
	if err := domain.ValidatePolicyCode(string(code)); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_POLICY_CODE", err.Error())
		return
	}

	if _, err := h.policyRepo.FindByCode(r.Context(), code); err != nil {
		writeStoreError(w, r, err)
		return
	}

	streams, templates, err := h.findReferences(r.Context(), code)
	if err != nil {
		serverError(w, r, "LIST_FAILED", "scan references for policy", err)
		return
	}
	if len(streams) > 0 || len(templates) > 0 {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":     "POLICY_IN_USE",
			"message":   "policy is referenced by one or more streams or templates",
			"streams":   streams,
			"templates": templates,
		})
		return
	}

	if err := h.policyRepo.Delete(r.Context(), code); err != nil {
		serverError(w, r, "DELETE_FAILED", "delete policy", err)
		return
	}
	h.reloadAuthorizer(r.Context())
	h.publishPolicyEvent(r, domain.EventPolicyDeleted, code)
	w.WriteHeader(http.StatusNoContent)
}

// findReferences returns the codes of every stream and template that binds to
// the policy. Linear scans are fine for the dataset sizes Open-Streamer targets.
func (h *PolicyHandler) findReferences(ctx context.Context, code domain.PolicyCode) (streams, templates []string, err error) {
	ss, err := h.streamRepo.List(ctx, store.StreamFilter{})
	if err != nil {
		return nil, nil, err
	}
	for _, s := range ss {
		if s != nil && s.PlaybackPolicy == code {
			streams = append(streams, string(s.Code))
		}
	}
	tpls, err := h.templateRepo.List(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, t := range tpls {
		if t != nil && t.PlaybackPolicy == code {
			templates = append(templates, string(t.Code))
		}
	}
	return streams, templates, nil
}
