# Open Streamer — local development shortcuts.
# Requires: Go (see go.mod), optional: golangci-lint, govulncheck, gofumpt.
# Production deploy: build/install.sh (Linux + systemd).

SHELL       := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

# --- paths ---
MAIN_PKG    := ./cmd/server
BIN_DIR     := bin
BIN_NAME    := open-streamer
INSTALL_SH  := build/install.sh

# Per-stream transcoder subprocess (in-process libavcodec pipeline).
# Built as a separate binary so the supervisor in internal/transcoder/
# service.go can crash-recover one stream without taking down the whole
# server. The native package needs cgo + libav-dev headers — install:
#   apt install libavcodec-dev libavfilter-dev libavformat-dev \
#               libswscale-dev libavutil-dev pkg-config
TRANSCODER_PKG  := ./cmd/open-streamer-transcoder
TRANSCODER_NAME := open-streamer-transcoder

# --- go ---
GO          ?= go
GOFLAGS     ?=
TESTFLAGS   ?= -race -shuffle=on -count=1 -timeout=5m
COVER_OUT   := coverage.out

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_.-]+:.*?##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: all
all: tidy vet build test ## tidy, vet, build, test (race)

# --- build & run ---

# --- version stamping ---
# Injected into pkg/version at build time. CI workflows can override
# VERSION (e.g. VERSION=v0.0.7 make build) to stamp release tags;
# local builds default to `git describe` + short SHA fallback.
VERSION_PKG := github.com/ntt0601zcoder/open-streamer/pkg/version
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILT_AT    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS     ?= -s -w \
               -X $(VERSION_PKG).Version=$(VERSION) \
               -X $(VERSION_PKG).Commit=$(COMMIT) \
               -X $(VERSION_PKG).BuiltAt=$(BUILT_AT)

.PHONY: build
build: ## Compile server binary to $(BIN_DIR)/$(BIN_NAME) with version stamping
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o $(BIN_DIR)/$(BIN_NAME) $(MAIN_PKG)

.PHONY: build-transcoder
build-transcoder: ## Compile per-stream transcoder subprocess (cgo + libav-dev required)
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=1 $(GO) build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o $(BIN_DIR)/$(TRANSCODER_NAME) $(TRANSCODER_PKG)

.PHONY: build-all
build-all: build build-transcoder ## Build both server + transcoder binaries

.PHONY: run
run: ## Run server without building a persistent binary
	$(GO) run $(GOFLAGS) $(MAIN_PKG)

.PHONY: install
install: ## Install server binary to $$GOBIN or $$GOPATH/bin (with version stamping)
	$(GO) install $(GOFLAGS) -ldflags="$(LDFLAGS)" $(MAIN_PKG)

.PHONY: build-dev
build-dev: ## Cross-compile a release-style tarball for a Go-less server (default linux/amd64, version=dev)
	./scripts/build-dev.sh

# --- quality ---

.PHONY: generate
generate: ## Run go generate (e.g. Swagger from swag annotations)
	$(GO) generate ./cmd/server/...

.PHONY: swagger
swagger: ## Regenerate api/docs (OpenAPI 2) via swag; run from repo root
	cd cmd/server && $(GO) run github.com/swaggo/swag/cmd/swag@latest init -g doc_swagger.go -o ../../api/docs --parseGoList=false -d .,../../internal/api,../../internal/api/handler,../../internal/api/apidocs,../../internal/domain,../../internal/manager,../../internal/publisher,../../internal/transcoder,../../internal/vod,../../config,../../pkg/version

.PHONY: hooks-install
hooks-install: ## Install repo git hooks (pre-commit auto-regenerates swagger)
	@hook_dst="$$(git rev-parse --git-path hooks)/pre-commit"; \
	hook_src="$$(git rev-parse --show-toplevel)/scripts/git-hooks/pre-commit"; \
	rm -f "$$hook_dst"; \
	ln -s "$$hook_src" "$$hook_dst"; \
	echo "linked $$hook_dst → $$hook_src"

.PHONY: tidy
tidy: ## go mod tidy
	$(GO) mod tidy

.PHONY: verify
verify: ## go mod verify
	$(GO) mod verify

.PHONY: vet
vet: ## go vet ./...
	$(GO) vet ./...

.PHONY: fmt
fmt: ## Format with gofmt (use gofumpt if installed)
	@command -v gofumpt >/dev/null 2>&1 && gofumpt -w . || $(GO) fmt ./...

.PHONY: lint
lint: ## golangci-lint run ./... (install: https://golangci-lint.run/)
	golangci-lint run ./...

.PHONY: vulncheck
vulncheck: ## govulncheck ./... (install: go install golang.org/x/vuln/cmd/govulncheck@latest)
	govulncheck ./...

.PHONY: check
check: tidy vet lint test ## Full local check (tidy, vet, lint, test)

# --- test ---

.PHONY: test
test: ## go test with race + shuffle
	$(GO) test $(TESTFLAGS) ./...

.PHONY: test-norace
test-norace: ## go test without -race (faster / constrained envs)
	$(GO) test -shuffle=on -count=1 -timeout=5m ./...

.PHONY: test-integration
test-integration: ## Spawn real ffmpeg to validate generated args (auto-skips on missing ffmpeg / CUDA)
	$(GO) test -tags integration -count=1 -timeout=3m -v ./internal/transcoder/...

.PHONY: cover
cover: ## Generate coverage.out and print total %
	$(GO) test -race -shuffle=on -count=1 -timeout=5m -coverprofile=$(COVER_OUT) ./...
	$(GO) tool cover -func=$(COVER_OUT) | tail -1

.PHONY: cover-html
cover-html: cover ## Open HTML coverage (macOS: open; else print path)
	$(GO) tool cover -html=$(COVER_OUT) -o coverage.html
	@echo "Wrote coverage.html"

# --- cleanup ---

.PHONY: clean
clean: ## Remove bin/, coverage artifacts
	rm -rf $(BIN_DIR) $(COVER_OUT) coverage.html

# --- deploy (Linux systemd) ---

.PHONY: install-service
install-service: build ## Install binary + systemd unit (requires sudo, Linux only)
	sudo $(INSTALL_SH) install

.PHONY: uninstall-service
uninstall-service: ## Stop service, remove binary + unit + user (data dir kept)
	sudo $(INSTALL_SH) uninstall
