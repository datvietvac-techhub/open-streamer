#!/usr/bin/env bash
# Build a "dev" release tarball locally so you can ship it to a server
# that has no Go toolchain installed.
#
# The output archive matches the layout produced by .github/workflows/release.yml,
# so you can untar it and run build/install.sh exactly the same way you handle
# the GitHub releases.
#
# Usage:
#   scripts/build-dev.sh                          # → linux/amd64, version=dev
#   scripts/build-dev.sh -o linux -a arm64        # raspberry pi-class server
#   VERSION=dev-test scripts/build-dev.sh         # custom version label
#   scripts/build-dev.sh --bin-only               # just the binary, no tarball
#
# After it finishes, push the tarball to the server, e.g.:
#   scp dist/open-streamer-dev-linux-amd64.tar.gz user@host:/tmp/
#   ssh user@host
#   cd /tmp && tar xzf open-streamer-dev-linux-amd64.tar.gz
#   sudo ./open-streamer-linux-amd64/build/install.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# --- defaults & flag parsing ------------------------------------------------

GOOS_TARGET="${GOOS_TARGET:-linux}"
GOARCH_TARGET="${GOARCH_TARGET:-amd64}"
VERSION="${VERSION:-dev}"
BIN_ONLY=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        -o|--os)        GOOS_TARGET="$2"; shift 2 ;;
        -a|--arch)      GOARCH_TARGET="$2"; shift 2 ;;
        -v|--version)   VERSION="$2"; shift 2 ;;
        --bin-only)     BIN_ONLY=1; shift ;;
        -h|--help)
            sed -n '2,20p' "${BASH_SOURCE[0]}"
            exit 0
            ;;
        *)
            echo "unknown flag: $1" >&2
            exit 2
            ;;
    esac
done

# --- preflight --------------------------------------------------------------

if ! command -v go >/dev/null 2>&1; then
    echo "error: go toolchain not found on this machine" >&2
    echo "(run this script on your laptop, not the server)" >&2
    exit 1
fi

# --- version stamping ------------------------------------------------------

COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
BUILT_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
VPKG="github.com/ntt0601zcoder/open-streamer/pkg/version"

# Mark dirty trees so a binary built with uncommitted changes is identifiable.
if ! git diff --quiet 2>/dev/null || ! git diff --cached --quiet 2>/dev/null; then
    COMMIT="${COMMIT}-dirty"
fi

LDFLAGS="-s -w \
    -X ${VPKG}.Version=${VERSION} \
    -X ${VPKG}.Commit=${COMMIT} \
    -X ${VPKG}.BuiltAt=${BUILT_AT}"

# --- build ------------------------------------------------------------------

STAGE_NAME="open-streamer-${GOOS_TARGET}-${GOARCH_TARGET}"
STAGE_DIR="dist/${STAGE_NAME}"

rm -rf "${STAGE_DIR}"
mkdir -p "${STAGE_DIR}/bin"

BIN_EXT=""
if [[ "${GOOS_TARGET}" == "windows" ]]; then
    BIN_EXT=".exe"
fi
BIN_PATH="${STAGE_DIR}/bin/open-streamer${BIN_EXT}"

echo "→ building ${GOOS_TARGET}/${GOARCH_TARGET} version=${VERSION} commit=${COMMIT}"

GOOS="${GOOS_TARGET}" GOARCH="${GOARCH_TARGET}" CGO_ENABLED=0 \
    go build \
        -trimpath \
        -ldflags="${LDFLAGS}" \
        -o "${BIN_PATH}" \
        ./cmd/server

# Mirror release.yml: ship installer + systemd unit for Linux targets.
if [[ "${GOOS_TARGET}" == "linux" ]]; then
    mkdir -p "${STAGE_DIR}/build"
    cp build/install.sh           "${STAGE_DIR}/build/"
    cp build/open-streamer.service "${STAGE_DIR}/build/"
    chmod +x "${STAGE_DIR}/build/install.sh" "${BIN_PATH}"

    # Bundle the per-stream native transcoder subprocess + its libav
    # 8 .so files. Build-time only for linux/amd64; install.sh on the
    # target unpacks them under /opt/open-streamer-native/ and writes
    # a systemd drop-in that exports LD_LIBRARY_PATH so the
    # subprocess finds the bundled libs instead of the host's libav
    # 6.1 (Ubuntu 24.04 LTS). Skip silently if the bundle hasn't been
    # built — `make build-dev` works for users who don't need the
    # transcoder; they just won't be able to start transcoded streams.
    TRANSCODER_BUNDLE="dist/transcoder-${GOOS_TARGET}-${GOARCH_TARGET}"
    if [[ "${GOARCH_TARGET}" == "amd64" && -f "${TRANSCODER_BUNDLE}/open-streamer-transcoder" ]]; then
        echo "→ bundling native transcoder ($(du -sh "${TRANSCODER_BUNDLE}" | awk '{print $1}'))"
        mkdir -p "${STAGE_DIR}/transcoder/bin" "${STAGE_DIR}/transcoder/lib"
        cp "${TRANSCODER_BUNDLE}/open-streamer-transcoder" "${STAGE_DIR}/transcoder/bin/"
        cp -a "${TRANSCODER_BUNDLE}/lib/." "${STAGE_DIR}/transcoder/lib/"
        chmod +x "${STAGE_DIR}/transcoder/bin/open-streamer-transcoder"
    elif [[ "${GOARCH_TARGET}" == "amd64" ]]; then
        echo "→ no native transcoder bundle at ${TRANSCODER_BUNDLE}; skipping"
        echo "  build it first with: make build-transcoder-linux"
    fi
fi

# Drop a VERSION file so ops can identify what's installed without
# running the binary.
{
    echo "version=${VERSION}"
    echo "commit=${COMMIT}"
    echo "built_at=${BUILT_AT}"
    echo "os=${GOOS_TARGET}"
    echo "arch=${GOARCH_TARGET}"
} > "${STAGE_DIR}/VERSION"

cp README.md LICENSE "${STAGE_DIR}/" 2>/dev/null || true

# --- package ----------------------------------------------------------------

if [[ "${BIN_ONLY}" -eq 1 ]]; then
    echo "✓ binary at: ${BIN_PATH}"
    exit 0
fi

ARCHIVE="dist/open-streamer-${VERSION}-${GOOS_TARGET}-${GOARCH_TARGET}.tar.gz"
# COPYFILE_DISABLE + (when supported) --no-mac-metadata silence the
# LIBARCHIVE.xattr.com.apple.provenance warnings GNU tar prints when
# extracting macOS-built archives on Linux.
NO_MAC_META=""
if tar --help 2>&1 | grep -q -- '--no-mac-metadata'; then
    NO_MAC_META="--no-mac-metadata"
fi
COPYFILE_DISABLE=1 tar ${NO_MAC_META} -C dist -czf "${ARCHIVE}" "${STAGE_NAME}"

# Sidecar checksum so you can verify the upload on the server.
(cd dist && shasum -a 256 "$(basename "${ARCHIVE}")" > "$(basename "${ARCHIVE}").sha256")

echo "✓ tarball: ${ARCHIVE}"
echo "  sha256:  $(awk '{print $1}' "${ARCHIVE}.sha256")"
echo
echo "next steps:"
echo "  scp ${ARCHIVE} user@host:/tmp/"
echo "  ssh user@host 'cd /tmp && tar xzf $(basename "${ARCHIVE}") && sudo ./${STAGE_NAME}/build/install.sh'"
