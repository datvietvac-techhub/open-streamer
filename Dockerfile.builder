# Dockerfile.builder — reproducible build environment for the native
# transcoder subprocess (cgo + go-astiav + libavcodec).
#
# Build the image ONCE:
#   make builder-image                  # tags ghcr.io/ntt0601zcoder/open-streamer-builder:v1
#   docker push ...                     # optional, only when sharing across machines
#
# Then every transcoder build is fast (no apt-get update, no Go download)
# because Dockerfile.transcoder `FROM open-streamer-builder:v1`.
#
# Pinned dependencies:
#   - debian:sid-slim    — ships FFmpeg 8.1.x (matches go-astiav v0.41 ABI)
#   - Go 1.25.10         — matches go.mod toolchain directive
#   - libav-dev 8.1.x    — header set the cgo bindings expect
#
# Bumping any of these requires a new builder image tag so existing
# checkouts keep reproducing identical binaries until they're ready.
#
# Why debian:sid (unstable) rather than trixie (testing): sid is the
# only Debian branch shipping FFmpeg 8 right now. Trixie tops at 7.1
# which go-astiav v0.41 rejects at compile time (missing codec ID
# constants added in FFmpeg 8). Once trixie inherits FFmpeg 8 we'll
# move there for stable apt mirrors.

FROM debian:sid-slim

# apt + libav + build tools — single layer so docker layer cache
# stays usable when the source changes.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git pkg-config \
    build-essential \
    libavcodec-dev libavfilter-dev libavformat-dev \
    libswscale-dev libavutil-dev libavdevice-dev \
    libswresample-dev \
    && rm -rf /var/lib/apt/lists/*

# Go toolchain matching go.mod. Override at build time:
#   docker build --build-arg GO_VERSION=1.26.0 ...
ARG GO_VERSION=1.25.10
RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" \
    | tar -C /usr/local -xz
ENV PATH=/usr/local/go/bin:/root/go/bin:$PATH \
    GOPATH=/root/go \
    GOCACHE=/root/.cache/go-build \
    GOPROXY=https://proxy.golang.org,direct

# Sanity stamp so `docker run open-streamer-builder go version` works
# and CI logs show what was actually used.
RUN go version && gcc --version | head -1 && pkg-config --modversion libavcodec

WORKDIR /src
