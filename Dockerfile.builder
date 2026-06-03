# Dockerfile.builder — reproducible build environment for the native
# transcoder subprocess (cgo + go-astiav + libav 8).
#
# Build the image ONCE:
#   make builder-image                  # tags open-streamer-builder:v1
#
# Why the nvidia/cuda 12.2 -devel-ubuntu22.04 base:
#
#   * CUDA 12.2 toolkit — FFmpeg's --enable-cuda-nvcc embeds the
#     scale_cuda kernels as PTX that the host driver JITs at runtime.
#     PTX is versioned by the TOOLKIT, and the target hosts run driver
#     535 (CUDA 12.2 PTX ceiling). A newer toolkit (12.4+ / 12.6) emits
#     PTX the 535 driver refuses to load (CUDA_ERROR_UNSUPPORTED_PTX_
#     VERSION). sm_75 SASS doesn't help — FFmpeg ships PTX, not cubin —
#     so the toolkit MUST be ≤ 12.2. Bump only after every host's driver
#     is upgraded (alongside nv-codec-headers below).
#   * ubuntu22.04 (glibc 2.35) — older than the target's 2.39, so the
#     bundled .so files keep a 2.35 ABI ceiling that loads fine on 2.39
#     (glibc is backward compatible; only a NEWER glibc than the target
#     breaks, which is why Debian sid's 2.42 was rejected).
#
# The -devel tag ships nvcc + CUDA headers; the runtime needs only
# libcuda from the host driver (dlopen'd via ffnvcodec), so no CUDA
# runtime lib gets bundled.
#
# FFmpeg 8 is not in apt; we compile from source (~5 min one-time).

FROM nvidia/cuda:12.2.2-devel-ubuntu22.04

# Mirror retry tuning — Ubuntu archives occasionally rate-limit when
# pulling the full build-essential set.
RUN printf 'Acquire::Retries "10";\nAcquire::http::Timeout "30";\nAcquire::https::Timeout "30";\nAcquire::ForceIPv4 "true";\n' \
      > /etc/apt/apt.conf.d/80-retries

# 1. Toolchain + FFmpeg build deps. Same layer so the apt cache is
#    reused across rebuilds when go.mod / source changes but the
#    builder image doesn't.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git pkg-config \
    build-essential nasm yasm \
    libx264-dev libx265-dev \
    libfdk-aac-dev libmp3lame-dev libopus-dev libvorbis-dev \
    libfreetype-dev libfontconfig1-dev libharfbuzz-dev \
    fonts-dejavu-core \
    zlib1g-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# nv-codec-headers — NVIDIA's open-source headers for NVENC / NVDEC /
# NPP. Required at FFmpeg configure time for --enable-nvenc / cuvid;
# the actual libcuda.so + libnvidia-encode.so come from the NVIDIA
# driver at runtime on the target host.
#
# Header / driver ABI compatibility (NVENC API version is bumped on
# every header line; the runtime driver MUST expose ≥ the API the
# headers were compiled against, otherwise nvEncOpenEncodeSessionEx
# returns NV_ENC_ERR_INVALID_VERSION and libavcodec surfaces:
#   "Driver does not support the required nvenc API version.
#    Required: X.Y Found: A.B"):
#
#   n12.0.x → NVENC API 12.0 — driver ≥ 525
#   n12.1.x → NVENC API 12.1 — driver ≥ 530    ← matches 535 hosts
#   n12.2.x → NVENC API 12.2 — driver ≥ 550.54
#   n13.0.x → NVENC API 13.0 — driver ≥ 565
#
# Target hosts run driver 535.x (NVENC API 12.1 only). Pin to the
# last n12.1 release. Bump only after every production host has had
# its driver upgraded — otherwise the encoder open call fails at
# runtime even though FFmpeg compiles cleanly.
ARG NV_CODEC_HEADERS_VERSION=n12.1.14.0
RUN git clone --depth 1 --branch ${NV_CODEC_HEADERS_VERSION} \
        https://github.com/FFmpeg/nv-codec-headers.git /tmp/nv-codec-headers && \
    cd /tmp/nv-codec-headers && \
    make install PREFIX=/usr/local && \
    rm -rf /tmp/nv-codec-headers

# 2. Compile FFmpeg 8.1 from source. configure flags trimmed to
#    encoders/decoders/muxers the transcoder actually uses — keeps
#    the resulting .so files small and avoids dragging in obscure
#    device-IO deps (libraw1394, libjack, libcdio_paranoia, ...) that
#    libavdevice in distro packages pulls in but we never call.
ARG FFMPEG_VERSION=8.1.1
RUN curl -fsSL "https://ffmpeg.org/releases/ffmpeg-${FFMPEG_VERSION}.tar.xz" | tar -xJ -C /tmp && \
    cd /tmp/ffmpeg-${FFMPEG_VERSION} && \
    ./configure \
        --prefix=/usr/local \
        --enable-shared --disable-static \
        --enable-gpl --enable-nonfree \
        --enable-libx264 --enable-libx265 \
        --enable-libfdk-aac --enable-libmp3lame --enable-libopus --enable-libvorbis \
        --enable-libfreetype --enable-libfontconfig --enable-libharfbuzz \
        --enable-nvenc --enable-cuvid \
        --enable-ffnvcodec \
        --enable-cuda-nvcc \
        --nvcc=/usr/local/cuda/bin/nvcc \
        --nvccflags="-gencode arch=compute_75,code=sm_75 -O2" \
        --extra-cflags=-I/usr/local/cuda/include \
        --disable-doc --disable-debug \
        --disable-ffplay --disable-ffprobe \
        --disable-indev=alsa --disable-indev=jack --disable-indev=oss \
        --disable-indev=v4l2 --disable-indev=fbdev --disable-indev=xcbgrab \
        --disable-outdev=alsa --disable-outdev=oss --disable-outdev=v4l2 --disable-outdev=fbdev \
        --disable-network \
        --disable-iconv \
        > /tmp/ffmpeg-configure.log 2>&1 && \
    make -j"$(nproc)" > /tmp/ffmpeg-build.log 2>&1 && \
    make install > /dev/null && \
    ldconfig && \
    cd / && rm -rf /tmp/ffmpeg-${FFMPEG_VERSION} /tmp/ffmpeg-*.log

# 3. Go toolchain matching go.mod.
ARG GO_VERSION=1.25.11
RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" \
    | tar -C /usr/local -xz
ENV PATH=/usr/local/go/bin:/root/go/bin:$PATH \
    GOPATH=/root/go \
    GOCACHE=/root/.cache/go-build \
    GOPROXY=https://proxy.golang.org,direct \
    PKG_CONFIG_PATH=/usr/local/lib/pkgconfig

# 4. Sanity stamp — CI logs show what's actually inside.
RUN go version && gcc --version | head -1 && \
    pkg-config --modversion libavcodec libavformat libavfilter libswscale libavutil

WORKDIR /src
