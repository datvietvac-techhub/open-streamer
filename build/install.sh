#!/usr/bin/env bash
# Open Streamer installer — ONE script to install/upgrade (or remove) the
# server + native transcoder as a systemd service on Linux.
#
# It runs three ways, auto-detected:
#
#   1. Standalone (easiest — for end users):
#        curl -fsSL <raw-url>/build/install.sh -o install.sh
#        sudo bash install.sh            # prompts for a release tag (empty = latest)
#        sudo bash install.sh v4.0.0     # installs a specific tag, no prompt
#      Downloads the release archive from GitHub, verifies its SHA256, then
#      installs. No Go, no repo checkout needed.
#
#   2. Inside an extracted release archive or a git checkout that already
#      has bin/open-streamer (e.g. after `make build`):
#        sudo ./build/install.sh         # installs what's already on disk
#
#   3. From an explicit directory:
#        sudo ./build/install.sh --local /path/to/extracted-archive
#
# Other commands:
#   sudo ./build/install.sh uninstall    # stop + remove service, binary, transcoder
#   sudo ./build/install.sh status       # systemctl status
#        ./build/install.sh --help
#
# Override the download repo:  OPEN_STREAMER_REPO=owner/name sudo -E bash install.sh
#
# Idempotent: safe to re-run to upgrade. The data dir (/var/lib/open-streamer —
# config + DVR) is preserved across upgrades and kept on uninstall.

set -euo pipefail

# ── Config ───────────────────────────────────────────────────────────────────
REPO="${OPEN_STREAMER_REPO:-datvietvac-techhub/open-streamer}"
GITHUB="https://github.com/${REPO}"
API="https://api.github.com/repos/${REPO}"

BIN_DST="/usr/local/bin/open-streamer"
UNIT_DST="/etc/systemd/system/open-streamer.service"
DATA_DIR="/var/lib/open-streamer"
SERVICE_USER="open-streamer"
SERVICE_NAME="open-streamer"
OUTPUT_SUBDIRS=("hls" "dash" "dvr")

# Native transcoder subprocess + bundled libav .so files install here. They
# ship only in the linux/amd64 archive (bin/open-streamer-transcoder + lib/).
TRANSCODER_DST_DIR="/opt/open-streamer-native"
TRANSCODER_BIN_LINK="/usr/local/bin/open-streamer-transcoder"
TRANSCODER_DROPIN_DIR="/etc/systemd/system/open-streamer.service.d"
TRANSCODER_DROPIN_FILE="${TRANSCODER_DROPIN_DIR}/native-transcoder.conf"

# Globals filled in at runtime.
ARCH=""; SRC=""; TAG=""; LOCAL_DIR=""; CMD="install"
WORK=""; CLEANUP_WORK=0

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MAYBE_ROOT="$(dirname "$SCRIPT_DIR")"   # parent of build/ — a checkout/archive root

log()  { printf '\033[36m[install]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[warn]\033[0m %s\n' "$*" >&2; }
err()  { printf '\033[31m[error]\033[0m %s\n' "$*" >&2; }

# ── Preconditions ─────────────────────────────────────────────────────────────
require_root()    { [[ $EUID -eq 0 ]] || { err "must run as root (use sudo)"; exit 1; }; }
require_linux()   { [[ "$(uname -s)" == "Linux" ]] || { err "this installer only supports Linux; on other OSes run ./bin/open-streamer directly"; exit 1; }; }
require_systemd() { command -v systemctl >/dev/null 2>&1 || { err "systemctl not found — this installer requires systemd"; exit 1; }; }

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64)  ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *) err "unsupported arch: $(uname -m)"; exit 1 ;;
  esac
}

# Pick curl or wget once.
setup_downloader() {
  if command -v curl >/dev/null 2>&1; then
    dl()  { curl -fL  --progress-bar -o "$1" "$2"; }
    dlq() { curl -fsSL -o "$1" "$2"; }
    dls() { curl -fsSL "$1"; }
  elif command -v wget >/dev/null 2>&1; then
    dl()  { wget --show-progress -qO "$1" "$2"; }
    dlq() { wget -qO "$1" "$2"; }
    dls() { wget -qO- "$1"; }
  else
    err "need either curl or wget to download a release"; exit 1
  fi
}

# Resolve the latest release tag via the GitHub API (no jq dependency).
resolve_latest_tag() {
  local json
  json="$(dls "${API}/releases/latest")" || { err "could not query latest release at ${API}/releases/latest"; exit 1; }
  TAG="$(printf '%s' "$json" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
  [[ -n "$TAG" ]] || { err "could not parse the latest tag — pass one explicitly, e.g. $0 v4.0.0"; exit 1; }
}

# Download + verify + extract the given tag. Sets SRC to the extracted dir.
fetch_release() {
  local tag="$1"
  local archive="open-streamer-${tag}-linux-${ARCH}.tar.gz"
  local url="${GITHUB}/releases/download/${tag}/${archive}"
  local sums_url="${GITHUB}/releases/download/${tag}/SHA256SUMS"

  WORK="$(mktemp -d -t "open-streamer-${tag}.XXXXXX")"; CLEANUP_WORK=1
  log "downloading ${url}"
  dl "${WORK}/${archive}" "${url}" || {
    err "download failed — does ${GITHUB}/releases/tag/${tag} have a linux/${ARCH} archive?"; exit 1; }

  log "verifying checksum"
  if dlq "${WORK}/SHA256SUMS" "${sums_url}" 2>/dev/null; then
    local expected actual
    expected="$(awk -v f="$archive" '$2==f {print $1}' "${WORK}/SHA256SUMS")"
    if [[ -n "$expected" ]]; then
      actual="$(sha256sum "${WORK}/${archive}" | awk '{print $1}')"
      [[ "$expected" == "$actual" ]] || { err "checksum mismatch (expected $expected, got $actual)"; exit 1; }
      log "checksum OK"
    else
      warn "no SHA256SUMS entry for ${archive} — skipping verification"
    fi
  else
    warn "SHA256SUMS not published — skipping verification"
  fi

  log "extracting"
  tar -xzf "${WORK}/${archive}" -C "${WORK}"
  SRC="${WORK}/open-streamer-linux-${ARCH}"
  [[ -d "$SRC" ]] || SRC="$(find "$WORK" -mindepth 1 -maxdepth 1 -type d ! -name '*.tar*' | head -n1)"
}

# ── Install steps (operate on $SRC) ─────────────────────────────────────────
ensure_user() {
  id -u "$SERVICE_USER" >/dev/null 2>&1 && return
  log "creating system user: $SERVICE_USER"
  useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
}

ensure_gpu_groups() {
  # GPU device nodes are gated by group membership (NVIDIA → video, render
  # nodes → render). Add the service user to whichever groups exist.
  for grp in video render; do
    if getent group "$grp" >/dev/null 2>&1; then
      if ! id -nG "$SERVICE_USER" | tr ' ' '\n' | grep -qx "$grp"; then
        log "adding $SERVICE_USER to group: $grp"
        usermod -a -G "$grp" "$SERVICE_USER"
      fi
    fi
  done
}

ensure_data_dirs() {
  log "ensuring data directory: $DATA_DIR"
  install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0755 "$DATA_DIR"
  for sub in "${OUTPUT_SUBDIRS[@]}"; do
    install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0755 "${DATA_DIR}/${sub}"
  done
}

stop_if_running() {
  if systemctl is-active --quiet "$SERVICE_NAME"; then
    log "stopping running service before replace"
    systemctl stop "$SERVICE_NAME"
  fi
}

install_binary() {
  log "installing server binary → $BIN_DST"
  install -m 0755 "$SRC/bin/open-streamer" "$BIN_DST"
}

install_unit() {
  # Unit is embedded so the script is self-contained regardless of $SRC.
  log "installing systemd unit → $UNIT_DST"
  cat >"$UNIT_DST" <<'EOF'
[Unit]
Description=Open Streamer — live video media server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=open-streamer
Group=open-streamer
WorkingDirectory=/var/lib/open-streamer
Environment=OPEN_STREAMER_STORAGE_DRIVER=json
Environment=OPEN_STREAMER_STORAGE_JSON_DIR=/var/lib/open-streamer
ExecStart=/usr/local/bin/open-streamer
Restart=on-failure
RestartSec=5
TimeoutStopSec=30
StandardOutput=journal
StandardError=journal
LimitNOFILE=65536
ProtectSystem=full
ProtectHome=true
ReadWritePaths=/var/lib/open-streamer
PrivateTmp=true
NoNewPrivileges=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
RestrictNamespaces=true
LockPersonality=true
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6

[Install]
WantedBy=multi-user.target
EOF
  chmod 0644 "$UNIT_DST"
  systemctl daemon-reload
}

install_transcoder() {
  # The native transcoder + bundled libav 8 .so ship only in the linux/amd64
  # archive, laid out as bin/open-streamer-transcoder + lib/. When absent
  # (other arches, or a server-only build) transcoded streams return
  # ErrNotImplemented; passthrough/copy streams keep working.
  if [[ ! -f "$SRC/bin/open-streamer-transcoder" || ! -d "$SRC/lib" ]]; then
    log "no native transcoder in this archive — passthrough only (transcoded streams → ErrNotImplemented)"
    rm -f "$TRANSCODER_BIN_LINK" "$TRANSCODER_DROPIN_FILE" 2>/dev/null || true
    return
  fi
  log "installing native transcoder → $TRANSCODER_DST_DIR"
  install -d -m 0755 "$TRANSCODER_DST_DIR/bin" "$TRANSCODER_DST_DIR/lib"
  install -m 0755 "$SRC/bin/open-streamer-transcoder" "$TRANSCODER_DST_DIR/bin/open-streamer-transcoder"
  # Wipe + reinstall lib/ so deps dropped in a newer bundle don't linger.
  rm -rf "${TRANSCODER_DST_DIR:?}/lib/"*
  cp -a "$SRC/lib/." "$TRANSCODER_DST_DIR/lib/"

  # Service.resolveBinaryPath() looks next to the main binary, then $PATH.
  # Symlink so the transcoder is reachable via /usr/local/bin without putting
  # the libav-linked binary (and its .so) on the default loader path.
  ln -sf "$TRANSCODER_DST_DIR/bin/open-streamer-transcoder" "$TRANSCODER_BIN_LINK"

  # Scope LD_LIBRARY_PATH to the service so the spawned subprocess loads the
  # bundled libav 8 instead of the host's (often older) system libav. The main
  # binary is pure Go — no libav linkage — so this only matters at exec time.
  install -d -m 0755 "$TRANSCODER_DROPIN_DIR"
  cat >"$TRANSCODER_DROPIN_FILE" <<EOF
[Service]
Environment="LD_LIBRARY_PATH=${TRANSCODER_DST_DIR}/lib"
EOF
  chmod 0644 "$TRANSCODER_DROPIN_FILE"
  systemctl daemon-reload
}

start_and_verify() {
  log "enabling + starting service"
  systemctl enable "$SERVICE_NAME" >/dev/null
  systemctl start "$SERVICE_NAME"
  sleep 2
  if ! systemctl is-active --quiet "$SERVICE_NAME"; then
    err "service failed to start — inspect: journalctl -u $SERVICE_NAME -n 80 --no-pager"
    exit 1
  fi
  log "service is active"
}

print_version() {
  [[ -f "$SRC/VERSION" ]] || return 0
  log "release metadata:"
  sed 's/^/  /' "$SRC/VERSION"
}

# ── Commands ──────────────────────────────────────────────────────────────────
cmd_install() {
  require_root; require_linux; require_systemd

  if [[ -n "$LOCAL_DIR" ]]; then
    SRC="$LOCAL_DIR"
    log "installing from --local: $SRC"
  elif [[ -z "$TAG" && -f "$MAYBE_ROOT/bin/open-streamer" ]]; then
    # Running from inside an extracted archive / git checkout: install it
    # rather than downloading. An explicit TAG always forces a download.
    SRC="$MAYBE_ROOT"
    log "installing from local layout: $SRC"
  else
    detect_arch; setup_downloader
    local tag="$TAG"
    if [[ -z "$tag" ]]; then
      # No tag on the command line: ask the operator rather than silently
      # picking a version. Empty answer falls back to the latest release.
      if [[ -t 0 ]]; then
        read -r -p "Release tag to install (e.g. v4.0.0; leave empty for the latest): " tag
      else
        err "no release tag given and not running interactively — pass one explicitly:"
        err "  $0 v4.0.0        (available tags: ${GITHUB}/releases)"
        exit 1
      fi
    fi
    if [[ -z "$tag" ]]; then resolve_latest_tag; tag="$TAG"; log "using latest release: $tag"; fi
    [[ "$tag" =~ ^v ]] || tag="v$tag"
    fetch_release "$tag"
  fi

  [[ -n "$SRC" && -f "$SRC/bin/open-streamer" ]] || {
    err "bin/open-streamer not found under: ${SRC:-<unset>}"; exit 1; }
  [[ -x "$SRC/bin/open-streamer" ]] || chmod +x "$SRC/bin/open-streamer"

  print_version
  ensure_user
  ensure_gpu_groups
  ensure_data_dirs
  stop_if_running
  install_binary
  install_unit
  install_transcoder
  start_and_verify

  log "installation complete"
  log "  status: systemctl status $SERVICE_NAME"
  log "  logs:   journalctl -u $SERVICE_NAME -f"
  log "  api:    curl http://localhost:8080/config | jq"
  log "  data:   $DATA_DIR  (config, hls/, dash/, dvr/)"
}

cmd_uninstall() {
  require_root; require_linux; require_systemd

  if [[ -f "$UNIT_DST" ]]; then
    log "stopping + disabling $SERVICE_NAME"
    systemctl disable --now "$SERVICE_NAME" || true
    rm -f "$UNIT_DST"
    systemctl daemon-reload
  fi
  [[ -f "$BIN_DST" ]] && { log "removing $BIN_DST"; rm -f "$BIN_DST"; }

  # Native transcoder cleanup — drop-in removed before daemon-reload.
  if [[ -f "$TRANSCODER_DROPIN_FILE" ]]; then rm -f "$TRANSCODER_DROPIN_FILE"; systemctl daemon-reload; fi
  rm -f "$TRANSCODER_BIN_LINK"
  [[ -d "$TRANSCODER_DST_DIR" ]] && { log "removing $TRANSCODER_DST_DIR"; rm -rf "$TRANSCODER_DST_DIR"; }

  if id -u "$SERVICE_USER" >/dev/null 2>&1; then
    pkill -u "$SERVICE_USER" 2>/dev/null || true
    sleep 1
    if userdel "$SERVICE_USER" 2>/dev/null; then
      log "removed service user: $SERVICE_USER"
    else
      warn "could not remove user $SERVICE_USER (leftover processes?) — skipped"
    fi
  fi

  log "uninstalled. Data kept at: $DATA_DIR"
  log "purge data manually if desired: rm -rf $DATA_DIR"
}

cmd_status() { require_systemd; systemctl --no-pager status "$SERVICE_NAME" || true; }

usage() {
  cat <<EOF
Open Streamer installer (repo: ${REPO})

Usage:
  sudo bash install.sh            ask for a release tag (empty = latest), then download + install
  sudo bash install.sh vX.Y.Z     download + install a specific tag (no prompt)
  sudo ./build/install.sh         install from the current extracted archive / checkout
  sudo ./build/install.sh --local DIR    install from an extracted archive at DIR
  sudo ./build/install.sh uninstall      stop + remove service, binary, transcoder
  sudo ./build/install.sh status         systemctl status
       ./build/install.sh --help

Env:
  OPEN_STREAMER_REPO=owner/name   override the download repo (use sudo -E)

Releases: ${GITHUB}/releases
EOF
}

# ── Arg parsing ────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    install|uninstall|status) CMD="$1"; shift ;;
    --local) LOCAL_DIR="${2:?--local requires a directory}"; shift 2 ;;
    -h|--help|help) usage; exit 0 ;;
    v[0-9]*|[0-9]*) TAG="$1"; shift ;;
    *) err "unknown argument: $1"; usage; exit 1 ;;
  esac
done

trap '[[ "$CLEANUP_WORK" == 1 && -n "$WORK" ]] && rm -rf "$WORK"' EXIT

case "$CMD" in
  install)   cmd_install ;;
  uninstall) cmd_uninstall ;;
  status)    cmd_status ;;
  *)         err "unknown command: $CMD"; usage; exit 1 ;;
esac
