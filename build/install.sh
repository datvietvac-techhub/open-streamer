#!/usr/bin/env bash
# Install Open Streamer as a systemd service on Linux.
#
# Two supported layouts (auto-detected from script location):
#
#   1. Release archive (production deploy — no Go required):
#        open-streamer-<ver>-linux-<arch>/
#        ├── bin/open-streamer
#        ├── build/install.sh         ← you are here
#        └── build/open-streamer.service
#
#      Download the archive from https://github.com/ntt0601zcoder/open-streamer/releases,
#      extract, then: sudo build/install.sh
#
#   2. Git checkout (local install on a dev machine with Go installed):
#        open-streamer/
#        ├── bin/open-streamer        ← built via `make build` first
#        ├── build/install.sh
#        └── build/open-streamer.service
#
# Usage:
#   sudo ./build/install.sh           # install or upgrade in place
#   sudo ./build/install.sh uninstall # stop, disable, remove binary + service + user
#
# Idempotent: safe to re-run to upgrade the binary. Data dir is never touched.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN_SRC="${REPO_ROOT}/bin/open-streamer"
UNIT_SRC="${REPO_ROOT}/build/open-streamer.service"

BIN_DST="/usr/local/bin/open-streamer"
UNIT_DST="/etc/systemd/system/open-streamer.service"
DATA_DIR="/var/lib/open-streamer"
SERVICE_USER="open-streamer"
SERVICE_NAME="open-streamer"

# Native transcoder subprocess + bundled libav 8 .so files. The
# `transcoder/` sub-tree only exists in release archives that
# scripts/build-dev.sh produced AFTER `make build-transcoder-linux` —
# absent when an operator builds the main binary alone. Skip silently
# in that case; transcoded streams will refuse to start with
# ErrNotImplemented but passthrough streams keep working.
TRANSCODER_SRC_DIR="${REPO_ROOT}/transcoder"
TRANSCODER_DST_DIR="/opt/open-streamer-native"
TRANSCODER_BIN_LINK="/usr/local/bin/open-streamer-transcoder"
TRANSCODER_DROPIN_DIR="/etc/systemd/system/open-streamer.service.d"
TRANSCODER_DROPIN_FILE="${TRANSCODER_DROPIN_DIR}/native-transcoder.conf"

# Output sub-directories under DATA_DIR. Default GlobalConfig points to
# /out/{hls,dash,dvr}, but those are not writable by the service user. We
# create these so users can repoint the config to /var/lib/open-streamer/...
# via REST without hitting permission errors first.
OUTPUT_SUBDIRS=("hls" "dash" "dvr")

log()  { printf '\033[36m[install]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[warn]\033[0m %s\n' "$*" >&2; }
err()  { printf '\033[31m[error]\033[0m %s\n' "$*" >&2; }

require_root() {
  if [[ $EUID -ne 0 ]]; then
    err "must run as root (use sudo)"
    exit 1
  fi
}

require_linux() {
  if [[ "$(uname -s)" != "Linux" ]]; then
    err "systemd installer only supports Linux."
    err "On other platforms, run the binary directly: ./bin/open-streamer"
    exit 1
  fi
}

require_systemd() {
  if ! command -v systemctl >/dev/null 2>&1; then
    err "systemctl not found — this installer requires systemd."
    exit 1
  fi
}

require_binary() {
  if [[ ! -f "$BIN_SRC" ]]; then
    err "binary not found at: $BIN_SRC"
    err ""
    err "If you extracted a release archive, the layout is wrong — re-extract or"
    err "redownload from: https://github.com/ntt0601zcoder/open-streamer/releases"
    err ""
    err "If this is a git checkout, build it first:    make build"
    exit 1
  fi
  if [[ ! -x "$BIN_SRC" ]]; then
    chmod +x "$BIN_SRC"
  fi
  if [[ ! -f "$UNIT_SRC" ]]; then
    err "systemd unit not found at: $UNIT_SRC"
    exit 1
  fi
}

ensure_user() {
  if id -u "$SERVICE_USER" >/dev/null 2>&1; then
    return
  fi
  log "creating system user: $SERVICE_USER"
  useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
}

ensure_gpu_groups() {
  # Group membership is the standard mechanism to grant access to GPU device
  # nodes (NVIDIA exposes /dev/nvidia* via the `video` group; Intel/AMD render
  # nodes via the `render` group). Add the service user to whichever exist.
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
    log "stopping running service before binary replace"
    systemctl stop "$SERVICE_NAME"
  fi
}

install_binary() {
  log "installing binary → $BIN_DST"
  install -m 0755 "$BIN_SRC" "$BIN_DST"
}

install_unit() {
  log "installing systemd unit → $UNIT_DST"
  install -m 0644 "$UNIT_SRC" "$UNIT_DST"
  systemctl daemon-reload
}

install_transcoder_bundle() {
  if [[ ! -d "$TRANSCODER_SRC_DIR" ]]; then
    log "no native transcoder bundle in archive — skipping (transcoded streams will return ErrNotImplemented)"
    rm -f "$TRANSCODER_BIN_LINK" "$TRANSCODER_DROPIN_FILE" 2>/dev/null || true
    return
  fi
  log "installing native transcoder → $TRANSCODER_DST_DIR"
  install -d -m 0755 "$TRANSCODER_DST_DIR/bin" "$TRANSCODER_DST_DIR/lib"
  install -m 0755 "$TRANSCODER_SRC_DIR/bin/open-streamer-transcoder" \
                  "$TRANSCODER_DST_DIR/bin/open-streamer-transcoder"
  # Wipe + reinstall lib/ so removed deps in newer bundles don't linger.
  rm -rf "$TRANSCODER_DST_DIR/lib/"*
  cp -a "$TRANSCODER_SRC_DIR/lib/." "$TRANSCODER_DST_DIR/lib/"

  # Service.resolveBinaryPath() looks next to the main binary first,
  # then $PATH. Symlink so the transcoder is reachable via /usr/local/
  # bin without polluting it with libav-linked binaries (the actual
  # .so files live under /opt/open-streamer-native/lib/).
  ln -sf "$TRANSCODER_DST_DIR/bin/open-streamer-transcoder" "$TRANSCODER_BIN_LINK"

  # Systemd drop-in: scope LD_LIBRARY_PATH to the open-streamer
  # process so the spawned subprocess finds the bundled libav 8 .so
  # files instead of the host's libav 6.1 (Ubuntu 24.04). The main
  # binary is pure Go — no libav linkage — so this only matters when
  # the supervisor execs the subprocess.
  install -d -m 0755 "$TRANSCODER_DROPIN_DIR"
  cat >"$TRANSCODER_DROPIN_FILE" <<EOF
[Service]
Environment="LD_LIBRARY_PATH=${TRANSCODER_DST_DIR}/lib"
EOF
  chmod 0644 "$TRANSCODER_DROPIN_FILE"
  systemctl daemon-reload
}

start_and_verify() {
  log "enabling and starting service"
  systemctl enable "$SERVICE_NAME" >/dev/null
  systemctl start "$SERVICE_NAME"

  # Give it a moment to settle, then check it's actually running.
  sleep 2
  if ! systemctl is-active --quiet "$SERVICE_NAME"; then
    err "service failed to start"
    err "investigate with: journalctl -u $SERVICE_NAME -n 80 --no-pager"
    exit 1
  fi
  log "service is active"
}

print_summary() {
  log "installation complete"
  log "  status:  systemctl status $SERVICE_NAME"
  log "  logs:    journalctl -u $SERVICE_NAME -f"
  log "  config:  curl http://localhost:8080/config | jq"
  log "  data:    $DATA_DIR  (config, hls/, dash/, dvr/)"
}

print_version() {
  # VERSION file is included in release archives. Absent in git-checkout layout.
  local vfile="${REPO_ROOT}/VERSION"
  if [[ -f "$vfile" ]]; then
    log "installing from release archive:"
    sed 's/^/  /' "$vfile" | while read -r line; do log "$line"; done
  else
    log "installing from git checkout (no VERSION file)"
  fi
}

cmd_install() {
  require_root
  require_linux
  require_systemd
  require_binary

  print_version
  ensure_user
  ensure_gpu_groups
  ensure_data_dirs
  stop_if_running
  install_binary
  install_unit
  install_transcoder_bundle
  start_and_verify
  print_summary
}

cmd_uninstall() {
  require_root
  require_linux
  require_systemd

  if [[ -f "$UNIT_DST" ]]; then
    log "stopping and disabling $SERVICE_NAME"
    systemctl disable --now "$SERVICE_NAME" || true
    rm -f "$UNIT_DST"
    systemctl daemon-reload
  fi

  if [[ -f "$BIN_DST" ]]; then
    log "removing binary: $BIN_DST"
    rm -f "$BIN_DST"
  fi

  # Native transcoder cleanup. Drop-in must go before daemon-reload.
  if [[ -f "$TRANSCODER_DROPIN_FILE" ]]; then
    rm -f "$TRANSCODER_DROPIN_FILE"
    systemctl daemon-reload
  fi
  rm -f "$TRANSCODER_BIN_LINK"
  if [[ -d "$TRANSCODER_DST_DIR" ]]; then
    log "removing native transcoder bundle: $TRANSCODER_DST_DIR"
    rm -rf "$TRANSCODER_DST_DIR"
  fi

  if id -u "$SERVICE_USER" >/dev/null 2>&1; then
    # Kill any leftover processes owned by the user before deleting the account.
    pkill -u "$SERVICE_USER" 2>/dev/null || true
    sleep 1
    if userdel "$SERVICE_USER" 2>/dev/null; then
      log "removed service user: $SERVICE_USER"
    else
      warn "could not remove user $SERVICE_USER (may have running processes); skip"
    fi
  fi

  log "uninstalled service. Data dir kept at: $DATA_DIR"
  log "remove data manually if desired:  rm -rf $DATA_DIR"
}

cmd_status() {
  require_systemd
  systemctl --no-pager status "$SERVICE_NAME" || true
}

case "${1:-install}" in
  install)   cmd_install ;;
  uninstall) cmd_uninstall ;;
  status)    cmd_status ;;
  -h|--help|help)
    # Print the leading comment block (everything from line 2 up to the first
    # blank line that follows a non-comment line, i.e. before `set -euo`).
    awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"
    ;;
  *)
    err "unknown command: $1"
    err "usage: $0 [install|uninstall|status|help]"
    exit 1
    ;;
esac
