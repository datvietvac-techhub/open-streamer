#!/usr/bin/env bash
# deploy-native-test.sh — push the native-transcoder bundle to a test
# server and wire systemd to pick up the new binaries.
#
# Usage:
#   scripts/deploy-native-test.sh <ssh-host>
#
# Example:
#   scripts/deploy-native-test.sh thuannguyen@10.241.1.84
#
# Prerequisites on the build host:
#   - dist/open-streamer-linux-amd64/bin/open-streamer (from `make build-dev`)
#   - dist/transcoder-linux-amd64/open-streamer-transcoder + lib/
#       (from `make build-transcoder-linux`)
#   - sshpass installed (or use ssh-agent for key-based auth)
#
# What it does on the target:
#   1. Backs up current /usr/local/bin/open-streamer
#   2. Installs new main binary in place
#   3. Installs transcoder + bundled libs to /opt/open-streamer-native/
#   4. Writes systemd drop-in that exports LD_LIBRARY_PATH for the
#      transcoder subprocess so it finds the bundled libav 8 .so
#      files instead of the system's libav 6.
#   5. Restarts open-streamer.service
#
# Rollback:
#   ssh <host> 'sudo install /usr/local/bin/open-streamer.bak \
#                            /usr/local/bin/open-streamer \
#                && sudo systemctl restart open-streamer'

set -euo pipefail

HOST="${1:-}"
if [[ -z "$HOST" ]]; then
    echo "usage: $0 <ssh-host>" >&2
    exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MAIN_BIN="$REPO_ROOT/dist/open-streamer-linux-amd64/bin/open-streamer"
TC_BIN="$REPO_ROOT/dist/transcoder-linux-amd64/open-streamer-transcoder"
TC_LIB="$REPO_ROOT/dist/transcoder-linux-amd64/lib"

for f in "$MAIN_BIN" "$TC_BIN"; do
    [[ -f "$f" ]] || { echo "missing: $f" >&2; exit 1; }
done
[[ -d "$TC_LIB" ]] || { echo "missing lib dir: $TC_LIB" >&2; exit 1; }

echo "→ shipping main binary + transcoder bundle to $HOST..."
ssh "$HOST" 'mkdir -p /tmp/ost-deploy && rm -rf /tmp/ost-deploy/*'
scp "$MAIN_BIN" "$HOST":/tmp/ost-deploy/open-streamer.new
scp "$TC_BIN"  "$HOST":/tmp/ost-deploy/open-streamer-transcoder.new
scp -r "$TC_LIB" "$HOST":/tmp/ost-deploy/lib

echo "→ installing on target (requires sudo)..."
ssh -t "$HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
echo "thuan123" | sudo -S bash -c '
    set -euo pipefail
    # 1. Backup current main binary.
    cp /usr/local/bin/open-streamer /usr/local/bin/open-streamer.bak.$(date +%s)
    install -m755 /tmp/ost-deploy/open-streamer.new /usr/local/bin/open-streamer

    # 2. Stage transcoder + libs under /opt so the bundled libav doesnt
    #    pollute the system library search path globally — the systemd
    #    drop-in below scopes LD_LIBRARY_PATH to the open-streamer
    #    process only.
    mkdir -p /opt/open-streamer-native/bin /opt/open-streamer-native/lib
    install -m755 /tmp/ost-deploy/open-streamer-transcoder.new \
                  /opt/open-streamer-native/bin/open-streamer-transcoder
    rm -rf /opt/open-streamer-native/lib/*
    cp -a /tmp/ost-deploy/lib/. /opt/open-streamer-native/lib/

    # 3. Service.resolveBinaryPath() looks next to the main binary
    #    first, then $PATH. Symlink so it finds the transcoder without
    #    polluting /usr/local/bin with libav-linked binaries.
    ln -sf /opt/open-streamer-native/bin/open-streamer-transcoder \
           /usr/local/bin/open-streamer-transcoder

    # 4. Systemd drop-in: export LD_LIBRARY_PATH so the transcoder
    #    subprocess (spawned by open-streamer) finds the bundled
    #    libav 8 .so files instead of system libav 6. The main
    #    open-streamer binary is pure Go — no libav linkage — so this
    #    only matters when the supervisor execs the subprocess.
    mkdir -p /etc/systemd/system/open-streamer.service.d
    cat >/etc/systemd/system/open-streamer.service.d/native-transcoder.conf <<EOF
[Service]
Environment="LD_LIBRARY_PATH=/opt/open-streamer-native/lib"
EOF
    systemctl daemon-reload

    # 5. Restart.
    systemctl restart open-streamer
    sleep 2
    systemctl status open-streamer --no-pager | head -10
'
REMOTE

echo ""
echo "→ deploy done; tail journal:"
echo "    ssh $HOST 'sudo journalctl -u open-streamer -f -n 50'"
echo ""
echo "→ rollback:"
echo "    ssh $HOST 'sudo install \$(ls /usr/local/bin/open-streamer.bak.* | tail -1) /usr/local/bin/open-streamer && sudo systemctl restart open-streamer'"
