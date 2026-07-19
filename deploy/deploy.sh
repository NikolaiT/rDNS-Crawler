#!/usr/bin/env bash
# Cross-compile the crawler for linux/amd64, push it to every node with a
# systemd unit, and start each shard. Re-runnable: rebuilds, re-uploads, and
# restarts (crawler resumes from its on-disk cursor).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config

# 1. Build a static-ish linux binary locally (no Go needed on the nodes).
info "building linux/amd64 binary"
( cd "$ROOT_DIR" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags='-s -w' -o bin/rdns-crawler-linux-amd64 ./cmd/rdns-crawler )
bin="$ROOT_DIR/bin/rdns-crawler-linux-amd64"

# 2. For each node: upload binary + unit, enable + (re)start.
rows=()
while IFS= read -r __line; do rows+=("$__line"); done < <(list_nodes)
[[ ${#rows[@]} -gt 0 ]] || die "no nodes found — run ./up.sh first"

for row in "${rows[@]}"; do
  IFS=$'\t' read -r id name ip status shard <<< "$row"
  [[ "$ip" != "null" && -n "$ip" ]] || { info "  $name has no IP yet, skipping"; continue; }
  info "deploying to $name (shard $shard/$NODES) @ $ip"

  # Wait for cloud-init readiness (unbound up, /opt/rdns present). Hard-fail if
  # it never appears — starting the crawler without a resolver poisons the shard.
  ready=0
  for _ in {1..60}; do
    if ssh_node "$ip" 'test -f /opt/rdns/READY' 2>/dev/null; then ready=1; break; fi
    sleep 5
  done
  [[ "$ready" == 1 ]] || die "$name: cloud-init never finished (no /opt/rdns/READY) — check 'cloud-init status' on the node"

  ssh_node "$ip" 'systemctl stop rdns-crawler 2>/dev/null; mkdir -p /opt/rdns/OUTPUT' || true
  scp_node "$bin" "$ip" /opt/rdns/rdns-crawler
  ssh_node "$ip" 'chmod +x /opt/rdns/rdns-crawler'

  # Render the systemd unit for this shard.
  unit=$(sed \
    -e "s|@SHARDS@|$NODES|g" \
    -e "s|@SHARD_ID@|$shard|g" \
    -e "s|@CONCURRENCY@|$CONCURRENCY|g" \
    -e "s|@TIMEOUT@|$TIMEOUT|g" \
    -e "s|@RETRIES@|$RETRIES|g" \
    -e "s|@LIMIT@|$LIMIT|g" \
    -e "s|@SAMPLE_PERCENT@|$SAMPLE_PERCENT|g" \
    "$DEPLOY_DIR/rdns-crawler.service.tmpl")
  echo "$unit" | ssh_node "$ip" 'cat > /etc/systemd/system/rdns-crawler.service'

  ssh_node "$ip" 'systemctl daemon-reload && systemctl enable --now rdns-crawler'
  info "  started shard $shard"
done

echo
info "all shards deployed. Monitor with ./status.sh"
