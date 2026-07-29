#!/usr/bin/env bash
# Cross-compile the crawler for linux/amd64, push it to every node with a
# systemd unit, and start each shard. Re-runnable: rebuilds, re-uploads, and
# restarts (crawler resumes from its on-disk cursor).
#
# MODE=crawl   → full-space baseline pass (rdns-crawler.service.tmpl)
# MODE=recrawl → targeted update pass: uploads the previous pass's shard file
#                to each node and re-crawls only the RECRAWL_STATUSES IPs
#                (rdns-recrawl.service.tmpl).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config

# 0. In recrawl mode, validate the previous pass before touching any node:
#    one complete shard file per node, or the target set would be partial.
old_dir=""
if [[ "$MODE" == "recrawl" ]]; then
  old_dir="$(abs_path "$OLD_COLLECTED_DIR")"
  [[ -d "$old_dir" ]] || die "OLD_COLLECTED_DIR $old_dir does not exist"
  shopt -s nullglob
  old_files=("$old_dir"/shard-*.rdnsz)
  shopt -u nullglob
  [[ "${#old_files[@]}" == "$NODES" ]] || die "recrawl needs exactly NODES=$NODES previous shard files in $old_dir, found ${#old_files[@]} — the update pass must inherit the previous pass's sharding"
  for ((i=0; i<NODES; i++)); do
    [[ -f "$old_dir/shard-$i.rdnsz" ]] || die "missing previous shard file: $old_dir/shard-$i.rdnsz"
  done
  info "mode: recrawl (statuses: $RECRAWL_STATUSES) from $old_dir"
else
  info "mode: crawl (full space, sample $SAMPLE_PERCENT%)"
fi

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

  # In recrawl mode, ship the previous pass's shard file (the re-crawl plan).
  # Skip the upload when the remote copy already has the right size (~57 MB
  # per shard — re-runs of deploy.sh shouldn't re-push them all).
  if [[ "$MODE" == "recrawl" ]]; then
    prev="$old_dir/shard-$shard.rdnsz"
    lsize=$(stat -f%z "$prev" 2>/dev/null || stat -c%s "$prev")
    rsize=$(ssh_node "$ip" "stat -c%s /opt/rdns/prev-shard-$shard.rdnsz 2>/dev/null" </dev/null || echo 0)
    if [[ "$lsize" != "$rsize" ]]; then
      info "  uploading previous shard ($((lsize / 1024 / 1024)) MB)"
      scp_node "$prev" "$ip" "/opt/rdns/prev-shard-$shard.rdnsz"
    else
      info "  previous shard already on node (size matches)"
    fi
  fi

  # Render the systemd unit for this shard.
  if [[ "$MODE" == "recrawl" ]]; then
    tmpl="$DEPLOY_DIR/rdns-recrawl.service.tmpl"
  else
    tmpl="$DEPLOY_DIR/rdns-crawler.service.tmpl"
  fi
  unit=$(sed \
    -e "s|@SHARDS@|$NODES|g" \
    -e "s|@SHARD_ID@|$shard|g" \
    -e "s|@CONCURRENCY@|$CONCURRENCY|g" \
    -e "s|@TIMEOUT@|$TIMEOUT|g" \
    -e "s|@RETRIES@|$RETRIES|g" \
    -e "s|@LIMIT@|$LIMIT|g" \
    -e "s|@SAMPLE_PERCENT@|$SAMPLE_PERCENT|g" \
    -e "s|@RECRAWL_STATUSES@|$RECRAWL_STATUSES|g" \
    "$tmpl")
  echo "$unit" | ssh_node "$ip" 'cat > /etc/systemd/system/rdns-crawler.service'

  ssh_node "$ip" 'systemctl daemon-reload && systemctl enable --now rdns-crawler'
  info "  started shard $shard ($MODE)"
done

echo
info "all shards deployed. Monitor with ./status.sh"
