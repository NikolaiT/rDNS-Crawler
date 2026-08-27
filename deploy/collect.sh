#!/usr/bin/env bash
# Pull each shard's compact .rdnsz file down to ./OUTPUT/collected/.
#
# The merge step (--merge) is now OPTIONAL and legacy: rDNS-Processor reads the
# .rdnsz shard files directly (point it at OUTPUT/collected/), which is ~10x
# smaller and faster than the exploded text TSV. Only use --merge if you need a
# single flat "ipInt<TAB>ptr,ptr" file for another tool.
#
# Usage:
#   ./collect.sh            # rsync shard files locally (feed dir to rDNS-Processor)
#   ./collect.sh --merge    # ALSO produce OUTPUT/collected/merged.ptr.tsv (legacy)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config

merge=false
[[ "${1:-}" == "--merge" ]] && merge=true

# COLLECT_DIR comes from config.env. For a recrawl pass it MUST differ from
# OLD_COLLECTED_DIR (lib.sh enforces this) so the baseline is never clobbered.
dest="$(abs_path "$COLLECT_DIR")"
mkdir -p "$dest"

rows=()
while IFS= read -r __line; do rows+=("$__line"); done < <(list_nodes)
[[ ${#rows[@]} -gt 0 ]] || die "no nodes found"

for row in "${rows[@]}"; do
  IFS=$'\t' read -r id name ip status shard <<< "$row"
  [[ -n "$ip" && "$ip" != "null" ]] || continue
  info "collecting shard $shard from $ip"
  rsync -avz --partial \
    -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $SSH_KEY_FILE" \
    "root@$ip:/opt/rdns/OUTPUT/shard-${shard}.rdnsz" "$dest/" || \
    info "  (shard $shard not present yet)"
done

echo
info "collected files:"
ls -lh "$dest"/*.rdnsz 2>/dev/null || info "  none yet"

# --- verify: every expected shard present with a clean (finalized) header ----
# A shard whose node is still crawling has a zero-filled header (the crawler
# only patches magic+stats on clean close) and fails the check: collect again
# after the node's rdns-crawler/rdns-recrawl service is inactive.
bin="$ROOT_DIR/bin/rdns-crawler"
if [[ -x "$bin" ]]; then
  echo
  ok=0; bad=(); missing=()
  for ((s = 0; s < NODES; s++)); do
    f="$dest/shard-${s}.rdnsz"
    if [[ ! -f "$f" ]]; then
      missing+=("$s")
    elif "$bin" info "$f" >/dev/null 2>&1; then
      ok=$((ok + 1))
    else
      bad+=("$s")
    fi
  done
  if [[ $ok -eq $NODES ]]; then
    info "verify: all $NODES shards present with finalized headers ✓"
  else
    info "verify: $ok/$NODES shards complete"
    [[ ${#missing[@]} -gt 0 ]] && info "  MISSING: shards ${missing[*]}"
    [[ ${#bad[@]} -gt 0 ]] && info "  PARTIAL (node still crawling? header not finalized): shards ${bad[*]}"
    info "  re-run ./collect.sh once the node(s) show 'inactive done' in ./status.sh"
  fi
else
  info "verify skipped: $bin not built (go build -o bin/rdns-crawler ./cmd/rdns-crawler)"
fi

if [[ "$MODE" == "recrawl" ]]; then
  info "re-crawl pass collected. Run the full update pipeline (verify → compare"
  info "→ merge → export) with one command:"
  echo "  ../../rDNS-Processor/update.sh --pass $dest"
  info "or just the comparison statistics:"
  echo "  (cd $ROOT_DIR && ./bin/rdns-crawler compare \\"
  echo "     --old $(abs_path "$OLD_COLLECTED_DIR") \\"
  echo "     --new $dest \\"
  echo "     --statuses $RECRAWL_STATUSES \\"
  echo "     --json $dest/compare-stats.json)"
fi
if ! $merge; then
  info "feed the shards straight to rDNS-Processor (no merge needed):"
  echo "  node ../rDNS-Processor/src/index.js process $dest"
  echo "  node ../rDNS-Processor/src/index.js analyze $dest"
fi

if $merge; then
  bin="$ROOT_DIR/bin/rdns-crawler"
  [[ -x "$bin" ]] || bin="$ROOT_DIR/bin/rdns-crawler-linux-amd64"
  [[ -x "$bin" ]] || die "no rdns-crawler binary in $ROOT_DIR/bin — run 'go build -o bin/rdns-crawler ./cmd/rdns-crawler'"
  out="$dest/merged.ptr.tsv"
  info "merging → $out (compact: ipInt<TAB>ptr,ptr)"
  : > "$out"
  for f in "$dest"/shard-*.rdnsz; do
    [[ -e "$f" ]] || continue
    "$bin" dump --compact "$f" >> "$out"
  done
  info "merged $(wc -l < "$out") PTR-bearing records → $(du -h "$out" | awk '{print $1}')"
  info "feed it to rDNS-Processor:"
  echo "  node ../rDNS-Processor/src/index.js analyze $out"
  echo "  node ../rDNS-Processor/src/index.js process $out"
fi
