#!/usr/bin/env bash
# Destroy all rDNS-Crawler nodes. Prompts unless --yes is given.
# Reminder: run ./collect.sh first if you still want the data!
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config

rows=()
while IFS= read -r __line; do rows+=("$__line"); done < <(list_nodes)
[[ ${#rows[@]} -gt 0 ]] || { info "no nodes to delete"; exit 0; }

echo "About to DELETE these servers:" >&2
printf '  %s\n' "${rows[@]}" >&2

if [[ "${1:-}" != "--yes" ]]; then
  read -rp "Type 'delete' to confirm: " ans
  [[ "$ans" == "delete" ]] || { info "aborted"; exit 1; }
fi

for row in "${rows[@]}"; do
  IFS=$'\t' read -r id name ip status shard <<< "$row"
  info "deleting $name (id=$id)"
  api DELETE "/servers/$id" >/dev/null
done
info "done."
