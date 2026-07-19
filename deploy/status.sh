#!/usr/bin/env bash
# Show per-node crawl progress as one aligned table: service state, sweep
# position, live counters (parsed from the crawler's journal progress line),
# rate, output size, and a fleet totals row.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config

rows=()
while IFS= read -r __line; do rows+=("$__line"); done < <(list_nodes)
[[ ${#rows[@]} -gt 0 ]] || die "no nodes found"

fmt="%-8s %-16s %-8s %6s %8s %12s %11s %11s %11s %8s  %s\n"
printf "$fmt" NODE IP SVC SWEPT OUT DONE PTR NXDOMAIN FAIL RATE CURSOR

t_done=0; t_ptr=0; t_nx=0; t_fail=0; t_rate=0

for row in "${rows[@]}"; do
  IFS=$'\t' read -r id name ip status shard <<< "$row"
  if [[ -z "$ip" || "$ip" == "null" ]]; then
    printf "$fmt" "$name" "-" "$status" - - - - - - - -
    continue
  fi
  probe=$(ssh_node "$ip" '
    st=$(systemctl is-active rdns-crawler 2>/dev/null || true)
    res=$(systemctl show -p Result --value rdns-crawler 2>/dev/null || true)
    cur=$(cat /opt/rdns/OUTPUT/shard-*.rdnsz.cursor 2>/dev/null | head -1 || true)
    sz=$(du -h /opt/rdns/OUTPUT/shard-*.rdnsz 2>/dev/null | awk "{print \$1}" | head -1 || true)
    inv=$(systemctl show -p InvocationID --value rdns-crawler 2>/dev/null)
    # Grab the last progress line; look back far enough to see past the final
    # summary block a finished run prints after its last progress line.
    prog=$(journalctl -o cat --no-pager ${inv:+_SYSTEMD_INVOCATION_ID=$inv} -u rdns-crawler -n 80 2>/dev/null | tr "\r" "\n" | grep -F " done | " | tail -1)
    printf "%s\t%s\t%s\t%s\t%s" "${st:-dead}" "${res:--}" "${cur:--}" "${sz:--}" "${prog:--}"
  ' 2>/dev/null || printf "unreachable\t-\t-\t-\t-")
  IFS=$'\t' read -r svc res cur sz prog <<< "$probe"

  # Progress line: "[rdns] N done | N ptr | N empty | N nxdomain | N fail | N/s"
  done_=-; ptr=-; nx=-; fail=-; rate=-
  if [[ "$prog" == *" done | "* ]]; then
    read -r done_ ptr nx fail rate <<< "$(awk '{print $2, $5, $11, $14, $17}' <<< "$prog")"
    t_done=$((t_done + done_)); t_ptr=$((t_ptr + ptr))
    t_nx=$((t_nx + nx)); t_fail=$((t_fail + fail))
    [[ "$svc" == active ]] && t_rate=$((t_rate + ${rate%/s}))
  fi

  # Sweep column: finished shards → done/FAILED; running → cursor as a rough
  # fraction of the 32-bit space (approximate: ignores reserved gaps).
  if [[ "$svc" == active ]]; then
    pct="-"
    if [[ "$cur" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
      pct=$(awk -v a="${BASH_REMATCH[1]}" -v b="${BASH_REMATCH[2]}" -v c="${BASH_REMATCH[3]}" -v d="${BASH_REMATCH[4]}" \
        'BEGIN { printf "%.1f%%", (a*2^24 + b*2^16 + c*2^8 + d) / 2^32 * 100 }')
    fi
  elif [[ "$res" == success ]]; then
    pct="done"; rate="-"
  else
    pct="FAILED"; rate="-"
  fi

  printf "$fmt" "$name" "$ip" "${svc:-?}" "$pct" "${sz:--}" "$done_" "$ptr" "$nx" "$fail" "$rate" "${cur:--}"
done

printf "$fmt" TOTAL "" "" "" "" "$t_done" "$t_ptr" "$t_nx" "$t_fail" "${t_rate}/s" ""
