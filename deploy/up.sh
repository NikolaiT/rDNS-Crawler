#!/usr/bin/env bash
# Create the fleet of Hetzner Cloud nodes (one per shard), each with its own
# public IPv4 and a local unbound resolver (via cloud-init). Idempotent: skips
# nodes that already exist.
#
# Capacity handling: Hetzner occasionally has no capacity for a server type in
# one location (HTTP 412 resource_unavailable). Each node falls back to the
# other LOCATIONS before being counted as failed, and a failure never aborts
# the loop — the script creates as much of the fleet as possible per run and
# exits non-zero listing whatever is still missing (just re-run it; capacity
# is transient).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config

key=$(ensure_ssh_key)
user_data=$(cat "$DEPLOY_DIR/cloud-init.yaml")
read -ra locs <<< "$LOCATIONS"

info "creating $NODES × $SERVER_TYPE ($IMAGE) across: $LOCATIONS"
failed=()
for ((i=0; i<NODES; i++)); do
  name="${SERVER_PREFIX}-${i}"

  existing=$(api GET "/servers?name=$name" | jq -r '.servers[0].id // empty')
  if [[ -n "$existing" ]]; then
    info "  $name already exists (id=$existing), skipping"
    continue
  fi

  created=""
  for ((try=0; try<${#locs[@]}; try++)); do
    loc="${locs[$(((i + try) % ${#locs[@]}))]}"
    body=$(jq -n \
      --arg name "$name" --arg st "$SERVER_TYPE" --arg img "$IMAGE" \
      --arg loc "$loc" --arg key "$key" --arg ud "$user_data" --arg shard "$i" \
      '{name:$name, server_type:$st, image:$img, location:$loc,
        ssh_keys:[$key], user_data:$ud,
        labels:{app:"rdns-crawler", shard:$shard},
        public_net:{enable_ipv4:true, enable_ipv6:true}}')

    if ip=$(api POST /servers "$body" | jq -r '.server.public_net.ipv4.ip') \
       && [[ -n "$ip" && "$ip" != "null" ]]; then
      info "  created $name (shard $i) in $loc → $ip"
      created=1
      break
    fi
    info "  $name: no capacity in $loc, trying next location"
  done
  if [[ -z "$created" ]]; then
    info "  $name: FAILED in all locations"
    failed+=("$name")
  fi
done

echo
info "fleet:"
list_nodes
echo
if [[ ${#failed[@]} -gt 0 ]]; then
  die "${#failed[@]} node(s) not created: ${failed[*]} — re-run ./up.sh in a bit (capacity is transient), or switch SERVER_TYPE (e.g. cpx22) in config.env"
fi
info "next: wait ~60s for cloud-init, then ./deploy.sh"
