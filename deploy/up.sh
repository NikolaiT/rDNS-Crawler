#!/usr/bin/env bash
# Create the fleet of Hetzner Cloud nodes (one per shard), each with its own
# public IPv4 and a local unbound resolver (via cloud-init). Idempotent: skips
# nodes that already exist.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config

key=$(ensure_ssh_key)
user_data=$(cat "$DEPLOY_DIR/cloud-init.yaml")
read -ra locs <<< "$LOCATIONS"

info "creating $NODES × $SERVER_TYPE ($IMAGE) across: $LOCATIONS"
for ((i=0; i<NODES; i++)); do
  name="${SERVER_PREFIX}-${i}"
  loc="${locs[$((i % ${#locs[@]}))]}"

  existing=$(api GET "/servers?name=$name" | jq -r '.servers[0].id // empty')
  if [[ -n "$existing" ]]; then
    info "  $name already exists (id=$existing), skipping"
    continue
  fi

  body=$(jq -n \
    --arg name "$name" --arg st "$SERVER_TYPE" --arg img "$IMAGE" \
    --arg loc "$loc" --arg key "$key" --arg ud "$user_data" --arg shard "$i" \
    '{name:$name, server_type:$st, image:$img, location:$loc,
      ssh_keys:[$key], user_data:$ud,
      labels:{app:"rdns-crawler", shard:$shard},
      public_net:{enable_ipv4:true, enable_ipv6:true}}')

  ip=$(api POST /servers "$body" | jq -r '.server.public_net.ipv4.ip')
  info "  created $name (shard $i) in $loc → $ip"
done

echo
info "fleet:"
list_nodes
echo
info "next: wait ~60s for cloud-init, then ./deploy.sh"
