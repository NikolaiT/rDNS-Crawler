#!/usr/bin/env bash
# Shared helpers for the rDNS-Crawler Hetzner Cloud deploy scripts.
# Sourced by up.sh / deploy.sh / status.sh / collect.sh / down.sh.
set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$DEPLOY_DIR/.." && pwd)"
API="https://api.hetzner.cloud/v1"
LABEL="app=rdns-crawler"

die() { echo "error: $*" >&2; exit 1; }
info() { echo "[deploy] $*" >&2; }

load_config() {
  local cfg="$DEPLOY_DIR/config.env"
  [[ -f "$cfg" ]] || die "missing $cfg (copy config.env.example and fill it in)"
  # shellcheck disable=SC1090
  source "$cfg"
  : "${HCLOUD_TOKEN:?set HCLOUD_TOKEN in config.env}"
  [[ "$HCLOUD_TOKEN" != "changeme" ]] || die "set a real HCLOUD_TOKEN in config.env"
  : "${NODES:=5}"
  : "${SERVER_TYPE:=cx23}"
  : "${IMAGE:=ubuntu-24.04}"
  : "${LOCATIONS:=nbg1 fsn1 hel1}"
  : "${SERVER_PREFIX:=rdns}"
  : "${SSH_KEY_NAME:=rdns-crawler}"
  : "${SSH_KEY_FILE:=$HOME/.ssh/id_ed25519}"
  : "${CONCURRENCY:=800}"
  : "${TIMEOUT:=3s}"
  : "${RETRIES:=2}"
  : "${LIMIT:=0}"
  : "${SAMPLE_PERCENT:=100}"
  command -v jq >/dev/null || die "jq is required (brew install jq)"
  command -v curl >/dev/null || die "curl is required"
}

# api METHOD PATH [json-body]
# On a non-2xx response, prints the API's JSON error body to stderr (so 4xx/5xx
# are debuggable) and returns non-zero.
api() {
  local method="$1" path="$2" body="${3:-}"
  local args=(-sS -X "$method" -H "Authorization: Bearer $HCLOUD_TOKEN" -H "Content-Type: application/json" -w $'\n%{http_code}')
  [[ -n "$body" ]] && args+=(-d "$body")
  local resp code out
  resp=$(curl "${args[@]}" "$API$path")
  code=$(printf '%s' "$resp" | tail -n1)
  out=$(printf '%s' "$resp" | sed '$d')
  if [[ "$code" -lt 200 || "$code" -ge 300 ]]; then
    echo "API $method $path -> HTTP $code" >&2
    echo "$out" | jq . >&2 2>/dev/null || echo "$out" >&2
    return 1
  fi
  printf '%s' "$out"
}

# Ensure our SSH public key is registered; echo its name.
ensure_ssh_key() {
  local pub="${SSH_KEY_FILE}.pub"
  [[ -f "$pub" ]] || die "public key $pub not found (ssh-keygen -t ed25519 -f $SSH_KEY_FILE)"
  local existing
  existing=$(api GET "/ssh_keys?name=$SSH_KEY_NAME" | jq -r '.ssh_keys[0].name // empty')
  if [[ -z "$existing" ]]; then
    info "registering SSH key '$SSH_KEY_NAME'"
    api POST /ssh_keys "$(jq -n --arg n "$SSH_KEY_NAME" --arg k "$(cat "$pub")" '{name:$n,public_key:$k}')" >/dev/null
  fi
  echo "$SSH_KEY_NAME"
}

# List our servers as TSV: id  name  ip  status  shard
list_nodes() {
  api GET "/servers?label_selector=$LABEL" \
    | jq -r '.servers[] | [.id, .name, .public_net.ipv4.ip, .status, (.labels.shard // "?")] | @tsv' \
    | sort -t$'\t' -k5 -n
}

node_ip() { # node_ip <shard>
  api GET "/servers?label_selector=$LABEL,shard=$1" | jq -r '.servers[0].public_net.ipv4.ip // empty'
}

ssh_node() { # ssh_node <ip> <cmd...>
  local ip="$1"; shift
  ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 \
    -i "$SSH_KEY_FILE" "root@$ip" "$@"
}

scp_node() { # scp_node <src> <ip> <dst>
  scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 \
    -i "$SSH_KEY_FILE" "$1" "root@$2:$3"
}
