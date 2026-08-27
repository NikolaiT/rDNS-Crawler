#!/usr/bin/env bash
# finish-pass2.sh — complete the second rDNS crawl end-to-end once the last
# node (rdns-4) is done. Safe to start any time:
#
#   ./finish-pass2.sh --wait     # poll the fleet every 10 min, then run all
#   ./finish-pass2.sh            # abort if a node is still crawling
#
# Pipeline:
#   1. collect.sh                       pull the final shard(s), verify headers
#   2. rDNS-Processor/update.sh         verify → compare → merge → dataset-current
#                                       → export package + samples + stats
#                                       + crawl-history entry + upload
#   3. gen-blog-stats + fill-blog       final numbers/tables into the article
#                                       "Update: Second IPv4 Reverse DNS Crawl"
#                                       and the blog-listing teaser
#   4. ipapi.is compile + verify        full static build, no ⟦…⟧ leftovers,
#      + sync.sh                        crawl-history row present, then deploy
#
# Flags: --wait (poll until fleet idle), --skip-deploy (stop before sync.sh)
#
# Re-running after a failure: everything is idempotent except the merged
# dataset dir — if the script died mid-merge/export, `rm -rf` the dataset dir
# it names and re-run.
#
# NOT automated (deliberate): fleet teardown (deploy/down.sh) and git commits.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config

PASS_DIR="$(abs_path "$COLLECT_DIR")"
OUT_DIR="$ROOT_DIR/OUTPUT/dataset-2026-08-02"
PROC_DIR="$(cd "$ROOT_DIR/../rDNS-Processor" && pwd)"
SITE_DIR="$(cd "$ROOT_DIR/../ipapi.is" && pwd)"
ARTICLE="$SITE_DIR/templates/blog/update-second-ipv4-reverse-dns-crawl.html"
BLOG_LIST="$SITE_DIR/templates/blog.html"
BIN="$ROOT_DIR/bin/rdns-crawler"
# NB: lib.sh owns $LABEL (the Hetzner label selector) — don't shadow it.
HISTORY_LABEL="Update pass (has_ptr + timeout + servfail)"
TUNING="concurrency=800,timeout=5s,attempts=3"

WAIT=false; SKIP_DEPLOY=false
for a in "$@"; do
  case "$a" in
    --wait) WAIT=true ;;
    --skip-deploy) SKIP_DEPLOY=true ;;
    *) die "unknown flag: $a (only --wait / --skip-deploy)" ;;
  esac
done

[[ -x "$BIN" ]] || die "$BIN not built (go build -o bin/rdns-crawler ./cmd/rdns-crawler)"
[[ "$MODE" == "recrawl" ]] || die "config.env MODE is '$MODE', expected recrawl (pass 2 config)"

# --- 0. wait until every shard is either locally final or its node is idle ---
# A locally valid header means that node's crawl is complete and collected;
# only the others need an SSH status check.
pending_shards() {
  local s f ip st out=()
  for ((s = 0; s < NODES; s++)); do
    f="$PASS_DIR/shard-${s}.rdnsz"
    if [[ -f "$f" ]] && "$BIN" info "$f" >/dev/null 2>&1; then continue; fi
    ip=$(node_ip "$s" 2>/dev/null || true)
    if [[ -z "$ip" ]]; then out+=("$s:no-node"); continue; fi
    st=$(ssh_node "$ip" 'systemctl is-active rdns-crawler' 2>/dev/null </dev/null || true)
    [[ "$st" == "inactive" || "$st" == "failed" ]] || out+=("$s:$st")
  done
  echo "${out[*]:-}"
}

info "checking fleet state..."
while true; do
  pending="$(pending_shards)"
  [[ -z "$pending" ]] && { info "all nodes done ✓"; break; }
  if $WAIT; then
    info "$(date +%H:%M:%S) still crawling: $pending — next check in 10 min"
    sleep 600
  else
    die "still crawling: $pending — re-run when done, or use --wait"
  fi
done

# --- 1. collect + verify ------------------------------------------------------
info "collecting final shards..."
"$DEPLOY_DIR/collect.sh"
for ((s = 0; s < NODES; s++)); do
  f="$PASS_DIR/shard-${s}.rdnsz"
  [[ -f "$f" ]] || die "shard $s missing after collect"
  "$BIN" info "$f" >/dev/null 2>&1 || die "shard $s has a non-finalized header after collect"
done
info "verify: all $NODES shards final ✓"

# --- 2. compare + merge + export + upload --------------------------------------
if [[ -e "$OUT_DIR" ]]; then
  die "$OUT_DIR already exists — if the previous run died mid-merge/export, rm -rf it and re-run"
fi
"$PROC_DIR/update.sh" --pass "$PASS_DIR" --out "$OUT_DIR" \
  --label "$HISTORY_LABEL" --tuning "$TUNING" --upload

# --- 3. blog article: final numbers --------------------------------------------
info "generating blog numbers..."
node "$ROOT_DIR/tools/gen-blog-stats.js" \
  "$PASS_DIR/compare-stats.json" "$OUT_DIR/merge-stats.json" \
  --pass-dir "$PASS_DIR" --tokens-out "$PASS_DIR/blog-tokens.json" \
  > "$PASS_DIR/blog-stats-report.txt"
info "review file: $PASS_DIR/blog-stats-report.txt"

node "$ROOT_DIR/tools/fill-blog.js" "$ARTICLE" "$PASS_DIR/blog-tokens.json"
node "$ROOT_DIR/tools/fill-blog.js" "$BLOG_LIST" "$PASS_DIR/blog-tokens.json"
# NB: literal ⟦ below — bash 3.2 (macOS default) has no $'\u27e6'.
if grep -qF "⟦" "$ARTICLE" "$BLOG_LIST"; then
  die "unresolved ⟦…⟧ placeholders remain in the blog templates — inspect before deploying"
fi
info "blog templates fully filled ✓"

# --- 4. site build + verify + deploy ---------------------------------------------
info "building the site (full compile)..."
(cd "$SITE_DIR" && node --max-old-space-size=15000 compile.js >/dev/null)

[[ -f "$SITE_DIR/html/blog/update-second-ipv4-reverse-dns-crawl.html" ]] \
  || die "compiled blog page missing"
grep -RqF "⟦" "$SITE_DIR/html" \
  && die "compiled site still contains ⟦…⟧ placeholders" || true
grep -qi 'update pass' "$SITE_DIR/html/reverse-dns.html" \
  || die "crawl-history table has no update-pass row — check rdnsCrawlHistory.json"
info "compiled site verified ✓"

if $SKIP_DEPLOY; then
  info "--skip-deploy: NOT publishing. Deploy later with: cd $SITE_DIR && ./sync.sh"
else
  info "deploying the site..."
  (cd "$SITE_DIR" && ./sync.sh)
fi

echo
info "PASS 2 COMPLETE."
info "  dataset:   $OUT_DIR (dataset-current → next pass baseline)"
info "  stats:     $PASS_DIR/compare-report.txt + blog-stats-report.txt"
info "  article:   https://ipapi.is/blog/update-second-ipv4-reverse-dns-crawl.html"
info "  product:   https://ipapi.is/reverse-dns.html#crawl-history"
info "remaining manual steps:"
info "  1. proofread the article, then re-deploy if you edit (cd ../ipapi.is && ./sync.sh)"
info "  2. tear down the fleet (STOPS BILLING): cd $DEPLOY_DIR && ./down.sh"
info "  3. commit: rDNS-Crawler, rDNS-Processor, ipapi.is, ip_api_data"
