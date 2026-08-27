#!/usr/bin/env node
/**
 * gen-blog-stats.js — turn the compare/merge JSON stats of the second rDNS
 * crawl into the HTML fragments + numbers for the ipapi.is blog article
 * "Update: Second IPv4 Reverse DNS Crawl".
 *
 * Usage:
 *   node tools/gen-blog-stats.js <compare-stats.json> [merge-stats.json]
 *                                [--pass-dir <dir>] [--tokens-out <file.json>]
 *
 * Prints human-readable RAW_* blocks for review, and with --tokens-out writes
 * a JSON map of every ⟦TOKEN⟧ used by the article template + blog card —
 * feed it to tools/fill-blog.js. --pass-dir (the collected pass shards) is
 * used to compute the pass duration from file headers/mtimes.
 */
'use strict';

const fs = require('fs');
const path = require('path');

const positional = [];
let passDir = null;
let tokensOut = null;
{
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--pass-dir') passDir = argv[++i];
    else if (argv[i] === '--tokens-out') tokensOut = argv[++i];
    else positional.push(argv[i]);
  }
}
if (!positional[0]) {
  console.error('usage: gen-blog-stats.js <compare-stats.json> [merge-stats.json] [--pass-dir d] [--tokens-out f]');
  process.exit(1);
}
const cmp = JSON.parse(fs.readFileSync(positional[0], 'utf8'));
const mrg = positional[1] ? JSON.parse(fs.readFileSync(positional[1], 'utf8')) : null;

// Pass-1 published dataset (baseline constants).
const PASS1 = {
  has_ptr: 1039377899,
  noerror_empty: 18730272,
  nxdomain: 1935783736,
  servfail: 146869237,
  refused: 0,
  timeout: 561438988,
  net_error: 58300,
  lame_delegation: 0,
};
const TOTAL_SPACE = 3702258432;

const fmt = (n) => Number(n).toLocaleString('en-US');
const pct = (n, d, digits = 2) => `${((100 * n) / d).toFixed(digits)}%`;
const compact = (n) => {
  if (n >= 1e9) return `${(n / 1e9).toFixed(2)}B`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return String(n);
};
const block = (name, body) => console.log(`\n===== ${name} =====\n${body}`);

const t = cmp.transitions;
const sum = (obj) => Object.values(obj).reduce((a, b) => a + b, 0);
const tok = {};

/* ---------------------------------------------------------------- headline */
block('RAW_HEADLINE', JSON.stringify(cmp.headline, null, 2));
block('RAW_PASSES', JSON.stringify({ old: cmp.old, new: cmp.new, days_between: cmp.days_between, targets: cmp.targets, dups: { old: cmp.old_dup_targets, new: cmp.new_dup_records, unexpected: cmp.unexpected_new } }, null, 2));

tok.PASS2_QUERIED = fmt(cmp.new.total_queried);
tok.DAYS_BETWEEN = String(Math.round(cmp.days_between));
tok.PASS2_COST_PCT = `${Math.round((100 * cmp.new.total_queried) / cmp.old.total_queried)}%`;

/* -------------------------------------------------------------- pass duration */
// Started = average node start (compare header average is fine: the fleet
// starts within minutes); finished = newest shard mtime (rsync preserves it).
if (passDir) {
  const files = fs.readdirSync(passDir).filter((f) => f.endsWith('.rdnsz')).map((f) => path.join(passDir, f));
  if (files.length) {
    const endMs = Math.max(...files.map((f) => fs.statSync(f).mtimeMs));
    const startMs = new Date(cmp.new.created).getTime();
    const days = (endMs - startMs) / 86400e3;
    const half = Math.round(days * 2) / 2;
    tok.PASS2_DURATION = Number.isInteger(half) ? `~${half} days` : `~${Math.floor(half)}½ days`;
  }
}
if (!tok.PASS2_DURATION) tok.PASS2_DURATION = '~4½ days';
tok.MERGE_RUNTIME = 'half an hour';

/* ------------------------------------------------- timeout transition table */
{
  const tt = t.timeout || {};
  const crawled = sum(tt) - (tt.missing || 0);
  const order = ['has_ptr', 'nxdomain', 'noerror_empty', 'timeout', 'servfail', 'refused', 'net_error', 'lame_delegation'];
  const label = {
    has_ptr: '<strong>a valid PTR record</strong>',
    nxdomain: 'authoritative <code>nxdomain</code>',
    noerror_empty: 'clean empty answer (<code>noerror_empty</code>)',
    timeout: 'still <code>timeout</code>',
    servfail: '<code>servfail</code>',
    refused: '<code>refused</code>',
    net_error: 'network error',
    lame_delegation: 'lame delegation',
  };
  const rows = order
    .filter((k) => tt[k])
    .map((k) => `                <tr>\n                  <td>${label[k]}</td>\n                  <td>${fmt(tt[k])}</td>\n                  <td>${pct(tt[k], crawled)}</td>\n                </tr>`)
    .join('\n');
  tok.TIMEOUT_TRANSITION_TABLE = `<table class="table is-striped is-fullwidth">
              <thead>
                <tr>
                  <th>July <code>timeout</code> &rarr; August status</th>
                  <th>IP Addresses</th>
                  <th>Share</th>
                </tr>
              </thead>
              <tbody>
${rows}
              </tbody>
            </table>`;
  block('RAW_TIMEOUT', JSON.stringify({ crawled, ...tt }, null, 2));

  tok.TO_PTR = fmt(tt.has_ptr || 0);
  tok.TO_PTR_PCT = pct(tt.has_ptr || 0, crawled, 1);
  tok.TO_NX_PCT = pct(tt.nxdomain || 0, crawled, 1);
  tok.TO_DEFINITIVE_PCT = pct((tt.has_ptr || 0) + (tt.nxdomain || 0) + (tt.noerror_empty || 0), crawled, 1);
  tok.TO_STILL_PCT = pct(tt.timeout || 0, crawled, 1);
  tok.TO_SF_PCT = pct(tt.servfail || 0, crawled, 1);
}

/* --------------------------------------------------------- servfail numbers */
{
  const sf = t.servfail || {};
  const crawled = sum(sf) - (sf.missing || 0);
  tok.SF_STILL_PCT = pct(sf.servfail || 0, crawled, 1);
  tok.SF_PTR = fmt(sf.has_ptr || 0);
  tok.SF_PTR_PCT = pct(sf.has_ptr || 0, crawled, 2);
  block('RAW_SERVFAIL', JSON.stringify({ crawled, ...sf }, null, 2));
}

/* ------------------------------------------------------------ gained + TLDs */
{
  const g = cmp.gained;
  const tld = (name) => {
    const e = (g.top_tlds || []).find((x) => x.tld === name);
    return e ? compact(e.n) : '?';
  };
  tok.GAINED_TOTAL = fmt(g.total);
  tok.TLD_NET = tld('net'); tok.TLD_COM = tld('com'); tok.TLD_JP = tld('jp'); tok.TLD_IT = tld('it');
  tok.GAINED_FC_PCT = pct(g.fcrdns_match, g.fcrdns_checked, 1);
  const tlds = (g.top_tlds || []).slice(0, 10).map((e) => `${e.tld} (${compact(e.n)})`).join(', ');
  block('RAW_GAINED', `total=${fmt(g.total)}  fcrdns_checked=${fmt(g.fcrdns_checked)}  fcrdns_match=${fmt(g.fcrdns_match)}\ntop TLDs: ${tlds}`);
}

/* ------------------------------------------------------------- churn table */
{
  const h = t.has_ptr || {};
  const crawled = sum(h) - (h.missing || 0);
  const unchanged = cmp.ptr_unchanged;
  const changed = cmp.ptr_changed;
  const removed = (h.nxdomain || 0) + (h.noerror_empty || 0);
  const transient = (h.timeout || 0) + (h.servfail || 0) + (h.refused || 0) + (h.net_error || 0) + (h.lame_delegation || 0);
  const rows = [
    ['<strong>Same hostname(s)</strong>', unchanged, 'the record is stable'],
    ['<strong>Different hostname(s)</strong>', changed, 'renamed, re-assigned or re-provisioned'],
    ['Now <code>nxdomain</code> / empty', removed, 'the PTR record was deleted (authoritative negative)'],
    ['Transient failure (<code>timeout</code>, <code>servfail</code>, &hellip;)', transient, 'zone or resolver had a bad day &mdash; grace policy applies'],
  ]
    .map(([l, n, note]) => `                <tr>\n                  <td>${l}</td>\n                  <td>${fmt(n)}</td>\n                  <td>${pct(n, crawled)}</td>\n                  <td>${note}</td>\n                </tr>`)
    .join('\n');
  tok.CHURN_TABLE = `<table class="table is-striped is-fullwidth">
              <thead>
                <tr>
                  <th>July <code>has_ptr</code> &rarr; August answer</th>
                  <th>IP Addresses</th>
                  <th>Share</th>
                  <th>Meaning</th>
                </tr>
              </thead>
              <tbody>
${rows}
              </tbody>
            </table>`;
  block('RAW_CHURN', JSON.stringify({ crawled, unchanged, changed, removed, transient, per_day_pct: cmp.headline.ptr_changed_per_day_pct, raw: h }, null, 2));

  tok.CHURN_SURVIVE_PCT = pct(h.has_ptr || 0, crawled, 2);
  tok.CHURN_UNCHANGED_PCT = pct(unchanged, crawled, 2);
  tok.CHURN_CHANGED_PCT = pct(changed, crawled, 2);
  tok.CHURN_PER_DAY = cmp.headline.ptr_changed_per_day_pct.toFixed(4);
  tok.REMOVED_PCT = pct(removed, crawled, 2);
  tok.REMOVED_TOTAL = fmt(removed);
  tok.NET_GAIN = fmt(cmp.gained.total - removed);
}

/* ------------------------------------------------------------------ fcrdns */
{
  const fc = cmp.fcrdns;
  tok.FC_BOTH = compact(fc.both_checked);
  tok.FC_STAY_PCT = pct(fc.match_both, fc.both_checked, 1);
  tok.FC_LOST_PCT = pct(fc.match_old_only, fc.both_checked, 2);
  tok.FC_GAIN_PCT = pct(fc.match_new_only, fc.both_checked, 2);
  block('RAW_FCRDNS', JSON.stringify(fc, null, 2));
}

/* -------------------------------------------------------------------- merge */
if (mrg) {
  const mc = mrg.merged_counts;
  const delta = mc.has_ptr - PASS1.has_ptr;
  tok.MERGED_HAS_PTR = fmt(mc.has_ptr);
  tok.PTR_DELTA_PCT = `${delta >= 0 ? '+' : ''}${pct(delta, PASS1.has_ptr, 1)}`;
  tok.GRACE_KEPT = fmt(mrg.ptr_grace_kept);
  tok.GRACE_PCT = pct(mrg.ptr_grace_kept, cmp.targets.has_ptr, 1);
  tok.PTR_NET_TABLE = `${delta >= 0 ? '+' : ''}${fmt(delta)}`;
  tok.MERGED_PTR_SHARE = pct(mc.has_ptr, TOTAL_SPACE, 2);
  tok.MERGED_TIMEOUT = compact(mc.timeout);
  tok.MERGED_TIMEOUT_SHARE = pct(mc.timeout, TOTAL_SPACE, 2);
  const nxd = mc.nxdomain - PASS1.nxdomain;
  tok.NX_DELTA = `${nxd >= 0 ? '+' : '\u2212'}${compact(Math.abs(nxd))}`;

  const order = ['has_ptr', 'nxdomain', 'timeout', 'servfail', 'noerror_empty', 'refused', 'net_error', 'lame_delegation'];
  const rows = order
    .filter((k) => PASS1[k] || mc[k])
    .map((k) => {
      const a = PASS1[k] || 0;
      const b = mc[k] || 0;
      const d = b - a;
      const dStr = d === 0 ? '&plusmn;0' : `${d > 0 ? '+' : '&minus;'}${fmt(Math.abs(d))}`;
      const strong = k === 'has_ptr';
      return `                <tr>\n                  <td><code>${k}</code></td>\n                  <td>${fmt(a)} (${pct(a, TOTAL_SPACE)})</td>\n                  <td>${strong ? '<strong>' : ''}${fmt(b)} (${pct(b, TOTAL_SPACE)})${strong ? '</strong>' : ''}</td>\n                  <td>${dStr}</td>\n                </tr>`;
    })
    .join('\n');
  tok.MERGED_TABLE = `<table class="table is-striped is-fullwidth">
              <thead>
                <tr>
                  <th>Status</th>
                  <th>July 2026 (baseline)</th>
                  <th>August 2026 (updated)</th>
                  <th>Change</th>
                </tr>
              </thead>
              <tbody>
${rows}
              </tbody>
            </table>`;

  // Teaser for the blog listing card (templates/blog.html).
  tok.SECOND_CRAWL_TEASER = `Four weeks after the first complete sweep of the IPv4 reverse DNS space, we re-crawled
                  every address whose answer could have changed — ${compact(cmp.new.total_queried)} targets with a far more
                  patient timeout budget. The result: ${compact(cmp.gained.total)} newly resolved hostnames, a measured PTR
                  churn rate of ${tok.CHURN_CHANGED_PCT} per month, and an updated Reverse DNS Database that now holds
                  ${compact(mc.has_ptr)} PTR records — ${tok.PTR_DELTA_PCT} over the July release. This update covers what the
                  561 million July timeouts really were, how much a billion-record dataset changes in 27 days, and the
                  merge semantics that keep a maintained dataset honest.`;

  block('RAW_MERGE', JSON.stringify({ total: mrg.merged_total, gained: mrg.ptr_gained, removed_auth: mrg.ptr_removed_auth, changed: mrg.ptr_changed, unchanged: mrg.ptr_unchanged, grace_kept: mrg.ptr_grace_kept, targets_not_crawled: mrg.targets_not_crawled, dups: { old: mrg.old_dup_records, new: mrg.new_dup_records, unexpected: mrg.unexpected_new }, counts: mc }, null, 2));
}

/* ------------------------------------------------------------------- output */
block('TOKENS', JSON.stringify(tok, null, 2));
if (tokensOut) {
  fs.writeFileSync(tokensOut, JSON.stringify(tok, null, 2));
  console.error(`[gen-blog-stats] ${Object.keys(tok).length} tokens → ${tokensOut}`);
}
