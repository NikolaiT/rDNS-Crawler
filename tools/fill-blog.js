#!/usr/bin/env node
/**
 * fill-blog.js — replace ⟦TOKEN⟧ placeholders in a template from a JSON map
 * (the --tokens-out file of gen-blog-stats.js). Values may be multi-line
 * (tables). Reports every replacement and lists any ⟦…⟧ that remain.
 *
 * Usage:
 *   node tools/fill-blog.js <template.html> <tokens.json> [--dry]
 *
 * Exit code 0 = file written (or dry) — leftover placeholders do NOT fail the
 * run (other templates may legitimately be filled in several passes); the
 * caller greps for ⟦ when it wants a hard guarantee.
 */
'use strict';

const fs = require('fs');

const [, , tplPath, jsonPath, flag] = process.argv;
if (!tplPath || !jsonPath) {
  console.error('usage: fill-blog.js <template.html> <tokens.json> [--dry]');
  process.exit(1);
}

let html = fs.readFileSync(tplPath, 'utf8');
const tokens = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));

let applied = 0;
for (const [key, val] of Object.entries(tokens)) {
  const token = `\u27e6${key}\u27e7`; // ⟦KEY⟧
  const n = html.split(token).length - 1;
  if (n > 0) {
    html = html.split(token).join(String(val));
    const preview = String(val).replace(/\s+/g, ' ').slice(0, 60);
    console.log(`  ${token} → "${preview}${String(val).length > 60 ? '…' : ''}" (${n}×)`);
    applied += n;
  }
}

const leftover = [...new Set(html.match(/\u27e6[^\u27e7]*\u27e7/g) || [])];
if (flag !== '--dry') fs.writeFileSync(tplPath, html);
console.log(`\n${applied} replacements${flag === '--dry' ? ' (dry run, not written)' : ` written to ${tplPath}`}`);
if (leftover.length) {
  console.log('remaining placeholders:');
  for (const l of leftover) console.log(`  ${l}`);
} else {
  console.log('no placeholders remain ✓');
}
