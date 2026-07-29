# FCrDNS in rDNS-Crawler

**Question: does the crawler also crawl FCrDNS status? Would it be hard to implement?**

**Answer: it is already implemented, enabled by default, and persisted end-to-end.**
For every IP that has a PTR record, the crawler resolves each PTR hostname's `A`
records back and flags whether any of them equals the original IP
(forward-confirmed reverse DNS). Nothing needs to be built — though the current
*boolean* could be upgraded to a richer 4-state status with ~half a day of work
(see [Possible upgrade](#possible-upgrade-from-bool-to-a-4-state-fcrdns-status)).

## Where it lives

| Piece | Location | What it does |
|---|---|---|
| CLI flag | `cmd/rdns-crawler/main.go` (`registerCommon`) | `--fcrdns`, **default `true`**, shared by all three modes (`test`, `crawl`, `sweep`) |
| The check | `internal/crawler/crawler.go` (`process`) | After a `has_ptr` result: for each PTR name, resolve `A` records and compare against the original IP; first match wins |
| Forward lookup | `internal/resolver/resolver.go` (`LookupA`) | Same rotating-resolver + retry machinery as the PTR path. CNAME chains are handled implicitly (the recursive resolver returns the chased `A` records in the answer section) |
| JSONL output | `internal/model/record.go` | `fcrdns_match: true/false` on `has_ptr` rows; omitted when not checked |
| `.rdnsz` v2 store | `internal/store/format.go` | Bit 4 = *fcrdns checked*, bit 5 = *fcrdns match* in each record's status/flags byte — costs nothing extra on disk |
| Dump round-trip | `cmd/rdns-crawler/main.go` (`runDump`) | `dump` restores `fcrdns_match` into the JSONL |
| Run summary | `internal/store/writer.go` (`PrintSummary`) | Prints `FCrDNS match N / M` at the end of a run |
| Hetzner fleet | `deploy/rdns-crawler.service.tmpl` | Passes no `--fcrdns` flag → default `true` → **the distributed full-space crawl forward-confirms too** |
| Master-DB design | `DESIGN.md` | State-record flags reserve bit0/bit1 for `fcrdns_checked` / `fcrdns_match`; CSV export includes `fcrdns` |

Verified against real output (`OUTPUT/rdns-test-*.jsonl`):

```json
{"ip":"184.27.32.74","status":"ok","ptr":["a184-27-32-74.deploy.static.akamaitechnologies.com"],"fcrdns_match":true, ...}
{"ip":"67.195.176.121","status":"ok","ptr":["unknown.yahoo.com"],"fcrdns_match":false, ...}
```

## Semantics (what the bool means)

- `fcrdns_match` is only set when the IP has at least one PTR name **and**
  `--fcrdns` was on. All other rows omit the field (`nil` internally,
  `FCChecked=false` in the store).
- `true` = at least one PTR hostname has an `A` record equal to the queried IP
  (the standard "any name confirms" FCrDNS definition). The loop short-circuits
  on the first match.
- Only `A` (IPv4) records are compared — correct for an IPv4 crawler; `AAAA`
  is irrelevant here.
- Cost: roughly one extra `A` query per `has_ptr` IP (~⅓ of routable space),
  so a full pass adds on the order of 1.2B forward queries. This is already
  priced into the fleet tuning since it runs with the default on.

## Known limitations of the boolean

1. **`false` conflates three different outcomes**:
   - *mismatch* — the name resolves, but to different IP(s);
   - *no forward record* — NXDOMAIN / NOERROR-empty (very common for generic
     ISP PTRs);
   - *check failed* — all forward lookups timed out or SERVFAILed
     (`LookupA` errors are swallowed with `continue`), so a transient failure
     is recorded identically to a genuine mismatch.
2. Per-name results are not kept — only the aggregate verdict for the IP.
3. The `FCrDNS match N / M` counters shown in the summary are not persisted in
   the `.rdnsz` header (only per-status counts are), so after a `--resume` the
   summary undercounts them. The per-record bits on disk are authoritative.
4. The deprecated `collect.sh --merge` flat TSV drops the fcrdns field — the
   normal `.rdnsz` → rDNS-Processor path keeps it (the store reader exposes
   `Rec.FCChecked` / `Rec.FCMatch`).

## Possible upgrade: from bool to a 4-state FCrDNS status

If the dataset should distinguish the failure modes above, replace the bool
with an enum: `match` / `mismatch` / `no_forward` / `check_error`. Estimated
effort: **~half a day**, no architectural changes:

- `resolver.LookupA` already receives the full DNS response — additionally
  return the rcode (or a small `AResult` struct) so the caller can tell
  NXDOMAIN/empty apart from transport errors.
- `crawler.process` — classify: any name matched → `match`; some name resolved
  but none matched → `mismatch`; all names NXDOMAIN/empty → `no_forward`; all
  attempts errored → `check_error`.
- `model.Record` — add an `fcrdns` string field (keep `fcrdns_match` for
  backward compatibility).
- `.rdnsz` — **bits 6–7 of the status/flags byte are still free**, so a 2-bit
  reason code fits into v2 without a format break: old readers ignore the
  bits, and old files decode as reason `0`.
- `dump` + `PrintSummary` — surface the new state.

Until then: treat `fcrdns_match=true` as a strong positive signal, and
`false` as "not confirmed" rather than "provably forged".
