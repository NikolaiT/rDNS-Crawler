# rDNS commercial dataset — architecture & update strategy

This document describes how the crawler feeds a **maintained, commercially
sellable reverse-DNS dataset** for the whole IPv4 space, and how re-crawls are
scheduled intelligently so the data stays fresh without re-querying 3.7 billion
addresses blindly every cycle.

Status: the **crawler + observation format (`.rdnsz` v2)** are implemented, and
so is the first slice of the update strategy: **`recrawl`** (status-filtered
re-crawl of a previous pass — the simplest form of the due-set idea in §5,
targeting `has_ptr` + `timeout`) and **`compare`** (pass-vs-pass statistics:
timeout recovery, PTR churn, transition matrix — the measurement layer the
scheduler's TTL policy will feed on). The **master state DB, updater, zone
prober, and scheduler** described here are the next build; this doc is the spec.

---

## 1. Two artifacts, not one

The single most important design decision: separate the *observations* from the
*maintained state*.

```
                 crawl runs (per node, per pass)
   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
   │ shard-0.rdnsz│   │ shard-1.rdnsz│ … │ shard-N.rdnsz│   ← OBSERVATION LOG
   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘      (append-only, compact)
          └─────────────────┼──────────────────┘
                            ▼
                   ┌──────────────────┐
                   │   UPDATER/MERGE   │  merge observations into current state,
                   └────────┬─────────┘  compute first/last_seen, last_changed, …
                            ▼
                   ┌──────────────────┐
                   │  MASTER STATE DB  │  direct-indexed over 2^32 + PTR dictionary
                   └────────┬─────────┘
                 ┌──────────┼───────────┐
                 ▼          ▼           ▼
            snapshots   scheduler    (future) delta feed
          (CSV/MMDB/…)  (what to     
                         crawl next)  
```

- **Observation log** = the `.rdnsz` v2 files each crawl node produces. They
  record *every queried IP* with its status (`has_ptr`, `noerror_empty`,
  `nxdomain`, `servfail`, `refused`, `timeout`, `net_error`, `lame_delegation`)
  and the PTR names for hits. One file = one shard's slice of one pass. The
  file header's `Created` timestamp is the "observed at" time for every record
  in it — so we do **not** store a per-IP timestamp in the log (keeps it tiny);
  timestamps live in the master DB.
- **Master state DB** = the product. Current best-known state of all IPs plus
  history-derived fields. Snapshots and (later) delta feeds are exported from it.

Why separate: observations are immutable facts cheap to ship from many nodes;
the master state needs random-access updates, history, and derived scheduling
metadata. Conflating them (as v1 did) makes both jobs worse.

---

## 2. Master state DB layout

IPv4 is a **dense 32-bit keyspace**, so the natural structure is a
**direct-indexed fixed-size array** indexed by the integer IP — O(1) update and
lookup, trivial sequential scans for export, and mmap-friendly so it doesn't
need to fit in RAM. No B-tree/KV overhead per key.

### 2.1 State array (`state.bin`)

One fixed-size record per IPv4 address, indexed by `ip_int` (0 .. 2^32-1). At
**16 bytes/record that is 64 GB**; reserved ranges can be left zeroed (sparse
file → they cost no real disk). A 12-byte layout (48 GB) is also viable if we
drop a field. Proposed 16-byte record:

| Bytes | Field | Notes |
|------:|-------|-------|
| 1 | `status` | current status code (model.StatusCode) |
| 1 | `flags` | bit0 fcrdns_checked, bit1 fcrdns_match, bit2 zone_delegated, … |
| 1 | `consecutive_failures` | saturating; drives backoff |
| 1 | `observation_count` | saturating at 255; stability signal |
| 2 | `first_seen` | days since epoch (2020-01-01) — 179 yrs of range |
| 2 | `last_seen` | days since epoch |
| 2 | `last_changed` | days since epoch (PTR value or status class changed) |
| 4 | `ptr_id` | 0 = none; else offset/id into the PTR dictionary |
| 2 | `reserved` | future (e.g. zone_id high bits, category) |

`days-since-epoch` at 2 bytes is deliberate: reverse DNS changes slowly, per-day
resolution is plenty, and it keeps the record small and highly compressible for
snapshot export.

### 2.2 PTR dictionary (`ptr_dict`)

Hostnames are variable-length and **massively repetitive** across IPs (same zone
templates, same provider suffixes). Store them once:

- A content-addressed dictionary: `ptr_id -> template string`, where the
  template has the IP substituted out (same transform as `internal/store`).
- Only ~1/3 of IPs have a PTR, and those collapse to far fewer distinct
  templates, so the dictionary is small relative to the state array.
- Implementation options: an append-only string heap + a hash index
  (`fnv(template) -> ptr_id`) kept in a sidecar, or an embedded KV (LMDB) purely
  for the dictionary. The state array holds only the 4-byte `ptr_id`.

### 2.3 PTR history (optional, `ptr_history/`)

Since you asked to keep history: an append-only per-change log
`(ip, day, old_ptr_id, new_ptr_id)`. This is what powers a future **delta/change
feed** product and analytics ("this /24 flipped naming template on date X").
Kept out of the hot state array so it doesn't bloat random-access updates.

### 2.4 Zone table (`zones.bin`) — see §4

Per reverse zone (typically /24): delegation status, health, dominant naming
template, last probe day, adaptive TTL. ~16M /24s × small record = tens of MB.

---

## 3. The updater (merge step)

Runs after each collection. For every observation record `(ip, status, ptr,
fcrdns)` with the run's `day`:

```
s = state[ip]
if s.first_seen == 0: s.first_seen = day
s.last_seen = day
s.observation_count = min(s.observation_count+1, 255)

if status == has_ptr:
    new_id = dict.intern(template(ptr))
    if s.ptr_id != new_id:
        history.append(ip, day, s.ptr_id, new_id)   # churn event
        s.ptr_id = new_id
        s.last_changed = day
    s.consecutive_failures = 0
else:
    if class_changed(s.status, status): s.last_changed = day
    if is_failure(status): s.consecutive_failures = min(+1, 255)
    else: s.consecutive_failures = 0   # nxdomain/noerror_empty are "clean" negatives
    # keep last known ptr_id for a grace period? (policy, see below)

s.status = status
s.flags  = fcrdns bits | zone bits
```

Idempotent and order-independent per IP except for `last_changed`, which needs
observations applied in `day` order (merge shards of the same pass together;
process passes chronologically).

**Grace policy for disappearing PTRs:** a single `timeout`/`servfail` should not
immediately erase a known-good PTR (transient resolver/zone issues are common).
Policy: retain `ptr_id` but mark the failure and bump `consecutive_failures`;
only clear the PTR after N consecutive *authoritative* negatives (nxdomain /
noerror_empty), not after transient failures. This is exactly why the full
failure taxonomy matters — `servfail`/`timeout` (transient) are treated very
differently from `nxdomain` (authoritative "gone").

---

## 4. Zone-aware crawling (the big efficiency win)

Reverse DNS for IPv4 is delegated **per zone** — classically the `/24`
(`x.y.z.in-addr.arpa`), sometimes larger, sometimes RFC 2317 classless
sub-delegations. Two facts follow:

1. **Failures cluster by zone.** If a `/24`'s reverse zone has no delegation or
   its nameservers `SERVFAIL`, *all 256* addresses fail the same way. Querying
   them individually is wasted work.
2. **Naming templates cluster by zone.** A `/24` is almost always one ISP/host
   using one template (`dynamic-*.isp.net`, `ec2-*.compute.amazonaws.com`).

So we add a **zone prober** that, per `/24`:

- Queries the zone `SOA`/`NS` once. Outcomes:
  - **No delegation / NXDOMAIN at zone** → mark zone `undelegated`; set all 256
    IPs to a cheap negative and schedule the *zone* for infrequent re-probe
    (months). This skips huge swaths of the no-PTR 2/3 of the space with **1
    query instead of 256**.
  - **Delegated & healthy** → crawl the 256 PTRs (or a sample first, see below).
  - **Delegated but `SERVFAIL`/unreachable** → mark `lame_delegation` / unhealthy;
    back off the whole zone, don't hammer 256 dead lookups.
- Records per-zone: `delegated`, `healthy`, dominant template, `last_probe_day`,
  and an adaptive TTL.

**Sampling optimization:** for a delegated zone, probe a few addresses (e.g.
`.1`, `.100`, `.200`) to learn the template and whether it's densely populated
before spending all 256 queries — useful on later refresh passes.

Zone-awareness typically cuts full-space query volume by a large factor because
most of the address space is either undelegated or stable.

---

## 5. Adaptive re-crawl scheduler

Goal: crawl the *right* IPs/zones at the right time instead of uniform full
passes. Each IP (or preferably each zone) has a **due date** =
`last_seen + TTL(state)`. A pass crawls everything past due, highest priority
first.

### Base TTL by current state

| State | Base TTL | Rationale |
|-------|----------|-----------|
| `has_ptr`, stable (high `observation_count`, `last_changed` old) | 60–90 d | datacenter/static rarely changes |
| `has_ptr`, dynamic template (`dyn`/`pool`/`dhcp` in name) or recent `last_changed` | 7–14 d | residential pools churn |
| `nxdomain` / `noerror_empty` (clean negative) | 45–90 d | occasionally revisit for new assignments |
| `undelegated` zone | 90–180 d | rarely changes; probe at zone level, not per IP |
| `servfail` / `net_error` / `timeout` (transient) | exponential backoff: `min(base * 2^consecutive_failures, cap)` starting ~1–3 d | retry soon, then back off |
| `lame_delegation` zone | long, zone-level | broken infra; don't hammer |

### Volatility feedback

- If a re-crawl finds the PTR **unchanged**, lengthen TTL (multiply, capped).
- If it **changed**, shorten TTL (this IP/zone is volatile).
- This makes the crawler spend its query budget where data actually moves.

### Priority & budget

Given a per-cycle query budget (fleet capacity), sort due items by a priority
score (commercial value × staleness × volatility) and crawl until the budget is
spent. The sharded `crawl` mode already distributes work by `ip % shards`; the
scheduler layers a *due-set* on top (feed each node a shard-filtered due list
instead of the full space).

---

## 6. Commercial deliverables

Current target: **current-state snapshot exports** (from the master DB):

- **CSV**: `ip,ptr,status,fcrdns,first_seen,last_seen,last_changed` (or a
  hits-only variant `ip,ptr`).
- **MMDB (MaxMind DB)**: for drop-in reverse-DNS enrichment; keys CIDR→data,
  aligns with how `ip_api` consumes geo/ASN.
- **Parquet**: columnar, for analytics customers.
- Snapshots are produced by a single sequential scan of `state.bin` joined to
  the PTR dictionary — fast and simple because the array is IP-ordered.

Designed-in for later (fields already captured, no re-crawl needed to start):

- **Delta / change feed**: diff two snapshots, or stream from `ptr_history/` —
  "what PTRs changed this month" is a strong recurring-revenue product.
- **Per-IP history / time-series** and a **live lookup API** over the master DB.

---

## 7. Build order (proposed next steps)

0. ~~Status-filtered re-crawl + pass comparison~~ — **done**: `recrawl` streams
   the `has_ptr`/`timeout` targets straight from a previous pass's `.rdnsz`
   shard (no master DB needed yet), and `compare` measures the outcome
   (recovery rate, churn, transitions). This de-risks the scheduler design: the
   churn/recovery numbers it produces are exactly the inputs the TTL policy in
   §5 needs.
1. `internal/masterdb`: mmap'd `state.bin` + PTR dictionary; `Get/Apply` API.
2. `cmd/rdns-db merge <*.rdnsz>`: updater that folds observation files into the
   master DB (with the grace policy).
3. `cmd/rdns-db export --format csv|mmdb|parquet`: snapshot writer.
4. `cmd/rdns-db due --shards N --shard-id i`: emit the due-set for a node (feeds
   `crawl` with a targeted IP list instead of the whole shard).
5. Zone layer: `zones.bin` + a `probe` mode (SOA/NS per /24) + updater wiring.
6. Scheduler policy module (TTL + volatility + backoff) driving `due`.

Each piece is independently testable and slots onto the already-built crawler +
`.rdnsz` v2 observation format.
