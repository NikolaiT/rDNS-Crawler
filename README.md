# rDNS-Crawler

A fast, highly parallel reverse-DNS (PTR) crawler for the IPv4 address space,
written in Go. It powers the [ipapi.is](https://ipapi.is) Reverse DNS Database
and IP-intelligence pipeline.

It **never crawls reserved / bogon IPv4 space** — the full list of IANA
special-purpose / bogon ranges is defined in
[`internal/ipgen/ipgen.go`](internal/ipgen/ipgen.go) (`ReservedIPv4Ranges`):
`0/8`, `10/8`, `100.64/10`, `127/8`, `169.254/16`, `172.16/12`, `192.0.0/24`,
`192.0.2/24`, `192.88.99/24`, `192.168/16`, `198.18/15`, `198.51.100/24`,
`203.0.113/24`, `224/4`, `240/4`. Both the random and sequential IP generators
skip these, so only the ~3.7B routable addresses are ever queried.

## Features

- **Highly parallel**: a worker pool of goroutines keeps thousands of PTR
  lookups in flight at once.
- **Distributed**: shard the whole IPv4 space across N nodes (`crawl` mode), each
  with its own public IP, with resume-from-checkpoint. One-command Hetzner Cloud
  fleet in [`deploy/`](deploy/).
- **Resolver rotation**: spreads queries across a pool of public recursive
  resolvers (Cloudflare, Google, Quad9, OpenDNS) to raise throughput before any
  one provider rate-limits you. At scale, point it at a local `unbound` (the
  Hetzner deploy installs one per node).
- **Forward-confirmed rDNS (FCrDNS)**: optionally resolves each PTR hostname's
  `A` record back and flags whether it matches the original IP.
- **Reserved-space aware**: random and sequential IP generators both skip
  reserved ranges.
- **Full failure taxonomy**: every queried IP is classified as `has_ptr`,
  `noerror_empty`, `nxdomain`, `servfail`, `refused`, `timeout`, `net_error`, or
  `lame_delegation`. Distinguishing authoritative negatives (`nxdomain`) from
  transient failures (`servfail`/`timeout`) is what makes smart re-crawl
  scheduling possible.
- **Compact storage** (`.rdnsz` v2): a purpose-built binary format that records
  **every queried IP** (status included, not just hits), delta-encodes the IPs,
  templates each record's own IP out of its hostname, and zstd-compresses in
  blocks. ~**7 bytes per queried IP** (failures compress to almost nothing), so a
  full-IPv4 pass stays small while retaining the negative-result signal needed
  for updates. JSONL is still available via `--format jsonl`, and `dump` converts
  `.rdnsz` back to JSONL losslessly.
- **Built for a maintained dataset**: the crawler output is an *observation log*
  that feeds a master state DB with adaptive, zone-aware re-crawl scheduling. See
  **[`DESIGN.md`](DESIGN.md)** for the commercial dataset architecture.

## Requirements

- Go 1.26+
- Outbound UDP/53 to the configured resolvers

## Build

```bash
go build -o bin/rdns-crawler ./cmd/rdns-crawler
```

## Quickstart: distributed 2% test crawl (Hetzner)

Provision the 5-node fleet and crawl a random 2% of the routable IPv4 space
(configured in `deploy/config.env` via `SAMPLE_PERCENT=2`; set it to `100` for a
full run — nothing else changes). See [`deploy/README.md`](deploy/README.md) for
prerequisites (Hetzner API token, SSH key, `jq`).

```bash
cd deploy

# 1. Create the 5 Hetzner nodes (billing starts here). Ubuntu + unbound via cloud-init.
./up.sh

# 2. Wait ~60s for cloud-init to finish installing unbound on each node.
sleep 60

# 3. Build the linux binary, push it + the systemd unit to every node, start each shard.
./deploy.sh

# 4. Check progress anytime (read-only, repeatable).
./status.sh
# or watch it: watch -n 30 ./status.sh
```

When the crawl is done (cursors near `255.x`) or you want the data:

```bash
./collect.sh   # pull all shards to ../OUTPUT/collected/ (shard-*.rdnsz)
./down.sh      # DELETE the 5 servers (stops billing) — do this after collecting!
```

Then feed the results into the processor by pointing it **straight at the shard
directory** — `rDNS-Processor` reads the compact `.rdnsz` files directly, block
by block, so there's **no merge/explosion step**:

```bash
node ../../rDNS-Processor/src/index.js analyze ../OUTPUT/collected
node ../../rDNS-Processor/src/index.js process ../OUTPUT/collected
```

### Quickstart: pass 2+ (targeted update crawl)

Once a baseline exists in `OUTPUT/collected`, subsequent passes re-crawl only
the IPs whose previous status is in `RECRAWL_STATUSES` (the current config uses
`has_ptr,timeout,servfail` — 1.75B IPs, 47% of the space) — set in
`deploy/config.env` via `MODE=recrawl` (plus a fresh `COLLECT_DIR` and a more
patient `TIMEOUT`). The commands are identical:

```bash
cd deploy
./up.sh && sleep 60 && ./deploy.sh   # deploy.sh also uploads each node's old shard (~57 MB)
./status.sh                          # SWEPT column = done/total targets
# ...days later...
./collect.sh && ./down.sh

# the payoff — update statistics (timeout recovery, PTR churn, transitions);
# --statuses must match RECRAWL_STATUSES (collect.sh prints the exact command):
cd ..
./bin/rdns-crawler compare --old OUTPUT/collected --new OUTPUT/collected-pass2 \
  --statuses has_ptr,timeout,servfail \
  --json OUTPUT/collected-pass2/compare-stats.json
```

> The old `./collect.sh --merge` (which explodes the shards into a single flat
> `merged.ptr.tsv`, `ipInt<TAB>ptr,ptr`) is still available but **no longer
> needed** — it's ~10× larger than the `.rdnsz` shards and drops the fcrdns /
> status fields. Only use it if some other tool needs a flat text file.

Notes:

- `./up.sh` is idempotent — if a location is out of `cx23` capacity it errors on
  that one node; just re-run it and it fills only the missing ones.
- If `./deploy.sh` runs before a node's cloud-init is ready, it waits/retries on
  that node automatically.
- `./deploy.sh` is re-runnable: bump `CONCURRENCY` in `config.env` and re-run;
  each shard resumes from its cursor.
- Don't forget `./down.sh` at the end — it's the only thing that stops billing.

## Usage

### Test mode (implemented)

Sample N random **non-reserved** IPv4 addresses and resolve their PTR records:

```bash
./bin/rdns-crawler test --count 1000
```

Common flags (see `rdns-crawler test -h`):

| Flag | Default | Meaning |
|------|---------|---------|
| `--count` | `1000` | number of random non-reserved IPv4 to crawl |
| `--concurrency` | `512` | parallel in-flight lookups |
| `--timeout` | `3s` | per-query timeout |
| `--retries` | `2` | retries per lookup (rotating resolvers) |
| `--fcrdns` | `true` | forward-confirm PTR names back to the IP |
| `--qps` | `0` | global max queries/sec (`0` = unlimited) |
| `--resolvers` | built-in | comma-separated resolver list |
| `--resolvers-file` | — | file with one resolver per line (overrides `--resolvers`) |
| `--out` | `OUTPUT/rdns-test-<ts>.jsonl` | output JSONL path |
| `--seed` | time-based | PRNG seed for reproducible sampling |

Example with a custom resolver list and rate cap:

```bash
./bin/rdns-crawler test --count 5000 --concurrency 1000 \
  --resolvers 1.1.1.1,8.8.8.8,9.9.9.9 --qps 2000
```

> Public resolvers rate-limit aggressively. For large runs, point `--resolvers`
> at your own recursive resolver(s) (e.g. a local `unbound`) and raise
> `--concurrency`.

### Crawl mode (distributed full-space)

Shard the routable IPv4 space across `--shards` nodes; this node handles the IPs
where `ip % shards == shard-id`. Writes the compact `.rdnsz` format by default
and checkpoints a resume cursor next to the output file.

```bash
# node 2 of a 5-node fleet, resuming, using a local recursive resolver
./bin/rdns-crawler crawl --shards 5 --shard-id 2 --resume \
  --resolvers 127.0.0.1:53 --concurrency 800 \
  --out OUTPUT/shard-2.rdnsz
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--shards` | `1` | total number of nodes |
| `--shard-id` | `0` | this node's index in `[0, shards)` |
| `--start` | `0.0.0.0` | resume from this IP (inclusive) |
| `--resume` | `false` | resume from the `<out>.cursor` checkpoint |
| `--limit` | `0` | cap addresses emitted for this shard (`0` = whole shard) |
| `--sample-percent` | `100` | crawl a random sample of this % of routable IPv4 (e.g. `2` for a 2% test); split evenly across shards, deterministic per IP |
| `--format` | `rdnsz` | `rdnsz` (compact) or `jsonl` |

Sampling uses a hash of the IP, so the sampled set is the same on every pass
(ideal for testing re-crawl/update logic) and independent of the shard split.

Interleaved sharding (rather than contiguous /8 blocks) keeps every node's load
and progress even — they all sweep the same regions at once, so no single node
gets stuck on a densely-populated block.

For the managed 5-node Hetzner deployment, see **[`deploy/README.md`](deploy/README.md)**.

### Recrawl mode (targeted update pass)

After a baseline pass, you never sweep all 3.7B addresses again. `recrawl`
streams its targets from a previous pass's `.rdnsz` shard and re-queries only
the IPs whose previous status is in `--statuses`:

```bash
# re-crawl shard 3's previous hits + timeouts, with a more patient timeout
./bin/rdns-crawler recrawl --from OUTPUT/collected/shard-3.rdnsz \
  --statuses has_ptr,timeout --resume \
  --resolvers 127.0.0.1:53 --concurrency 800 --timeout 5s --retries 2 \
  --out OUTPUT/pass2/shard-3.rdnsz
```

- The default target set `has_ptr,timeout` covers **what can change** (existing
  PTRs → churn detection) and **what the previous tuning missed** (timeouts —
  raise `--timeout` so slow authoritative servers finally answer). After the
  2026-07 baseline that is 1.60B of 3.70B IPs (43%). Adding `servfail` (as the
  fleet config does) also revisits broken delegations that may have been fixed
  (+147M IPs).
- Shard identity (`shards`/`shard-id`) is **inherited from the old file's
  header**, so a fleet of N nodes re-crawls old shard *i* on node *i* and the
  outputs line up 1:1 with the baseline for `compare`.
- The old file is fully validated first (a truncated plan file is a hard error),
  and the exact target count is known up front.
- `--resume` uses a count cursor (`<out>.cursor`, `done/total` format): the
  filtered target stream is deterministic, so "skip the first N" is an exact
  resume. On restart it rewinds one checkpoint (~131K targets) so in-flight
  losses become harmless duplicates instead of holes.

| Flag | Default | Meaning |
|------|---------|---------|
| `--from` | — | previous-pass `.rdnsz` shard (required) |
| `--statuses` | `has_ptr,timeout` | previous statuses to re-crawl |
| `--resume` | `false` | resume from the `<out>.cursor` checkpoint |
| `--limit` | `0` | cap targets this run (smoke tests) |

plus all common flags (`--timeout`, `--retries`, `--concurrency`, …).

### Compare mode (update statistics)

`compare` joins a previous pass with its re-crawl and reports what the update
actually bought — per shard pair (old shard *i* ↔ new shard *i*), streamed, at
~1.3 GB peak RAM for 20×80M-target shards:

```bash
./bin/rdns-crawler compare --old OUTPUT/collected --new OUTPUT/collected-pass2 \
  --json OUTPUT/collected-pass2/compare-stats.json
```

Reported (text + optional JSON):

- **Timeout recovery**: of the previously timed-out set, the % that now yields
  a PTR, the % now definitive (`has_ptr`/`nxdomain`/`noerror_empty`), the %
  still timing out — plus the full transition breakdown.
- **PTR churn**: of the previously valid set, the % unchanged / changed
  (order/case-insensitive PTR-set comparison) / authoritatively removed /
  transiently failing, with a per-day churn rate.
- **FCrDNS shifts** (confirmed → unconfirmed and vice versa), **net PTR
  movement** (gained from timeouts vs. authoritatively lost), and the **top
  TLDs of recovered PTRs** (what the timeout set turned out to be).
- Bookkeeping: targets not yet crawled, duplicates from restarts, truncated
  shards (comparing while the fleet still runs is fine).

### Info mode

Header summaries without decoding the data:

```bash
./bin/rdns-crawler info OUTPUT/collected            # per shard + aggregate
```

### Sweep mode (legacy)

Sequentially crawl a contiguous block; superseded by `crawl` for real work.

```bash
./bin/rdns-crawler sweep --start 1.0.0.0 --count 100000
```

### Dump mode

Decode a `.rdnsz` file back to JSONL (default) or `ip<TAB>ptr` TSV:

```bash
./bin/rdns-crawler dump OUTPUT/shard-2.rdnsz            # JSONL to stdout
./bin/rdns-crawler dump OUTPUT/shard-2.rdnsz --compact  # ipInt<TAB>ptr,ptr (smallest; for rDNS-Processor)
./bin/rdns-crawler dump OUTPUT/shard-2.rdnsz --text     # ip<TAB>ptr,ptr
```

## Storage: the `.rdnsz` v2 format

Full IPv4 is ~3.7B routable addresses, and ~2/3 have **no** reverse record. v2
keeps **every queried IP** (so the updater knows the state of negatives) yet
stays tiny by:

1. **One record per IP = varint IP-delta + a 1-byte status/flags field**, with
   PTR names only for `has_ptr`. The 8-way status taxonomy is stored per record;
   per-status totals are in the 128-byte header.
2. **IP delta + varint** encoding within IP-sorted blocks.
3. **IP templating**: the record's own IP is substituted out of its hostname
   (`ec2-141-230-114-32...` → marker), perfectly reversible at decode time and
   leaving highly repetitive residue.
4. **Block zstd** compression (independent blocks → streamable, mergeable).

Measured on a real random sample: **~7 bytes per queried IP** (long runs of
identical negative statuses compress to almost nothing) — while retaining the
full result, not just hits. Verified lossless by unit tests
(`internal/store/store_test.go`) and the `dump` round-trip.

> No per-IP timestamp is stored in the observation file: the header's crawl
> timestamp is the "observed at" time for the whole file. Per-IP
> `first_seen`/`last_seen`/`last_changed` live in the master state DB
> (see [`DESIGN.md`](DESIGN.md)).

## Output format (JSONL — via `--format jsonl` or `dump`)

One JSON object per line:

```json
{"ip":"184.27.32.74","ip_int":3088785482,"status":"has_ptr","ptr":["a184-27-32-74.deploy.static.akamaitechnologies.com"],"fcrdns_match":true,"resolver":"8.8.4.4:53","attempts":1,"latency_ms":56,"rcode":"NOERROR","ts":"2026-07-01T14:05:11Z"}
```

| Field | Description |
|-------|-------------|
| `ip` / `ip_int` | dotted-quad and uint32 form |
| `status` | `has_ptr`, `noerror_empty`, `nxdomain`, `servfail`, `refused`, `timeout`, `net_error`, `lame_delegation` |
| `ptr` | PTR hostnames (present when `status=has_ptr`) |
| `fcrdns_match` | forward-confirmation result (omitted when not checked) |
| `resolver` | resolver that answered the (final) attempt |
| `attempts` | number of tries used |
| `latency_ms` | reverse-lookup latency |
| `rcode` | raw DNS rcode name (`NOERROR`, `NXDOMAIN`, `SERVFAIL`, …) when a response was received |
| `error` | transport error detail (timeouts / network errors) |
| `ts` | UTC timestamp |

## Project structure

```
cmd/rdns-crawler/main.go   CLI: test / crawl / recrawl / compare / info / sweep / dump
internal/ipgen             reserved ranges + random / sequential / sharded generators
internal/resolver          PTR + A lookups over UDP, resolver rotation, retries, rcode capture
internal/crawler           parallel worker pool, FCrDNS, progress, QPS limiter
internal/store             compact .rdnsz v2 writer/reader + target streaming (+ tests)
internal/compare           pass-vs-pass update statistics (recovery, churn, transitions)
internal/output            JSONL writer + console summary table
internal/model             shared Record type + status taxonomy + status masks
deploy/                    Hetzner Cloud fleet: up / deploy / status / collect / down
DESIGN.md                  commercial master-DB / updater / scheduler architecture
```

## Feeding results into ipapi.is

Point the sibling [`rDNS-Processor`](../rDNS-Processor) project **directly at the
`.rdnsz` shards** (a single file or a whole directory) — it has a native
streaming `.rdnsz` reader, so no `dump`/merge is required. It turns PTR hostnames
into ipapi.is signals (`is_datacenter`, `is_vpn`, `company.type`,
residential/dynamic, …):

```bash
node ../rDNS-Processor/src/index.js process OUTPUT/collected            # dir of shards
node ../rDNS-Processor/src/index.js process OUTPUT/collected/shard-0.rdnsz  # single shard
```

`dump` is only needed when an *external* tool wants JSONL/TSV instead of
`.rdnsz`.
