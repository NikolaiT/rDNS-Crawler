# rDNS-Crawler — distributed Hetzner Cloud deployment

Provisions a fleet of **N Ubuntu nodes** on Hetzner Cloud (each with its own
public IPv4), installs a **local `unbound` recursive resolver** on each, and runs
one **shard** of the IPv4 crawl per node. Results are stored in the compact
`.rdnsz` format and pulled back for processing by
[`rDNS-Processor`](../../rDNS-Processor), which reads the `.rdnsz` shards
directly (no merge step).

Everything is plain **Bash + the Hetzner Cloud API** (`curl` + `jq`) plus
`cloud-init`. No Terraform, no agents.

## Why this design

- **Own IP per node + local unbound.** Public resolvers (1.1.1.1, 8.8.8.8) will
  throttle or block billions of PTR queries. Each node instead runs `unbound` and
  recurses to authoritative servers directly from its own address, so throughput
  scales with the fleet rather than hitting a shared rate limit.
- **Interleaved sharding** (`ip % shards == shard-id`). Every node sweeps the same
  regions of the address space simultaneously, so load and progress stay even and
  resume is just a per-node cursor.
- **Compact storage.** `.rdnsz` v2 records **every queried IP** (status included,
  not just hits) yet averages **~7 B/queried IP** after delta/template/zstd, so
  even a full-IPv4 result set is small to store and transfer.

## Prerequisites

- A Hetzner Cloud project + **API token** (Read & Write).
- Local tools: `bash`, `curl`, `jq`, `ssh`, `rsync`, and Go (for the
  cross-compile in `deploy.sh`).
- An SSH keypair (default `~/.ssh/id_ed25519`).

## Configure

```bash
cd deploy
cp config.env.example config.env
$EDITOR config.env        # set HCLOUD_TOKEN, NODES, SERVER_TYPE, etc.
```

`config.env` is gitignored (it holds your token).

> **Server type / locations:** the default `cx23` (Intel) line is EU-only
> (`nbg1`, `fsn1`, `hel1`). For US/Asia locations use an AMD `cpx` type (e.g.
> `cpx22`) and add those locations to `LOCATIONS`.

### Test run vs. full run

`SAMPLE_PERCENT` in `config.env` controls how much of the space is crawled:

- `SAMPLE_PERCENT=2` — a random, evenly-distributed **2% test** (~74M IPs total,
  ~14–15M per node). Same commands as below; good for validating the pipeline
  end-to-end before committing to a full scan.
- `SAMPLE_PERCENT=100` — the full routable space (production).

The sample is a deterministic hash of the IP, so re-runs hit the same set and
each node still resumes from its own cursor. Nothing else changes between a test
and a full run except this number.

## Run

```bash
./up.sh          # create N servers (idempotent); cloud-init installs unbound
# wait ~60s for cloud-init
./deploy.sh      # cross-compile, upload binary + systemd unit, start each shard
./status.sh      # per-node: service state, resume cursor, output size
```

Let it run. When you want the data:

```bash
./collect.sh             # rsync each shard's .rdnsz to ../OUTPUT/collected/
./down.sh                # destroy the fleet (prompts; --yes to skip)
```

Then process — point `rDNS-Processor` at the shard **directory**; it streams the
`.rdnsz` files directly (no merge needed):

```bash
node ../../rDNS-Processor/src/index.js process ../OUTPUT/collected
```

> `./collect.sh --merge` still exists (it explodes the shards into a single flat
> `merged.ptr.tsv`), but it's legacy — the processor reads `.rdnsz` natively, and
> the merged file is ~10× larger and loses the fcrdns/status fields.

## Scripts

| Script | Purpose |
|--------|---------|
| `up.sh` | Create the fleet (one node per shard) with cloud-init. Idempotent. |
| `deploy.sh` | Cross-compile `linux/amd64`, push binary + `rdns-crawler.service`, start/restart each shard (resumes from cursor). Re-runnable. |
| `status.sh` | Per-node service state, resume cursor, and output size. |
| `collect.sh` | `rsync` shard `.rdnsz` files to `../OUTPUT/collected/` (feed this dir straight to `rDNS-Processor`); legacy `--merge` also explodes them into a flat `merged.ptr.tsv`. |
| `down.sh` | Delete all fleet servers (confirm with `delete`, or `--yes`). |
| `lib.sh` | Shared Hetzner API + SSH helpers. |
| `cloud-init.yaml` | Node bootstrap: unbound, sysctl/ulimits, `/opt/rdns`. |
| `rdns-crawler.service.tmpl` | systemd unit template (shard/concurrency/etc. substituted per node). |

## Operational notes

- **Resume**: each shard checkpoints `OUTPUT/shard-<id>.rdnsz.cursor` (the last
  IP it swept). The systemd unit runs with `--resume`, so a reboot or `deploy.sh`
  re-run continues from that cursor and **appends** to the existing `.rdnsz`
  (its cumulative header stats are preserved). Resume refuses to write if the
  file belongs to a different shard, guarding against a misconfigured `--out`.
- **Tuning**: raise `CONCURRENCY` in `config.env` if nodes are CPU/network idle;
  `unbound` on `cx23` (4 threads) handles high concurrency comfortably.
- **Cost control**: `./down.sh` deletes the servers. Always `./collect.sh` first.
- **Politeness**: reverse-DNS is low-impact, but a full scan is still a lot of
  queries. Keep per-node concurrency reasonable and prefer the local resolver.

## Merging shards (optional)

You normally **don't need to merge** — `rDNS-Processor` reads a directory of
`shard-*.rdnsz` files directly. Shards are disjoint by construction
(`ip % shards`), so if some external tool needs a single stream, merging is just
concatenating their decoded records:

```bash
for f in ../OUTPUT/collected/shard-*.rdnsz; do
  ../bin/rdns-crawler dump "$f"
done > all.jsonl
```

Each `.rdnsz` header records its `shard/shards`, so you can verify coverage with
`rdns-crawler dump` (it prints the header stats to stderr).
