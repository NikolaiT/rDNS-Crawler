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

### Baseline pass vs. update pass (`MODE`)

`MODE` in `config.env` selects what each node crawls:

- `MODE=crawl` — the full-space **baseline** pass described above.
- `MODE=recrawl` — the targeted **update** pass: each node re-crawls only the
  IPs of the previous pass whose status is in `RECRAWL_STATUSES` (default
  `has_ptr,timeout` — the set that can change plus the set the previous tuning
  missed; ~43% of the space after a first full sweep).

In recrawl mode, `deploy.sh` uploads `OLD_COLLECTED_DIR/shard-<i>.rdnsz`
(~57 MB) to node *i* as its crawl plan — so **`NODES` must equal the previous
pass's shard count** (enforced). Set `COLLECT_DIR` to a fresh directory so
`collect.sh` doesn't overwrite the baseline; when collection is done it prints
the ready-to-run `rdns-crawler compare` command that produces the update
statistics (timeout recovery rate, PTR churn, transition matrix).

After comparing, fold the pass into the baseline with `rdns-crawler merge`
(see the main README): the merged directory is the updated full-space dataset —
feed it to the export / rDNS-Processor and use it as `OLD_COLLECTED_DIR` for
the next pass.

> The whole post-collection chain (verify → compare → merge → `dataset-current`
> symlink → commercial export) is automated by
> [`rDNS-Processor/update.sh`](../../rDNS-Processor/update.sh); the end-to-end
> runbook with all operational gotchas lives in
> [`rDNS-Processor/UPDATE-PLAYBOOK.md`](../../rDNS-Processor/UPDATE-PLAYBOOK.md).

Raise `TIMEOUT` for the update pass (see the tuning comment in
`config.env.example`): recovering previously timed-out IPs is the whole point,
and the smaller target set easily affords the patience.

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
| `deploy.sh` | Cross-compile `linux/amd64`, push binary + systemd unit (+ the previous shard file in `MODE=recrawl`), start/restart each shard (resumes from cursor). Re-runnable. |
| `status.sh` | Per-node service state, resume cursor/progress, and output size. |
| `collect.sh` | `rsync` shard `.rdnsz` files to `COLLECT_DIR` (feed this dir straight to `rDNS-Processor`); legacy `--merge` also explodes them into a flat `merged.ptr.tsv`. |
| `down.sh` | Delete all fleet servers (confirm with `delete`, or `--yes`). |
| `lib.sh` | Shared Hetzner API + SSH helpers + config validation. |
| `cloud-init.yaml` | Node bootstrap: unbound, sysctl/ulimits, `/opt/rdns`. |
| `rdns-crawler.service.tmpl` | systemd unit template for `MODE=crawl` (shard/concurrency/etc. substituted per node). |
| `rdns-recrawl.service.tmpl` | systemd unit template for `MODE=recrawl` (streams targets from the uploaded previous shard). |

## Operational notes

- **Resume**: each shard checkpoints `OUTPUT/shard-<id>.rdnsz.cursor`. In
  `crawl` mode the cursor is the last IP swept; in `recrawl` mode it is
  `done/total` counts into the target stream (resume rewinds one checkpoint, so
  a crash re-crawls up to ~131K targets rather than skipping in-flight ones —
  duplicates are dedup'd by `compare` and the updater). The systemd unit runs
  with `--resume`, so a reboot or `deploy.sh` re-run continues from that cursor
  and **appends** to the existing `.rdnsz` (its cumulative header stats are
  preserved). Resume refuses to write if the file belongs to a different shard,
  guarding against a misconfigured `--out`.
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
