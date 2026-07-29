// Command rdns-crawler crawls reverse-DNS (PTR) data for the IPv4 address space,
// skipping reserved/bogon ranges (as defined by the monorepo's ip_address_tools).
//
// Modes:
//
//	test     Sample N random NON-reserved IPv4 addresses and resolve their PTR.
//	crawl    Distributed full-space crawl. Shards the address space across N nodes
//	         (ip %% shards == shard-id), resumes from a checkpoint, and writes the
//	         compact .rdnsz format by default. This is what the Hetzner fleet runs
//	         on a first (baseline) pass.
//	recrawl  Targeted update pass: re-crawl only the IPs of a previous pass's
//	         .rdnsz shard whose status matched --statuses (default
//	         has_ptr,timeout). This is what the fleet runs on every pass after
//	         the first — ~43%% of the space instead of 100%%.
//	compare  Join a previous pass with a re-crawl pass and print update
//	         statistics (timeout recovery rate, PTR churn, transition matrix).
//	info     Print .rdnsz header summaries (per file + aggregate).
//	sweep    (legacy) Sequentially crawl a contiguous block; JSONL by default.
//	dump     Read a .rdnsz file back to JSONL/text (for inspection or handoff to
//	         rDNS-Processor).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"rdns-crawler/internal/compare"
	"rdns-crawler/internal/crawler"
	"rdns-crawler/internal/ipgen"
	"rdns-crawler/internal/model"
	"rdns-crawler/internal/output"
	"rdns-crawler/internal/resolver"
	"rdns-crawler/internal/store"
)

const outputDir = "OUTPUT"

// sink is the common contract for both output backends (JSONL and .rdnsz).
type sink interface {
	Consume(<-chan model.Record)
	Close() error
	PrintSummary(io.Writer)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "test":
		runTest(os.Args[2:])
	case "crawl":
		runCrawl(os.Args[2:])
	case "recrawl":
		runRecrawl(os.Args[2:])
	case "compare":
		runCompare(os.Args[2:])
	case "info":
		runInfo(os.Args[2:])
	case "sweep":
		runSweep(os.Args[2:])
	case "dump":
		runDump(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `rDNS-Crawler — reverse-DNS crawler for the IPv4 address space

Usage:
  rdns-crawler test    [flags]              Sample random non-reserved IPv4 → resolve PTR
  rdns-crawler crawl   [flags]              Distributed, sharded, resumable full-space crawl
  rdns-crawler recrawl [flags]              Re-crawl a previous shard's has_ptr/timeout IPs
  rdns-crawler compare [flags]              Statistics: previous pass vs re-crawl pass
  rdns-crawler info    <file|dir> ...       Print .rdnsz header summaries
  rdns-crawler sweep   [flags]              (legacy) crawl a contiguous IPv4 block
  rdns-crawler dump    <file.rdnsz> [flags] Decode a .rdnsz file to JSONL/text

Run "rdns-crawler <mode> -h" for flags.
`)
}

// commonFlags are shared by the crawling modes.
type commonFlags struct {
	concurrency   int
	timeout       time.Duration
	retries       int
	fcrdns        bool
	qps           int
	resolvers     string
	resolversFile string
	out           string
	format        string // "rdnsz" or "jsonl"
}

func registerCommon(fs *flag.FlagSet, defFormat string) *commonFlags {
	c := &commonFlags{}
	fs.IntVar(&c.concurrency, "concurrency", 512, "number of parallel workers (in-flight lookups)")
	fs.DurationVar(&c.timeout, "timeout", 3*time.Second, "per-query timeout")
	fs.IntVar(&c.retries, "retries", 2, "retries per lookup (rotating resolvers)")
	fs.BoolVar(&c.fcrdns, "fcrdns", true, "forward-confirm PTR names back to the IP")
	fs.IntVar(&c.qps, "qps", 0, "global max queries/sec (0 = unlimited)")
	fs.StringVar(&c.resolvers, "resolvers", "", "comma-separated resolver list (default: built-in public resolvers)")
	fs.StringVar(&c.resolversFile, "resolvers-file", "", "file with one resolver per line (overrides --resolvers)")
	fs.StringVar(&c.out, "out", "", "output path (default: OUTPUT/<mode>-<timestamp>.<ext>)")
	fs.StringVar(&c.format, "format", defFormat, "output format: rdnsz (compact) or jsonl")
	return c
}

func (c *commonFlags) resolverList() ([]string, error) {
	if c.resolversFile != "" {
		data, err := os.ReadFile(c.resolversFile)
		if err != nil {
			return nil, fmt.Errorf("reading resolvers file: %w", err)
		}
		var list []string
		for _, ln := range strings.Split(string(data), "\n") {
			ln = strings.TrimSpace(ln)
			if ln == "" || strings.HasPrefix(ln, "#") {
				continue
			}
			list = append(list, ln)
		}
		if len(list) == 0 {
			return nil, fmt.Errorf("resolvers file %q is empty", c.resolversFile)
		}
		return list, nil
	}
	if strings.TrimSpace(c.resolvers) != "" {
		return strings.Split(c.resolvers, ","), nil
	}
	return resolver.DefaultResolvers, nil
}

func (c *commonFlags) ext() string {
	if c.format == "jsonl" {
		return "jsonl"
	}
	return "rdnsz"
}

func (c *commonFlags) outPath(mode string) (string, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return "", err
	}
	if c.out != "" {
		if dir := filepath.Dir(c.out); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return "", err
			}
		}
		return c.out, nil
	}
	ts := time.Now().Format("2006-01-02T15-04-05")
	return filepath.Join(outputDir, fmt.Sprintf("rdns-%s-%s.%s", mode, ts, c.ext())), nil
}

// newSink builds the configured output backend. resume only applies to the
// compact .rdnsz backend (JSONL always truncates).
func (c *commonFlags) newSink(path string, shards, shardID uint16, resume bool) (sink, error) {
	if c.format == "jsonl" {
		return output.New(path)
	}
	if resume {
		return store.NewWriterResume(path, shards, shardID)
	}
	return store.NewWriter(path, shards, shardID)
}

func runTest(args []string) {
	fs := flag.NewFlagSet("test", flag.ExitOnError)
	common := registerCommon(fs, "jsonl")
	count := fs.Int("count", 1000, "number of random non-reserved IPv4 addresses to crawl")
	seed := fs.Int64("seed", time.Now().UnixNano(), "PRNG seed for reproducible sampling")
	fs.Parse(args)

	res, wr, crw, err := setup(common, "test", 0, 0, false)
	if err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "[rdns] test: %d random non-reserved IPv4 | concurrency=%d | fcrdns=%v | %d resolvers | format=%s\n",
		*count, common.concurrency, common.fcrdns, len(res.Servers()), common.format)

	ctx, stop := signalCtx()
	defer stop()

	ips := ipgen.GenerateRandom(ctx, *count, *seed)
	pipeline(ctx, crw, ips, wr, nil, "")
}

func runCrawl(args []string) {
	fs := flag.NewFlagSet("crawl", flag.ExitOnError)
	common := registerCommon(fs, "rdnsz")
	shards := fs.Uint("shards", 1, "total number of crawler nodes")
	shardID := fs.Uint("shard-id", 0, "this node's shard index in [0, shards)")
	startIP := fs.String("start", "0.0.0.0", "resume from this IPv4 (inclusive); overridden by --resume")
	limit := fs.Uint64("limit", 0, "max addresses to emit for this shard (0 = whole space)")
	samplePct := fs.Float64("sample-percent", 100, "crawl a random sample of this %% of routable IPv4 (100 = full space)")
	resume := fs.Bool("resume", false, "resume from the checkpoint file next to --out")
	fs.Parse(args)

	if *shardID >= *shards {
		fatal(fmt.Errorf("--shard-id %d must be < --shards %d", *shardID, *shards))
	}
	sampleThreshold := ipgen.SampleThresholdForPercent(*samplePct)

	start := ipgen.IPToInt(*startIP)
	outPath, err := common.outPath("crawl")
	if err != nil {
		fatal(err)
	}
	cursorPath := outPath + ".cursor"
	if *resume {
		if v, ok := readCursor(cursorPath); ok {
			start = v + 1
			fmt.Fprintf(os.Stderr, "[rdns] resuming shard %d/%d from %s\n", *shardID, *shards, ipgen.IntToIP(start))
		}
	}

	res, wr, crw, err := setup(common, "crawl", uint16(*shards), uint16(*shardID), *resume)
	if err != nil {
		fatal(err)
	}

	sampleDesc := "full space"
	if sampleThreshold != 0 {
		sampleDesc = fmt.Sprintf("%.4g%% sample", *samplePct)
	}
	fmt.Fprintf(os.Stderr, "[rdns] crawl: shard %d/%d | %s | start=%s | concurrency=%d | %d resolvers | format=%s\n",
		*shardID, *shards, sampleDesc, ipgen.IntToIP(start), common.concurrency, len(res.Servers()), common.format)

	ctx, stop := signalCtx()
	defer stop()

	ips, cursor := ipgen.GenerateShardedFull(ctx, ipgen.ShardConfig{
		Shards:          uint32(*shards),
		ShardID:         uint32(*shardID),
		StartIP:         start,
		Limit:           *limit,
		SampleThreshold: sampleThreshold,
	})
	pipeline(ctx, crw, ips, wr, cursor, cursorPath)
}

// runRecrawl re-crawls the IPs of a previous pass's .rdnsz shard whose status
// is in --statuses. The shard identity (shards/shard-id) is inherited from the
// old file's header, so the output slots into the same fleet layout and can be
// compared 1:1 afterwards. Resume is count-based: the cursor stores how many
// targets of the (deterministic) filtered stream have been handed to workers.
func runRecrawl(args []string) {
	fs := flag.NewFlagSet("recrawl", flag.ExitOnError)
	common := registerCommon(fs, "rdnsz")
	from := fs.String("from", "", "previous-pass .rdnsz shard to re-crawl from (required)")
	statuses := fs.String("statuses", "has_ptr,timeout", "comma-separated previous statuses to re-crawl")
	limit := fs.Uint64("limit", 0, "max targets to crawl this run (0 = all; useful for smoke tests)")
	resume := fs.Bool("resume", false, "resume from the checkpoint file next to --out")
	fs.Parse(args)

	if *from == "" {
		fatal(errors.New("recrawl: --from <previous-shard.rdnsz> is required"))
	}
	mask, err := model.ParseStatuses(*statuses)
	if err != nil {
		fatal(err)
	}
	oldHdr, err := store.ReadHeader(*from)
	if err != nil {
		fatal(fmt.Errorf("reading previous shard: %w", err))
	}

	// Full validation pass over the old file. This both hard-fails on a
	// truncated/corrupt observation file (silently crawling a partial target
	// set would poison the update) and gives us the exact expected total.
	expected, err := store.CountTargets(*from, mask)
	if err != nil {
		fatal(fmt.Errorf("previous shard %s is not usable as a re-crawl plan: %w", *from, err))
	}

	outPath, err := common.outPath("recrawl")
	if err != nil {
		fatal(err)
	}
	cursorPath := outPath + ".cursor"
	var skip uint64
	if *resume {
		if n, ok := readCountCursor(cursorPath); ok {
			// Rewind one checkpoint interval: targets handed out but not yet
			// completed when the previous run died are re-crawled (harmless
			// duplicates) instead of skipped (holes).
			if n > store.DefaultTargetCheckpoint {
				skip = n - store.DefaultTargetCheckpoint
			}
			fmt.Fprintf(os.Stderr, "[rdns] resuming at target %d/%d (cursor %d, rewound one checkpoint)\n", skip, expected, n)
		}
	}

	servers, err := common.resolverList()
	if err != nil {
		fatal(err)
	}
	res, err := resolver.New(servers, common.timeout, common.retries)
	if err != nil {
		fatal(err)
	}
	wr, err := common.newSink(outPath, oldHdr.Shards, oldHdr.ShardID, *resume)
	if err != nil {
		fatal(err)
	}
	crw := crawler.New(crawler.Config{
		Concurrency: common.concurrency,
		FCrDNS:      common.fcrdns,
		QPS:         common.qps,
		ProgressOut: os.Stderr,
		ProgressInt: time.Second,
	}, res)

	fmt.Fprintf(os.Stderr, "[rdns] recrawl: shard %d/%d | %d targets (%s) from %s | timeout=%s retries=%d | concurrency=%d | %d resolvers\n",
		oldHdr.ShardID, oldHdr.Shards, expected, mask, filepath.Base(*from),
		common.timeout, common.retries, common.concurrency, len(res.Servers()))

	ctx, stop := signalCtx()
	defer stop()

	ips, cursor, errch := store.StreamTargets(ctx, *from, mask, skip, *limit, 0)
	go func() {
		for n := range cursor {
			writeCountCursor(cursorPath, n, expected)
		}
	}()
	pipeline(ctx, crw, ips, wr, nil, "")
	if err := <-errch; err != nil {
		fatal(fmt.Errorf("target stream from %s failed mid-crawl: %w", *from, err))
	}
}

// runCompare joins a previous pass with a re-crawl pass and prints the update
// statistics (and optionally writes them as JSON).
func runCompare(args []string) {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	oldPath := fs.String("old", "", "previous pass: .rdnsz file or directory of shards (required)")
	newPath := fs.String("new", "", "re-crawl pass: .rdnsz file or directory of shards (required)")
	statuses := fs.String("statuses", "has_ptr,timeout", "previous statuses that formed the re-crawl target set")
	jsonOut := fs.String("json", "", "also write the full stats as JSON to this path")
	topTLDs := fs.Int("top-tlds", 20, "how many TLDs of gained PTRs to report")
	fs.Parse(args)

	if *oldPath == "" || *newPath == "" {
		fatal(errors.New("compare: --old and --new are required"))
	}
	mask, err := model.ParseStatuses(*statuses)
	if err != nil {
		fatal(err)
	}
	res, err := compare.Run(compare.Options{
		OldPath: *oldPath,
		NewPath: *newPath,
		Mask:    mask,
		TopTLDs: *topTLDs,
	}, os.Stderr)
	if err != nil {
		fatal(err)
	}
	res.RenderText(os.Stdout)
	if *jsonOut != "" {
		data, err := json.MarshalIndent(res, "", "  ")
		if err != nil {
			fatal(err)
		}
		if err := os.WriteFile(*jsonOut, data, 0o644); err != nil {
			fatal(err)
		}
		fmt.Fprintf(os.Stderr, "[rdns] stats JSON written to %s\n", *jsonOut)
	}
}

// runInfo prints header summaries (per file and aggregate) for .rdnsz files.
func runInfo(args []string) {
	var paths []string
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			continue
		}
		fi, err := os.Stat(a)
		if err != nil {
			fatal(err)
		}
		if fi.IsDir() {
			m, _ := filepath.Glob(filepath.Join(a, "*.rdnsz"))
			paths = append(paths, m...)
		} else {
			paths = append(paths, a)
		}
	}
	if len(paths) == 0 {
		fatal(errors.New("usage: rdns-crawler info <file.rdnsz|dir> ..."))
	}

	var total uint64
	var counts [model.NumStatusCodes]uint64
	for _, p := range paths {
		h, err := store.ReadHeader(p)
		if err != nil {
			fatal(err)
		}
		total += h.Total
		for i := 0; i < model.NumStatusCodes; i++ {
			counts[i] += h.Counts[i]
		}
		fmt.Printf("%-24s shard %2d/%d  total=%-12d has_ptr=%-12d timeout=%-12d created=%s\n",
			filepath.Base(p), h.ShardID, h.Shards, h.Total,
			h.Count(model.CodeHasPTR), h.Count(model.CodeTimeout),
			time.Unix(h.Created, 0).UTC().Format("2006-01-02"))
	}
	if len(paths) > 1 {
		fmt.Printf("\naggregate over %d files: total=%d\n", len(paths), total)
	} else {
		fmt.Printf("\ntotal=%d\n", total)
	}
	for c := model.StatusCode(0); c < model.NumStatusCodes; c++ {
		if counts[c] == 0 {
			continue
		}
		fmt.Printf("  %-16s %-13d %5.2f%%\n", model.StatusString(c), counts[c], 100*float64(counts[c])/float64(total))
	}
	recrawlSet := counts[model.CodeHasPTR] + counts[model.CodeTimeout]
	if total > 0 && recrawlSet > 0 {
		fmt.Printf("  re-crawl target set (has_ptr+timeout): %d (%.1f%%)\n", recrawlSet, 100*float64(recrawlSet)/float64(total))
	}
}

func runSweep(args []string) {
	fs := flag.NewFlagSet("sweep", flag.ExitOnError)
	common := registerCommon(fs, "jsonl")
	startIP := fs.String("start", "1.0.0.0", "first IPv4 address of the block to sweep")
	count := fs.Uint64("count", 100000, "number of non-reserved addresses to crawl")
	fs.Parse(args)

	fmt.Fprintln(os.Stderr, "[rdns] NOTE: sweep is legacy; prefer 'crawl' for distributed full-space work.")
	start := ipgen.IPToInt(*startIP)
	if start == 0 && *startIP != "0.0.0.0" {
		fatal(fmt.Errorf("invalid --start %q", *startIP))
	}

	res, wr, crw, err := setup(common, "sweep", 0, 0, false)
	if err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "[rdns] sweep: %d non-reserved IPv4 from %s | concurrency=%d | %d resolvers | format=%s\n",
		*count, *startIP, common.concurrency, len(res.Servers()), common.format)

	ctx, stop := signalCtx()
	defer stop()

	ips := ipgen.GenerateSequential(ctx, start, *count)
	pipeline(ctx, crw, ips, wr, nil, "")
}

// runDump decodes a .rdnsz file back to JSONL (default) or a plain "ip\tptr" TSV.
func runDump(args []string) {
	// Accept flags in any position relative to the file argument (Go's flag
	// package otherwise stops at the first positional).
	var flagArgs []string
	var path string
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			flagArgs = append(flagArgs, a)
		} else if path == "" {
			path = a
		}
	}
	fs := flag.NewFlagSet("dump", flag.ExitOnError)
	asText := fs.Bool("text", false, "emit 'ip<TAB>ptr[,ptr...]' (has_ptr rows only) instead of JSONL")
	asCompact := fs.Bool("compact", false, "emit 'ipInt<TAB>ptr[,ptr...]' (has_ptr rows only) — the smallest form, for rDNS-Processor")
	fs.Parse(flagArgs)
	if path == "" {
		fatal(fmt.Errorf("usage: rdns-crawler dump <file.rdnsz> [--compact|--text]"))
	}

	h, err := store.ReadHeader(path)
	if err != nil {
		fatal(err)
	}
	var breakdown strings.Builder
	for c := model.StatusCode(0); c < model.NumStatusCodes; c++ {
		if n := h.Count(c); n > 0 {
			fmt.Fprintf(&breakdown, " %s=%d", model.StatusString(c), n)
		}
	}
	fmt.Fprintf(os.Stderr, "[rdns] %s: v%d shard %d/%d | total=%d |%s\n",
		filepath.Base(path), h.Version, h.ShardID, h.Shards, h.Total, breakdown.String())

	bw := bufio.NewWriterSize(os.Stdout, 1<<20)
	defer bw.Flush()
	enc := json.NewEncoder(bw)

	err = store.Scan(path, func(r store.Rec) error {
		// Compact: "ipInt<TAB>ptr,ptr" — smallest form, has_ptr rows only. This
		// is the format rDNS-Processor consumes (the full JSONL is far larger
		// and carries fields the processor doesn't need).
		if *asCompact {
			if r.Status == model.CodeHasPTR {
				fmt.Fprintf(bw, "%d\t%s\n", r.IP, strings.Join(r.PTR, ","))
			}
			return nil
		}
		if *asText {
			// Only PTR-bearing rows are meaningful as ip<TAB>ptr.
			if r.Status == model.CodeHasPTR {
				fmt.Fprintf(bw, "%s\t%s\n", ipgen.IntToIP(r.IP), strings.Join(r.PTR, ","))
			}
			return nil
		}
		fc := r.FCMatch
		rec := model.Record{
			IP:     ipgen.IntToIP(r.IP),
			IPInt:  r.IP,
			Status: model.StatusString(r.Status),
			PTR:    r.PTR,
		}
		if r.FCChecked {
			rec.FCrDNS = &fc
		}
		return enc.Encode(&rec)
	})
	// A truncated tail is expected when dumping a file collected from a shard
	// that was still crawling — decode what's there and warn, don't abort.
	if err != nil && !errors.Is(err, store.ErrTruncatedTail) {
		fatal(err)
	}
	if errors.Is(err, store.ErrTruncatedTail) {
		fmt.Fprintf(os.Stderr, "[rdns] %s: %v (dumped all complete blocks)\n", filepath.Base(path), err)
	}
}

// setup builds the resolver, output sink, and crawler from common flags.
func setup(common *commonFlags, mode string, shards, shardID uint16, resume bool) (*resolver.Resolver, sink, *crawler.Crawler, error) {
	servers, err := common.resolverList()
	if err != nil {
		return nil, nil, nil, err
	}
	res, err := resolver.New(servers, common.timeout, common.retries)
	if err != nil {
		return nil, nil, nil, err
	}
	outPath, err := common.outPath(mode)
	if err != nil {
		return nil, nil, nil, err
	}
	wr, err := common.newSink(outPath, shards, shardID, resume)
	if err != nil {
		return nil, nil, nil, err
	}
	crw := crawler.New(crawler.Config{
		Concurrency: common.concurrency,
		FCrDNS:      common.fcrdns,
		QPS:         common.qps,
		ProgressOut: os.Stderr,
		ProgressInt: time.Second,
	}, res)
	return res, wr, crw, nil
}

// pipeline wires the crawler output into the sink and blocks until done. If a
// cursor channel is supplied, the latest resume position is checkpointed to
// cursorPath as the crawl progresses.
func pipeline(ctx context.Context, crw *crawler.Crawler, ips <-chan string, wr sink, cursor <-chan uint32, cursorPath string) {
	results := make(chan model.Record, 4096)
	done := make(chan struct{})
	go func() {
		wr.Consume(results)
		close(done)
	}()

	if cursor != nil && cursorPath != "" {
		go func() {
			for v := range cursor {
				writeCursor(cursorPath, v)
			}
		}()
	}

	crw.Run(ctx, ips, results) // closes results when workers finish
	<-done

	if err := wr.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: closing output: %v\n", err)
	}
	wr.PrintSummary(os.Stdout)
}

func readCursor(path string) (uint32, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	v := ipgen.IPToInt(strings.TrimSpace(string(data)))
	if v == 0 {
		return 0, false
	}
	return v, true
}

// Count cursor for recrawl mode: "<done>/<total>\n". The total is informative
// (status.sh renders done/total as a percentage); only <done> is parsed back.
func readCountCursor(path string) (uint64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(data))
	if i := strings.IndexByte(s, '/'); i >= 0 {
		s = s[:i]
	}
	var n uint64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, false
	}
	return n, true
}

func writeCountCursor(path string, done, total uint64) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(fmt.Sprintf("%d/%d\n", done, total)), 0o644); err == nil {
		os.Rename(tmp, path)
	}
}

func writeCursor(path string, v uint32) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(ipgen.IntToIP(v)+"\n"), 0o644); err == nil {
		os.Rename(tmp, path)
	}
}

func signalCtx() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
