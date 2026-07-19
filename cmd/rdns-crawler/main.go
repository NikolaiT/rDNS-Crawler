// Command rdns-crawler crawls reverse-DNS (PTR) data for the IPv4 address space,
// skipping reserved/bogon ranges (as defined by the monorepo's ip_address_tools).
//
// Modes:
//
//	test   Sample N random NON-reserved IPv4 addresses and resolve their PTR.
//	crawl  Distributed full-space crawl. Shards the address space across N nodes
//	       (ip %% shards == shard-id), resumes from a checkpoint, and writes the
//	       compact .rdnsz format by default. This is what the Hetzner fleet runs.
//	sweep  (legacy) Sequentially crawl a contiguous block; JSONL by default.
//	dump   Read a .rdnsz file back to JSONL/text (for inspection or handoff to
//	       rDNS-Processor).
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
  rdns-crawler test  [flags]              Sample random non-reserved IPv4 → resolve PTR
  rdns-crawler crawl [flags]              Distributed, sharded, resumable full-space crawl
  rdns-crawler sweep [flags]              (legacy) crawl a contiguous IPv4 block
  rdns-crawler dump  <file.rdnsz> [flags] Decode a .rdnsz file to JSONL/text

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
