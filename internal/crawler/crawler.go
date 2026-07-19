// Package crawler orchestrates highly parallel reverse-DNS crawling: a pool of
// worker goroutines pull IPs from an input channel, resolve PTR (and optionally
// forward-confirm), and push results to an output channel.
package crawler

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"rdns-crawler/internal/model"
	"rdns-crawler/internal/resolver"
)

// Config controls concurrency and per-lookup behavior.
type Config struct {
	Concurrency int           // number of worker goroutines
	FCrDNS      bool          // forward-confirm PTR names back to the IP
	QPS         int           // global max queries/sec (0 = unlimited)
	ProgressOut *os.File      // where to print live progress (nil = silent)
	ProgressInt time.Duration // progress print interval
}

// Crawler ties a Config to a Resolver.
type Crawler struct {
	cfg   Config
	res   *resolver.Resolver
	stats stats
}

type stats struct {
	processed atomic.Int64
	byCode    [model.NumStatusCodes]atomic.Int64
}

// New creates a Crawler.
func New(cfg Config, res *resolver.Resolver) *Crawler {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.ProgressInt <= 0 {
		cfg.ProgressInt = time.Second
	}
	return &Crawler{cfg: cfg, res: res}
}

// Run consumes IPs from ips, resolves them across the worker pool, and writes
// results to out. It closes out when all workers finish. Blocks until done.
func (c *Crawler) Run(ctx context.Context, ips <-chan string, out chan<- model.Record) {
	start := time.Now()

	// Optional global rate limiter shared by all workers.
	var tokens <-chan time.Time
	if c.cfg.QPS > 0 {
		t := time.NewTicker(time.Second / time.Duration(c.cfg.QPS))
		defer t.Stop()
		tokens = t.C
	}

	// Live progress printer.
	stopProgress := make(chan struct{})
	var progressWG sync.WaitGroup
	if c.cfg.ProgressOut != nil {
		progressWG.Add(1)
		go func() {
			defer progressWG.Done()
			c.printProgress(start, stopProgress)
		}()
	}

	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ip := range ips {
				if tokens != nil {
					select {
					case <-tokens:
					case <-ctx.Done():
						return
					}
				}
				rec := c.process(ctx, ip)
				select {
				case out <- rec:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	wg.Wait()
	close(stopProgress)
	progressWG.Wait()
	close(out)
}

// process resolves a single IP into a Record, including FCrDNS when enabled.
func (c *Crawler) process(ctx context.Context, ip string) model.Record {
	ptr := c.res.LookupPTR(ctx, ip)

	rec := model.Record{
		IP:        ip,
		IPInt:     ipToInt(ip),
		Status:    ptr.Status,
		PTR:       ptr.Names,
		Resolver:  ptr.Resolver,
		Attempts:  ptr.Attempts,
		LatencyMs: ptr.Latency.Milliseconds(),
		Rcode:     ptr.Rcode,
		TS:        time.Now().UTC().Format(time.RFC3339),
	}
	if ptr.Err != nil {
		rec.Error = ptr.Err.Error()
	}

	// Forward-confirm: does any PTR hostname resolve (A) back to this IP?
	if c.cfg.FCrDNS && model.HasPTR(ptr.Status) {
		target := net.ParseIP(ip)
		match := false
		for _, name := range ptr.Names {
			addrs, err := c.res.LookupA(ctx, name)
			if err != nil {
				continue
			}
			for _, a := range addrs {
				if a.Equal(target) {
					match = true
					break
				}
			}
			if match {
				break
			}
		}
		rec.FCrDNS = &match
	}

	c.tally(ptr.Status)
	return rec
}

func (c *Crawler) tally(status string) {
	c.stats.processed.Add(1)
	c.stats.byCode[model.Code(status)].Add(1)
}

func (c *Crawler) printProgress(start time.Time, stop <-chan struct{}) {
	// On a TTY we redraw one line with \r. Under systemd/pipes journald only
	// records completed lines, so emit a newline-terminated line instead, and
	// less often to keep the journal small.
	isTTY := false
	if fi, err := c.cfg.ProgressOut.Stat(); err == nil {
		isTTY = fi.Mode()&os.ModeCharDevice != 0
	}
	interval := c.cfg.ProgressInt
	if !isTTY && interval < 30*time.Second {
		interval = 30 * time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	print := func() {
		elapsed := time.Since(start).Seconds()
		done := c.stats.processed.Load()
		rate := 0.0
		if elapsed > 0 {
			rate = float64(done) / elapsed
		}
		// hits + the two dominant negatives, plus a combined failure count.
		hits := c.stats.byCode[model.CodeHasPTR].Load()
		empty := c.stats.byCode[model.CodeNoErrorEmpty].Load()
		nx := c.stats.byCode[model.CodeNXDomain].Load()
		fails := c.stats.byCode[model.CodeServFail].Load() +
			c.stats.byCode[model.CodeRefused].Load() +
			c.stats.byCode[model.CodeTimeout].Load() +
			c.stats.byCode[model.CodeNetError].Load()
		line := fmt.Sprintf("[rdns] %d done | %d ptr | %d empty | %d nxdomain | %d fail | %.0f/s",
			done, hits, empty, nx, fails, rate)
		if isTTY {
			fmt.Fprintf(c.cfg.ProgressOut, "\r%s   ", line)
		} else {
			fmt.Fprintln(c.cfg.ProgressOut, line)
		}
	}
	for {
		select {
		case <-tick.C:
			print()
		case <-stop:
			print()
			if isTTY {
				fmt.Fprintln(c.cfg.ProgressOut)
			}
			return
		}
	}
}

func ipToInt(s string) uint32 {
	ip := net.ParseIP(s)
	if ip == nil {
		return 0
	}
	ip = ip.To4()
	if ip == nil {
		return 0
	}
	return uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])
}
