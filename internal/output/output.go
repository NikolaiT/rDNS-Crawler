// Package output writes crawl results to JSONL and prints an inspectable
// console summary. A single goroutine should own a Writer (Consume reads the
// results channel), so no locking is required.
package output

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"rdns-crawler/internal/model"
)

const maxSamples = 20

// Writer streams records to a JSONL file and accumulates summary stats.
type Writer struct {
	f   *os.File
	bw  *bufio.Writer
	enc *json.Encoder

	Path      string
	Start     time.Time
	Total     int
	byCode    [model.NumStatusCodes]int
	FCChecked int
	FCMatch   int

	tldCounts map[string]int
	samples   []model.Record
}

// New opens (creates/truncates) the JSONL output file.
func New(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	return &Writer{
		f:         f,
		bw:        bw,
		enc:       json.NewEncoder(bw),
		Path:      path,
		Start:     time.Now(),
		tldCounts: make(map[string]int),
	}, nil
}

// Consume drains the results channel until it is closed.
func (w *Writer) Consume(ch <-chan model.Record) {
	for rec := range ch {
		w.write(rec)
	}
}

func (w *Writer) write(rec model.Record) {
	_ = w.enc.Encode(&rec) // JSONL: Encode appends a newline

	w.Total++
	w.byCode[model.Code(rec.Status)]++
	if model.HasPTR(rec.Status) {
		if len(w.samples) < maxSamples {
			w.samples = append(w.samples, rec)
		}
		for _, name := range rec.PTR {
			w.tldCounts[tldOf(name)]++
		}
	}
	if rec.FCrDNS != nil {
		w.FCChecked++
		if *rec.FCrDNS {
			w.FCMatch++
		}
	}
}

// Close flushes and closes the output file.
func (w *Writer) Close() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}

// PrintSummary renders a human-friendly report of the crawl.
func (w *Writer) PrintSummary(out io.Writer) {
	elapsed := time.Since(w.Start)
	rate := 0.0
	if elapsed.Seconds() > 0 {
		rate = float64(w.Total) / elapsed.Seconds()
	}

	fmt.Fprintln(out)
	fmt.Fprintln(out, strings.Repeat("=", 68))
	fmt.Fprintln(out, "  rDNS-Crawler — results")
	fmt.Fprintln(out, strings.Repeat("=", 68))

	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "  Crawled\t%d IPs in %s (%.0f/s)\n", w.Total, elapsed.Round(time.Millisecond), rate)
	for c := model.StatusCode(0); c < model.NumStatusCodes; c++ {
		if w.byCode[c] == 0 {
			continue
		}
		fmt.Fprintf(tw, "  %s\t%d\t%s\n", model.StatusString(c), w.byCode[c], pct(w.byCode[c], w.Total))
	}
	if w.FCChecked > 0 {
		fmt.Fprintf(tw, "  FCrDNS match\t%d / %d checked\t%s\n", w.FCMatch, w.FCChecked, pct(w.FCMatch, w.FCChecked))
	}
	tw.Flush()

	if len(w.tldCounts) > 0 {
		fmt.Fprintln(out, "\n  Top TLDs (of PTR hostnames):")
		for _, kv := range topN(w.tldCounts, 10) {
			fmt.Fprintf(out, "    .%-12s %d\n", kv.key, kv.val)
		}
	}

	if len(w.samples) > 0 {
		fmt.Fprintln(out, "\n  Sample PTR records:")
		st := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(st, "    IP\tPTR\tFCrDNS")
		for _, s := range w.samples {
			fc := "-"
			if s.FCrDNS != nil {
				if *s.FCrDNS {
					fc = "match"
				} else {
					fc = "no"
				}
			}
			fmt.Fprintf(st, "    %s\t%s\t%s\n", s.IP, strings.Join(s.PTR, ", "), fc)
		}
		st.Flush()
	}

	fmt.Fprintf(out, "\n  Output: %s\n", w.Path)
	fmt.Fprintln(out, strings.Repeat("=", 68))
}

func pct(n, total int) string {
	if total == 0 {
		return "0.0%"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(n)/float64(total))
}

func tldOf(host string) string {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	i := strings.LastIndex(host, ".")
	if i < 0 || i == len(host)-1 {
		return "(none)"
	}
	return host[i+1:]
}

type kv struct {
	key string
	val int
}

func topN(m map[string]int, n int) []kv {
	items := make([]kv, 0, len(m))
	for k, v := range m {
		items = append(items, kv{k, v})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].val != items[j].val {
			return items[i].val > items[j].val
		}
		return items[i].key < items[j].key
	})
	if len(items) > n {
		items = items[:n]
	}
	return items
}
