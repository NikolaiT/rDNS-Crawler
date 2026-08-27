package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/klauspost/compress/zstd"

	"rdns-crawler/internal/model"
)

// Writer buffers records into IP-sorted blocks and writes them zstd-compressed.
// It satisfies the same Consume/Close/PrintSummary contract as output.Writer so
// the crawl pipeline can target either sink. v2 stores every queried IP.
type Writer struct {
	f   *os.File
	bw  *bufio.Writer
	enc *zstd.Encoder

	Path    string
	Start   time.Time
	Shards  uint16
	ShardID uint16

	Total     uint64
	counts    [model.NumStatusCodes]uint64
	FCChecked uint64
	FCMatch   uint64

	buf       []model.Record // current block, flushed at blockRecs
	tldCounts map[string]int
	scratch   []byte
}

// NewWriter creates a fresh .rdnsz file (truncating any existing one).
func NewWriter(path string, shards, shardID uint16) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if _, err := f.Write(make([]byte, headerSize)); err != nil {
		f.Close()
		return nil, err
	}
	return newWriter(f, path, shards, shardID)
}

// NewWriterResume appends to an existing .rdnsz file, seeding running stats from
// its header so patched totals stay cumulative across restarts. Missing/empty
// file → behaves like NewWriter. A shard mismatch is an error (guards against a
// misconfigured --out clobbering another shard's data).
func NewWriterResume(path string, shards, shardID uint16) (*Writer, error) {
	fi, statErr := os.Stat(path)
	if statErr != nil || fi.Size() < headerSize {
		return NewWriter(path, shards, shardID)
	}
	h, err := ReadHeader(path)
	if err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	if h.Shards != shards || h.ShardID != shardID {
		return nil, fmt.Errorf("resume: %s belongs to shard %d/%d, not %d/%d",
			path, h.ShardID, h.Shards, shardID, shards)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	w, err := newWriter(f, path, shards, shardID)
	if err != nil {
		return nil, err
	}
	w.Total = h.Total
	w.counts = h.Counts
	if h.Created != 0 {
		w.Start = time.Unix(h.Created, 0)
	}
	return w, nil
}

func newWriter(f *os.File, path string, shards, shardID uint16) (*Writer, error) {
	bw := bufio.NewWriterSize(f, 1<<20)
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Writer{
		f:         f,
		bw:        bw,
		enc:       enc,
		Path:      path,
		Start:     time.Now(),
		Shards:    shards,
		ShardID:   shardID,
		buf:       make([]model.Record, 0, blockRecs),
		tldCounts: make(map[string]int),
	}, nil
}

// Consume drains the results channel until it is closed.
func (w *Writer) Consume(ch <-chan model.Record) {
	for rec := range ch {
		w.add(rec)
	}
}

// Add appends a single record — the non-channel entry point for batch
// producers (the merge updater) that don't run a crawl pipeline.
func (w *Writer) Add(rec model.Record) { w.add(rec) }

func (w *Writer) add(rec model.Record) {
	w.Total++
	w.counts[model.Code(rec.Status)]++
	if rec.FCrDNS != nil {
		w.FCChecked++
		if *rec.FCrDNS {
			w.FCMatch++
		}
	}
	if model.HasPTR(rec.Status) {
		for _, n := range rec.PTR {
			w.tldCounts[tldOf(n)]++
		}
	}

	// v2: store every queried IP (status is always recorded).
	w.buf = append(w.buf, rec)
	if len(w.buf) >= blockRecs {
		w.flushBlock()
	}
}

// flushBlock encodes, compresses and writes the buffered records as one block.
func (w *Writer) flushBlock() {
	if len(w.buf) == 0 {
		return
	}
	sort.Slice(w.buf, func(i, j int) bool { return w.buf[i].IPInt < w.buf[j].IPInt })

	raw := w.scratch[:0]
	var prev uint32
	for i, rec := range w.buf {
		if i == 0 {
			raw = appendUvarint(raw, uint64(rec.IPInt))
		} else {
			raw = appendUvarint(raw, uint64(rec.IPInt-prev))
		}
		prev = rec.IPInt

		sf := byte(model.Code(rec.Status)) & statusMask
		if rec.FCrDNS != nil {
			sf |= flagFCChecked
			if *rec.FCrDNS {
				sf |= flagFCMatch
			}
		}
		raw = append(raw, sf)

		if model.HasPTR(rec.Status) {
			names := rec.PTR
			if len(names) > maxPTRNames {
				names = names[:maxPTRNames]
			}
			raw = appendUvarint(raw, uint64(len(names)))
			for _, n := range names {
				t := encodeTemplate(rec.IPInt, n)
				raw = appendUvarint(raw, uint64(len(t)))
				raw = append(raw, t...)
			}
		}
	}
	w.scratch = raw[:0]

	comp := w.enc.EncodeAll(raw, nil)
	var hdr [12]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(len(comp)))
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(w.buf)))
	binary.LittleEndian.PutUint32(hdr[8:], uint32(len(raw)))
	w.bw.Write(hdr[:])
	w.bw.Write(comp)

	w.buf = w.buf[:0]
}

// Close flushes the final block and patches the header with final stats.
func (w *Writer) Close() error {
	w.flushBlock()
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.enc.Close(); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := w.f.Write(w.header()); err != nil {
		return err
	}
	return w.f.Close()
}

func (w *Writer) header() []byte {
	h := make([]byte, headerSize)
	copy(h[offMagic:], magic)
	h[offVersion] = formatVer
	binary.LittleEndian.PutUint16(h[offShards:], w.Shards)
	binary.LittleEndian.PutUint16(h[offShardID:], w.ShardID)
	binary.LittleEndian.PutUint64(h[offCreated:], uint64(w.Start.Unix()))
	binary.LittleEndian.PutUint64(h[offTotal:], w.Total)
	for i := 0; i < model.NumStatusCodes; i++ {
		binary.LittleEndian.PutUint64(h[offCounts+i*8:], w.counts[i])
	}
	return h
}

func appendUvarint(b []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(b, tmp[:n]...)
}

// PrintSummary mirrors output.Writer's report with the full status taxonomy and
// on-disk size + bytes/record.
func (w *Writer) PrintSummary(out io.Writer) {
	elapsed := time.Since(w.Start)
	rate := 0.0
	if elapsed.Seconds() > 0 {
		rate = float64(w.Total) / elapsed.Seconds()
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, strings.Repeat("=", 68))
	fmt.Fprintln(out, "  rDNS-Crawler — results (.rdnsz v2)")
	fmt.Fprintln(out, strings.Repeat("=", 68))

	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "  Crawled\t%d IPs in %s (%.0f/s)\n", w.Total, elapsed.Round(time.Millisecond), rate)
	for c := model.StatusCode(0); c < model.NumStatusCodes; c++ {
		n := w.counts[c]
		if n == 0 {
			continue
		}
		fmt.Fprintf(tw, "  %s\t%d\t%s\n", model.StatusString(c), n, upct(n, w.Total))
	}
	if w.FCChecked > 0 {
		fmt.Fprintf(tw, "  FCrDNS match\t%d / %d\t%s\n", w.FCMatch, w.FCChecked, upct(w.FCMatch, w.FCChecked))
	}
	tw.Flush()

	if fi, err := os.Stat(w.Path); err == nil {
		perRec := 0.0
		if w.Total > 0 {
			perRec = float64(fi.Size()) / float64(w.Total)
		}
		fmt.Fprintf(out, "\n  On disk: %s (%.3f bytes / queried IP)\n", humanBytes(fi.Size()), perRec)
	}
	if len(w.tldCounts) > 0 {
		fmt.Fprintln(out, "\n  Top TLDs (of PTR hostnames):")
		for _, kv := range topN(w.tldCounts, 10) {
			fmt.Fprintf(out, "    .%-12s %d\n", kv.key, kv.val)
		}
	}
	fmt.Fprintf(out, "\n  Output: %s\n", w.Path)
	fmt.Fprintln(out, strings.Repeat("=", 68))
}

func upct(n, total uint64) string {
	if total == 0 {
		return "0.0%"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(n)/float64(total))
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}
