package store

import (
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"rdns-crawler/internal/model"
)

// TestTemplateRoundTrip verifies the IP-templating transform is exactly
// reversible, including mixed-case hostnames and forward/reverse, dashed/dotted
// embeddings of the IP.
func TestTemplateRoundTrip(t *testing.T) {
	cases := []struct {
		ip   uint32
		host string
	}{
		{1091922644, "static.212.106.21.65.clients.your-server.de"},
		{2380689952, "ec2-141-230-114-32.ap-southeast-1.compute.amazonaws.com"},
		{1054001134, "62-210-199-238.rev.poneytelecom.eu"},
		{1488786684, "88-189-20-252.subs.proxad.net"},
		{16909060, "KD1-2-3-4.example.JP"},
		{16909060, "host-4.3.2.1.rev.example.net"},
		{3232235777, "no-ip-here.example.com"},
	}
	for _, c := range cases {
		enc := encodeTemplate(c.ip, c.host)
		got := decodeTemplate(c.ip, enc)
		if got != c.host {
			t.Errorf("round-trip mismatch: ip=%d host=%q -> %q", c.ip, c.host, got)
		}
	}
}

// TestWriterReaderRoundTrip (v2) writes a mix of statuses through the writer and
// reads them back, asserting that EVERY queried IP is stored (not just hits),
// per-status header counts are correct, PTR names and FCrDNS flags survive, and
// shard metadata is preserved.
func TestWriterReaderRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.rdnsz")

	tr, fa := true, false
	in := []model.Record{
		{IP: "141.230.114.32", IPInt: 2380689952, Status: model.StatusHasPTR, PTR: []string{"ec2-141-230-114-32.ap-southeast-1.compute.amazonaws.com"}, FCrDNS: &tr},
		{IP: "62.210.199.238", IPInt: 1054001134, Status: model.StatusHasPTR, PTR: []string{"62-210-199-238.rev.poneytelecom.eu"}, FCrDNS: &fa},
		{IP: "8.8.8.8", IPInt: 134744072, Status: model.StatusHasPTR, PTR: []string{"dns.google"}},
		{IP: "1.2.3.4", IPInt: 16909060, Status: model.StatusNoErrorEmpty},
		{IP: "1.2.3.5", IPInt: 16909061, Status: model.StatusNXDomain},
		{IP: "5.6.7.8", IPInt: 84281096, Status: model.StatusTimeout},
		{IP: "9.9.9.9", IPInt: 151587081, Status: model.StatusServFail},
		{IP: "9.9.9.10", IPInt: 151587082, Status: model.StatusRefused},
		{IP: "9.9.9.11", IPInt: 151587083, Status: model.StatusNetError},
	}

	w, err := NewWriter(path, 5, 2)
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan model.Record, len(in))
	for _, r := range in {
		ch <- r
	}
	close(ch)
	w.Consume(ch)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	h, err := ReadHeader(path)
	if err != nil {
		t.Fatal(err)
	}
	if h.Version != formatVer {
		t.Fatalf("version = %d, want %d", h.Version, formatVer)
	}
	if h.Total != uint64(len(in)) {
		t.Fatalf("total = %d, want %d", h.Total, len(in))
	}
	wantCounts := map[model.StatusCode]uint64{
		model.CodeHasPTR:       3,
		model.CodeNoErrorEmpty: 1,
		model.CodeNXDomain:     1,
		model.CodeTimeout:      1,
		model.CodeServFail:     1,
		model.CodeRefused:      1,
		model.CodeNetError:     1,
	}
	for c, want := range wantCounts {
		if got := h.Count(c); got != want {
			t.Errorf("count[%s] = %d, want %d", model.StatusString(c), got, want)
		}
	}
	if h.Shards != 5 || h.ShardID != 2 {
		t.Fatalf("shard metadata lost: shards=%d id=%d", h.Shards, h.ShardID)
	}

	// Read every record back.
	gotStatus := map[uint32]model.StatusCode{}
	gotPTR := map[uint32][]string{}
	fc := map[uint32]bool{}
	fcChecked := map[uint32]bool{}
	total := 0
	if err := Scan(path, func(r Rec) error {
		total++
		gotStatus[r.IP] = r.Status
		if len(r.PTR) > 0 {
			gotPTR[r.IP] = r.PTR
		}
		fc[r.IP] = r.FCMatch
		fcChecked[r.IP] = r.FCChecked
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if total != len(in) {
		t.Fatalf("scanned %d records, want %d (all statuses must be stored)", total, len(in))
	}
	if gotStatus[16909061] != model.CodeNXDomain {
		t.Errorf("nxdomain status lost for 1.2.3.5")
	}
	if gotStatus[151587081] != model.CodeServFail {
		t.Errorf("servfail status lost for 9.9.9.9")
	}

	wantPTR := map[uint32][]string{
		2380689952: {"ec2-141-230-114-32.ap-southeast-1.compute.amazonaws.com"},
		1054001134: {"62-210-199-238.rev.poneytelecom.eu"},
		134744072:  {"dns.google"},
	}
	for ip, names := range wantPTR {
		g := gotPTR[ip]
		sort.Strings(g)
		sort.Strings(names)
		if !reflect.DeepEqual(g, names) {
			t.Errorf("ip %d: got %v want %v", ip, g, names)
		}
	}
	if !fcChecked[2380689952] || !fc[2380689952] {
		t.Errorf("fcrdns match flag lost for EC2 record")
	}
	if !fcChecked[1054001134] || fc[1054001134] {
		t.Errorf("fcrdns no-match flag lost for poneytelecom record")
	}
	if fcChecked[134744072] {
		t.Errorf("dns.google should have fcrdns unchecked")
	}
}

// TestResumeAppend verifies that resuming an existing file appends records and
// keeps cumulative header counts.
func TestResumeAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "resume.rdnsz")

	write := func(w *Writer, recs []model.Record) {
		ch := make(chan model.Record, len(recs))
		for _, r := range recs {
			ch <- r
		}
		close(ch)
		w.Consume(ch)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}

	w1, err := NewWriter(path, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	write(w1, []model.Record{
		{IP: "1.0.0.1", IPInt: 16777217, Status: model.StatusHasPTR, PTR: []string{"a.example.com"}},
		{IP: "1.0.0.2", IPInt: 16777218, Status: model.StatusNXDomain},
	})

	w2, err := NewWriterResume(path, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	write(w2, []model.Record{
		{IP: "1.0.0.3", IPInt: 16777219, Status: model.StatusTimeout},
	})

	h, err := ReadHeader(path)
	if err != nil {
		t.Fatal(err)
	}
	if h.Total != 3 {
		t.Fatalf("cumulative total = %d, want 3", h.Total)
	}
	if h.Count(model.CodeHasPTR) != 1 || h.Count(model.CodeNXDomain) != 1 || h.Count(model.CodeTimeout) != 1 {
		t.Fatalf("cumulative counts wrong: %+v", h.Counts)
	}

	// Wrong shard must be refused.
	if _, err := NewWriterResume(path, 3, 2); err == nil {
		t.Errorf("expected shard-mismatch error on resume")
	}

	n := 0
	Scan(path, func(Rec) error { n++; return nil })
	if n != 3 {
		t.Errorf("scanned %d, want 3", n)
	}
}
