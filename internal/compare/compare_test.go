package compare

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"rdns-crawler/internal/model"
	"rdns-crawler/internal/store"
)

func writeShard(t *testing.T, dir, name string, recs []model.Record) string {
	t.Helper()
	path := filepath.Join(dir, name)
	w, err := store.NewWriter(path, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan model.Record, len(recs))
	for _, r := range recs {
		ch <- r
	}
	close(ch)
	w.Consume(ch)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func rec(ip uint32, status string, ptr []string, fc *bool) model.Record {
	return model.Record{IPInt: ip, Status: status, PTR: ptr, FCrDNS: fc}
}

func TestCompareRun(t *testing.T) {
	tr := true
	oldDir := filepath.Join(t.TempDir(), "old")
	newDir := filepath.Join(t.TempDir(), "new")
	os.MkdirAll(oldDir, 0o755)
	os.MkdirAll(newDir, 0o755)

	base := uint32(1 << 24) // 1.0.0.0
	writeShard(t, oldDir, "shard-0.rdnsz", []model.Record{
		rec(base+1, model.StatusHasPTR, []string{"a.example.com"}, &tr), // unchanged + FC both
		rec(base+2, model.StatusHasPTR, []string{"b1.example.com"}, nil),  // changed
		rec(base+3, model.StatusHasPTR, []string{"c.example.com"}, nil),   // removed (nxdomain)
		rec(base+4, model.StatusHasPTR, []string{"d.example.com"}, nil),   // transient (timeout)
		rec(base+5, model.StatusHasPTR, []string{"e.example.com"}, nil),   // missing in new pass
		rec(base+11, model.StatusHasPTR, []string{"X.Example.COM."}, nil), // case-only diff → unchanged
		rec(base+12, model.StatusHasPTR, []string{"p1.example.org", "p2.example.org"}, nil), // order-only diff
		rec(base+6, model.StatusTimeout, nil, nil), // recovered → has_ptr
		rec(base+7, model.StatusTimeout, nil, nil), // → nxdomain
		rec(base+8, model.StatusTimeout, nil, nil), // still timeout
		rec(base+9, model.StatusNXDomain, nil, nil), // outside target set
		rec(base+10, model.StatusServFail, nil, nil),
	})
	writeShard(t, newDir, "shard-0.rdnsz", []model.Record{
		rec(base+1, model.StatusHasPTR, []string{"a.example.com"}, &tr),
		rec(base+1, model.StatusHasPTR, []string{"a.example.com"}, &tr), // duplicate (restart overlap)
		rec(base+2, model.StatusHasPTR, []string{"b2.example.com"}, nil),
		rec(base+3, model.StatusNXDomain, nil, nil),
		rec(base+4, model.StatusTimeout, nil, nil),
		rec(base+11, model.StatusHasPTR, []string{"x.example.com"}, nil),
		rec(base+12, model.StatusHasPTR, []string{"p2.example.org", "p1.example.org"}, nil),
		rec(base+6, model.StatusHasPTR, []string{"recovered.example.net"}, &tr),
		rec(base+7, model.StatusNXDomain, nil, nil),
		rec(base+8, model.StatusTimeout, nil, nil),
		rec(base+99, model.StatusHasPTR, []string{"stray.example.com"}, nil), // not a target
	})

	mask, _ := model.ParseStatuses("has_ptr,timeout")
	res, err := Run(Options{OldPath: oldDir, NewPath: newDir, Mask: mask}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	if got := res.Targets[model.StatusHasPTR]; got != 7 {
		t.Errorf("has_ptr targets = %d, want 7", got)
	}
	if got := res.Targets[model.StatusTimeout]; got != 3 {
		t.Errorf("timeout targets = %d, want 3", got)
	}
	if res.PTRUnchanged != 3 {
		t.Errorf("unchanged = %d, want 3 (exact, case-only, order-only)", res.PTRUnchanged)
	}
	if res.PTRChanged != 1 {
		t.Errorf("changed = %d, want 1", res.PTRChanged)
	}

	hp := res.Transitions[model.StatusHasPTR]
	if hp[model.StatusHasPTR] != 4 || hp[model.StatusNXDomain] != 1 || hp[model.StatusTimeout] != 1 || hp["missing"] != 1 {
		t.Errorf("has_ptr transitions wrong: %+v", hp)
	}
	to := res.Transitions[model.StatusTimeout]
	if to[model.StatusHasPTR] != 1 || to[model.StatusNXDomain] != 1 || to[model.StatusTimeout] != 1 {
		t.Errorf("timeout transitions wrong: %+v", to)
	}

	if res.Gained.Total != 1 || res.Gained.FCMatch != 1 {
		t.Errorf("gained = %+v, want total=1 fcmatch=1", res.Gained)
	}
	if len(res.Gained.TopTLDs) != 1 || res.Gained.TopTLDs[0].TLD != "net" {
		t.Errorf("gained TLDs = %+v, want [.net]", res.Gained.TopTLDs)
	}

	if res.NewDupRecords != 1 {
		t.Errorf("new dups = %d, want 1", res.NewDupRecords)
	}
	if res.UnexpectedNew != 1 {
		t.Errorf("unexpected new = %d, want 1", res.UnexpectedNew)
	}
	if res.FC.BothChecked != 1 || res.FC.MatchBoth != 1 {
		t.Errorf("fc stats = %+v, want bothChecked=1 matchBoth=1", res.FC)
	}

	h := res.Headline
	if h.TimeoutCrawled != 3 || h.HasPTRCrawled != 6 {
		t.Errorf("crawled counts: timeout=%d has_ptr=%d, want 3 and 6", h.TimeoutCrawled, h.HasPTRCrawled)
	}
	wantPct := 100.0 / 3.0
	if h.TimeoutNowPTRPct < wantPct-0.01 || h.TimeoutNowPTRPct > wantPct+0.01 {
		t.Errorf("timeout→ptr pct = %.4f, want %.4f", h.TimeoutNowPTRPct, wantPct)
	}

	// Render must not panic and should mention the headline number.
	res.RenderText(io.Discard)
}

// TestCompareServfailMask verifies the three-status target set used by the
// pass-2 fleet config (has_ptr,timeout,servfail): servfail targets show up in
// the transitions, and a servfail→has_ptr recovery counts as a gained PTR.
func TestCompareServfailMask(t *testing.T) {
	oldDir := filepath.Join(t.TempDir(), "old")
	newDir := filepath.Join(t.TempDir(), "new")
	os.MkdirAll(oldDir, 0o755)
	os.MkdirAll(newDir, 0o755)

	tr := true
	base := uint32(1 << 24)
	writeShard(t, oldDir, "shard-0.rdnsz", []model.Record{
		rec(base+1, model.StatusHasPTR, []string{"keep.example.com"}, nil),
		rec(base+2, model.StatusTimeout, nil, nil),
		rec(base+3, model.StatusServFail, nil, nil), // recovers → has_ptr
		rec(base+4, model.StatusServFail, nil, nil), // still servfail
	})
	writeShard(t, newDir, "shard-0.rdnsz", []model.Record{
		rec(base+1, model.StatusHasPTR, []string{"keep.example.com"}, nil),
		rec(base+2, model.StatusHasPTR, []string{"woke-up.example.org"}, nil),
		rec(base+3, model.StatusHasPTR, []string{"fixed-dnssec.example.de"}, &tr),
		rec(base+4, model.StatusServFail, nil, nil),
	})

	mask, err := model.ParseStatuses("has_ptr,timeout,servfail")
	if err != nil {
		t.Fatal(err)
	}
	res, err := Run(Options{OldPath: oldDir, NewPath: newDir, Mask: mask}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	if got := res.Targets[model.StatusServFail]; got != 2 {
		t.Errorf("servfail targets = %d, want 2", got)
	}
	sf := res.Transitions[model.StatusServFail]
	if sf[model.StatusHasPTR] != 1 || sf[model.StatusServFail] != 1 {
		t.Errorf("servfail transitions wrong: %+v", sf)
	}
	// Gains come from BOTH timeout and servfail recoveries.
	if res.Gained.Total != 2 {
		t.Errorf("gained = %d, want 2 (timeout→ptr + servfail→ptr)", res.Gained.Total)
	}
	if res.Gained.FCMatch != 1 {
		t.Errorf("gained fcrdns match = %d, want 1", res.Gained.FCMatch)
	}
	res.RenderText(io.Discard)
}

func TestCompareShardMismatch(t *testing.T) {
	oldDir := t.TempDir()
	newDir := t.TempDir()
	// old claims shards=1, new claims shards=2 → must refuse.
	writeShard(t, oldDir, "shard-0.rdnsz", []model.Record{rec(1<<24+1, model.StatusTimeout, nil, nil)})
	path := filepath.Join(newDir, "shard-0.rdnsz")
	w, err := store.NewWriter(path, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan model.Record, 1)
	ch <- rec(1<<24+1, model.StatusHasPTR, []string{"a.example.com"}, nil)
	close(ch)
	w.Consume(ch)
	w.Close()

	mask, _ := model.ParseStatuses("has_ptr,timeout")
	if _, err := Run(Options{OldPath: oldDir, NewPath: newDir, Mask: mask}, io.Discard); err == nil {
		t.Fatal("expected shard-layout mismatch error")
	}
}
