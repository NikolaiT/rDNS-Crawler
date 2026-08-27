package merge

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
	for _, r := range recs {
		w.Add(r)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func rec(ip uint32, status string, ptr []string, fc *bool) model.Record {
	return model.Record{IPInt: ip, Status: status, PTR: ptr, FCrDNS: fc}
}

// readAll decodes a merged shard back into ip → record for assertions.
func readAll(t *testing.T, path string) map[uint32]store.Rec {
	t.Helper()
	out := map[uint32]store.Rec{}
	err := store.Scan(path, func(r store.Rec) error {
		if _, dup := out[r.IP]; dup {
			t.Errorf("duplicate ip %d in merged output", r.IP)
		}
		out[r.IP] = r
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestMergeRun(t *testing.T) {
	tr, fa := true, false
	oldDir := filepath.Join(t.TempDir(), "old")
	newDir := filepath.Join(t.TempDir(), "new")
	outDir := filepath.Join(t.TempDir(), "merged")
	os.MkdirAll(oldDir, 0o755)
	os.MkdirAll(newDir, 0o755)

	base := uint32(1 << 24) // 1.0.0.0
	writeShard(t, oldDir, "shard-0.rdnsz", []model.Record{
		rec(base+1, model.StatusHasPTR, []string{"a.example.com"}, &tr),  // unchanged
		rec(base+2, model.StatusHasPTR, []string{"b1.example.com"}, nil), // changed
		rec(base+3, model.StatusHasPTR, []string{"c.example.com"}, nil),  // removed (nxdomain)
		rec(base+4, model.StatusHasPTR, []string{"d.example.com"}, &tr),  // grace: new pass timeout
		rec(base+5, model.StatusHasPTR, []string{"e.example.com"}, nil),  // missing in new pass
		rec(base+6, model.StatusTimeout, nil, nil),                       // recovered → has_ptr
		rec(base+7, model.StatusTimeout, nil, nil),                       // → nxdomain
		rec(base+8, model.StatusTimeout, nil, nil),                       // → servfail (transient, new wins)
		rec(base+9, model.StatusNXDomain, nil, nil),                      // outside target set
		rec(base+9, model.StatusNXDomain, nil, nil),                      // old duplicate
		rec(base+10, model.StatusServFail, nil, nil),                     // → has_ptr
	})
	writeShard(t, newDir, "shard-0.rdnsz", []model.Record{
		rec(base+1, model.StatusHasPTR, []string{"a.example.com"}, &tr),
		rec(base+1, model.StatusHasPTR, []string{"other.example.com"}, nil), // new duplicate (dropped)
		rec(base+2, model.StatusHasPTR, []string{"b2.example.com"}, &fa),
		rec(base+3, model.StatusNXDomain, nil, nil),
		rec(base+4, model.StatusTimeout, nil, nil),
		rec(base+6, model.StatusHasPTR, []string{"recovered.example.net"}, &tr),
		rec(base+7, model.StatusNXDomain, nil, nil),
		rec(base+8, model.StatusServFail, nil, nil),
		rec(base+10, model.StatusHasPTR, []string{"fixed.example.de"}, nil),
		rec(base+99, model.StatusHasPTR, []string{"stray.example.com"}, nil), // unexpected
	})

	mask, _ := model.ParseStatuses("has_ptr,timeout,servfail")
	res, err := Run(Options{OldPath: oldDir, NewPath: newDir, OutDir: outDir, Mask: mask}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	got := readAll(t, filepath.Join(outDir, "shard-0.rdnsz"))
	if len(got) != 10 {
		t.Fatalf("merged records = %d, want 10 (stray + dups excluded)", len(got))
	}

	check := func(ip uint32, status model.StatusCode, ptr string) {
		t.Helper()
		r, ok := got[ip]
		if !ok {
			t.Fatalf("ip %d missing from merged output", ip)
		}
		if r.Status != status {
			t.Errorf("ip %d status = %s, want %s", ip, model.StatusString(r.Status), model.StatusString(status))
		}
		if ptr != "" && (len(r.PTR) != 1 || r.PTR[0] != ptr) {
			t.Errorf("ip %d ptr = %v, want [%s]", ip, r.PTR, ptr)
		}
	}
	check(base+1, model.CodeHasPTR, "a.example.com")          // unchanged, first new obs wins
	check(base+2, model.CodeHasPTR, "b2.example.com")         // updated name
	check(base+3, model.CodeNXDomain, "")                     // authoritative removal
	check(base+4, model.CodeHasPTR, "d.example.com")          // grace kept
	check(base+5, model.CodeHasPTR, "e.example.com")          // missing → baseline kept
	check(base+6, model.CodeHasPTR, "recovered.example.net")  // timeout recovery
	check(base+7, model.CodeNXDomain, "")                     // timeout → clean negative
	check(base+8, model.CodeServFail, "")                     // transient → newest transient
	check(base+9, model.CodeNXDomain, "")                     // untouched (outside target set)
	check(base+10, model.CodeHasPTR, "fixed.example.de")      // servfail recovery

	// Grace keeps the baseline's fcrdns bits.
	if r := got[base+4]; !r.FCChecked || !r.FCMatch {
		t.Errorf("grace-kept record lost fcrdns bits: %+v", r)
	}

	if res.Gained != 2 {
		t.Errorf("gained = %d, want 2", res.Gained)
	}
	if res.PTRChanged != 1 || res.PTRUnchanged != 1 {
		t.Errorf("changed/unchanged = %d/%d, want 1/1", res.PTRChanged, res.PTRUnchanged)
	}
	if res.RemovedAuth != 1 {
		t.Errorf("removedAuth = %d, want 1", res.RemovedAuth)
	}
	if res.GraceKept != 1 {
		t.Errorf("graceKept = %d, want 1", res.GraceKept)
	}
	if res.MissingKept != 1 {
		t.Errorf("missingKept = %d, want 1", res.MissingKept)
	}
	if res.OldDups != 1 || res.NewDups != 1 || res.UnexpectedNew != 1 {
		t.Errorf("bookkeeping old/new/unexpected = %d/%d/%d, want 1/1/1", res.OldDups, res.NewDups, res.UnexpectedNew)
	}
	if res.MergedTotal != 10 {
		t.Errorf("mergedTotal = %d, want 10", res.MergedTotal)
	}
	if res.MergedCounts[model.StatusHasPTR] != 6 {
		t.Errorf("merged has_ptr = %d, want 6", res.MergedCounts[model.StatusHasPTR])
	}

	// The merged output header must carry the same identity and be usable as
	// the next baseline.
	h, err := store.ReadHeader(filepath.Join(outDir, "shard-0.rdnsz"))
	if err != nil {
		t.Fatal(err)
	}
	if h.Shards != 1 || h.ShardID != 0 {
		t.Errorf("merged header identity = %d/%d, want 0/1", h.ShardID, h.Shards)
	}
	if h.Total != 10 || h.Count(model.CodeHasPTR) != 6 {
		t.Errorf("merged header stats total=%d has_ptr=%d, want 10/6", h.Total, h.Count(model.CodeHasPTR))
	}

	res.RenderText(io.Discard)
}

func TestMergeRefusesMissingNewShard(t *testing.T) {
	oldDir := filepath.Join(t.TempDir(), "old")
	newDir := filepath.Join(t.TempDir(), "new")
	os.MkdirAll(oldDir, 0o755)
	os.MkdirAll(newDir, 0o755)

	// Two old shards, only one new shard.
	writeTwo := func(dir string, both bool) {
		w, _ := store.NewWriter(filepath.Join(dir, "shard-0.rdnsz"), 2, 0)
		w.Add(rec(2, model.StatusTimeout, nil, nil))
		w.Close()
		if both {
			w, _ = store.NewWriter(filepath.Join(dir, "shard-1.rdnsz"), 2, 1)
			w.Add(rec(3, model.StatusTimeout, nil, nil))
			w.Close()
		}
	}
	writeTwo(oldDir, true)
	writeTwo(newDir, false)

	mask, _ := model.ParseStatuses("has_ptr,timeout")
	_, err := Run(Options{OldPath: oldDir, NewPath: newDir, OutDir: t.TempDir(), Mask: mask}, io.Discard)
	if err == nil {
		t.Fatal("expected error for missing new shard without --allow-partial")
	}

	// With AllowPartial the baseline is copied through.
	outDir := t.TempDir()
	res, err := Run(Options{OldPath: oldDir, NewPath: newDir, OutDir: outDir, Mask: mask, AllowPartial: true}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if res.MergedTotal != 2 {
		t.Errorf("mergedTotal = %d, want 2", res.MergedTotal)
	}
	if _, err := os.Stat(filepath.Join(outDir, "shard-1.rdnsz")); err != nil {
		t.Errorf("copied-through shard missing: %v", err)
	}
}

func TestMergeShardIdentityMismatch(t *testing.T) {
	oldDir := t.TempDir()
	newDir := t.TempDir()
	writeShard(t, oldDir, "shard-0.rdnsz", []model.Record{rec(1, model.StatusTimeout, nil, nil)})
	w, _ := store.NewWriter(filepath.Join(newDir, "shard-0.rdnsz"), 2, 0) // claims shards=2
	w.Add(rec(1, model.StatusHasPTR, []string{"a.example.com"}, nil))
	w.Close()

	mask, _ := model.ParseStatuses("has_ptr,timeout")
	if _, err := Run(Options{OldPath: oldDir, NewPath: newDir, OutDir: t.TempDir(), Mask: mask}, io.Discard); err == nil {
		t.Fatal("expected shard identity mismatch error")
	}
}
