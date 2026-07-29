package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"rdns-crawler/internal/model"
)

// buildFixture writes a small .rdnsz file with a deterministic mix of statuses:
// IPs 1.0.0.0+i, cycling has_ptr, timeout, nxdomain, servfail. Returns the
// path and the expected filtered IPs for mask {has_ptr, timeout} in file order.
func buildFixture(t *testing.T, n int) (string, []string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "old.rdnsz")
	w, err := NewWriter(path, 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	statuses := []string{model.StatusHasPTR, model.StatusTimeout, model.StatusNXDomain, model.StatusServFail}
	var want []string
	ch := make(chan model.Record, n)
	base := uint32(1 << 24) // 1.0.0.0
	for i := 0; i < n; i++ {
		ip := base + uint32(i)
		st := statuses[i%len(statuses)]
		rec := model.Record{
			IP:     fmt.Sprintf("1.0.%d.%d", (i>>8)&0xff, i&0xff),
			IPInt:  ip,
			Status: st,
		}
		if st == model.StatusHasPTR {
			rec.PTR = []string{fmt.Sprintf("host-%d.example.com", i)}
		}
		if st == model.StatusHasPTR || st == model.StatusTimeout {
			want = append(want, rec.IP)
		}
		ch <- rec
	}
	close(ch)
	w.Consume(ch)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path, want
}

func collect(t *testing.T, ips <-chan string, cursor <-chan uint64, errch <-chan error) ([]string, uint64) {
	t.Helper()
	var got []string
	var last uint64
	done := make(chan struct{})
	go func() {
		for c := range cursor {
			last = c
		}
		close(done)
	}()
	for ip := range ips {
		got = append(got, ip)
	}
	<-done
	if err := <-errch; err != nil {
		t.Fatalf("stream error: %v", err)
	}
	return got, last
}

func TestCountTargets(t *testing.T) {
	path, want := buildFixture(t, 1000)
	mask, err := model.ParseStatuses("has_ptr,timeout")
	if err != nil {
		t.Fatal(err)
	}
	n, err := CountTargets(path, mask)
	if err != nil {
		t.Fatal(err)
	}
	if n != uint64(len(want)) {
		t.Fatalf("CountTargets = %d, want %d", n, len(want))
	}
	// Single-status masks.
	onlyPTR, _ := model.ParseStatuses("has_ptr")
	n, _ = CountTargets(path, onlyPTR)
	if n != 250 {
		t.Fatalf("has_ptr count = %d, want 250", n)
	}
}

func TestStreamTargetsAll(t *testing.T) {
	path, want := buildFixture(t, 1000)
	mask, _ := model.ParseStatuses("has_ptr,timeout")
	ips, cur, errch := StreamTargets(context.Background(), path, mask, 0, 0, 64)
	got, last := collect(t, ips, cur, errch)
	if len(got) != len(want) {
		t.Fatalf("streamed %d targets, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("target[%d] = %s, want %s (order must be deterministic)", i, got[i], want[i])
		}
	}
	if last != uint64(len(want)) {
		t.Fatalf("final cursor = %d, want %d", last, len(want))
	}
}

func TestStreamTargetsSkipResume(t *testing.T) {
	path, want := buildFixture(t, 1000)
	mask, _ := model.ParseStatuses("has_ptr,timeout")

	// First run: limit 120 targets, as if interrupted.
	ips, cur, errch := StreamTargets(context.Background(), path, mask, 0, 120, 64)
	got1, cur1 := collect(t, ips, cur, errch)
	if len(got1) != 120 {
		t.Fatalf("run1 emitted %d, want 120", len(got1))
	}
	if cur1 != 120 {
		t.Fatalf("run1 final cursor = %d, want 120", cur1)
	}

	// Resume from the cursor: must produce exactly the remainder, in order.
	ips, cur, errch = StreamTargets(context.Background(), path, mask, cur1, 0, 64)
	got2, cur2 := collect(t, ips, cur, errch)
	if len(got1)+len(got2) != len(want) {
		t.Fatalf("run1+run2 = %d targets, want %d", len(got1)+len(got2), len(want))
	}
	all := append(append([]string{}, got1...), got2...)
	for i := range want {
		if all[i] != want[i] {
			t.Fatalf("resumed stream diverged at %d: %s != %s", i, all[i], want[i])
		}
	}
	if cur2 != uint64(len(want)) {
		t.Fatalf("run2 final cursor = %d, want %d", cur2, len(want))
	}
}

func TestStreamTargetsCancel(t *testing.T) {
	path, _ := buildFixture(t, 1000)
	mask, _ := model.ParseStatuses("has_ptr,timeout")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ips, cur, errch := StreamTargets(ctx, path, mask, 0, 0, 64)
	// Read a few, then cancel mid-stream.
	var got []string
	for ip := range ips {
		got = append(got, ip)
		if len(got) == 50 {
			cancel()
		}
	}
	var last uint64
	for c := range cur {
		last = c
	}
	if err := <-errch; err != nil {
		t.Fatalf("cancel must not surface as error, got %v", err)
	}
	// The final cursor must never overshoot what was actually delivered
	// (a hole on resume); it may undershoot (dups are fine).
	if last > uint64(len(got)) {
		t.Fatalf("cursor %d > delivered %d — resume would skip undelivered targets", last, len(got))
	}
}
