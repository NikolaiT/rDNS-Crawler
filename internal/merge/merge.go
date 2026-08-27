// Package merge folds a re-crawl (update) pass into its baseline pass and
// writes the merged full-space dataset as fresh .rdnsz shards — the updater
// step of DESIGN.md §3, reduced to the fields the observation format carries.
// The merged directory is what the commercial export ships and what the next
// re-crawl uses as its baseline.
//
// For every IP of the baseline the merged output contains exactly one record:
//
//   - baseline status outside the re-crawl target set → baseline record
//     (the IP was not re-crawled; nothing newer exists)
//   - re-crawled and the new pass answered has_ptr or an authoritative
//     negative (nxdomain / noerror_empty) → the new record
//   - re-crawled but the new pass only failed transiently (timeout / servfail /
//     refused / net_error / lame_delegation):
//   - baseline had a PTR → keep the baseline record (grace policy: a single
//     transient failure must not erase a known-good PTR)
//   - otherwise → the new record (still failing; newest observation wins)
//   - re-crawled but the new pass never observed the IP (crash hole) →
//     baseline record
//
// Pairing, dedup and memory layout follow internal/compare: shards pair by
// header shard-id, the new pass is loaded into a sorted in-memory table (PTR
// names in a byte arena), the old pass streams against it, and duplicate IPs
// keep the first occurrence.
package merge

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"rdns-crawler/internal/model"
	"rdns-crawler/internal/store"
)

// Options selects the two passes, the target-set definition and the output.
type Options struct {
	OldPath string // baseline pass: .rdnsz file or directory of shards
	NewPath string // re-crawl pass: .rdnsz file or directory of shards
	OutDir  string // output directory for the merged shard-<i>.rdnsz files
	// Mask defines which old statuses formed the re-crawl target set. Records
	// outside it are copied through unchanged.
	Mask model.StatusMask
	// AllowPartial tolerates missing or truncated new shards (their targets
	// keep the baseline record). Off by default: a partially collected
	// re-crawl must not silently masquerade as a full update.
	AllowPartial bool
}

// ShardResult is the per-shard bookkeeping of one merged pair.
type ShardResult struct {
	Shard     int    `json:"shard"`
	OutFile   string `json:"out_file"`
	OldTotal  uint64 `json:"old_total"`
	NewTotal  uint64 `json:"new_total"`
	Merged    uint64 `json:"merged"`
	Truncated bool   `json:"truncated_new,omitempty"`
}

// Result aggregates the merge across all shard pairs.
type Result struct {
	Old     store.Header `json:"-"`
	OutDir  string       `json:"out_dir"`
	Created time.Time    `json:"created"` // newest new-pass shard timestamp

	TargetStatuses string        `json:"target_statuses"`
	Shards         []ShardResult `json:"shards"`

	// MergedCounts is the status histogram of the merged dataset (sums the
	// output shard headers, so it is exactly what export will report).
	MergedTotal  uint64            `json:"merged_total"`
	MergedCounts map[string]uint64 `json:"merged_counts"`

	// How the target set moved.
	Gained       uint64 `json:"ptr_gained"`        // not has_ptr → has_ptr
	RemovedAuth  uint64 `json:"ptr_removed_auth"`  // has_ptr → nxdomain/noerror_empty
	PTRChanged   uint64 `json:"ptr_changed"`       // has_ptr → has_ptr, name set differs
	PTRUnchanged uint64 `json:"ptr_unchanged"`     // has_ptr → has_ptr, same names
	GraceKept    uint64 `json:"ptr_grace_kept"`    // has_ptr kept through transient failure
	MissingKept  uint64 `json:"targets_not_crawled"` // targets with no new observation

	// Bookkeeping oddities (all expected to be ~0).
	OldDups       uint64 `json:"old_dup_records"`
	NewDups       uint64 `json:"new_dup_records"`
	UnexpectedNew uint64 `json:"unexpected_new"`
}

const (
	entStatusMask = 0x0f
	entFCChecked  = 1 << 4
	entFCMatch    = 1 << 5
	entUsed       = 1 << 7
)

// newEnt is one record of the re-crawl pass: 16 bytes plus its share of the
// name arena. Names of a record are stored NUL-joined in the arena.
type newEnt struct {
	ip      uint32
	sf      uint8
	nameOff uint32
	nameLen uint32
}

// nameSep joins the PTR names of one record inside the arena. DNS presentation
// format never contains a raw NUL.
const nameSep = "\x00"

// Run executes the merge. Progress lines are written to progress.
func Run(opts Options, progress io.Writer) (*Result, error) {
	oldShards, err := discover(opts.OldPath)
	if err != nil {
		return nil, fmt.Errorf("old pass: %w", err)
	}
	newShards, err := discover(opts.NewPath)
	if err != nil {
		return nil, fmt.Errorf("new pass: %w", err)
	}
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return nil, err
	}

	res := &Result{
		OutDir:         opts.OutDir,
		TargetStatuses: opts.Mask.String(),
		MergedCounts:   map[string]uint64{},
	}

	// 512 MiB bitmap over the full IPv4 space, reused across shards, for
	// old-side dedup (pass-1 restart overlaps).
	seen := make([]uint64, 1<<26)

	var mergedCounts [model.NumStatusCodes]uint64
	for _, sid := range sortedKeys(oldShards) {
		newPath, ok := newShards[sid]
		if !ok {
			if !opts.AllowPartial {
				return nil, fmt.Errorf("no new shard for old shard %d — collect the full re-crawl or pass --allow-partial", sid)
			}
			fmt.Fprintf(progress, "[merge] shard %-2d: no new file — copying baseline through\n", sid)
			newPath = ""
		}
		sr, err := mergeShard(oldShards[sid], newPath, opts, seen, res, &mergedCounts, progress)
		if err != nil {
			return nil, fmt.Errorf("shard %d: %w", sid, err)
		}
		res.Shards = append(res.Shards, sr)
	}

	for c := model.StatusCode(0); c < model.NumStatusCodes; c++ {
		if mergedCounts[c] > 0 {
			res.MergedCounts[model.StatusString(c)] = mergedCounts[c]
		}
		res.MergedTotal += mergedCounts[c]
	}
	return res, nil
}

// discover expands a file-or-directory path into shardID → file.
func discover(path string) (map[int]string, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	var files []string
	if fi.IsDir() {
		files, err = filepath.Glob(filepath.Join(path, "*.rdnsz"))
		if err != nil {
			return nil, err
		}
	} else {
		files = []string{path}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no .rdnsz files in %s", path)
	}
	shards := map[int]string{}
	for _, f := range files {
		h, err := store.ReadHeader(f)
		if err != nil {
			return nil, err
		}
		if prev, dup := shards[int(h.ShardID)]; dup {
			return nil, fmt.Errorf("both %s and %s claim shard %d", prev, f, h.ShardID)
		}
		shards[int(h.ShardID)] = f
	}
	return shards, nil
}

// loadNewShard reads one re-crawl shard into a sorted, deduplicated table.
func loadNewShard(path string, res *Result) (ents []newEnt, arena []byte, hdr store.Header, truncated bool, err error) {
	hdr, err = store.ReadHeader(path)
	if err != nil {
		return nil, nil, hdr, false, err
	}
	ents = make([]newEnt, 0, hdr.Total)
	scanErr := store.Scan(path, func(r store.Rec) error {
		e := newEnt{ip: r.IP, sf: uint8(r.Status) & entStatusMask}
		if r.FCChecked {
			e.sf |= entFCChecked
			if r.FCMatch {
				e.sf |= entFCMatch
			}
		}
		if r.Status == model.CodeHasPTR {
			joined := strings.Join(r.PTR, nameSep)
			if len(arena)+len(joined) > math.MaxUint32 {
				return errors.New("PTR name arena exceeds 4 GiB — shard too large for uint32 offsets")
			}
			e.nameOff = uint32(len(arena))
			e.nameLen = uint32(len(joined))
			arena = append(arena, joined...)
		}
		ents = append(ents, e)
		return nil
	})
	if scanErr != nil {
		if !errors.Is(scanErr, store.ErrTruncatedTail) {
			return nil, nil, hdr, false, scanErr
		}
		truncated = true
	}

	// Sort by IP; stable so the FIRST observation in file order wins on dedup
	// (same rule as compare).
	slices.SortStableFunc(ents, func(a, b newEnt) int {
		if a.ip < b.ip {
			return -1
		} else if a.ip > b.ip {
			return 1
		}
		return 0
	})
	out := 0
	for i := range ents {
		if out > 0 && ents[out-1].ip == ents[i].ip {
			res.NewDups++
			continue
		}
		ents[out] = ents[i]
		out++
	}
	return ents[:out], arena, hdr, truncated, nil
}

// mergeShard merges one old/new shard pair into OutDir/shard-<id>.rdnsz.
// An empty newPath copies the baseline through (allow-partial case).
func mergeShard(oldPath, newPath string, opts Options, seen []uint64, res *Result, mergedCounts *[model.NumStatusCodes]uint64, progress io.Writer) (ShardResult, error) {
	start := time.Now()
	oldHdr, err := store.ReadHeader(oldPath)
	if err != nil {
		return ShardResult{}, err
	}

	var ents []newEnt
	var arena []byte
	var newHdr store.Header
	var truncated bool
	if newPath != "" {
		ents, arena, newHdr, truncated, err = loadNewShard(newPath, res)
		if err != nil {
			return ShardResult{}, err
		}
		if truncated && !opts.AllowPartial {
			return ShardResult{}, fmt.Errorf("%s is truncated (collected mid-crawl) — re-collect or pass --allow-partial", newPath)
		}
		if newHdr.Shards != oldHdr.Shards || newHdr.ShardID != oldHdr.ShardID {
			return ShardResult{}, fmt.Errorf("shard identity mismatch: old %d/%d vs new %d/%d",
				oldHdr.ShardID, oldHdr.Shards, newHdr.ShardID, newHdr.Shards)
		}
	}
	loadDur := time.Since(start).Round(time.Second)

	outPath := filepath.Join(opts.OutDir, fmt.Sprintf("shard-%d.rdnsz", oldHdr.ShardID))
	w, err := store.NewWriter(outPath, oldHdr.Shards, oldHdr.ShardID)
	if err != nil {
		return ShardResult{}, err
	}
	// The merged shard's header timestamp is the re-crawl's observation time
	// (falls back to the baseline's when the shard was copied through).
	w.Start = time.Unix(oldHdr.Created, 0)
	if newPath != "" {
		w.Start = time.Unix(newHdr.Created, 0)
	}
	if t := time.Unix(newHdr.Created, 0); newPath != "" && t.After(res.Created) {
		res.Created = t
	}

	clear(seen)
	sr := ShardResult{Shard: int(oldHdr.ShardID), OutFile: outPath, NewTotal: uint64(len(ents)), Truncated: truncated}

	writeOld := func(r store.Rec) {
		w.Add(toRecord(r))
		mergedCounts[r.Status]++
		sr.Merged++
	}

	scanErr := store.Scan(oldPath, func(r store.Rec) error {
		if seen[r.IP>>6]&(1<<(r.IP&63)) != 0 {
			res.OldDups++
			return nil
		}
		seen[r.IP>>6] |= 1 << (r.IP & 63)
		sr.OldTotal++

		if !opts.Mask.Has(r.Status) {
			writeOld(r)
			return nil
		}
		i := sort.Search(len(ents), func(k int) bool { return ents[k].ip >= r.IP })
		if i >= len(ents) || ents[i].ip != r.IP {
			res.MissingKept++
			writeOld(r)
			return nil
		}
		e := &ents[i]
		e.sf |= entUsed
		newStatus := model.StatusCode(e.sf & entStatusMask)

		switch {
		case newStatus == model.CodeHasPTR:
			names := strings.Split(string(arena[e.nameOff:e.nameOff+e.nameLen]), nameSep)
			if r.Status == model.CodeHasPTR {
				if hashNames(r.PTR) == hashNames(names) {
					res.PTRUnchanged++
				} else {
					res.PTRChanged++
				}
			} else {
				res.Gained++
			}
			w.Add(entRecord(r.IP, e, names))
			mergedCounts[model.CodeHasPTR]++
			sr.Merged++

		case newStatus == model.CodeNXDomain || newStatus == model.CodeNoErrorEmpty:
			if r.Status == model.CodeHasPTR {
				res.RemovedAuth++
			}
			w.Add(entRecord(r.IP, e, nil))
			mergedCounts[newStatus]++
			sr.Merged++

		default: // transient failure in the new pass
			if r.Status == model.CodeHasPTR {
				// Grace policy: keep the known-good PTR, don't let a single
				// timeout/servfail erase it.
				res.GraceKept++
				writeOld(r)
			} else {
				w.Add(entRecord(r.IP, e, nil))
				mergedCounts[newStatus]++
				sr.Merged++
			}
		}
		return nil
	})
	if scanErr != nil {
		// The baseline must always be complete — a truncated old shard would
		// silently shrink the dataset.
		w.Close()
		os.Remove(outPath)
		return ShardResult{}, fmt.Errorf("baseline %s: %w", oldPath, scanErr)
	}

	for i := range ents {
		if ents[i].sf&entUsed == 0 {
			res.UnexpectedNew++
		}
	}
	if err := w.Close(); err != nil {
		return ShardResult{}, err
	}
	fmt.Fprintf(progress, "[merge] shard %-2d: %s old + %s new → %s merged (load %s, total %s)\n",
		oldHdr.ShardID, group(sr.OldTotal), group(sr.NewTotal), group(sr.Merged),
		loadDur, time.Since(start).Round(time.Second))
	return sr, nil
}

func toRecord(r store.Rec) model.Record {
	rec := model.Record{IPInt: r.IP, Status: model.StatusString(r.Status), PTR: r.PTR}
	if r.FCChecked {
		fc := r.FCMatch
		rec.FCrDNS = &fc
	}
	return rec
}

func entRecord(ip uint32, e *newEnt, names []string) model.Record {
	rec := model.Record{IPInt: ip, Status: model.StatusString(model.StatusCode(e.sf & entStatusMask)), PTR: names}
	if e.sf&entFCChecked != 0 {
		fc := e.sf&entFCMatch != 0
		rec.FCrDNS = &fc
	}
	return rec
}

// hashNames mirrors compare.hashNames: an order- and case-insensitive FNV-1a
// hash of a PTR name set.
func hashNames(names []string) uint64 {
	if len(names) == 0 {
		return 0
	}
	norm := make([]string, len(names))
	for i, n := range names {
		norm[i] = strings.ToLower(strings.TrimSuffix(n, "."))
	}
	sort.Strings(norm)
	const offset64, prime64 = 14695981039346656037, 1099511628211
	h := uint64(offset64)
	for _, n := range norm {
		for i := 0; i < len(n); i++ {
			h ^= uint64(n[i])
			h *= prime64
		}
		h ^= 0
		h *= prime64
	}
	return h
}

func sortedKeys(m map[int]string) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

// RenderText writes the human-readable merge report.
func (r *Result) RenderText(w io.Writer) {
	line := strings.Repeat("=", 72)
	fmt.Fprintln(w, line)
	fmt.Fprintln(w, "  rDNS merge — re-crawl folded into baseline")
	fmt.Fprintln(w, line)
	fmt.Fprintf(w, "  merged %d shard pairs → %s (%s records)\n", len(r.Shards), r.OutDir, group(r.MergedTotal))
	fmt.Fprintf(w, "  target set: %s | dataset timestamp: %s\n\n", r.TargetStatuses, r.Created.UTC().Format("2006-01-02"))

	fmt.Fprintf(w, "  Merged dataset by status:\n")
	for c := model.StatusCode(0); c < model.NumStatusCodes; c++ {
		n := r.MergedCounts[model.StatusString(c)]
		if n == 0 {
			continue
		}
		fmt.Fprintf(w, "    %-16s %14s  %6.2f%%\n", model.StatusString(c), group(n), 100*float64(n)/float64(r.MergedTotal))
	}

	fmt.Fprintf(w, "\n  Applied updates (relative to the baseline):\n")
	fmt.Fprintf(w, "    + %s PTRs gained (previously non-resolving)\n", group(r.Gained))
	fmt.Fprintf(w, "    ~ %s PTRs changed, %s unchanged\n", group(r.PTRChanged), group(r.PTRUnchanged))
	fmt.Fprintf(w, "    - %s PTRs authoritatively removed (nxdomain/noerror_empty)\n", group(r.RemovedAuth))
	fmt.Fprintf(w, "    = %s PTRs kept through transient failures (grace policy)\n", group(r.GraceKept))
	if r.MissingKept > 0 {
		fmt.Fprintf(w, "    ! %s targets had no new observation (baseline kept)\n", group(r.MissingKept))
	}
	if r.OldDups+r.NewDups+r.UnexpectedNew > 0 {
		fmt.Fprintf(w, "\n  Bookkeeping: %s dup old | %s dup new | %s unexpected new (all skipped)\n",
			group(r.OldDups), group(r.NewDups), group(r.UnexpectedNew))
	}
	fmt.Fprintln(w, line)
}

// group renders 1234567 as "1,234,567".
func group(n uint64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
	}
	for i := pre; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}
