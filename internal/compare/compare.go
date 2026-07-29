// Package compare joins two crawl passes (previous vs new .rdnsz shards) and
// computes the re-crawl statistics that make a maintained dataset measurable:
// how much of the previously timed-out set now resolves, how much of the
// previously valid set changed its PTR, the full old→new status transition
// matrix, FCrDNS shifts, and what the recovered timeouts turned out to be.
//
// Pairing is by shard id (from the .rdnsz headers): old shard-i is joined with
// new shard-i. Within a pair the old file's target records are loaded into a
// sorted in-memory slice (16 bytes per target) and the new file is streamed
// against it, so a 20-shard × 80M-target comparison peaks at ~1.3 GB of RAM.
package compare

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"rdns-crawler/internal/model"
	"rdns-crawler/internal/store"
)

// Options selects the two passes and the target-set definition.
type Options struct {
	OldPath string // .rdnsz file or directory of shards (previous pass)
	NewPath string // .rdnsz file or directory of shards (re-crawl pass)
	// Mask defines which old statuses formed the re-crawl target set. Records
	// outside it are ignored on the old side (they were not re-crawled).
	Mask    model.StatusMask
	TopTLDs int // how many TLDs to keep for the gained-PTR histogram (0 = 20)
}

// PassInfo describes one side of the comparison.
type PassInfo struct {
	Path         string    `json:"path"`
	Files        int       `json:"files"`
	Shards       uint16    `json:"shards"`
	Created      time.Time `json:"created"` // average of the shard headers
	TotalQueried uint64    `json:"total_queried"`
}

// FCStats tracks forward-confirmation transitions on the has_ptr→has_ptr set
// where both passes actually performed the check.
type FCStats struct {
	BothChecked  uint64 `json:"both_checked"`
	MatchBoth    uint64 `json:"match_both"`
	MatchOldOnly uint64 `json:"match_old_only"` // was confirmed, no longer is
	MatchNewOnly uint64 `json:"match_new_only"` // newly confirmed
	MatchNeither uint64 `json:"match_neither"`
}

// TLDCount is one entry of the gained-PTR TLD histogram.
type TLDCount struct {
	TLD string `json:"tld"`
	N   uint64 `json:"n"`
}

// GainedStats describes the PTRs recovered from previously non-resolving
// targets (any target status except has_ptr — i.e. timeout, servfail, … →
// now has_ptr).
type GainedStats struct {
	Total     uint64     `json:"total"`
	FCChecked uint64     `json:"fcrdns_checked"`
	FCMatch   uint64     `json:"fcrdns_match"`
	TopTLDs   []TLDCount `json:"top_tlds"`
}

// Headline carries the key percentages precomputed, so JSON consumers don't
// have to re-derive them from the matrix.
type Headline struct {
	TimeoutTargets       uint64  `json:"timeout_targets"`
	TimeoutCrawled       uint64  `json:"timeout_crawled"`
	TimeoutNowPTRPct     float64 `json:"timeout_now_has_ptr_pct"`     // of crawled timeout set
	TimeoutNowDefinitive float64 `json:"timeout_now_definitive_pct"`  // has_ptr+nxdomain+noerror_empty
	TimeoutStillTimeout  float64 `json:"timeout_still_timeout_pct"`   //
	HasPTRTargets        uint64  `json:"has_ptr_targets"`
	HasPTRCrawled        uint64  `json:"has_ptr_crawled"`
	PTRUnchangedPct      float64 `json:"ptr_unchanged_pct"` // of crawled has_ptr set
	PTRChangedPct        float64 `json:"ptr_changed_pct"`
	PTRRemovedPct        float64 `json:"ptr_removed_pct"`   // authoritative negative now
	PTRTransientPct      float64 `json:"ptr_transient_pct"` // timeout/servfail/... now
	ChangedPerDayPct     float64 `json:"ptr_changed_per_day_pct"`
}

// Result is the aggregated comparison across all paired shards.
type Result struct {
	Old         PassInfo `json:"old"`
	New         PassInfo `json:"new"`
	DaysBetween float64  `json:"days_between"`

	TargetStatuses     string `json:"target_statuses"`
	ComparedShards     []int  `json:"compared_shards"`
	MissingNewShards   []int  `json:"missing_new_shards"`   // old shards with no new file yet
	TruncatedNewShards []int  `json:"truncated_new_shards"` // new files collected mid-crawl

	// Targets: per old status, how many IPs formed the target set (compared
	// shards only). Transitions: old status → new status (or "missing" when
	// the target has no record in the new pass yet).
	Targets     map[string]uint64            `json:"targets"`
	Transitions map[string]map[string]uint64 `json:"transitions"`

	PTRUnchanged uint64 `json:"ptr_unchanged"`
	PTRChanged   uint64 `json:"ptr_changed"`

	FC     FCStats     `json:"fcrdns"`
	Gained GainedStats `json:"gained"`

	Headline Headline `json:"headline"`

	// Bookkeeping oddities (all expected to be ~0).
	OldDupTargets uint64 `json:"old_dup_targets"` // duplicate IPs in old files (pass-1 restarts)
	NewDupRecords uint64 `json:"new_dup_records"` // duplicate IPs in new files (re-crawl restarts)
	UnexpectedNew uint64 `json:"unexpected_new"`  // new records whose IP is not in the target set
}

const missingCol = model.NumStatusCodes // virtual "no new observation" column

// oldEnt is one target of the previous pass: 16 bytes, kept in a sorted slice.
type oldEnt struct {
	ip     uint32
	status model.StatusCode
	flags  uint8 // bit0 fcChecked, bit1 fcMatch, bit2 seen-in-new
	hash   uint64
}

const (
	fFCChecked = 1 << 0
	fFCMatch   = 1 << 1
	fSeen      = 1 << 2
)

// Run executes the comparison. Progress lines are written to progress (pass
// io.Discard to silence).
func Run(opts Options, progress io.Writer) (*Result, error) {
	if opts.TopTLDs <= 0 {
		opts.TopTLDs = 20
	}
	oldShards, oldInfo, err := discover(opts.OldPath)
	if err != nil {
		return nil, fmt.Errorf("old pass: %w", err)
	}
	newShards, newInfo, err := discover(opts.NewPath)
	if err != nil {
		return nil, fmt.Errorf("new pass: %w", err)
	}
	if oldInfo.Shards != newInfo.Shards {
		return nil, fmt.Errorf("shard layout mismatch: old pass has shards=%d, new pass shards=%d — the re-crawl must inherit the previous pass's sharding",
			oldInfo.Shards, newInfo.Shards)
	}

	res := &Result{
		Old:            oldInfo,
		New:            newInfo,
		TargetStatuses: opts.Mask.String(),
		Targets:        map[string]uint64{},
		Transitions:    map[string]map[string]uint64{},
	}
	res.DaysBetween = newInfo.Created.Sub(oldInfo.Created).Hours() / 24

	var matrix [model.NumStatusCodes][model.NumStatusCodes + 1]uint64
	tlds := map[string]uint64{}

	var ents []oldEnt
	for _, sid := range sortedKeys(oldShards) {
		newPath, ok := newShards[sid]
		if !ok {
			res.MissingNewShards = append(res.MissingNewShards, sid)
			continue
		}
		res.ComparedShards = append(res.ComparedShards, sid)

		start := time.Now()
		ents = ents[:0]
		if err := loadOldTargets(oldShards[sid], opts.Mask, &ents); err != nil {
			return nil, fmt.Errorf("old shard %d (%s): %w", sid, oldShards[sid], err)
		}
		slices.SortFunc(ents, func(a, b oldEnt) int {
			if a.ip < b.ip {
				return -1
			} else if a.ip > b.ip {
				return 1
			}
			return 0
		})
		dups := dedupSorted(&ents)
		res.OldDupTargets += dups
		for i := range ents {
			res.Targets[model.StatusString(ents[i].status)]++
		}
		loadDur := time.Since(start).Round(time.Second)

		start = time.Now()
		truncated, err := joinNewPass(newPath, ents, &matrix, res, tlds)
		if err != nil {
			return nil, fmt.Errorf("new shard %d (%s): %w", sid, newPath, err)
		}
		if truncated {
			res.TruncatedNewShards = append(res.TruncatedNewShards, sid)
		}
		// Targets never observed in the new pass.
		for i := range ents {
			if ents[i].flags&fSeen == 0 {
				matrix[ents[i].status][missingCol]++
			}
		}
		fmt.Fprintf(progress, "[compare] shard %-2d: %d targets (load %s) joined in %s%s\n",
			sid, len(ents), loadDur, time.Since(start).Round(time.Second),
			map[bool]string{true: " [new file truncated — still crawling?]", false: ""}[truncated])
	}

	finalize(res, &matrix, tlds, opts)
	return res, nil
}

// discover expands a file-or-directory path into shardID → file, and returns
// the pass-level info (avg created time, summed totals).
func discover(path string) (map[int]string, PassInfo, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, PassInfo{}, err
	}
	var files []string
	if fi.IsDir() {
		files, err = filepath.Glob(filepath.Join(path, "*.rdnsz"))
		if err != nil {
			return nil, PassInfo{}, err
		}
	} else {
		files = []string{path}
	}
	if len(files) == 0 {
		return nil, PassInfo{}, fmt.Errorf("no .rdnsz files in %s", path)
	}
	shards := map[int]string{}
	info := PassInfo{Path: path, Files: len(files)}
	var createdSum int64
	for _, f := range files {
		h, err := store.ReadHeader(f)
		if err != nil {
			return nil, PassInfo{}, err
		}
		if prev, dup := shards[int(h.ShardID)]; dup {
			return nil, PassInfo{}, fmt.Errorf("both %s and %s claim shard %d", prev, f, h.ShardID)
		}
		shards[int(h.ShardID)] = f
		if info.Shards == 0 {
			info.Shards = h.Shards
		} else if info.Shards != h.Shards {
			return nil, PassInfo{}, fmt.Errorf("%s: shards=%d differs from other files (%d)", f, h.Shards, info.Shards)
		}
		info.TotalQueried += h.Total
		createdSum += h.Created
	}
	info.Created = time.Unix(createdSum/int64(len(files)), 0).UTC()
	return shards, info, nil
}

// loadOldTargets appends the mask-matching records of one old shard to ents.
// The old file must be complete: a truncated previous pass would silently
// shrink the baseline, so it is an error.
func loadOldTargets(path string, mask model.StatusMask, ents *[]oldEnt) error {
	return store.Scan(path, func(r store.Rec) error {
		if !mask.Has(r.Status) {
			return nil
		}
		e := oldEnt{ip: r.IP, status: r.Status}
		if r.FCChecked {
			e.flags |= fFCChecked
			if r.FCMatch {
				e.flags |= fFCMatch
			}
		}
		if r.Status == model.CodeHasPTR {
			e.hash = hashNames(r.PTR)
		}
		*ents = append(*ents, e)
		return nil
	})
}

// dedupSorted removes duplicate IPs from a sorted slice in place (keeps the
// first), returning how many were dropped.
func dedupSorted(ents *[]oldEnt) uint64 {
	s := *ents
	if len(s) < 2 {
		return 0
	}
	out := 1
	var dropped uint64
	for i := 1; i < len(s); i++ {
		if s[i].ip == s[out-1].ip {
			dropped++
			continue
		}
		s[out] = s[i]
		out++
	}
	*ents = s[:out]
	return dropped
}

// joinNewPass streams the new shard against the sorted old targets,
// accumulating the transition matrix and change stats. A truncated tail on the
// new side is tolerated (the shard may have been collected mid-crawl); it is
// reported via the bool.
func joinNewPass(path string, ents []oldEnt, matrix *[model.NumStatusCodes][model.NumStatusCodes + 1]uint64, res *Result, tlds map[string]uint64) (bool, error) {
	err := store.Scan(path, func(r store.Rec) error {
		i := sort.Search(len(ents), func(k int) bool { return ents[k].ip >= r.IP })
		if i >= len(ents) || ents[i].ip != r.IP {
			res.UnexpectedNew++
			return nil
		}
		e := &ents[i]
		if e.flags&fSeen != 0 {
			res.NewDupRecords++
			return nil
		}
		e.flags |= fSeen
		matrix[e.status][r.Status]++

		if e.status == model.CodeHasPTR && r.Status == model.CodeHasPTR {
			if e.hash == hashNames(r.PTR) {
				res.PTRUnchanged++
			} else {
				res.PTRChanged++
			}
			if e.flags&fFCChecked != 0 && r.FCChecked {
				res.FC.BothChecked++
				switch {
				case e.flags&fFCMatch != 0 && r.FCMatch:
					res.FC.MatchBoth++
				case e.flags&fFCMatch != 0:
					res.FC.MatchOldOnly++
				case r.FCMatch:
					res.FC.MatchNewOnly++
				default:
					res.FC.MatchNeither++
				}
			}
		}
		if e.status != model.CodeHasPTR && r.Status == model.CodeHasPTR {
			res.Gained.Total++
			if r.FCChecked {
				res.Gained.FCChecked++
				if r.FCMatch {
					res.Gained.FCMatch++
				}
			}
			for _, n := range r.PTR {
				tlds[tldOf(n)]++
			}
		}
		return nil
	})
	if errors.Is(err, store.ErrTruncatedTail) {
		return true, nil
	}
	return false, err
}

// finalize converts the accumulator matrix into the serializable result and
// computes the headline percentages.
func finalize(res *Result, matrix *[model.NumStatusCodes][model.NumStatusCodes + 1]uint64, tlds map[string]uint64, opts Options) {
	for old := model.StatusCode(0); old < model.NumStatusCodes; old++ {
		if !opts.Mask.Has(old) {
			continue
		}
		row := map[string]uint64{}
		for nw := 0; nw <= int(model.NumStatusCodes); nw++ {
			n := matrix[old][nw]
			if n == 0 {
				continue
			}
			if nw == missingCol {
				row["missing"] = n
			} else {
				row[model.StatusString(model.StatusCode(nw))] = n
			}
		}
		res.Transitions[model.StatusString(old)] = row
	}

	// Gained-PTR TLD histogram.
	type kv struct {
		k string
		v uint64
	}
	all := make([]kv, 0, len(tlds))
	for k, v := range tlds {
		all = append(all, kv{k, v})
	}
	slices.SortFunc(all, func(a, b kv) int {
		if a.v != b.v {
			if a.v > b.v {
				return -1
			}
			return 1
		}
		return strings.Compare(a.k, b.k)
	})
	if len(all) > opts.TopTLDs {
		all = all[:opts.TopTLDs]
	}
	for _, e := range all {
		res.Gained.TopTLDs = append(res.Gained.TopTLDs, TLDCount{TLD: e.k, N: e.v})
	}

	// Headline percentages.
	h := &res.Headline
	to := matrix[model.CodeTimeout]
	h.TimeoutTargets = res.Targets[model.StatusTimeout]
	h.TimeoutCrawled = rowSum(to) - to[missingCol]
	if h.TimeoutCrawled > 0 {
		c := float64(h.TimeoutCrawled)
		h.TimeoutNowPTRPct = 100 * float64(to[model.CodeHasPTR]) / c
		h.TimeoutNowDefinitive = 100 * float64(to[model.CodeHasPTR]+to[model.CodeNXDomain]+to[model.CodeNoErrorEmpty]) / c
		h.TimeoutStillTimeout = 100 * float64(to[model.CodeTimeout]) / c
	}
	hp := matrix[model.CodeHasPTR]
	h.HasPTRTargets = res.Targets[model.StatusHasPTR]
	h.HasPTRCrawled = rowSum(hp) - hp[missingCol]
	if h.HasPTRCrawled > 0 {
		c := float64(h.HasPTRCrawled)
		h.PTRUnchangedPct = 100 * float64(res.PTRUnchanged) / c
		h.PTRChangedPct = 100 * float64(res.PTRChanged) / c
		h.PTRRemovedPct = 100 * float64(hp[model.CodeNXDomain]+hp[model.CodeNoErrorEmpty]) / c
		h.PTRTransientPct = 100 * float64(hp[model.CodeTimeout]+hp[model.CodeServFail]+hp[model.CodeRefused]+hp[model.CodeNetError]+hp[model.CodeLameDelegation]) / c
		if res.DaysBetween > 0 {
			h.ChangedPerDayPct = h.PTRChangedPct / res.DaysBetween
		}
	}
}

func rowSum(row [model.NumStatusCodes + 1]uint64) uint64 {
	var s uint64
	for _, v := range row {
		s += v
	}
	return s
}

// hashNames produces an order- and case-insensitive 64-bit FNV-1a hash of a
// PTR name set, used to detect changes without storing the names.
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

func tldOf(host string) string {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	i := strings.LastIndex(host, ".")
	if i < 0 || i == len(host)-1 {
		return "(none)"
	}
	return host[i+1:]
}

func sortedKeys(m map[int]string) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

// sortedStatusKeys orders status names by their code (stable report order).
func sortedStatusKeys(m map[string]uint64) []string {
	keys := make([]string, 0, len(m))
	for c := model.StatusCode(0); c < model.NumStatusCodes; c++ {
		if _, ok := m[model.StatusString(c)]; ok {
			keys = append(keys, model.StatusString(c))
		}
	}
	return keys
}

// RenderText writes the human-readable report.
func (r *Result) RenderText(w io.Writer) {
	line := strings.Repeat("=", 72)
	fmt.Fprintln(w, line)
	fmt.Fprintln(w, "  rDNS re-crawl comparison")
	fmt.Fprintln(w, line)
	fmt.Fprintf(w, "  old pass: %s\n            %d files | %d shards | %s | %s IPs queried\n",
		r.Old.Path, r.Old.Files, r.Old.Shards, r.Old.Created.Format("2006-01-02"), group(r.Old.TotalQueried))
	fmt.Fprintf(w, "  new pass: %s\n            %d files | %d shards | %s | %s IPs queried\n",
		r.New.Path, r.New.Files, r.New.Shards, r.New.Created.Format("2006-01-02"), group(r.New.TotalQueried))
	fmt.Fprintf(w, "  passes are %.1f days apart | target set: %s\n", r.DaysBetween, r.TargetStatuses)
	if len(r.MissingNewShards) > 0 {
		fmt.Fprintf(w, "  WARNING: no new file for old shards %v — their targets are excluded\n", r.MissingNewShards)
	}
	if len(r.TruncatedNewShards) > 0 {
		fmt.Fprintf(w, "  NOTE: new shards %v were truncated (collected mid-crawl); partial data compared\n", r.TruncatedNewShards)
	}

	h := r.Headline
	// --- timeout set ---
	if h.TimeoutTargets > 0 {
		fmt.Fprintf(w, "\n  Previously TIMEOUT (%s targets, %s crawled — %s):\n",
			group(h.TimeoutTargets), group(h.TimeoutCrawled), pctOf(h.TimeoutCrawled, h.TimeoutTargets))
		renderRow(w, r.Transitions[model.StatusTimeout], h.TimeoutCrawled)
		fmt.Fprintf(w, "    → %.2f%% of the crawled timeout set now yields a PTR hostname\n", h.TimeoutNowPTRPct)
		fmt.Fprintf(w, "    → %.2f%% now gives a definitive answer (has_ptr / nxdomain / noerror_empty)\n", h.TimeoutNowDefinitive)
		if r.Gained.FCChecked > 0 {
			fmt.Fprintf(w, "    → recovered PTRs forward-confirm (FCrDNS) at %s (%s of %s checked)\n",
				pctOf(r.Gained.FCMatch, r.Gained.FCChecked), group(r.Gained.FCMatch), group(r.Gained.FCChecked))
		}
	}

	// --- other non-resolving target sets (e.g. servfail when configured) ---
	for _, st := range sortedStatusKeys(r.Targets) {
		if st == model.StatusHasPTR || st == model.StatusTimeout {
			continue
		}
		targets := r.Targets[st]
		row := r.Transitions[st]
		var crawled uint64
		for k, v := range row {
			if k != "missing" {
				crawled += v
			}
		}
		fmt.Fprintf(w, "\n  Previously %s (%s targets, %s crawled — %s):\n",
			strings.ToUpper(st), group(targets), group(crawled), pctOf(crawled, targets))
		renderRow(w, row, crawled)
		fmt.Fprintf(w, "    → %s of the crawled %s set now yields a PTR hostname\n",
			pctOf(row[model.StatusHasPTR], crawled), st)
	}

	// --- has_ptr set ---
	if h.HasPTRTargets > 0 {
		fmt.Fprintf(w, "\n  Previously HAS_PTR (%s targets, %s crawled — %s):\n",
			group(h.HasPTRTargets), group(h.HasPTRCrawled), pctOf(h.HasPTRCrawled, h.HasPTRTargets))
		renderRow(w, r.Transitions[model.StatusHasPTR], h.HasPTRCrawled)
		fmt.Fprintf(w, "    → unchanged PTR    %s (%.2f%%)\n", group(r.PTRUnchanged), h.PTRUnchangedPct)
		fmt.Fprintf(w, "    → changed PTR      %s (%.2f%%)  ≈ %.4f%%/day churn\n", group(r.PTRChanged), h.PTRChangedPct, h.ChangedPerDayPct)
		fmt.Fprintf(w, "    → PTR removed      %.2f%% (authoritative nxdomain/noerror_empty)\n", h.PTRRemovedPct)
		fmt.Fprintf(w, "    → transient fail   %.2f%% (timeout/servfail/refused/net_error)\n", h.PTRTransientPct)
		if r.FC.BothChecked > 0 {
			fmt.Fprintf(w, "    → FCrDNS (both passes checked, %s IPs): %s stayed confirmed, %s lost, %s gained confirmation\n",
				group(r.FC.BothChecked), pctOf(r.FC.MatchBoth, r.FC.BothChecked),
				pctOf(r.FC.MatchOldOnly, r.FC.BothChecked), pctOf(r.FC.MatchNewOnly, r.FC.BothChecked))
		}
	}

	// --- net effect ---
	gained := r.Gained.Total
	lostAuth := r.Transitions[model.StatusHasPTR][model.StatusNXDomain] + r.Transitions[model.StatusHasPTR][model.StatusNoErrorEmpty]
	fmt.Fprintf(w, "\n  Net PTR movement (crawled targets only):\n")
	fmt.Fprintf(w, "    + %s PTRs gained from previously non-resolving targets\n", group(gained))
	fmt.Fprintf(w, "    - %s PTRs authoritatively removed from the has_ptr set\n", group(lostAuth))
	if gained >= lostAuth {
		fmt.Fprintf(w, "    = net +%s\n", group(gained-lostAuth))
	} else {
		fmt.Fprintf(w, "    = net -%s\n", group(lostAuth-gained))
	}

	if len(r.Gained.TopTLDs) > 0 {
		fmt.Fprintln(w, "\n  Top TLDs of gained PTRs:")
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		for _, t := range r.Gained.TopTLDs {
			fmt.Fprintf(tw, "    .%s\t%s\n", t.TLD, group(t.N))
		}
		tw.Flush()
	}

	if r.OldDupTargets+r.NewDupRecords+r.UnexpectedNew > 0 {
		fmt.Fprintf(w, "\n  Bookkeeping: %s dup old targets | %s dup new records | %s new records outside target set\n",
			group(r.OldDupTargets), group(r.NewDupRecords), group(r.UnexpectedNew))
	}
	fmt.Fprintln(w, line)
}

// renderRow prints one transition row sorted by count, as aligned columns.
func renderRow(w io.Writer, row map[string]uint64, crawled uint64) {
	type kv struct {
		k string
		v uint64
	}
	items := make([]kv, 0, len(row))
	for k, v := range row {
		if k == "missing" {
			continue
		}
		items = append(items, kv{k, v})
	}
	slices.SortFunc(items, func(a, b kv) int {
		if a.v != b.v {
			if a.v > b.v {
				return -1
			}
			return 1
		}
		return strings.Compare(a.k, b.k)
	})
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	for _, it := range items {
		fmt.Fprintf(tw, "    now %s\t%s\t%s\n", it.k, group(it.v), pctOf(it.v, crawled))
	}
	if m := row["missing"]; m > 0 {
		fmt.Fprintf(tw, "    not crawled yet\t%s\t\n", group(m))
	}
	tw.Flush()
}

func pctOf(n, total uint64) string {
	if total == 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f%%", 100*float64(n)/float64(total))
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
