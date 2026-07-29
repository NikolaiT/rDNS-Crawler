package store

import (
	"context"
	"fmt"

	"rdns-crawler/internal/model"
)

// DefaultTargetCheckpoint is how many produced targets pass between cursor
// notifications from StreamTargets. Mirrors ipgen's crawl checkpoint cadence.
const DefaultTargetCheckpoint = 1 << 17

// CountTargets scans a .rdnsz observation file and returns how many records
// have a status in mask. Any read error — including a truncated tail — is
// returned as-is: a re-crawl plan must be derived from a complete observation
// file, so the caller should treat every error as fatal rather than silently
// crawling a partial target set.
func CountTargets(path string, mask model.StatusMask) (uint64, error) {
	var n uint64
	err := Scan(path, func(r Rec) error {
		if mask.Has(r.Status) {
			n++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// StreamTargets streams the dotted-quad IPs of the records in path whose
// status is in mask, in file order (deterministic for a given file, which is
// what makes count-based resume exact).
//
// skip drops the first skip matching targets (resume). limit, when non-zero,
// caps how many targets are emitted after the skip. checkpointEvery controls
// the cursor cadence (0 = DefaultTargetCheckpoint).
//
// The cursor channel periodically receives the absolute number of targets
// produced so far — i.e. a value n means "targets [0, n) of the filtered
// stream were handed to the crawler" — which the caller persists for resume.
// Cursor values are only emitted once the skip region has been passed, so a
// resumed run never moves the cursor backwards.
//
// The error channel receives at most one value when the scan ends: nil on a
// clean end-of-file, or the scan error. Both other channels are closed first,
// so callers can drain the pipeline and then check the error.
func StreamTargets(ctx context.Context, path string, mask model.StatusMask, skip, limit, checkpointEvery uint64) (<-chan string, <-chan uint64, <-chan error) {
	if checkpointEvery == 0 {
		checkpointEvery = DefaultTargetCheckpoint
	}
	out := make(chan string, 1024)
	cursor := make(chan uint64, 1)
	errch := make(chan error, 1)

	go func() {
		defer close(errch)
		defer close(cursor)
		defer close(out)

		// produced = how many targets of the filtered stream have been handled
		// (skipped in a previous run, or handed to the crawler in this one).
		// It is safe to resume at this position: everything before it was
		// delivered; nothing at or after it was.
		var produced uint64
		var emitted uint64 // emitted this run (after skip)
		errStop := fmt.Errorf("stop")

		err := Scan(path, func(r Rec) error {
			if !mask.Has(r.Status) {
				return nil
			}
			if produced < skip {
				produced++
				return nil
			}
			if limit != 0 && emitted >= limit {
				return errStop
			}
			select {
			case out <- intToIP(r.IP):
				produced++
				emitted++
			case <-ctx.Done():
				// Not delivered — do NOT count it, or resume would skip it.
				return errStop
			}
			if produced%checkpointEvery == 0 {
				select {
				case cursor <- produced:
				default:
				}
			}
			return nil
		})
		// Persist the final position (guaranteed delivery: we are the only
		// sender, so drain any stale buffered value first).
		select {
		case <-cursor:
		default:
		}
		cursor <- produced
		if err == errStop || err == nil {
			errch <- nil
			return
		}
		errch <- err
	}()
	return out, cursor, errch
}

func intToIP(v uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d", v>>24, (v>>16)&0xff, (v>>8)&0xff, v&0xff)
}
