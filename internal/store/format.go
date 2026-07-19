// Package store implements a compact, self-describing on-disk format (.rdnsz)
// for reverse-DNS crawl results, plus a reader that reconstructs records.
//
// v2 (commercial dataset): unlike v1, EVERY queried IP is recorded — with its
// status code — not just the PTR hits. This is what makes a maintained,
// re-crawlable dataset possible: the updater needs to know the last-observed
// state of negatives (nxdomain vs noerror_empty vs servfail vs timeout …) to
// schedule smart re-crawls and to detect churn. Failures compress extremely
// well (long runs of the same status), so keeping them is cheap.
//
// Design goals (full IPv4 scan → keep bytes tiny):
//   - One record per queried IP: varint IP-delta + a 1-byte status/flags field,
//     and PTR names only when the status is has_ptr.
//   - The record's own IP is templated out of its hostname(s) (see template.go),
//     leaving highly repetitive residue.
//   - Records are written in IP-sorted blocks; each block is independently
//     zstd-compressed, so files stream, append, seek and merge cleanly.
//
// File layout:
//
//	Header (headerSize bytes, fixed)
//	repeated Block:  u32 compressedLen | u32 recordCount | u32 rawLen | zstd(payload)
//	(EOF)
//
// Header counts are patched in at Close() by seeking back to offset 0.
package store

const (
	magic       = "RDNS2"
	formatVer   = 2
	headerSize  = 128
	blockRecs   = 1 << 16 // records buffered before a block is flushed
	maxPTRNames = 32      // defensive cap on PTR names kept per IP
)

// record payload (inside a block, before zstd):
//
//	varint(ipDelta)      delta from previous IP in the block (first = absolute IP)
//	byte(statusFlags)    bits 0-3: status code (model.StatusCode)
//	                     bit 4: fcrdns checked, bit 5: fcrdns match
//	if status == has_ptr:
//	  varint(ptrCount)
//	  ptrCount × ( varint(len) | templatedBytes )
const (
	statusMask    = 0x0f
	flagFCChecked = 1 << 4
	flagFCMatch   = 1 << 5
)

// Header field byte offsets.
const (
	offMagic   = 0  // 5 bytes
	offVersion = 5  // 1
	offShards  = 6  // 2
	offShardID = 8  // 2
	offCreated = 10 // 8
	offTotal   = 18 // 8
	offCounts  = 26 // NumStatusCodes × 8
)
