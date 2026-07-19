// Package ipgen produces IPv4 addresses to crawl while skipping reserved
// (bogon / special-purpose) address space.
//
// The reserved ranges below are the standard IANA special-purpose / bogon
// blocks; both the random and sequential generators skip them so only routable
// IPv4 is ever queried.
package ipgen

import (
	"context"
	"fmt"
	"math/rand"
	"net"
)

// reservedRange is an inclusive [start,end] block of uint32 IPv4 integers.
type reservedRange struct {
	start uint32
	end   uint32
}

// ReservedIPv4Ranges lists the IANA special-purpose / bogon IPv4 blocks.
// https://en.wikipedia.org/wiki/Reserved_IP_addresses
var ReservedIPv4Ranges = []reservedRange{
	{0, 16777215},            // 0.0.0.0/8
	{167772160, 184549375},   // 10.0.0.0/8
	{1681915904, 1686110207}, // 100.64.0.0/10
	{2130706432, 2147483647}, // 127.0.0.0/8
	{2851995648, 2852061183}, // 169.254.0.0/16
	{2886729728, 2887778303}, // 172.16.0.0/12
	{3221225472, 3221225727}, // 192.0.0.0/24
	{3221225984, 3221226239}, // 192.0.2.0/24
	{3227017984, 3227018239}, // 192.88.99.0/24
	{3232235520, 3232301055}, // 192.168.0.0/16
	{3323068416, 3323199487}, // 198.18.0.0/15
	{3325256704, 3325256959}, // 198.51.100.0/24
	{3405803776, 3405804031}, // 203.0.113.0/24
	{3758096384, 4026531839}, // 224.0.0.0/4 (multicast)
	{4026531840, 4294967295}, // 240.0.0.0/4 (reserved)
}

// IsReserved reports whether an IPv4 integer falls in reserved/bogon space.
func IsReserved(v uint32) bool {
	for _, r := range ReservedIPv4Ranges {
		if v >= r.start && v <= r.end {
			return true
		}
	}
	return false
}

// IntToIP formats a uint32 as a dotted-quad IPv4 string.
func IntToIP(v uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d", v>>24, (v>>16)&0xff, (v>>8)&0xff, v&0xff)
}

// IPToInt parses a dotted-quad IPv4 string into a uint32 (0 if invalid).
func IPToInt(s string) uint32 {
	ip := net.ParseIP(s)
	if ip == nil {
		return 0
	}
	ip = ip.To4()
	if ip == nil {
		return 0
	}
	return uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])
}

// TotalRoutableIPv4 is the number of non-reserved IPv4 addresses.
func TotalRoutableIPv4() uint64 {
	var reserved uint64
	for _, r := range ReservedIPv4Ranges {
		reserved += uint64(r.end) - uint64(r.start) + 1
	}
	return (1 << 32) - reserved
}

// GenerateRandom emits n unique random non-reserved IPv4 addresses on the
// returned channel. It de-duplicates so a test run never resolves the same IP
// twice. The channel is closed when n addresses have been produced or ctx is
// cancelled.
func GenerateRandom(ctx context.Context, n int, seed int64) <-chan string {
	out := make(chan string, 1024)
	go func() {
		defer close(out)
		rng := rand.New(rand.NewSource(seed))
		seen := make(map[uint32]struct{}, n)
		for len(seen) < n {
			v := rng.Uint32()
			if IsReserved(v) {
				continue
			}
			if _, dup := seen[v]; dup {
				continue
			}
			seen[v] = struct{}{}
			select {
			case out <- IntToIP(v):
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// GenerateSequential emits up to count non-reserved IPv4 addresses starting at
// startIP (inclusive), skipping reserved blocks. Used by the (experimental)
// full-space sweep mode. Emits fewer than count if it reaches the end of space.
func GenerateSequential(ctx context.Context, startIP uint32, count uint64) <-chan string {
	out := make(chan string, 1024)
	go func() {
		defer close(out)
		var emitted uint64
		v := uint64(startIP)
		for emitted < count && v <= 0xffffffff {
			iv := uint32(v)
			if IsReserved(iv) {
				// Jump to the end of the reserved block we're inside.
				for _, r := range ReservedIPv4Ranges {
					if iv >= r.start && iv <= r.end {
						v = uint64(r.end) + 1
						break
					}
				}
				continue
			}
			select {
			case out <- IntToIP(iv):
				emitted++
				v++
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// ShardConfig configures a sharded full-space walk for distributed crawling.
type ShardConfig struct {
	Shards  uint32 // total number of nodes (>=1)
	ShardID uint32 // this node's index in [0, Shards)
	StartIP uint32 // resume point (inclusive); 0 = from the beginning
	Limit   uint64 // 0 = no limit (walk to the end of the address space)

	// SampleThreshold enables random sampling: an IP is in the sample when
	// hashLow(ip) < SampleThreshold. 0 disables sampling (crawl the full space).
	// For an f-fraction sample, set SampleThreshold = round(f * 2^32).
	SampleThreshold uint32
}

// SampleThresholdForPercent converts a percentage (e.g. 2.0 for 2%) into the
// 32-bit hash threshold used by ShardConfig. Returns 0 (sampling disabled) for
// percentages <= 0 or >= 100.
func SampleThresholdForPercent(percent float64) uint32 {
	if percent <= 0 || percent >= 100 {
		return 0
	}
	t := uint64(percent / 100.0 * 4294967296.0)
	if t == 0 {
		t = 1
	}
	if t > 0xffffffff {
		t = 0xffffffff
	}
	return uint32(t)
}

// mix64 is splitmix64 — a fast, well-distributed integer hash. We use its low
// 32 bits for the sample decision and its high 32 bits for shard assignment;
// the two halves are statistically independent, so sampling and sharding don't
// interfere (unlike ip%N tricks, which collide when the moduli share factors).
func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// inShardSample reports whether ip belongs to this node under cfg. When
// sampling is enabled, both the sample membership and the shard split are
// derived from a hash of the IP (pure function → reproducible and
// resume-safe). When disabled, sharding is the simple even ip%shards split.
func inShardSample(ip uint32, cfg ShardConfig) bool {
	if cfg.SampleThreshold != 0 {
		h := mix64(uint64(ip))
		if uint32(h) >= cfg.SampleThreshold {
			return false // not in the random sample
		}
		return uint32(h>>32)%cfg.Shards == cfg.ShardID
	}
	return ip%cfg.Shards == cfg.ShardID
}

// GenerateShardedFull walks the entire routable IPv4 space but only emits the
// addresses that belong to this shard (see inShardSample). With sampling
// disabled that is ip % Shards == ShardID; with sampling enabled it is a
// hash-based random subset split evenly across shards.
//
// Interleaving (rather than handing each node a contiguous /8 block) keeps every
// node's progress and load even — they all sweep the same regions of the address
// space at the same time, so a node in a densely-populated block isn't stuck
// doing all the work while others idle. Because the walk is a deterministic
// ascending scan and membership is a pure function of the IP, resuming is just
// "set StartIP to the last IP you stored + 1".
//
// The last emitted IP (as uint32) is reported on the returned progress channel
// periodically so the caller can checkpoint a resume cursor.
func GenerateShardedFull(ctx context.Context, cfg ShardConfig) (<-chan string, <-chan uint32) {
	if cfg.Shards == 0 {
		cfg.Shards = 1
	}
	out := make(chan string, 1024)
	cursor := make(chan uint32, 1)

	go func() {
		defer close(out)
		defer close(cursor)

		var emitted uint64
		v := uint64(cfg.StartIP)
		// Checkpoint every 128Ki emitted IPs: with a 2% sample at ~500 q/s that
		// is a cursor write every ~4 min, so a crash re-does minutes, not hours.
		const checkpointEvery = 1 << 17
		var sinceCheckpoint uint64

		for v <= 0xffffffff {
			if cfg.Limit != 0 && emitted >= cfg.Limit {
				return
			}
			iv := uint32(v)
			if IsReserved(iv) {
				for _, r := range ReservedIPv4Ranges {
					if iv >= r.start && iv <= r.end {
						v = uint64(r.end) + 1
						break
					}
				}
				continue
			}
			if !inShardSample(iv, cfg) {
				v++
				continue
			}
			select {
			case out <- IntToIP(iv):
				emitted++
				sinceCheckpoint++
				if sinceCheckpoint >= checkpointEvery {
					sinceCheckpoint = 0
					select {
					case cursor <- iv:
					default:
					}
				}
				v++
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, cursor
}
