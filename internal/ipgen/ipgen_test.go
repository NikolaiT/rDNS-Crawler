package ipgen

import (
	"math"
	"testing"
)

// TestSampleThresholdForPercent checks the percent→threshold conversion and its
// disabled sentinels.
func TestSampleThresholdForPercent(t *testing.T) {
	if got := SampleThresholdForPercent(0); got != 0 {
		t.Errorf("0%% -> %d, want 0 (disabled)", got)
	}
	if got := SampleThresholdForPercent(100); got != 0 {
		t.Errorf("100%% -> %d, want 0 (disabled)", got)
	}
	// 2% ≈ 0.02 * 2^32.
	f := 0.02 * 4294967296.0
	want := uint32(f)
	if got := SampleThresholdForPercent(2); got != want {
		t.Errorf("2%% -> %d, want %d", got, want)
	}
}

// TestSamplingRateAndShardSplit walks a large contiguous IP range and checks
// that (a) the sampled fraction is ~2%, (b) the sample is split roughly evenly
// across shards, and (c) shards are disjoint and their union equals the sample.
func TestSamplingRateAndShardSplit(t *testing.T) {
	const shards = 5
	const base = 1 << 24 // 1.0.0.0, safely out of reserved space
	const span = 4_000_000
	threshold := SampleThresholdForPercent(2)

	perShard := make([]int, shards)
	sampledTotal := 0
	seenByShard := make([]map[uint32]bool, shards)
	for i := range seenByShard {
		seenByShard[i] = map[uint32]bool{}
	}

	for ip := uint32(base); ip < base+span; ip++ {
		inAny := false
		for s := 0; s < shards; s++ {
			cfg := ShardConfig{Shards: shards, ShardID: uint32(s), SampleThreshold: threshold}
			if inShardSample(ip, cfg) {
				if inAny {
					t.Fatalf("ip %d claimed by more than one shard", ip)
				}
				inAny = true
				perShard[s]++
				seenByShard[s][ip] = true
			}
		}
		if inAny {
			sampledTotal++
		}
	}

	rate := float64(sampledTotal) / float64(span)
	if math.Abs(rate-0.02) > 0.002 { // within 0.2 percentage points
		t.Errorf("sample rate = %.4f, want ~0.02", rate)
	}

	// Even split: each shard within 15%% of the mean.
	mean := float64(sampledTotal) / shards
	for s, n := range perShard {
		if math.Abs(float64(n)-mean)/mean > 0.15 {
			t.Errorf("shard %d has %d (mean %.0f) — uneven split", s, n, mean)
		}
	}
}

// TestFullSpaceShardingDisjoint verifies that with sampling disabled the plain
// ip%%shards split is disjoint and complete over a range.
func TestFullSpaceShardingDisjoint(t *testing.T) {
	const shards = 5
	const base = 1 << 24
	const span = 100_000
	counts := make([]int, shards)
	for ip := uint32(base); ip < base+span; ip++ {
		hits := 0
		for s := 0; s < shards; s++ {
			if inShardSample(ip, ShardConfig{Shards: shards, ShardID: uint32(s)}) {
				hits++
				counts[s]++
			}
		}
		if hits != 1 {
			t.Fatalf("ip %d matched %d shards, want exactly 1", ip, hits)
		}
	}
}
