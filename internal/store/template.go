package store

import "strings"

// IP templating: the single biggest source of redundancy in reverse-DNS data is
// that the PTR hostname embeds the IP itself, e.g.
//
//	141.230.114.32 -> ec2-141-230-114-32.ap-southeast-1.compute.amazonaws.com
//	62.210.199.238 -> 62-210-199-238.rev.poneytelecom.eu
//
// We replace every representation of the record's own IP inside the hostname
// with a 1-byte marker. Because the IP is known at decode time (reconstructed
// from the delta stream), the transform is perfectly reversible. This shrinks
// the per-record payload before zstd and, more importantly, makes the residual
// bytes ("ec2-", ".compute.amazonaws.com") highly repetitive across records so
// zstd's dictionary matching becomes extremely effective.
//
// Markers use control bytes that never occur in DNS hostnames (which are
// printable ASCII, letters/digits/hyphen/dot).
const (
	mFwdDash = 0x01 // "141-230-114-32"
	mRevDash = 0x02 // "32-114-230-141"
	mFwdDot  = 0x03 // "141.230.114.32"
	mRevDot  = 0x04 // "32.114.230.141"
)

func octets(ip uint32) (a, b, c, d uint32) {
	return ip >> 24, (ip >> 16) & 0xff, (ip >> 8) & 0xff, ip & 0xff
}

func join4(w, x, y, z uint32, sep string) string {
	return itoa(w) + sep + itoa(x) + sep + itoa(y) + sep + itoa(z)
}

func itoa(v uint32) string {
	// small, allocation-light itoa for 0..255
	if v < 10 {
		return string([]byte{byte('0' + v)})
	}
	if v < 100 {
		return string([]byte{byte('0' + v/10), byte('0' + v%10)})
	}
	return string([]byte{byte('0' + v/100), byte('0' + (v/10)%10), byte('0' + v%10)})
}

// encodeTemplate substitutes the IP's representations in host with markers.
// Longer/dashed forms are replaced first so they win over shorter overlaps.
func encodeTemplate(ip uint32, host string) string {
	a, b, c, d := octets(ip)
	host = strings.ReplaceAll(host, join4(a, b, c, d, "-"), string(rune(mFwdDash)))
	host = strings.ReplaceAll(host, join4(d, c, b, a, "-"), string(rune(mRevDash)))
	host = strings.ReplaceAll(host, join4(a, b, c, d, "."), string(rune(mFwdDot)))
	host = strings.ReplaceAll(host, join4(d, c, b, a, "."), string(rune(mRevDot)))
	return host
}

// decodeTemplate restores an encoded host given its IP.
func decodeTemplate(ip uint32, s string) string {
	a, b, c, d := octets(ip)
	r := strings.NewReplacer(
		string(rune(mFwdDash)), join4(a, b, c, d, "-"),
		string(rune(mRevDash)), join4(d, c, b, a, "-"),
		string(rune(mFwdDot)), join4(a, b, c, d, "."),
		string(rune(mRevDot)), join4(d, c, b, a, "."),
	)
	return r.Replace(s)
}
