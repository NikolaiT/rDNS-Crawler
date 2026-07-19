package store

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"

	"rdns-crawler/internal/model"
)

// Header holds the file-level stats read from a .rdnsz header.
type Header struct {
	Version byte
	Shards  uint16
	ShardID uint16
	Total   uint64
	Counts  [model.NumStatusCodes]uint64
	Created int64
}

// ErrTruncatedTail means the file ended in the middle of a block — the writer
// was still appending (e.g. collected live) or was killed mid-flush. All blocks
// before the truncation were decoded normally; only the partial tail is lost.
var ErrTruncatedTail = errors.New("truncated trailing block (file still being written or interrupted)")

// Count returns the number of records with the given status code.
func (h Header) Count(c model.StatusCode) uint64 {
	if int(c) < model.NumStatusCodes {
		return h.Counts[c]
	}
	return 0
}

// Rec is a decoded stored record.
type Rec struct {
	IP        uint32
	Status    model.StatusCode
	PTR       []string // populated only when Status == has_ptr
	FCChecked bool
	FCMatch   bool
}

// ReadHeader parses just the fixed header of a .rdnsz file.
func ReadHeader(path string) (Header, error) {
	f, err := os.Open(path)
	if err != nil {
		return Header{}, err
	}
	defer f.Close()
	var h [headerSize]byte
	if _, err := io.ReadFull(f, h[:]); err != nil {
		return Header{}, err
	}
	if string(h[offMagic:offMagic+5]) != magic {
		return Header{}, fmt.Errorf("%s: bad magic (not a .rdnsz v2 file)", path)
	}
	hdr := Header{
		Version: h[offVersion],
		Shards:  binary.LittleEndian.Uint16(h[offShards:]),
		ShardID: binary.LittleEndian.Uint16(h[offShardID:]),
		Created: int64(binary.LittleEndian.Uint64(h[offCreated:])),
		Total:   binary.LittleEndian.Uint64(h[offTotal:]),
	}
	for i := 0; i < model.NumStatusCodes; i++ {
		hdr.Counts[i] = binary.LittleEndian.Uint64(h[offCounts+i*8:])
	}
	return hdr, nil
}

// Scan streams every stored record to fn, in file order (IP-sorted within
// blocks). Returning an error from fn stops the scan.
func Scan(path string, fn func(Rec) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)

	if _, err := io.CopyN(io.Discard, br, headerSize); err != nil {
		return err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer dec.Close()

	var hdr [12]byte
	for {
		_, err := io.ReadFull(br, hdr[:])
		if errors.Is(err, io.EOF) {
			return nil
		}
		// A partial block header means the file was truncated mid-write (e.g.
		// rsync'd from a still-running shard). Everything before is valid.
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return ErrTruncatedTail
		}
		if err != nil {
			return err
		}
		compLen := binary.LittleEndian.Uint32(hdr[0:])
		rawLen := binary.LittleEndian.Uint32(hdr[8:])

		comp := make([]byte, compLen)
		if _, err := io.ReadFull(br, comp); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return ErrTruncatedTail
			}
			return err
		}
		raw, err := dec.DecodeAll(comp, make([]byte, 0, rawLen))
		if err != nil {
			// A truncated final block can also surface as a decompress error.
			return fmt.Errorf("%s: block decompress: %w", path, err)
		}
		if err := decodeBlock(raw, fn); err != nil {
			return err
		}
	}
}

func decodeBlock(raw []byte, fn func(Rec) error) error {
	var prev uint32
	first := true
	off := 0
	for off < len(raw) {
		delta, n := binary.Uvarint(raw[off:])
		if n <= 0 {
			return errors.New("corrupt block: bad ip delta")
		}
		off += n
		var ip uint32
		if first {
			ip = uint32(delta)
			first = false
		} else {
			ip = prev + uint32(delta)
		}
		prev = ip

		if off >= len(raw) {
			return errors.New("corrupt block: missing status")
		}
		sf := raw[off]
		off++

		rec := Rec{
			IP:        ip,
			Status:    model.StatusCode(sf & statusMask),
			FCChecked: sf&flagFCChecked != 0,
			FCMatch:   sf&flagFCMatch != 0,
		}

		if rec.Status == model.CodeHasPTR {
			count, n := binary.Uvarint(raw[off:])
			if n <= 0 {
				return errors.New("corrupt block: bad ptr count")
			}
			off += n
			for i := uint64(0); i < count; i++ {
				l, n := binary.Uvarint(raw[off:])
				if n <= 0 {
					return errors.New("corrupt block: bad name len")
				}
				off += n
				if off+int(l) > len(raw) {
					return errors.New("corrupt block: name overruns")
				}
				name := decodeTemplate(ip, string(raw[off:off+int(l)]))
				off += int(l)
				rec.PTR = append(rec.PTR, name)
			}
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

// --- helpers shared by writer.go summary rendering ---

func tldOf(host string) string {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	i := strings.LastIndex(host, ".")
	if i < 0 || i == len(host)-1 {
		return "(none)"
	}
	return host[i+1:]
}

type kv struct {
	key string
	val int
}

func topN(m map[string]int, n int) []kv {
	items := make([]kv, 0, len(m))
	for k, v := range m {
		items = append(items, kv{k, v})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].val != items[j].val {
			return items[i].val > items[j].val
		}
		return items[i].key < items[j].key
	})
	if len(items) > n {
		items = items[:n]
	}
	return items
}
