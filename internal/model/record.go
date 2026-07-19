// Package model defines the shapes shared across the crawler.
package model

// Status values for a single rDNS lookup. These are the string form used in
// JSONL output and logs. The compact .rdnsz store uses the numeric StatusCode
// below (a stable 4-bit code) so the taxonomy can grow without breaking files.
//
// The taxonomy separates outcomes that matter for a maintained, re-crawled
// dataset:
//
//	has_ptr          the IP has at least one PTR record (a "hit")
//	noerror_empty    reverse zone answered NOERROR but with no PTR for this IP
//	nxdomain         authoritative "this name does not exist" (stable negative)
//	servfail         SERVFAIL — broken delegation / DNSSEC / auth failure
//	refused          REFUSED (or NOTIMP) — resolver/auth declined the query
//	timeout          all attempts timed out (dead host, loss, or rate-limit)
//	net_error        transport/network error that isn't a timeout
//	lame_delegation  zone delegated to nameservers that don't answer for it
//	                 (populated by the zone prober, not the flat PTR path)
const (
	StatusHasPTR         = "has_ptr"
	StatusNoErrorEmpty   = "noerror_empty"
	StatusNXDomain       = "nxdomain"
	StatusServFail       = "servfail"
	StatusRefused        = "refused"
	StatusTimeout        = "timeout"
	StatusNetError       = "net_error"
	StatusLameDelegation = "lame_delegation"

	// Deprecated aliases kept so older call sites/tests still compile. Prefer
	// the specific statuses above.
	StatusOK    = StatusHasPTR
	StatusNoPTR = StatusNoErrorEmpty
	StatusError = StatusNetError
)

// StatusCode is the compact on-disk representation of a status (0..15).
type StatusCode uint8

const (
	CodeHasPTR         StatusCode = 0
	CodeNoErrorEmpty   StatusCode = 1
	CodeNXDomain       StatusCode = 2
	CodeServFail       StatusCode = 3
	CodeRefused        StatusCode = 4
	CodeTimeout        StatusCode = 5
	CodeNetError       StatusCode = 6
	CodeLameDelegation StatusCode = 7

	NumStatusCodes = 8
)

var codeToStatus = [NumStatusCodes]string{
	CodeHasPTR:         StatusHasPTR,
	CodeNoErrorEmpty:   StatusNoErrorEmpty,
	CodeNXDomain:       StatusNXDomain,
	CodeServFail:       StatusServFail,
	CodeRefused:        StatusRefused,
	CodeTimeout:        StatusTimeout,
	CodeNetError:       StatusNetError,
	CodeLameDelegation: StatusLameDelegation,
}

var statusToCode = map[string]StatusCode{
	StatusHasPTR:         CodeHasPTR,
	StatusNoErrorEmpty:   CodeNoErrorEmpty,
	StatusNXDomain:       CodeNXDomain,
	StatusServFail:       CodeServFail,
	StatusRefused:        CodeRefused,
	StatusTimeout:        CodeTimeout,
	StatusNetError:       CodeNetError,
	StatusLameDelegation: CodeLameDelegation,
}

// Code maps a status string to its compact code (defaults to net_error).
func Code(status string) StatusCode {
	if c, ok := statusToCode[status]; ok {
		return c
	}
	return CodeNetError
}

// StatusString maps a compact code back to its string (defaults to net_error).
func StatusString(c StatusCode) string {
	if int(c) < NumStatusCodes {
		return codeToStatus[c]
	}
	return StatusNetError
}

// HasPTR reports whether a status represents a successful hit.
func HasPTR(status string) bool { return status == StatusHasPTR }

// Record is one crawled IPv4 address and its reverse-DNS result.
// It is the in-memory / JSONL shape; the compact store keeps a subset.
type Record struct {
	IP        string   `json:"ip"`
	IPInt     uint32   `json:"ip_int"`
	Status    string   `json:"status"`
	PTR       []string `json:"ptr,omitempty"`
	FCrDNS    *bool    `json:"fcrdns_match,omitempty"` // nil when FCrDNS not checked
	Resolver  string   `json:"resolver"`
	Attempts  int      `json:"attempts"`
	LatencyMs int64    `json:"latency_ms"`
	Rcode     string   `json:"rcode,omitempty"` // raw DNS rcode name, when applicable
	Error     string   `json:"error,omitempty"`
	TS        string   `json:"ts"`
}
