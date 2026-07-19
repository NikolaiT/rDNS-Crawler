// Package resolver performs PTR (reverse) and A (forward) DNS lookups over UDP
// against a rotating pool of recursive resolvers. It is safe for concurrent use
// from many goroutines.
package resolver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/miekg/dns"

	"rdns-crawler/internal/model"
)

// DefaultResolvers is a spread of fast, well-known public recursive resolvers.
// Rotating across them raises the effective query rate before any single
// provider starts rate-limiting us.
var DefaultResolvers = []string{
	"1.1.1.1:53",         // Cloudflare
	"1.0.0.1:53",         // Cloudflare
	"8.8.8.8:53",         // Google
	"8.8.4.4:53",         // Google
	"9.9.9.9:53",         // Quad9
	"149.112.112.112:53", // Quad9
	"208.67.222.222:53",  // OpenDNS
	"208.67.220.220:53",  // OpenDNS
}

// PTRResult is the outcome of a single reverse lookup.
type PTRResult struct {
	Names    []string
	Resolver string
	Attempts int
	Latency  time.Duration
	Status   string // one of model.Status*
	Rcode    string // raw DNS rcode name (e.g. NXDOMAIN, SERVFAIL), when we got a response
	Err      error
}

// Resolver holds the resolver pool and a reusable DNS client.
type Resolver struct {
	servers []string
	client  *dns.Client
	retries int
	counter uint64
}

// New builds a Resolver. Bare "ip" entries are given the default :53 port.
func New(servers []string, timeout time.Duration, retries int) (*Resolver, error) {
	norm := make([]string, 0, len(servers))
	for _, s := range servers {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if !strings.Contains(s, ":") {
			s += ":53"
		}
		norm = append(norm, s)
	}
	if len(norm) == 0 {
		return nil, errors.New("no resolvers configured")
	}
	if retries < 0 {
		retries = 0
	}
	return &Resolver{
		servers: norm,
		client:  &dns.Client{Net: "udp", Timeout: timeout},
		retries: retries,
	}, nil
}

// Servers returns the configured resolver pool (for logging).
func (r *Resolver) Servers() []string { return r.servers }

// pick returns a resolver from the pool, rotating on every call and applying an
// extra offset so retries land on a different server than the first attempt.
func (r *Resolver) pick(offset int) string {
	i := atomic.AddUint64(&r.counter, 1)
	return r.servers[(i+uint64(offset))%uint64(len(r.servers))]
}

// query runs a single-question exchange with retries across rotating servers.
//
// A definitive answer (NOERROR or NXDOMAIN) is returned immediately. Soft
// failures (SERVFAIL/REFUSED/…) are retried on other resolvers — a different
// recursive server may reach the authoritative side — but if they persist we
// still return the last response (non-nil, err==nil) so the caller can record
// the precise rcode. Only transport failures (timeout/network) return err!=nil
// with a nil response.
func (r *Resolver) query(ctx context.Context, name string, qtype uint16) (*dns.Msg, string, int, error) {
	m := new(dns.Msg)
	m.SetQuestion(name, qtype)
	m.RecursionDesired = true

	var (
		lastErr  error
		lastResp *dns.Msg
		server   string
		attempts int
	)
	for a := 0; a <= r.retries; a++ {
		attempts++
		server = r.pick(a)
		resp, _, err := r.client.ExchangeContext(ctx, m, server)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.Rcode == dns.RcodeSuccess || resp.Rcode == dns.RcodeNameError {
			return resp, server, attempts, nil
		}
		// Soft failure: remember it, try another resolver.
		lastResp = resp
		lastErr = fmt.Errorf("rcode=%s", dns.RcodeToString[resp.Rcode])
	}
	if lastResp != nil {
		return lastResp, server, attempts, nil
	}
	return nil, server, attempts, lastErr
}

// LookupPTR resolves the reverse-DNS (PTR) records for an IPv4 string.
func (r *Resolver) LookupPTR(ctx context.Context, ip string) PTRResult {
	start := time.Now()
	arpa, err := dns.ReverseAddr(ip)
	if err != nil {
		return PTRResult{Status: model.StatusError, Err: err, Latency: time.Since(start)}
	}

	resp, server, attempts, err := r.query(ctx, arpa, dns.TypePTR)
	res := PTRResult{Resolver: server, Attempts: attempts, Latency: time.Since(start)}
	if err != nil {
		// Transport-level failure (no usable response).
		res.Status = classifyErr(err)
		res.Err = err
		return res
	}

	res.Rcode = dns.RcodeToString[resp.Rcode]
	switch resp.Rcode {
	case dns.RcodeSuccess:
		for _, ans := range resp.Answer {
			if ptr, ok := ans.(*dns.PTR); ok {
				res.Names = append(res.Names, strings.TrimSuffix(ptr.Ptr, "."))
			}
		}
		if len(res.Names) == 0 {
			res.Status = model.StatusNoErrorEmpty
		} else {
			res.Status = model.StatusHasPTR
		}
	case dns.RcodeNameError:
		res.Status = model.StatusNXDomain
	case dns.RcodeServerFailure:
		res.Status = model.StatusServFail
	case dns.RcodeRefused, dns.RcodeNotImplemented:
		res.Status = model.StatusRefused
	default:
		res.Status = model.StatusNetError
	}
	return res
}

// LookupA resolves the A records of a hostname (used for forward-confirmation).
func (r *Resolver) LookupA(ctx context.Context, host string) ([]net.IP, error) {
	resp, _, _, err := r.query(ctx, dns.Fqdn(host), dns.TypeA)
	if err != nil {
		return nil, err
	}
	var ips []net.IP
	for _, ans := range resp.Answer {
		if a, ok := ans.(*dns.A); ok {
			ips = append(ips, a.A)
		}
	}
	return ips, nil
}

// classifyErr maps a transport error to a timeout vs. generic error status.
func classifyErr(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return model.StatusTimeout
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return model.StatusTimeout
	}
	if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "i/o timeout") {
		return model.StatusTimeout
	}
	return model.StatusNetError
}
