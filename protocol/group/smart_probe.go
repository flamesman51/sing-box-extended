package group

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	D "github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/common/networkquality"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	mDNS "github.com/miekg/dns"
)

var errSmartResolve = errors.New("smart probe resolution failed")

const smartProbeBodyLimit = 4096

func smartTCPProbe(ctx context.Context, dialer N.Dialer, operator int) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	operator = ((operator % 6) + 6) % 6
	address := "1.1.1.1"
	path := "/cdn-cgi/trace"
	switch operator % 3 {
	case 0:
		if operator >= 3 {
			address = "1.0.0.1"
		}
	case 1:
		address, path = "8.8.8.8", "/"
	case 2:
		address, path = "9.9.9.9", "/"
	}
	client := smartProbeHTTPClient(dialer)
	defer client.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+address+path, nil)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	response, err := client.Do(request)
	if err != nil {
		return 0, smartProbeContextError(ctx, err)
	}
	defer response.Body.Close()
	var body []byte
	if operator%3 == 0 {
		body, err = io.ReadAll(io.LimitReader(response.Body, smartProbeBodyLimit+1))
		if err != nil {
			return 0, smartProbeContextError(ctx, err)
		}
		if len(body) > smartProbeBodyLimit {
			return 0, smartProbeContextError(ctx, errors.New("smart probe response exceeds body limit"))
		}
	}
	if err = smartValidateTCPResponse(operator%3, address, response, body); err != nil {
		return 0, smartProbeContextError(ctx, err)
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

func smartValidateTCPResponse(operator int, address string, response *http.Response, body []byte) error {
	switch operator {
	case 0:
		if response.StatusCode != http.StatusOK {
			return errors.New("unexpected Cloudflare probe response")
		}
		for line := range bytes.SplitSeq(body, []byte{'\n'}) {
			if bytes.Equal(line, []byte("h="+address)) {
				return nil
			}
		}
		return errors.New("unexpected Cloudflare probe response")
	case 1:
		if response.StatusCode != http.StatusFound || !strings.Contains(response.Header.Get("Location"), "dns.google") {
			return errors.New("unexpected Google probe response")
		}
	case 2:
	default:
		return errors.New("unknown smart probe operator")
	}
	return nil
}

func smartProbeHTTPClient(dialer N.Dialer) *http.Client {
	client := networkquality.NewHTTPClient(dialer)
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	transport := client.Transport.(*http.Transport)
	transport.DisableCompression = true
	transport.MaxResponseHeaderBytes = 16 * 1024
	transport.MaxConnsPerHost = 1
	transport.MaxIdleConnsPerHost = 1
	transport.MaxIdleConns = 1
	transport.ForceAttemptHTTP2 = false
	transport.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
	return client
}

func smartProbeContextError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Each endpoint gets a bounded part of the remaining budget, leaving time for corroboration.
func smartProbeAttemptContext(ctx context.Context, remaining int) (context.Context, context.CancelFunc) {
	timeout := 3 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, max(time.Millisecond, time.Until(deadline)/time.Duration(remaining)))
	}
	return context.WithTimeout(ctx, timeout)
}

func smartResolveMember(ctx context.Context, dialer N.Dialer, host string) ([]netip.Addr, error) {
	if address, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{address}, nil
	}
	servers := [...]string{"1.1.1.1:53", "8.8.8.8:53", "9.9.9.9:53"}
	var failures []error
	for index, server := range servers {
		attempt, cancel := smartProbeAttemptContext(ctx, len(servers)-index)
		conn, err := dialer.DialContext(attempt, N.NetworkTCP, M.ParseSocksaddr(server))
		var addresses []netip.Addr
		if err == nil {
			stop := context.AfterFunc(attempt, func() { _ = conn.Close() })
			client := &mDNS.Client{Net: "tcp"}
			for _, qtype := range []uint16{mDNS.TypeA, mDNS.TypeAAAA} {
				message := new(mDNS.Msg).SetQuestion(mDNS.Fqdn(host), qtype)
				response, _, exchangeErr := client.ExchangeWithConnContext(attempt, message, &mDNS.Conn{Conn: conn})
				if exchangeErr != nil {
					err = exchangeErr
					break
				}
				resolved, resolveErr := smartDNSAddresses(message, response)
				if resolveErr != nil {
					err = resolveErr
					break
				}
				addresses = append(addresses, resolved...)
			}
			stop()
			_ = conn.Close()
		}
		cancel()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if len(addresses) != 0 {
			return addresses, nil
		}
		if err == nil {
			err = errors.New("empty DNS answer")
		}
		failures = append(failures, err)
	}
	return nil, fmt.Errorf("%w: %w", errSmartResolve, errors.Join(failures...))
}

func smartDNSAddresses(request, response *mDNS.Msg) ([]netip.Addr, error) {
	if response == nil || !response.Response || response.Id != request.Id || response.Truncated || response.Rcode != mDNS.RcodeSuccess || len(response.Question) != 1 || response.Question[0] != request.Question[0] {
		return nil, errors.New("invalid smart DNS response")
	}
	var addresses []netip.Addr
	for _, record := range response.Answer {
		var raw net.IP
		switch record := record.(type) {
		case *mDNS.A:
			raw = record.A
		case *mDNS.AAAA:
			raw = record.AAAA
		}
		if address, ok := netip.AddrFromSlice(raw); ok {
			addresses = append(addresses, address.Unmap())
		}
	}
	return addresses, nil
}

func smartUDPProbe(ctx context.Context, dialer N.Dialer, quic bool) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	hosts := [...]string{"stun.cloudflare.com", "stun.l.google.com"}
	ports := [...]uint16{3478, 19302}
	if quic {
		hosts = [...]string{"cloudflare-quic.com", "www.google.com"}
		ports = [...]uint16{443, 443}
	}
	var failures []error
	resolveFailed := false
	for index, host := range hosts {
		attempt, cancel := smartProbeAttemptContext(ctx, len(hosts)-index)
		addresses, err := smartResolveMember(attempt, dialer, host)
		if err == nil {
			for _, address := range addresses {
				var rtt time.Duration
				rtt, err = smartUDPExchange(attempt, dialer, M.SocksaddrFrom(address, ports[index]), quic)
				if err == nil {
					cancel()
					if ctx.Err() != nil {
						return 0, ctx.Err()
					}
					return rtt, nil
				}
				if attempt.Err() != nil {
					break
				}
			}
		} else {
			resolveFailed = true
		}
		cancel()
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		failures = append(failures, err)
	}
	if resolveFailed {
		return 0, fmt.Errorf("%w: %w", errSmartResolve, errors.Join(failures...))
	}
	return 0, errors.Join(failures...)
}

func smartUDPExchange(ctx context.Context, dialer N.Dialer, destination M.Socksaddr, quic bool) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	conn, err := dialer.ListenPacket(ctx, destination)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	size := 20
	if quic {
		size = 1200
	}
	request := make([]byte, size)
	if quic {
		if _, err = rand.Read(request); err != nil {
			return 0, err
		}
		request[0] = 0xc0
		binary.BigEndian.PutUint32(request[1:5], 0x0a0a0a0a)
		request[5], request[14] = 8, 8
	} else {
		request[1] = 1
		binary.BigEndian.PutUint32(request[4:8], 0x2112a442)
		if _, err = rand.Read(request[8:]); err != nil {
			return 0, err
		}
	}
	start := time.Now()
	if _, err = conn.WriteTo(request, destination); err != nil {
		return 0, smartProbeContextError(ctx, err)
	}
	var buffer [2048]byte
	for {
		n, source, readErr := conn.ReadFrom(buffer[:])
		if readErr != nil {
			return 0, smartProbeContextError(ctx, readErr)
		}
		if M.SocksaddrFromNet(source).Unwrap() != destination.Unwrap() {
			continue
		}
		valid := smartValidSTUN(buffer[:n], request)
		if quic {
			valid = smartValidVN(buffer[:n], request)
		}
		if valid {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			return time.Since(start), nil
		}
	}
}

func smartValidSTUN(packet, request []byte) bool {
	if len(packet) < 20 || len(request) < 20 || binary.BigEndian.Uint16(packet[:2]) != 0x0101 || binary.BigEndian.Uint32(packet[4:8]) != 0x2112a442 || !bytes.Equal(packet[8:20], request[8:20]) {
		return false
	}
	length := int(binary.BigEndian.Uint16(packet[2:4]))
	if length%4 != 0 || length != len(packet)-20 {
		return false
	}
	mapped := false
	for at := 20; at < len(packet); {
		if len(packet)-at < 4 {
			return false
		}
		kind, size := binary.BigEndian.Uint16(packet[at:at+2]), int(binary.BigEndian.Uint16(packet[at+2:at+4]))
		at += 4
		if size > len(packet)-at {
			return false
		}
		if kind == 0x0020 || kind == 0x0001 {
			if size < 4 || packet[at] != 0 {
				return false
			}
			family := packet[at+1]
			if family != 1 && family != 2 || family == 1 && size != 8 || family == 2 && size != 20 {
				return false
			}
			mapped = true
		}
		at += (size + 3) &^ 3
		if at > len(packet) {
			return false
		}
	}
	return mapped
}

func smartValidVN(packet, request []byte) bool {
	if len(packet) < 7 || len(request) < 23 || packet[0]&0x80 == 0 || binary.BigEndian.Uint32(packet[1:5]) != 0 {
		return false
	}
	dcidLength := int(packet[5])
	if dcidLength != 8 || len(packet) < 7+dcidLength {
		return false
	}
	if !bytes.Equal(packet[6:6+dcidLength], request[15:23]) {
		return false
	}
	at := 6 + dcidLength
	scidLength := int(packet[at])
	at++
	if scidLength != 8 || len(packet) < at+scidLength+4 || !bytes.Equal(packet[at:at+scidLength], request[6:14]) {
		return false
	}
	at += scidLength
	if (len(packet)-at)%4 != 0 {
		return false
	}
	for at < len(packet) {
		version := binary.BigEndian.Uint32(packet[at : at+4])
		if version == 0 || version == binary.BigEndian.Uint32(request[1:5]) {
			return false
		}
		at += 4
	}
	return true
}

func smartDirectProbe(ctx context.Context, country string, seed []byte) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	manager := service.FromContext[adapter.NetworkManager](ctx)
	if manager == nil || manager.DefaultNetworkInterface() == nil {
		return 0, errors.New("smart direct probe has no default interface")
	}
	dialOptions := option.DialerOptions{}
	dialOptions.BindInterface = manager.DefaultNetworkInterface().Name
	registry := service.FromContext[adapter.DNSTransportRegistry](ctx)
	if registry == nil {
		return 0, fmt.Errorf("%w: missing local DNS registry", errSmartResolve)
	}
	logger := log.NewNOPFactory().NewLogger("smart-direct")
	transport, err := registry.CreateDNSTransport(ctx, logger, "smart-direct-bootstrap", C.DNSTypeLocal, &option.LocalDNSServerOptions{RawLocalDNSServerOptions: option.RawLocalDNSServerOptions{DialerOptions: dialOptions}})
	if err != nil {
		return 0, fmt.Errorf("%w: %w", errSmartResolve, err)
	}
	scope := adapter.NewScope(ctx, logger)
	defer scope.Close()
	for _, stage := range adapter.ListStartStages {
		if err = transport.Start(stage, scope); err != nil {
			return 0, fmt.Errorf("%w: %w", errSmartResolve, err)
		}
	}
	dialer, err := D.NewDefault(ctx, dialOptions)
	if err != nil {
		return 0, err
	}
	resolver := func(ctx context.Context, host string) ([]netip.Addr, error) {
		var addresses []netip.Addr
		var failures []error
		for _, qtype := range []uint16{mDNS.TypeA, mDNS.TypeAAAA} {
			request := new(mDNS.Msg).SetQuestion(mDNS.Fqdn(host), qtype)
			response, exchangeErr := transport.Exchange(ctx, request)
			if exchangeErr == nil {
				var resolved []netip.Addr
				resolved, exchangeErr = smartDNSAddresses(request, response)
				addresses = append(addresses, resolved...)
			}
			failures = append(failures, exchangeErr)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if len(addresses) == 0 {
			return nil, errors.Join(errSmartResolve, errors.Join(failures...))
		}
		return addresses, nil
	}
	resolvedDialer := &smartResolvingDialer{Dialer: dialer, resolve: resolver}
	urls := smartDirectURLs(country)
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return 0, err
	}
	h := hmac.New(sha256.New, seed)
	_, _ = h.Write(nonce[:])
	startIndex := int(binary.BigEndian.Uint64(h.Sum(nil)[:8]) % uint64(len(urls)))
	var failures []error
	resolveFailed := false
	for index := range 2 {
		attempt, cancel := smartProbeAttemptContext(ctx, 2-index)
		client := smartProbeHTTPClient(resolvedDialer)
		request, requestErr := http.NewRequestWithContext(attempt, http.MethodGet, urls[(startIndex+index)%len(urls)], nil)
		start := time.Now()
		if requestErr == nil {
			var response *http.Response
			response, requestErr = client.Do(request)
			if requestErr == nil {
				_ = response.Body.Close()
			}
		}
		client.CloseIdleConnections()
		cancel()
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if requestErr == nil {
			return time.Since(start), nil
		}
		resolveFailed = resolveFailed || errors.Is(requestErr, errSmartResolve)
		failures = append(failures, requestErr)
	}
	if resolveFailed {
		return 0, fmt.Errorf("%w: %w", errSmartResolve, errors.Join(failures...))
	}
	return 0, errors.Join(failures...)
}

func smartDirectURLs(country string) []string {
	var hosts []string
	switch strings.ToUpper(country) {
	case "RU":
		hosts = []string{"ya.ru", "vk.com", "max.ru", "mail.ru", "dzen.ru", "ozon.ru", "wildberries.ru", "gosuslugi.ru"}
	case "CN":
		hosts = []string{"baidu.com", "qq.com", "taobao.com", "163.com"}
	case "IR":
		hosts = []string{"aparat.com", "digikala.com", "varzesh3.com"}
	default:
		return []string{"https://www.google.com/generate_204", "https://cp.cloudflare.com/generate_204", "https://detectportal.firefox.com/success.txt", "https://captive.apple.com/hotspot-detect.html"}
	}
	urls := make([]string, len(hosts))
	for index, host := range hosts {
		urls[index] = "https://" + host + "/"
	}
	return urls
}

type smartResolvingDialer struct {
	N.Dialer
	resolve func(context.Context, string) ([]netip.Addr, error)
}

func (d *smartResolvingDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if !destination.IsDomain() {
		return d.Dialer.DialContext(ctx, network, destination)
	}
	addresses, err := d.resolve(ctx, destination.Fqdn)
	if err != nil {
		return nil, err
	}
	var failures []error
	for index, address := range addresses {
		attempt, cancel := smartProbeAttemptContext(ctx, len(addresses)-index)
		conn, dialErr := d.Dialer.DialContext(attempt, network, M.SocksaddrFrom(address, destination.Port))
		cancel()
		if dialErr == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		failures = append(failures, dialErr)
	}
	return nil, errors.Join(failures...)
}

var errSmartMeasureCap = errors.New("smart measurement byte cap reached")

type smartMeasureMeter struct {
	mu              sync.Mutex
	remaining, used int64
	dials, maxDials int
}

func (m *smartMeasureMeter) allowance(size int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	size = int(min(int64(size), m.remaining))
	m.remaining -= int64(size)
	return size
}

func (m *smartMeasureMeter) account(allowed, actual int) {
	m.mu.Lock()
	m.remaining += int64(allowed - actual)
	m.used += int64(actual)
	m.mu.Unlock()
}

func (m *smartMeasureMeter) available() int64 { m.mu.Lock(); defer m.mu.Unlock(); return m.remaining }

type smartMeasureConn struct {
	net.Conn
	meter *smartMeasureMeter
}

func (c *smartMeasureConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	allowed := c.meter.allowance(len(p))
	if allowed == 0 {
		return 0, errSmartMeasureCap
	}
	n, err := c.Conn.Read(p[:allowed])
	c.meter.account(allowed, n)
	return n, err
}

func (c *smartMeasureConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	allowed := c.meter.allowance(len(p))
	if allowed == 0 {
		return 0, errSmartMeasureCap
	}
	n, err := c.Conn.Write(p[:allowed])
	c.meter.account(allowed, n)
	if err == nil && n < len(p) {
		err = errSmartMeasureCap
	}
	return n, err
}

type smartMeasureDialer struct {
	N.Dialer
	meter *smartMeasureMeter
}

func (d *smartMeasureDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if destination.Port != 53 {
		d.meter.mu.Lock()
		if d.meter.dials >= d.meter.maxDials {
			d.meter.mu.Unlock()
			return nil, errors.New("smart measurement connection limit reached")
		}
		d.meter.dials++
		d.meter.mu.Unlock()
	}
	conn, err := d.Dialer.DialContext(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	return &smartMeasureConn{Conn: conn, meter: d.meter}, nil
}

type smartZeroReader struct{}

func (smartZeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func smartMeasure(ctx context.Context, dialer N.Dialer, downloadURL, uploadURL string, capBytes int64, upload bool) (download, uploadMbps float64, bytes int64, err error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, 0, err
	}
	if capBytes < 1_100_000 {
		return 0, 0, 0, errSmartBudget
	}
	if !strings.Contains(downloadURL, "{bytes}") {
		return 0, 0, 0, errors.New("smart download URL lacks {bytes}")
	}
	meter := &smartMeasureMeter{remaining: capBytes, maxDials: 2}
	metered := &smartMeasureDialer{Dialer: dialer, meter: meter}
	resolved := &smartResolvingDialer{Dialer: metered, resolve: func(ctx context.Context, host string) ([]netip.Addr, error) {
		return smartResolveMember(ctx, metered, host)
	}}
	client := smartProbeHTTPClient(resolved)
	defer func() {
		client.CloseIdleConnections()
		meter.mu.Lock()
		bytes = (meter.used*11 + 9) / 10
		meter.mu.Unlock()
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	var uploadReserve int64
	if upload && capBytes >= 2_200_000 {
		uploadReserve = 1_050_000
	}
	payload := int64(1_000_000)
	first := true
	for {
		// Reserve outer overhead in the plan; the hard meter bounds framing overshoot to 10%.
		available := meter.available() - (capBytes - capBytes*10/11) - uploadReserve - 16_384
		if available <= 0 {
			break
		}
		if !first {
			payload = min(payload, available)
		}
		first = false
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, strings.ReplaceAll(downloadURL, "{bytes}", strconv.FormatInt(payload, 10)), nil)
		if requestErr != nil {
			return download, 0, 0, requestErr
		}
		response, requestErr := client.Do(request)
		if requestErr != nil {
			return download, 0, 0, requestErr
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			_ = response.Body.Close()
			return download, 0, 0, fmt.Errorf("smart download HTTP status %d", response.StatusCode)
		}
		start := time.Now()
		n, readErr := io.CopyN(io.Discard, response.Body, payload)
		elapsed := time.Since(start)
		_ = response.Body.Close()
		if n > 0 && elapsed > 0 {
			download = float64(n) * 8 / elapsed.Seconds() / 1_000_000
		}
		if readErr != nil {
			if errors.Is(readErr, errSmartMeasureCap) && download > 0 {
				break
			}
			return download, 0, 0, readErr
		}
		if elapsed >= 2*time.Second || payload >= available {
			break
		}
		payload *= 2
	}
	if uploadReserve > 0 && meter.available() >= 1_020_000 {
		meter.mu.Lock()
		meter.maxDials = meter.dials + 1
		meter.mu.Unlock()
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, io.LimitReader(smartZeroReader{}, 1_000_000))
		if requestErr != nil {
			return download, 0, 0, requestErr
		}
		request.ContentLength = 1_000_000
		request.Header.Set("Content-Type", "application/octet-stream")
		start := time.Now()
		response, requestErr := client.Do(request)
		if requestErr != nil {
			return download, 0, 0, requestErr
		}
		_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, smartProbeBodyLimit))
		_ = response.Body.Close()
		if readErr != nil {
			return download, 0, 0, readErr
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return download, 0, 0, fmt.Errorf("smart upload HTTP status %d", response.StatusCode)
		}
		uploadMbps = 8 / time.Since(start).Seconds()
	}
	return download, uploadMbps, 0, nil
}
