//go:build linux && with_wireguard && with_gvisor

package smartharness_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	mDNS "github.com/miekg/dns"
)

var harnessCertificate tls.Certificate

// Smart's net/http transport uses the Go system pool, not the box certificate store.
func TestMain(m *testing.M) {
	code, err := harnessMain(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

func harnessMain(m *testing.M) (int, error) {
	dir, err := os.MkdirTemp("", "smart-harness-ca-")
	if err != nil {
		return 1, err
	}
	defer os.RemoveAll(dir)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return 1, err
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Smart harness CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		return 1, err
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, DNSNames: []string{"fixture.invalid"}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
	for _, address := range []string{"1.1.1.1", "1.0.0.1", "8.8.8.8", "9.9.9.9"} {
		leaf.IPAddresses = append(leaf.IPAddresses, net.ParseIP(address))
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
	if err != nil {
		return 1, err
	}
	harnessCertificate = tls.Certificate{Certificate: [][]byte{leafDER, caDER}, PrivateKey: key}
	path := filepath.Join(dir, "ca.pem")
	if err = os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		return 1, err
	}
	if err = os.Setenv("SSL_CERT_FILE", path); err != nil {
		return 1, err
	}
	if err = os.Setenv("SSL_CERT_DIR", dir); err != nil {
		return 1, err
	}
	return m.Run(), nil
}

type probeFixtures struct {
	httpsPort          uint16
	dnsPort            uint16
	stunPort           uint16
	quicPort           uint16
	https              atomic.Int64
	dns                atomic.Int64
	stun               atomic.Int64
	quic               atomic.Int64
	downloads          atomic.Int64
	bulkBytes          atomic.Int64
	bulkRate           atomic.Int64
	downloadRate       atomic.Int64
	downloadBurst      atomic.Int64
	bulkRateOnDownload atomic.Int64
}

func newProbeFixtures(t *testing.T, overlay netip.Addr) *probeFixtures {
	t.Helper()
	f := new(probeFixtures)
	https := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.https.Add(1)
		switch r.URL.Path {
		case "/cdn-cgi/trace":
			fmt.Fprintf(w, "h=%s\n", r.Host)
		case "/":
			if r.Host == "8.8.8.8" {
				w.Header().Set("Location", "https://dns.google/")
				w.WriteHeader(http.StatusFound)
			} else {
				w.WriteHeader(http.StatusOK)
			}
		case "/download":
			size, err := strconv.ParseInt(r.URL.Query().Get("bytes"), 10, 64)
			if err != nil || size < 0 || size > 25_000_000 {
				http.Error(w, "invalid download size", http.StatusBadRequest)
				return
			}
			f.downloads.Add(1)
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			if rate := f.bulkRateOnDownload.Load(); rate > 0 {
				f.bulkRate.Store(rate)
			}
			writeFixturePayload(r.Context(), w, size, &f.downloadRate, f.downloadBurst.Load(), nil)
		case "/bulk":
			writeFixturePayload(r.Context(), w, -1, &f.bulkRate, 0, &f.bulkBytes)
		case "/upload":
			_, _ = io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, 1_000_000))
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	https.TLS = &tls.Config{Certificates: []tls.Certificate{harnessCertificate}, MinVersion: tls.VersionTLS12}
	https.StartTLS()
	t.Cleanup(https.Close)
	f.httpsPort = uint16(https.Listener.Addr().(*net.TCPAddr).Port)

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started, finished := make(chan struct{}), make(chan error, 1)
	dns := &mDNS.Server{Listener: listener, NotifyStartedFunc: func() { close(started) }, Handler: mDNS.HandlerFunc(func(w mDNS.ResponseWriter, request *mDNS.Msg) {
		f.dns.Add(1)
		response := new(mDNS.Msg).SetReply(request)
		for _, question := range request.Question {
			if question.Qtype == mDNS.TypeA {
				response.Answer = append(response.Answer, &mDNS.A{Hdr: mDNS.RR_Header{Name: question.Name, Rrtype: mDNS.TypeA, Class: mDNS.ClassINET, Ttl: 60}, A: net.IP(overlay.AsSlice())})
			}
		}
		_ = w.WriteMsg(response)
	})}
	go func() { finished <- dns.ActivateAndServe() }()
	<-started
	t.Cleanup(func() {
		if err := dns.Shutdown(); err != nil {
			t.Error(err)
		}
		if err := <-finished; err != nil {
			t.Error(err)
		}
	})
	f.dnsPort = uint16(listener.Addr().(*net.TCPAddr).Port)
	f.stunPort = fixtureUDP(t, func(request []byte, source *net.UDPAddr) []byte {
		if len(request) != 20 || binary.BigEndian.Uint16(request[:2]) != 1 || binary.BigEndian.Uint32(request[4:8]) != 0x2112a442 {
			return nil
		}
		f.stun.Add(1)
		response := make([]byte, 32)
		copy(response, request)
		binary.BigEndian.PutUint16(response[:2], 0x0101)
		binary.BigEndian.PutUint16(response[2:4], 12)
		binary.BigEndian.PutUint16(response[20:22], 0x0020)
		binary.BigEndian.PutUint16(response[22:24], 8)
		response[25] = 1
		binary.BigEndian.PutUint16(response[26:28], uint16(source.Port)^0x2112)
		ip := source.IP.To4()
		for i := range 4 {
			response[28+i] = ip[i] ^ request[4+i]
		}
		return response
	})
	f.quicPort = fixtureUDP(t, func(request []byte, _ *net.UDPAddr) []byte {
		if len(request) < 1200 || request[0]&0x80 == 0 || binary.BigEndian.Uint32(request[1:5]) != 0x0a0a0a0a || request[5] != 8 || request[14] != 8 {
			return nil
		}
		f.quic.Add(1)
		response := make([]byte, 27)
		response[0], response[5], response[14] = 0x80, 8, 8
		copy(response[6:14], request[15:23])
		copy(response[15:23], request[6:14])
		binary.BigEndian.PutUint32(response[23:], 1)
		return response
	})
	return f
}

func writeFixturePayload(ctx context.Context, w http.ResponseWriter, remaining int64, rate *atomic.Int64, burst int64, counter *atomic.Int64) {
	var buffer [16 * 1024]byte
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	if !timer.Stop() {
		<-timer.C
	}
	for remaining != 0 {
		size := int64(len(buffer))
		if remaining > 0 {
			size = min(size, remaining)
		}
		if burst > 0 {
			size = min(size, burst)
			burst -= size
		} else if bytesPerSecond := rate.Load(); bytesPerSecond > 0 {
			timer.Reset(time.Duration(float64(size) / float64(bytesPerSecond) * float64(time.Second)))
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
		}
		n, err := w.Write(buffer[:size])
		if counter != nil {
			counter.Add(int64(n))
		}
		if err != nil || ctx.Err() != nil {
			return
		}
		if remaining > 0 {
			remaining -= int64(n)
		}
	}
}

func fixtureUDP(t *testing.T, reply func([]byte, *net.UDPAddr) []byte) uint16 {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		var buffer [2048]byte
		for {
			n, from, readErr := conn.ReadFromUDP(buffer[:])
			if readErr != nil {
				return
			}
			if response := reply(buffer[:n], from); response != nil {
				_, _ = conn.WriteToUDP(response, from)
			}
		}
	}()
	t.Cleanup(func() { _ = conn.Close(); <-done })
	return uint16(conn.LocalAddr().(*net.UDPAddr).Port)
}
