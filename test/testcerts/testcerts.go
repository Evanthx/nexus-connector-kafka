// Package testcerts makes a CA, a server certificate and a client certificate for TLS and mTLS
// tests, written as PEM files to a test's temporary directory. Test only.
package testcerts

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Certs are the PEM files New wrote, and the CA's pool.
type Certs struct {
	CA                    string // the CA's certificate
	ServerCert, ServerKey string // for 127.0.0.1 and localhost
	ClientCert, ClientKey string
	Pool                  *x509.CertPool
}

// New makes a new CA and a server and client certificate it signs, in t.TempDir().
func New(t *testing.T) Certs {
	t.Helper()
	dir := t.TempDir()
	caKey := newKey(t)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	c := Certs{CA: writePEM(t, dir, "ca.pem", "CERTIFICATE", caDER), Pool: x509.NewCertPool()}
	c.Pool.AddCert(ca)
	c.ServerCert, c.ServerKey = issue(t, dir, "server", ca, caKey, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	c.ClientCert, c.ClientKey = issue(t, dir, "client", ca, caKey, &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "connector"},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	return c
}

// Server is a TLS configuration for a server with the server certificate. With requireClient, it
// also requires a client certificate the CA signed (mTLS).
func (c Certs) Server(t *testing.T, requireClient bool) *tls.Config {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(c.ServerCert, c.ServerKey)
	require.NoError(t, err)
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if requireClient {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = c.Pool
	}
	return cfg
}

func issue(t *testing.T, dir, name string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, tmpl *x509.Certificate) (certPath, keyPath string) {
	t.Helper()
	key := newKey(t)
	tmpl.NotBefore, tmpl.NotAfter = ca.NotBefore, ca.NotAfter
	tmpl.KeyUsage = x509.KeyUsageDigitalSignature
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	return writePEM(t, dir, name+".pem", "CERTIFICATE", der), writePEM(t, dir, name+"-key.pem", "PRIVATE KEY", keyDER)
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return key
}

func writePEM(t *testing.T, dir, name, typ string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600))
	return path
}
