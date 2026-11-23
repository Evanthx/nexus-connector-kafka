package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Mechanism is how Kafka checks the username and password (the SASL mechanism).
type Mechanism string

const (
	ScramSHA256 Mechanism = "SCRAM-SHA-256"
	ScramSHA512 Mechanism = "SCRAM-SHA-512"
	// Plain sends the password as it is, so use it only over TLS. Confluent Cloud and Azure Event
	// Hubs use it.
	Plain Mechanism = "PLAIN"
)

// Mechanisms are the mechanisms the programs support, the default first.
var Mechanisms = []Mechanism{ScramSHA256, ScramSHA512, Plain}

// TLSFiles are the files for a TLS connection to Kafka. Every path is optional: with no CA, the
// system's CAs check the brokers' certificates; with no client certificate, the client doesn't
// show one. ClientCert and ClientKey go together, for mTLS.
type TLSFiles struct {
	ServerCA   string
	ClientCert string
	ClientKey  string
}

// Load reads the files into a TLS configuration, reporting every problem at once.
func (f TLSFiles) Load() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	var errs []error
	if f.ServerCA != "" {
		pool, err := loadCA(f.ServerCA)
		if err != nil {
			errs = append(errs, err)
		}
		cfg.RootCAs = pool
	}
	switch {
	case f.ClientCert != "" && f.ClientKey != "":
		cert, err := tls.LoadX509KeyPair(f.ClientCert, f.ClientKey)
		if err != nil {
			errs = append(errs, fmt.Errorf("load the client certificate %s and its key %s: %w", f.ClientCert, f.ClientKey, err))
		}
		cfg.Certificates = []tls.Certificate{cert}
	case f.ClientCert != "":
		errs = append(errs, errors.New("a client certificate needs its private key too"))
	case f.ClientKey != "":
		errs = append(errs, errors.New("a client private key needs its certificate too"))
	default: // no client certificate
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return cfg, nil
}

func loadCA(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read the server CA certificate: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("the server CA certificate %s has no PEM certificate in it", path)
	}
	return pool, nil
}

// authOpts are the client options for TLS and the login in c.
func (c Config) authOpts() []kgo.Opt {
	var opts []kgo.Opt
	if c.TLS != nil {
		// kgo clones it and sets ServerName to each broker's host.
		opts = append(opts, kgo.DialTLSConfig(c.TLS))
	}
	if c.Username != "" {
		opts = append(opts, kgo.SASL(c.saslMechanism()))
	}
	return opts
}

// mechanism is c's Mechanism, ScramSHA256 if it isn't set.
func (c Config) mechanism() Mechanism {
	if c.Mechanism == "" {
		return ScramSHA256
	}
	return c.Mechanism
}

func (c Config) saslMechanism() sasl.Mechanism {
	switch c.mechanism() {
	case ScramSHA512:
		return scram.Auth{User: c.Username, Pass: c.Password}.AsSha512Mechanism()
	case Plain:
		return plain.Auth{User: c.Username, Pass: c.Password}.AsMechanism()
	default:
		return scram.Auth{User: c.Username, Pass: c.Password}.AsSha256Mechanism()
	}
}
