package kafka

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/test/testcerts"
	"github.com/twmb/franz-go/pkg/kfake"
)

// identifyWithin is Identify with a short deadline, since kfake closes the connection on a refused
// login and the client retries until its context ends.
func identifyWithin(cfg Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := Identify(ctx, cfg, testTopic)
	return err
}

// Each mechanism logs in with its user, and a wrong password or the next mechanism is refused.
func TestLogin(t *testing.T) {
	for i, m := range Mechanisms {
		t.Run(string(m), func(t *testing.T) {
			c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, testTopic),
				kfake.EnableSASL(), kfake.Superuser(string(m), "connector", "secret"))
			require.NoError(t, err)
			t.Cleanup(c.Close)
			cfg := Config{Brokers: c.ListenAddrs(), Username: "connector", Password: "secret", Mechanism: m}
			require.NoError(t, identifyWithin(cfg))

			wrong := cfg
			wrong.Password = "wrong"
			require.Error(t, identifyWithin(wrong))

			cfg.Mechanism = Mechanisms[(i+1)%len(Mechanisms)]
			require.Error(t, identifyWithin(cfg), "logging in with %s", cfg.Mechanism)
		})
	}
}

// An empty mechanism is SCRAM-SHA-256, so configurations from before KAFKA_SASL_MECHANISM still log in.
func TestLogin_DefaultIsScramSHA256(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, testTopic),
		kfake.EnableSASL(), kfake.Superuser(string(ScramSHA256), "connector", "secret"))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	require.NoError(t, identifyWithin(Config{Brokers: c.ListenAddrs(), Username: "connector", Password: "secret"}))
}

func TestTLS(t *testing.T) {
	certs := testcerts.New(t)
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, testTopic), kfake.TLS(certs.Server(t, false)))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	cfg := Config{Brokers: c.ListenAddrs()}

	tlsCfg, err := TLSFiles{ServerCA: certs.CA}.Load()
	require.NoError(t, err)
	cfg.TLS = tlsCfg
	require.NoError(t, identifyWithin(cfg))

	system, err := TLSFiles{}.Load()
	require.NoError(t, err)
	cfg.TLS = system
	require.ErrorContains(t, identifyWithin(cfg), "certificate", "the system's CAs don't trust the test CA")

	cfg.TLS = nil
	require.Error(t, identifyWithin(cfg), "plaintext to a TLS listener")
}

// With mTLS the broker refuses a client without a certificate its CA signed.
func TestMTLS(t *testing.T) {
	certs := testcerts.New(t)
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, testTopic), kfake.TLS(certs.Server(t, true)))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	cfg := Config{Brokers: c.ListenAddrs()}

	withCert, err := TLSFiles{ServerCA: certs.CA, ClientCert: certs.ClientCert, ClientKey: certs.ClientKey}.Load()
	require.NoError(t, err)
	cfg.TLS = withCert
	require.NoError(t, identifyWithin(cfg))

	noCert, err := TLSFiles{ServerCA: certs.CA}.Load()
	require.NoError(t, err)
	cfg.TLS = noCert
	require.Error(t, identifyWithin(cfg))

	other := testcerts.New(t)
	otherCA, err := TLSFiles{ServerCA: certs.CA, ClientCert: other.ClientCert, ClientKey: other.ClientKey}.Load()
	require.NoError(t, err)
	cfg.TLS = otherCA
	require.Error(t, identifyWithin(cfg), "a client certificate from another CA")
}

// TLS and a login together, as hosted Kafka services use them.
func TestTLSWithLogin(t *testing.T) {
	certs := testcerts.New(t)
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, testTopic), kfake.TLS(certs.Server(t, false)),
		kfake.EnableSASL(), kfake.Superuser(string(Plain), "connector", "secret"))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	tlsCfg, err := TLSFiles{ServerCA: certs.CA}.Load()
	require.NoError(t, err)
	cfg := Config{Brokers: c.ListenAddrs(), Username: "connector", Password: "secret", Mechanism: Plain, TLS: tlsCfg}
	require.NoError(t, identifyWithin(cfg))
}

func TestTLSFiles_Load(t *testing.T) {
	certs := testcerts.New(t)
	cfg, err := TLSFiles{}.Load()
	require.NoError(t, err)
	require.Nil(t, cfg.RootCAs, "the system's CAs")
	require.Empty(t, cfg.Certificates)

	_, err = TLSFiles{ClientCert: certs.ClientCert}.Load()
	require.EqualError(t, err, "a client certificate needs its private key too")
	_, err = TLSFiles{ClientKey: certs.ClientKey}.Load()
	require.EqualError(t, err, "a client private key needs its certificate too")

	notPEM := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(notPEM, []byte("not a certificate"), 0o600))
	_, err = TLSFiles{ServerCA: notPEM, ClientCert: certs.ClientCert, ClientKey: certs.ServerKey}.Load()
	require.ErrorContains(t, err, "has no PEM certificate in it")
	require.ErrorContains(t, err, "load the client certificate", "every problem at once")

	_, err = TLSFiles{ServerCA: filepath.Join(t.TempDir(), "missing.pem")}.Load()
	require.ErrorContains(t, err, "read the server CA certificate")
}
