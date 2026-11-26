package setup

import (
	"log/slog"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-kafka/test/testcerts"
)

func TestLoadKafka(t *testing.T) {
	cfg, err := LoadKafka(settings.Map{"KAFKA_BROKERS": "a:9092,b:9092"})
	require.NoError(t, err)
	require.Equal(t, []string{"a:9092", "b:9092"}, cfg.Brokers)

	_, err = LoadKafka(settings.Map{})
	require.EqualError(t, err, "set KAFKA_BROKERS: the Kafka brokers, as comma-separated host:port")
}

func TestLoadKafka_ClientLogLevel(t *testing.T) {
	env := settings.Map{"KAFKA_BROKERS": "b:9092"}
	cfg, err := LoadKafka(env)
	require.NoError(t, err)
	require.Equal(t, slog.LevelWarn, cfg.ClientLogLevel.Level(), "unset is warn")

	env["KAFKA_CLIENT_LOG_LEVEL"] = "debug"
	cfg, err = LoadKafka(env)
	require.NoError(t, err)
	require.Equal(t, slog.LevelDebug, cfg.ClientLogLevel.Level())

	env["KAFKA_CLIENT_LOG_LEVEL"] = "loud"
	_, err = LoadKafka(env)
	require.EqualError(t, err, `KAFKA_CLIENT_LOG_LEVEL must be error, warn, info or debug, not "loud"`)
}

func TestLoadKafka_Mechanism(t *testing.T) {
	env := settings.Map{"KAFKA_BROKERS": "b:9092", "KAFKA_USERNAME": "u", "KAFKA_PASSWORD": "p"}
	cfg, err := LoadKafka(env)
	require.NoError(t, err)
	require.Equal(t, kafka.ScramSHA256, cfg.Mechanism, "unset is SCRAM-SHA-256")

	env["KAFKA_SASL_MECHANISM"] = "plain"
	cfg, err = LoadKafka(env)
	require.NoError(t, err)
	require.Equal(t, kafka.Plain, cfg.Mechanism, "any case")

	env["KAFKA_SASL_MECHANISM"] = "GSSAPI"
	_, err = LoadKafka(env)
	require.EqualError(t, err, `KAFKA_SASL_MECHANISM must be SCRAM-SHA-256, SCRAM-SHA-512 or PLAIN, not "GSSAPI"`)

	delete(env, "KAFKA_USERNAME")
	env["KAFKA_SASL_MECHANISM"] = "SCRAM-SHA-512"
	_, err = LoadKafka(env)
	require.ErrorContains(t, err, "KAFKA_SASL_MECHANISM is set but KAFKA_USERNAME isn't")
}

func TestLoadKafka_TLS(t *testing.T) {
	certs := testcerts.New(t)
	load := func(extra settings.Map) (kafka.Config, error) {
		env := settings.Map{"KAFKA_BROKERS": "b:9092"}
		maps.Copy(env, extra)
		return LoadKafka(env)
	}

	cfg, err := load(nil)
	require.NoError(t, err)
	require.Nil(t, cfg.TLS, "off by default")

	cfg, err = load(settings.Map{"KAFKA_TLS": "true"})
	require.NoError(t, err)
	require.NotNil(t, cfg.TLS)
	require.Nil(t, cfg.TLS.RootCAs, "the system's CAs")

	cfg, err = load(settings.Map{"KAFKA_TLS_SERVER_CA_CERT_PATH": certs.CA})
	require.NoError(t, err)
	require.NotNil(t, cfg.TLS.RootCAs, "a path turns TLS on")

	cfg, err = load(settings.Map{"KAFKA_TLS_CLIENT_CERT_PATH": certs.ClientCert, "KAFKA_TLS_CLIENT_KEY_PATH": certs.ClientKey})
	require.NoError(t, err)
	require.Len(t, cfg.TLS.Certificates, 1)

	_, err = load(settings.Map{"KAFKA_TLS_CLIENT_CERT_PATH": certs.ClientCert})
	require.EqualError(t, err, "set KAFKA_TLS_CLIENT_CERT_PATH and KAFKA_TLS_CLIENT_KEY_PATH together, for mTLS")

	_, err = load(settings.Map{"KAFKA_TLS": "false", "KAFKA_TLS_SERVER_CA_CERT_PATH": certs.CA})
	require.ErrorContains(t, err, "KAFKA_TLS is false but a KAFKA_TLS_*_PATH is set")

	_, err = load(settings.Map{"KAFKA_TLS_SERVER_CA_CERT_PATH": certs.ClientKey})
	require.ErrorContains(t, err, "has no PEM certificate in it")

	_, err = load(settings.Map{"KAFKA_TLS": "maybe"})
	require.EqualError(t, err, `KAFKA_TLS must be true or false, not "maybe"`)
}

// Both programs check the Kafka rules at startup, not only LoadKafka.
func TestPrograms_HaveKafkaRules(t *testing.T) {
	env := settings.Map{"KAFKA_BROKERS": "b:9092", "KAFKA_SASL_MECHANISM": "PLAIN",
		"KAFKA_TOPICS": "t", "INBOUND_CONFIG": `{"routes": [{"topic": "t", "endpoint": "e", "deleteDeadLetters": true}]}`}
	for _, p := range []*settings.Program{Inbound, Outbound} {
		_, err := p.Load(env)
		require.ErrorContains(t, err, "KAFKA_SASL_MECHANISM is set but KAFKA_USERNAME isn't", p.Name)
	}
}
