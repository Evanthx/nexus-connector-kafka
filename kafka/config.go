// Package kafka is the Kafka code both ways of running the connector share: connecting, reading
// in batches, and writing. It has no Temporal code.
//
// It reads in two ways. The streamer uses a GroupMember, which joins a consumer group so Kafka
// splits the partitions among the running streamers. The nexusworker uses a GroupReader, which
// reads from a group's committed offsets without joining it, so any worker process can serve
// any read.
package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// StartFrom is where a group starts reading a partition it has no committed offset for.
type StartFrom string

const (
	// StartFromOldest starts at the oldest message still in the topic.
	StartFromOldest StartFrom = "oldest"
	// StartFromNewest starts after the newest message, so only messages written later are read.
	StartFromNewest StartFrom = "newest"
)

// Config is how to reach the topic. ConfigFromEnv reads it from the environment.
type Config struct {
	Brokers   []string
	Topic     string
	Username  string // SASL/SCRAM-SHA-256 username; empty for no authentication
	Password  string
	StartFrom StartFrom
}

// ConfigFromEnv reads KAFKA_BROKERS (comma-separated), KAFKA_TOPIC, KAFKA_USERNAME,
// KAFKA_PASSWORD, and KAFKA_START_FROM (oldest, the default, or newest).
func ConfigFromEnv() (Config, error) {
	c := Config{
		Topic:     os.Getenv("KAFKA_TOPIC"),
		Username:  os.Getenv("KAFKA_USERNAME"),
		Password:  os.Getenv("KAFKA_PASSWORD"),
		StartFrom: StartFrom(os.Getenv("KAFKA_START_FROM")),
	}
	if b := os.Getenv("KAFKA_BROKERS"); b != "" {
		c.Brokers = strings.Split(b, ",")
	}
	var missing []string
	if len(c.Brokers) == 0 {
		missing = append(missing, "KAFKA_BROKERS")
	}
	if c.Topic == "" {
		missing = append(missing, "KAFKA_TOPIC")
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return Config{}, fmt.Errorf("set %s", strings.Join(missing, " and "))
	}
	switch c.StartFrom {
	case "":
		c.StartFrom = StartFromOldest
	case StartFromOldest, StartFromNewest:
	default:
		return Config{}, fmt.Errorf("KAFKA_START_FROM must be %s or %s, not %q", StartFromOldest, StartFromNewest, c.StartFrom)
	}
	return c, nil
}

// TopicIdentity names the topic beyond its name. Cluster is the Kafka cluster's ID. TopicID
// changes if the topic is deleted and created again. Either is empty if the broker doesn't report
// it.
type TopicIdentity struct {
	Cluster string
	TopicID string
}

// Identify asks Kafka for the identity of the topic in cfg.
func Identify(ctx context.Context, cfg Config) (TopicIdentity, error) {
	cl, err := kgo.NewClient(cfg.clientOpts()...)
	if err != nil {
		return TopicIdentity{}, fmt.Errorf("create Kafka client: %w", err)
	}
	defer cl.Close()
	return identify(ctx, cl, cfg.Topic)
}

// WaitForTopic asks Kafka for the topic in cfg until it answers, so a program learns at startup
// whether it can reach Kafka, log in, and see the topic, rather than waiting silently. Each failed
// attempt is passed to failed, with the delay before the next; attempts are retryDelay apart and
// each may take up to attemptTimeout. It returns the topic's identity, or ctx's error if ctx ends
// first.
func WaitForTopic(ctx context.Context, cfg Config, retryDelay, attemptTimeout time.Duration, failed func(attempt int, err error)) (TopicIdentity, error) {
	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		id, err := Identify(attemptCtx, cfg)
		cancel()
		if err == nil {
			return id, nil
		}
		if ctx.Err() != nil {
			return TopicIdentity{}, ctx.Err()
		}
		failed(attempt, err)
		select {
		case <-time.After(retryDelay):
		case <-ctx.Done():
			return TopicIdentity{}, ctx.Err()
		}
	}
}

// ConnectAtStartup is WaitForTopic with the delays both programs use when they start, logging
// each failed attempt and then the connection. A wrong broker address, login, or topic name shows
// in the log at once, and the program carries on as soon as Kafka answers.
func ConnectAtStartup(ctx context.Context, cfg Config) error {
	const retryDelay, attemptTimeout = 5 * time.Second, 10 * time.Second
	brokers := strings.Join(cfg.Brokers, ",")
	id, err := WaitForTopic(ctx, cfg, retryDelay, attemptTimeout, func(attempt int, err error) {
		slog.Warn("Can't reach Kafka or see the topic yet; retrying",
			"brokers", brokers, "topic", cfg.Topic, "attempt", attempt, "retryIn", retryDelay, "error", err)
	})
	if err != nil {
		return fmt.Errorf("waiting for Kafka at %s: %w", brokers, err)
	}
	slog.Info("Connected to Kafka", "brokers", brokers, "topic", cfg.Topic, "cluster", id.Cluster)
	return nil
}

// identify asks Kafka, through cl, for topic's identity. It sends its own metadata request rather
// than using the client's cached metadata, which can be seconds old, so it notices a topic deleted
// and created again.
func identify(ctx context.Context, cl *kgo.Client, topic string) (TopicIdentity, error) {
	req := kmsg.NewPtrMetadataRequest()
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = kmsg.StringPtr(topic)
	req.Topics = []kmsg.MetadataRequestTopic{reqTopic}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return TopicIdentity{}, fmt.Errorf("describe topic %q: %w", topic, err)
	}
	for _, t := range resp.Topics {
		if t.Topic == nil || *t.Topic != topic {
			continue
		}
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			return TopicIdentity{}, fmt.Errorf("describe topic %q: %w", topic, err)
		}
		var id TopicIdentity
		if resp.ClusterID != nil {
			id.Cluster = *resp.ClusterID
		}
		if t.TopicID != ([16]byte{}) {
			id.TopicID = kadm.TopicID(t.TopicID).String()
		}
		return id, nil
	}
	return TopicIdentity{}, fmt.Errorf("describe topic %q: not in the cluster's metadata", topic)
}

// clientOpts are the options every client of the topic uses, plus extra.
func (c Config) clientOpts(extra ...kgo.Opt) []kgo.Opt {
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...)}
	if c.Username != "" {
		opts = append(opts, kgo.SASL(scram.Auth{User: c.Username, Pass: c.Password}.AsSha256Mechanism()))
	}
	return append(opts, extra...)
}

// readOpts are the options for clients that read. Records from aborted transactions are never
// returned.
func (c Config) readOpts(extra ...kgo.Opt) []kgo.Opt {
	return c.clientOpts(append([]kgo.Opt{kgo.FetchIsolationLevel(kgo.ReadCommitted())}, extra...)...)
}
