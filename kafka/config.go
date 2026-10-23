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
	"os"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
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
	md, err := kadm.NewClient(cl).Metadata(ctx, cfg.Topic)
	if err != nil {
		return TopicIdentity{}, fmt.Errorf("describe topic %q: %w", cfg.Topic, err)
	}
	topic, ok := md.Topics[cfg.Topic]
	if !ok {
		return TopicIdentity{}, fmt.Errorf("describe topic %q: not in the cluster's metadata", cfg.Topic)
	}
	if topic.Err != nil {
		return TopicIdentity{}, fmt.Errorf("describe topic %q: %w", cfg.Topic, topic.Err)
	}
	id := TopicIdentity{Cluster: md.Cluster}
	if topic.ID != (kadm.TopicID{}) {
		id.TopicID = topic.ID.String()
	}
	return id, nil
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
