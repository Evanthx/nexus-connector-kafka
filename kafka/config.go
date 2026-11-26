// Package kafka is the Kafka code the Outbound Connector and the Inbound Connector share:
// connecting, reading in batches, and writing. It has no Temporal code.
//
// The Inbound Connector reads with a GroupMember, which joins a consumer group so Kafka splits the
// partitions among the running Inbound Connectors. The Outbound Connector only writes.
package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// StartFrom is where a group starts reading a partition it has no committed offset for.
type StartFrom string

const (
	// StartFromOldest starts at the oldest message still in the topic.
	StartFromOldest StartFrom = "oldest"
	// StartFromNewest starts after the newest message, so only messages written later are read.
	StartFromNewest StartFrom = "newest"
)

// Config is how to reach Kafka. setup.Kafka and setup.LoadKafka read it from the environment
// variables in setup/kafka.go. Topics are separate, since one program serves several.
type Config struct {
	Brokers   []string
	Username  string // empty for no login
	Password  string
	Mechanism Mechanism // how Kafka checks the login; "" for ScramSHA256
	// TLS is the configuration for encrypted connections to the brokers; nil for plaintext.
	TLS *tls.Config
	// ClientLogLevel is the least severe of franz-go's own log lines the program logs
	// (KAFKA_CLIENT_LOG_LEVEL); nil for warn.
	ClientLogLevel slog.Leveler
}

// TopicIdentity names the topic beyond its name. Cluster is the Kafka cluster's ID. TopicID
// changes if the topic is deleted and created again. Either is empty if the broker doesn't report
// it. Internal is true for a topic Kafka keeps for itself, such as __consumer_offsets.
type TopicIdentity struct {
	Cluster  string
	TopicID  string
	Internal bool
}

// ErrInternalTopic is returned for a topic Kafka marks internal. The programs never read or write
// one, and waiting doesn't change it.
var ErrInternalTopic = errors.New("is a topic Kafka keeps for itself (internal); choose another topic")

// Identify asks Kafka for the identity of topic.
func Identify(ctx context.Context, cfg Config, topic string) (TopicIdentity, error) {
	cl, err := kgo.NewClient(cfg.clientOpts()...)
	if err != nil {
		return TopicIdentity{}, fmt.Errorf("create Kafka client: %w", err)
	}
	defer cl.Close()
	return identify(ctx, cl, topic)
}

// WaitForTopics asks Kafka for every topic until it sees them all, so a program learns at startup
// whether it can reach Kafka, log in, and see its topics, rather than waiting silently. Each
// failed attempt is passed to failed, with the topics not seen yet and why; attempts are
// retryDelay apart, and each asks for every topic in one metadata request that may take up to
// attemptTimeout, however many topics there are. An internal topic (ErrInternalTopic) ends the
// wait at once with an error. It returns the cluster ID, or ctx's error if ctx ends
// first.
func WaitForTopics(ctx context.Context, cfg Config, topics []string, retryDelay, attemptTimeout time.Duration, failed func(attempt int, missing []string, err error)) (string, error) {
	// An empty request would ask Kafka for every topic in the cluster.
	if len(topics) == 0 {
		return "", errors.New("no topics to wait for")
	}
	if slices.Contains(topics, "") {
		return "", errors.New("a topic to wait for is empty")
	}
	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		cluster, ids, errs := identifyAll(attemptCtx, cfg, topics)
		cancel()
		var missing []string
		var failures []error
		for _, topic := range topics {
			if err := errs[topic]; err != nil {
				missing = append(missing, topic)
				if !slices.ContainsFunc(failures, func(f error) bool { return f.Error() == err.Error() }) {
					failures = append(failures, err) // when Kafka can't be reached, every topic fails the same way
				}
				continue
			}
			if ids[topic].Internal {
				return "", fmt.Errorf("topic %q %w", topic, ErrInternalTopic)
			}
		}
		if len(missing) == 0 {
			return cluster, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		failed(attempt, missing, errors.Join(failures...))
		select {
		case <-time.After(retryDelay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// ConnectAtStartup is WaitForTopics with the delays both programs use when they start, logging
// each failed attempt and then the connection. A wrong broker address, login, or topic name shows
// in the log at once, and the program carries on as soon as Kafka answers for every topic.
func ConnectAtStartup(ctx context.Context, cfg Config, topics []string) error {
	const retryDelay, attemptTimeout = 5 * time.Second, 10 * time.Second
	brokers := strings.Join(cfg.Brokers, ",")
	cluster, err := WaitForTopics(ctx, cfg, topics, retryDelay, attemptTimeout, func(attempt int, missing []string, err error) {
		slog.Warn("Can't reach Kafka or see these topics yet; retrying",
			"brokers", brokers, "topics", strings.Join(missing, ","), "attempt", attempt, "retryIn", retryDelay, "error", err)
	})
	if err != nil {
		return fmt.Errorf("waiting for Kafka at %s: %w", brokers, err)
	}
	slog.Info("Connected to Kafka", "brokers", brokers, "topics", strings.Join(topics, ","), "cluster", cluster)
	return nil
}

// identify asks Kafka, through cl, for topic's identity. It sends its own metadata request rather
// than using the client's cached metadata, which can be seconds old, so it notices a topic deleted
// and created again.
func identify(ctx context.Context, cl *kgo.Client, topic string) (TopicIdentity, error) {
	ids, errs := identifyWith(ctx, cl, []string{topic})
	return ids[topic], errs[topic]
}

// identifyAll asks Kafka for every topic's identity in one metadata request, on one client. It
// returns the cluster ID (empty if no topic was seen), and for each topic its identity or why it
// couldn't be seen: every topic is in one map or the other.
func identifyAll(ctx context.Context, cfg Config, topics []string) (string, map[string]TopicIdentity, map[string]error) {
	cl, err := kgo.NewClient(cfg.clientOpts()...)
	if err != nil {
		errs := make(map[string]error, len(topics))
		for _, topic := range topics {
			errs[topic] = fmt.Errorf("create Kafka client: %w", err)
		}
		return "", nil, errs
	}
	defer cl.Close()
	ids, errs := identifyWith(ctx, cl, topics)
	for _, id := range ids {
		return id.Cluster, ids, errs
	}
	return "", ids, errs
}

// identifyWith asks Kafka, through cl, for every topic's identity in one metadata request.
func identifyWith(ctx context.Context, cl *kgo.Client, topics []string) (map[string]TopicIdentity, map[string]error) {
	ids := make(map[string]TopicIdentity, len(topics))
	errs := make(map[string]error)
	req := kmsg.NewPtrMetadataRequest()
	for _, topic := range topics {
		reqTopic := kmsg.NewMetadataRequestTopic()
		reqTopic.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, reqTopic)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		err = fmt.Errorf("describe topics: %w", err) // the same for every topic, so WaitForTopics reports it once
		for _, topic := range topics {
			errs[topic] = err
		}
		return ids, errs
	}
	for _, t := range resp.Topics {
		if t.Topic == nil {
			continue
		}
		topic := *t.Topic
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			errs[topic] = fmt.Errorf("describe topic %q: %w", topic, err)
			continue
		}
		id := TopicIdentity{Internal: t.IsInternal}
		if resp.ClusterID != nil {
			id.Cluster = *resp.ClusterID
		}
		if t.TopicID != ([16]byte{}) {
			id.TopicID = kadm.TopicID(t.TopicID).String()
		}
		ids[topic] = id
	}
	for _, topic := range topics {
		_, seen := ids[topic]
		if _, failed := errs[topic]; !seen && !failed {
			errs[topic] = fmt.Errorf("describe topic %q: not in the cluster's metadata", topic)
		}
	}
	return ids, errs
}

// clientOpts are the options every client of the topic uses, plus extra.
func (c Config) clientOpts(extra ...kgo.Opt) []kgo.Opt {
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.WithLogger(clientLogger{level: c.clientLogLevel()})}
	opts = append(opts, c.authOpts()...)
	return append(opts, extra...)
}

// clientLogLevel is ClientLogLevel, or warn if it isn't set.
func (c Config) clientLogLevel() slog.Level {
	if c.ClientLogLevel == nil {
		return slog.LevelWarn
	}
	return c.ClientLogLevel.Level()
}

// readOpts are the options for clients that read. Records from aborted transactions are never
// returned.
func (c Config) readOpts(extra ...kgo.Opt) []kgo.Opt {
	return c.clientOpts(append([]kgo.Opt{kgo.FetchIsolationLevel(kgo.ReadCommitted())}, extra...)...)
}
