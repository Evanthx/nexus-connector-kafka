// Command inbound is the Inbound Connector. It streams the topics its routes name into Temporal:
// for each route, it joins a consumer group and starts one standalone Nexus operation per partition
// of each batch, through the route's Nexus endpoint. It only sends messages in: it doesn't wait for
// results. Run several to share the topics' partitions; every process runs every route.
//
// Settings come from the environment: every variable it reads is listed in setup.Inbound, and the
// routes are JSON in INBOUND_CONFIG or INBOUND_CONFIG_FILE (setup.RouteFields). It connects to
// Temporal with the standard TEMPORAL_* variables and the temporal CLI's profile file; the
// namespace defaults to connectors.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/nexussend"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/startup"
	"github.com/temporalio/nexus-connector-kafka/setup"
	"go.temporal.io/sdk/client"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	log.Print("Stopped")
}

// run streams until interrupted. It returns only after closing its connections, so leaving the
// groups hands this Inbound Connector's partitions to the others at once, even when it stops on an
// error.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	env := settings.OS()
	s := startup.New("Inbound Connector", env)
	cfg, err := configFrom(env)
	s.Config(err)
	opts, c := s.Temporal()
	if err := s.Problems(); err != nil {
		return err
	}
	defer c.Close()

	// Logged before waiting for Kafka, so it shows even if Kafka is slow to answer.
	log.Print(cfg.report(env, opts))
	// Every route's topics, sources and dead-letter topics, before any route starts.
	topics := setup.Topics(cfg.routes)
	if err := s.Ready(ctx, func(ctx context.Context) error { return kafka.ConnectAtStartup(ctx, cfg.kafka, topics) }); err != nil {
		if errors.Is(err, startup.ErrStopped) {
			return nil
		}
		return err
	}

	var deadLetters *kafka.Writer
	if slices.ContainsFunc(cfg.routes, func(r setup.Route) bool { return !r.DeleteDeadLetters }) {
		if deadLetters, err = kafka.NewWriter(cfg.kafka); err != nil {
			return err
		}
		defer deadLetters.Close()
	}
	var readers []*reader
	for _, route := range cfg.routes {
		logger := slog.Default().With("topic", route.Topic)
		sender, err := newSender(c, route, logger)
		if err == nil {
			var r *reader
			r, err = newReader(cfg, route, sender, deadLetters, logger)
			readers = append(readers, r)
		}
		if err != nil {
			for _, r := range readers {
				if r != nil {
					r.member.Close()
				}
			}
			return err
		}
		log.Printf("Streaming topic %q as group %q to endpoint %q, operation %s/%s",
			route.Topic, route.Group, route.Endpoint, route.Service, route.Operation)
	}
	return runReaders(ctx, readers)
}

// newReader joins route's consumer group and returns a reader for it.
func newReader(cfg config, route setup.Route, sender nexussend.Sender, deadLetters *kafka.Writer, logger *slog.Logger) (*reader, error) {
	member, err := kafka.NewGroupMember(cfg.kafka, route.Topic, route.Group, route.StartFrom)
	if err != nil {
		return nil, err
	}
	r := &reader{
		member:            member,
		sender:            sender,
		identify:          member.Identity,
		topic:             route.Topic,
		group:             route.Group,
		deadLetterTopic:   route.DeadLetterTopic,
		deleteDeadLetters: route.DeleteDeadLetters,
		batchSize:         route.BatchSize,
		timeLimit:         route.BatchTimeLimit,
		maxInputBytes:     route.MaxInputBytes,
		logger:            logger,
		retryDelay:        nexussend.Backoff,
		statusInterval:    cfg.statusInterval,
		partitions:        member.Partitions,
	}
	if !route.DeleteDeadLetters {
		r.deadLetters = deadLetters
	}
	return r, nil
}

// runReaders runs every reader at once until ctx ends, then returns nil. If one fails, the others
// stop, and it returns that error. Each reader's member leaves its group before runReaders returns.
func runReaders(ctx context.Context, readers []*reader) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, r := range readers {
		g.Go(func() error {
			defer r.member.Close()
			if err := r.run(gctx); err != nil {
				return fmt.Errorf("streaming the topic %q stopped: %w", r.topic, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// newSender returns the sender for route's operations: it starts each one on route's endpoint,
// retrying until it starts, and, with maxMessagesPerMinute set, waits for each batch to fit in the
// rate first. The rate limit is outside the retries, so a retried start isn't counted twice.
func newSender(c client.Client, route setup.Route, logger *slog.Logger) (nexussend.Sender, error) {
	nexus, err := nexussend.NewNexus(c, route.Endpoint, route.Service, route.Operation, route.OperationTimeout, route.Ordered)
	if err != nil {
		return nil, err
	}
	return layers(nexus, route, nexussend.Backoff, logger)
}

// layers wraps base in the retries and, if route sets one, the rate limit, with the rate limit
// outside, so each batch is counted once however many attempts its start takes. Each route has its
// own, so one route's limit never slows another.
func layers(base nexussend.Sender, route setup.Route, retryDelay func(attempt int) time.Duration, logger *slog.Logger) (nexussend.Sender, error) {
	var sender nexussend.Sender = nexussend.NewRetrying(base, retryDelay, logger)
	if route.MaxMessagesPerMinute == 0 {
		return sender, nil
	}
	// The route rules keep batchSize <= maxMessagesPerMinute, and every operation holds at most one batch.
	return nexussend.NewRateLimited(sender, route.MaxMessagesPerMinute, route.BatchSize)
}
