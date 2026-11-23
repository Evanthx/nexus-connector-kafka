package main

import (
	"context"
	"time"
)

// This file is the status line the Inbound Connector logs while nothing is delivered, so a stream
// that stopped moving says why.

// reportStatus logs a status line every statusInterval in which nothing was delivered, until ctx
// ends. With partitions, a quiet topic looks like this; with none, the group gave this Inbound
// Connector nothing to read, which the log lines from the group explain.
func (r *reader) reportStatus(ctx context.Context) {
	ticker := time.NewTicker(r.statusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		since := time.Since(time.Unix(0, r.lastDelivered.Load())).Round(time.Second)
		if since < r.statusInterval {
			continue
		}
		if started := r.batchStarted.Load(); started != 0 {
			// Most often ordered mode waiting for a long operation; otherwise retries, which log
			// their own warnings, or the rate limit.
			r.logger.Info("Still delivering a batch: waiting for its operations to start, or with INBOUND_ORDERING=partition "+
				"to complete", "topic", r.topic, "for", time.Since(time.Unix(0, started)).Round(time.Second).String())
			continue
		}
		parts := r.partitions()
		if len(parts) == 0 {
			r.logger.Info("Waiting for messages, but the consumer group has given this Inbound Connector no partitions: "+
				"other Inbound Connectors in the group have them all, or the group hasn't finished joining",
				"topic", r.topic, "group", r.group, "lastDelivered", since.String()+" ago")
			continue
		}
		r.logger.Info("Waiting for messages", "topic", r.topic, "partitions", parts, "lastDelivered", since.String()+" ago")
	}
}
