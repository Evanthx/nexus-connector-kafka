package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/temporalio/nexus-connector-kafka/generated/kafkaapi"
	"github.com/temporalio/nexus-connector-kafka/kafka"
)

// This file is the operation IDs: the same batch from the same source always gets the same ID, so
// a redelivered batch joins the operation already started rather than starting another.

// source is the source key for this batch's operation IDs (see sourceKey). It asks Kafka for the
// topic's identity for every batch, retrying until it answers or ctx ends, so a topic deleted and
// created again while the Inbound Connector runs gets new IDs: the new topic's offsets mean
// different messages.
func (r *reader) source(ctx context.Context) (string, error) {
	for attempt := 1; ; attempt++ {
		id, err := r.identify(ctx)
		if err == nil {
			return sourceKey(id, r.topic, r.group), nil
		}
		r.logger.Warn("Could not get the topic's identity; retrying", "attempt", attempt, "error", err)
		if err := r.sleep(ctx, attempt); err != nil {
			return "", err
		}
	}
}

// sourceKey names where messages come from: the Kafka cluster, the topic (its ID changes if it
// is deleted and created again), and the consumer group. It is a hash, so names that contain the
// ID's separator can't run together: topic "a-b" with group "c" and topic "a" with group "b-c"
// get different keys.
func sourceKey(id kafka.TopicIdentity, topic, group string) string {
	b, _ := json.Marshal([]string{id.Cluster, id.TopicID, topic, group}) // strings always marshal
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}

// operationID identifies the operation for consecutive records of one partition: their source
// (see sourceKey), partition, and first and last offsets. Records delivered again in the same
// batch get the same ID, so they match the operation already started. A batch delivered again can
// end at a different offset, though, and then gets a new ID: see "Duplicates are possible" in docs/how-it-works.md.
func operationID(source string, recs []kafkaapi.Record) string {
	first, last := recs[0], recs[len(recs)-1]
	return fmt.Sprintf("kafka-%s-%d-%d-%d", source, first.Partition, first.Offset, last.Offset)
}
