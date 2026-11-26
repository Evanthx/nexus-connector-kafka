package testenv

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Refusing starts an in-memory Kafka with Topic, three partitions on three brokers, that refuses
// every write to a partition refuse returns true for, with err; it writes the others. It returns
// the settings to reach it and the number of produce requests it refused.
func Refusing(t *testing.T, refuse func(partition int32) bool, err *kerr.Error) (kafka.Config, func() int) {
	t.Helper()
	c, cerr := kfake.NewCluster(kfake.NumBrokers(3), kfake.SeedTopics(3, Topic))
	require.NoError(t, cerr)
	t.Cleanup(c.Close)
	// Each partition led by its own broker, so each produce request holds one partition, and a
	// refused partition doesn't take the others with it.
	for p := range int32(3) {
		require.NoError(t, c.MoveTopicPartition(Topic, p, p))
		require.Equal(t, p, c.LeaderFor(Topic, p))
	}
	var refused atomic.Int32
	c.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		pr, ok := req.(*kmsg.ProduceRequest)
		if !ok {
			return nil, nil, false
		}
		hit := false
		for _, rt := range pr.Topics {
			for _, rp := range rt.Partitions {
				hit = hit || refuse(rp.Partition)
			}
		}
		if !hit {
			return nil, nil, false // kfake writes it
		}
		refused.Add(1)
		resp, ok := pr.ResponseKind().(*kmsg.ProduceResponse)
		if !ok {
			return nil, nil, false
		}
		for _, rt := range pr.Topics {
			topic := kmsg.NewProduceResponseTopic()
			// Newer produce versions name the topic by ID: without it, kgo re-sends forever.
			topic.Topic, topic.TopicID = rt.Topic, rt.TopicID
			for _, rp := range rt.Partitions {
				p := kmsg.NewProduceResponseTopicPartition()
				p.Partition = rp.Partition
				p.ErrorCode = err.Code
				topic.Partitions = append(topic.Partitions, p)
			}
			resp.Topics = append(resp.Topics, topic)
		}
		return resp, nil, true
	})
	return kafka.Config{Brokers: c.ListenAddrs()}, func() int { return int(refused.Load()) }
}
