package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// MaxConcurrentReads caps how many consumer clients, and so concurrent reads, a GroupReader runs.
// A read waits for a free client rather than opening more broker connections, so a worker should
// run no more reads at once than this.
const MaxConcurrentReads = 8

// consumerPool reuses consumer clients, and so their broker connections and SASL sessions, across
// reads. Each client serves one read at a time: the read adds its partitions, polls, and removes
// them again, which drops anything still buffered before the client goes back.
type consumerPool struct {
	opts  []kgo.Opt
	slots chan struct{} // one token per client that may exist; caps the pool at MaxConcurrentReads
	mu    sync.Mutex
	free  []*kgo.Client
	all   map[*kgo.Client]bool
}

func newConsumerPool(opts []kgo.Opt) *consumerPool {
	return &consumerPool{opts: opts, slots: make(chan struct{}, MaxConcurrentReads), all: map[*kgo.Client]bool{}}
}

// get returns a free client, creating one if the pool is below its cap, or waits for one.
func (p *consumerPool) get(ctx context.Context) (*kgo.Client, error) {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.free); n > 0 {
		cl := p.free[n-1]
		p.free = p.free[:n-1]
		return cl, nil
	}
	cl, err := kgo.NewClient(p.opts...)
	if err != nil {
		<-p.slots
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	p.all[cl] = true
	return cl, nil
}

// put returns a client to the pool. A client whose read failed is closed instead, so a client
// left in a bad state isn't handed to the next read.
func (p *consumerPool) put(cl *kgo.Client, healthy bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer func() { <-p.slots }()
	if !healthy {
		delete(p.all, cl)
		cl.Close()
		return
	}
	p.free = append(p.free, cl)
}

func (p *consumerPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for cl := range p.all {
		cl.Close()
	}
	p.all, p.free = map[*kgo.Client]bool{}, nil
}
