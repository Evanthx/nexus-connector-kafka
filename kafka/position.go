package kafka

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

// ErrInvalidPosition means an ack isn't a well-formed position for the group and topic being
// read, or is for an earlier topic of the same name.
var ErrInvalidPosition = errors.New("invalid position")

// position is where a group's reader is: each partition's next offset. Callers treat it as an
// opaque string. It names the group and topic, so an ack from one group can't be committed for
// another, and the cluster and topic ID, so an ack from a topic that has since been deleted and
// created again isn't committed against the new one, whose offsets mean different messages.
type position struct {
	Group   string          `json:"g"`
	Topic   string          `json:"t"`
	Cluster string          `json:"c,omitempty"`
	TopicID string          `json:"i,omitempty"`
	Offsets map[int32]int64 `json:"o"`
}

// checkIdentity reports whether p was returned for the topic id names: the same cluster, and the
// same topic rather than an earlier one of the same name.
func (p position) checkIdentity(id TopicIdentity) error {
	if p.Cluster != id.Cluster || p.TopicID != id.TopicID {
		return fmt.Errorf("%w: it was returned for an earlier topic named %q, which has since been deleted and created again, "+
			"or for another cluster; read without an ack to continue from the group's committed offsets", ErrInvalidPosition, p.Topic)
	}
	return nil
}

func (p position) encode() string {
	b, _ := json.Marshal(p) // a struct of strings and integers always marshals
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodePosition reads an ack and checks it belongs to group and topic.
func decodePosition(s, group, topic string) (position, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return position{}, fmt.Errorf("%w: not a well-formed position", ErrInvalidPosition)
	}
	var p position
	if err := json.Unmarshal(b, &p); err != nil {
		return position{}, fmt.Errorf("%w: not a well-formed position", ErrInvalidPosition)
	}
	if p.Group != group {
		return position{}, fmt.Errorf("%w: it was returned for group %q, not %q", ErrInvalidPosition, p.Group, group)
	}
	if p.Topic != topic {
		return position{}, fmt.Errorf("%w: it was returned for topic %q, not %q", ErrInvalidPosition, p.Topic, topic)
	}
	for part, off := range p.Offsets {
		if part < 0 || off < 0 {
			return position{}, fmt.Errorf("%w: partition %d offset %d", ErrInvalidPosition, part, off)
		}
	}
	return p, nil
}
