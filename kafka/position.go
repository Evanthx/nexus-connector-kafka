package kafka

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

// ErrInvalidPosition means an ack isn't a position this connector returned for the group and
// topic being read.
var ErrInvalidPosition = errors.New("invalid position")

// position is where a group's reader is: each partition's next offset. Callers treat it as an
// opaque string. It names the group and topic, so an ack from one group can't be committed for
// another.
type position struct {
	Group   string          `json:"g"`
	Topic   string          `json:"t"`
	Offsets map[int32]int64 `json:"o"`
}

func (p position) encode() string {
	b, _ := json.Marshal(p) // a struct of strings and integers always marshals
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodePosition reads an ack and checks it belongs to group and topic.
func decodePosition(s, group, topic string) (position, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return position{}, fmt.Errorf("%w: not one returned by a read", ErrInvalidPosition)
	}
	var p position
	if err := json.Unmarshal(b, &p); err != nil {
		return position{}, fmt.Errorf("%w: not one returned by a read", ErrInvalidPosition)
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
