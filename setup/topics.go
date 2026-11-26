package setup

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

// topicNameChars is what Kafka allows in a topic name.
var topicNameChars = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// CheckTopicName reports what is wrong with name as a Kafka topic name, or nil: 1 to 249 of
// letters, digits, '.', '_' and '-', and not "." or "..". Names starting with '_' are allowed;
// topics Kafka marks internal are refused when the program connects.
func CheckTopicName(name string) error {
	switch {
	case name == "":
		return errors.New("a topic name can't be empty")
	case len(name) > 249:
		return fmt.Errorf("topic name %q is longer than Kafka's 249 characters", name)
	case name == "." || name == "..":
		return fmt.Errorf("topic name %q isn't allowed by Kafka", name)
	case !topicNameChars.MatchString(name):
		return fmt.Errorf("topic name %q has characters Kafka doesn't allow: use letters, digits, '.', '_' and '-'", name)
	}
	return nil
}

// KafkaTopics is KAFKA_TOPICS, the Outbound Connector's allowlist: exact topic names, in order.
var KafkaTopics = settings.Define(settings.Variable{
	Name:     "KAFKA_TOPICS",
	Required: "yes",
	Help: "The topics callers may write to, as a comma-separated list of exact names, such as `orders,billing`. " +
		"A write names its topic; one not on this list is refused (`Rejected`) and nothing is written. " +
		"Every topic must exist when the Outbound Connector starts. Topics Kafka marks internal are refused.",
}, func(v string) ([]string, error) {
	if v == "" {
		return nil, settings.Required("KAFKA_TOPICS", "the topics callers may write to, comma-separated")
	}
	var topics []string
	var errs []error
	seen := map[string]bool{}
	for i, t := range strings.Split(v, ",") {
		t = strings.TrimSpace(t)
		switch {
		case t == "":
			errs = append(errs, fmt.Errorf("KAFKA_TOPICS: entry %d is empty; remove the extra comma", i+1))
			continue
		case seen[t]:
			errs = append(errs, fmt.Errorf("KAFKA_TOPICS lists %q twice", t))
			continue
		default:
		}
		if err := CheckTopicName(t); err != nil {
			errs = append(errs, fmt.Errorf("KAFKA_TOPICS: %w", err))
			continue
		}
		seen[t] = true
		topics = append(topics, t)
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return topics, nil
})
