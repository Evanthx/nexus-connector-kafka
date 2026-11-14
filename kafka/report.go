package kafka

import (
	"fmt"
	"strings"

	"github.com/temporalio/nexus-connector-kafka/settings"
)

// Setting is one environment variable as a program will use it, for the report a program logs
// when it starts.
type Setting struct {
	Name  string
	Value string
}

// EnvSetting is the setting for the environment variable name, which a program resolved to value
// from env. The value is marked as the default when the variable isn't set in env.
func EnvSetting(env settings.Env, name string, value any) Setting {
	v := fmt.Sprint(value)
	if settings.Get(env, name) == "" {
		v += " (default)"
	}
	return Setting{Name: name, Value: v}
}

// Section is a titled group of settings in a report.
type Section struct {
	Title    string
	Settings []Setting
}

// Settings are the Kafka settings in c, read from env, as a report section. It never shows the
// password, only whether one is set.
func (c Config) Settings(env settings.Env) Section {
	login := "(not set: no authentication)"
	if c.Username != "" {
		login = c.Username + " (SASL/SCRAM-SHA-256; KAFKA_PASSWORD is not set)"
		if c.Password != "" {
			login = c.Username + " (SASL/SCRAM-SHA-256; KAFKA_PASSWORD is set)"
		}
	}
	return Section{Title: "Kafka", Settings: []Setting{
		{Name: "KAFKA_BROKERS", Value: strings.Join(c.Brokers, ",")},
		{Name: "KAFKA_TOPIC", Value: c.Topic},
		{Name: "KAFKA_USERNAME", Value: login},
		EnvSetting(env, "KAFKA_START_FROM", c.StartFrom),
	}}
}

// StartFromMeaning says in a sentence where a consumer group starts reading a partition it has no
// committed offset for.
func (c Config) StartFromMeaning() string {
	if c.StartFrom == StartFromNewest {
		return "starts at the end, so only messages written from then on are read"
	}
	return "starts at the oldest message still in the topic"
}

// Report formats a startup report: each section's settings, aligned, then what program (such as
// "relay") will do, one statement per line.
func Report(program string, sections []Section, behavior []string) string {
	width := 0
	for _, section := range sections {
		for _, s := range section.Settings {
			width = max(width, len(s.Name))
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s%s configuration (values marked \"default\" come from variables that aren't set):\n",
		strings.ToUpper(program[:1]), program[1:])
	for _, section := range sections {
		fmt.Fprintf(&b, "  %s\n", section.Title)
		for _, s := range section.Settings {
			fmt.Fprintf(&b, "    %-*s  %s\n", width, s.Name, s.Value)
		}
	}
	fmt.Fprintf(&b, "What the %s will do:\n", program)
	for _, line := range behavior {
		fmt.Fprintf(&b, "  - %s\n", line)
	}
	return strings.TrimSuffix(b.String(), "\n")
}
