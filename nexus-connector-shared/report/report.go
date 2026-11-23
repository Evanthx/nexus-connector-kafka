// Package report formats the report a connector program logs when it starts: its settings, grouped
// and aligned, then what it will do with them, one statement per line.
package report

import (
	"fmt"
	"strings"
	"time"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

// Setting is one environment variable as a program will use it.
type Setting struct {
	Name  string
	Value string
}

// Env is the setting for the environment variable name, which a program resolved to value from
// env. The value is marked as the default when the variable isn't set in env.
func Env(env settings.Env, name string, value any) Setting {
	v := fmt.Sprint(value)
	if d, ok := value.(time.Duration); ok {
		v = Duration(d)
	}
	if settings.Get(env, name) == "" {
		v += " (default)"
	}
	return Setting{Name: name, Value: v}
}

// Duration is d as people write it: 10m, 24h or 1h30m rather than 10m0s, 24h0m0s or 1h30m0s.
func Duration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

// Section is a titled group of settings.
type Section struct {
	Title    string
	Settings []Setting
}

// Format is the report: each section's settings, aligned, then what program (such as "Outbound
// Connector") will do, one statement per line. An empty program is called "program".
func Format(program string, sections []Section, behavior []string) string {
	if program == "" {
		program = "program"
	}
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
