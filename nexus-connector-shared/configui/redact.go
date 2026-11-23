package configui

import (
	"strings"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

// This file keeps secrets out of the Test's messages.

// redactions are what redactAll replaces, as pairs of old and new: the value of every External
// variable with a Mask, and its aliases', by its masked value; then its secret parts, and the
// value of every secret of p, set in env, by [REDACTED].
func redactions(p *settings.Program, env settings.Env) []string {
	var masked, secrets []string
	for _, v := range p.Variables() {
		if v.Mask == nil {
			continue
		}
		for _, name := range append([]string{v.Name}, v.Aliases...) {
			if value := settings.Get(env, name); value != "" {
				m, parts := v.Mask(value)
				masked = append(masked, value, m)
				secrets = append(secrets, parts...)
			}
		}
	}
	for _, kv := range env.Environ() {
		name, v, _ := strings.Cut(kv, "=")
		if p.IsSecret(name) || (p.External(name) != nil && p.External(name).Secret) {
			secrets = append(secrets, v)
		}
	}
	out := masked
	for _, s := range secrets {
		if s != "" {
			out = append(out, s, "[REDACTED]")
		}
	}
	return out
}

// redactAll makes the replacements in resp's messages: every secret becomes [REDACTED], a proxy
// URL its masked form. The checks already name variables, never values; this makes sure no
// message an outside library wrote shows one.
func redactAll(resp *TestResponse, replacements []string) {
	r := strings.NewReplacer(replacements...)
	redact := func(lines []string) {
		for i, line := range lines {
			lines[i] = r.Replace(line)
		}
	}
	for i := range resp.Checks {
		redact(resp.Checks[i].Problems)
		redact(resp.Checks[i].Warnings)
		redact(resp.Checks[i].Passed)
		skipped := []string{resp.Checks[i].Skipped}
		redact(skipped)
		resp.Checks[i].Skipped = skipped[0]
	}
}
