// Package settings declares the environment variables a program reads, each with its help text,
// default, whether it is required or secret, and how to check it, and reads them.
//
// One declaration serves three readers:
//
//   - The program loads its configuration with Program.Load, which checks every variable, family
//     and rule and reports every problem at once, then reads each typed value with From.
//   - The setup page (configui) shows the same Program, and its Test calls the same Load with the
//     page's values in place of the process's.
//   - docs/setup.md's table of variables is generated from it (see Markdown).
//
// So a new variable goes in the program's settings file only. This package is the same in every Go
// connector repo; copy it as is.
package settings

import (
	"errors"
	"maps"
	"slices"
	"strings"
)

// Program is one program and every environment variable it reads, in sections, in the order the
// setup page and the docs show them.
type Program struct {
	ID       string // short and unique among a repo's programs, such as "connector"
	Name     string // for people, such as "HTTP connector"
	Command  string // how to run it, such as "go run ./cmd/worker"
	Sections []*Section
	// Rules check variables against each other once each is valid on its own, such as "set A or B,
	// not both". Each returns every problem it finds, joined, or nil.
	Rules []func(env Env) error
}

// Section is a group of variables and families shown under one heading.
type Section struct {
	Title     string
	Variables []*Variable
	Families  []*Family
}

// Variable is one environment variable a program reads.
type Variable struct {
	Name string
	// Help explains the variable for people, on the setup page and in the docs. Write `NAME` in
	// backticks for code; the page shows it as code too.
	Help string
	// Default says what applies when the variable is unset, for people, such as "5" or "none".
	Default string
	// Required is "" if the variable is optional, "yes" if it is required, or the condition under
	// which it is, such as "unless DELETE_DEAD_LETTERS is true". It is for people: the check
	// enforces it, so its error can say what to set.
	Required string
	// Secret variables' values are never shown, logged, or sent to the setup page.
	Secret bool
	// Docs is Markdown added after Help in the generated docs only, such as a link to more detail.
	Docs string
	// External names what reads the variable straight from the process's environment, such as
	// "Go's HTTP client and gRPC", for a variable that a library reads and that the program can't
	// hand an Env to. The program's own code never reads it. The setup page shows its value from
	// the environment it was started in, read-only, and its Test runs with that value, as the
	// program would where it is set. "" for a variable the program reads through an Env.
	External string
	// Aliases are other names External reads the same setting under, in order, when this one is
	// unset or empty, such as https_proxy for HTTPS_PROXY. Only for an External variable.
	Aliases []string
	// Mask, for an External variable that holds a secret only in part, such as a proxy URL with a
	// password, returns the value with the secret parts masked, for showing, and the secret parts,
	// which the setup page removes from anything it shows. See MaskURLPassword. A wholly secret
	// variable sets Secret instead.
	Mask func(value string) (masked string, secrets []string)
	// check checks the value, "" when the variable is unset, and returns it parsed. nil for a
	// variable listed only to explain it, such as one the Temporal SDK reads.
	check func(value string) (any, error)
}

// Check reports what is wrong with value for v, "" meaning unset, or nil if nothing is.
func (v *Variable) Check(value string) error {
	if v.check == nil {
		return nil
	}
	_, err := v.check(value)
	return err
}

// Typed is a Variable whose value parses to T.
type Typed[T any] struct{ *Variable }

// Define declares v, checked and parsed by parse. parse gets the value as set, "" when unset, and
// its error should name the variable and never quote a secret's value.
func Define[T any](v Variable, parse func(value string) (T, error)) Typed[T] {
	v.check = func(value string) (any, error) { return parse(value) }
	return Typed[T]{&v}
}

// Get reads and parses t from env alone, without the rest of its program.
func (t Typed[T]) Get(env Env) (T, error) {
	return typed[T](t.check(Get(env, t.Name)))
}

// From is t's value in vals, which Load made. It is T's zero value if t isn't in the program.
func (t Typed[T]) From(vals *Values) T {
	v, _ := typed[T](vals.parsed[t.Variable], nil)
	return v
}

// Family is a set of variables that share a prefix and a name chosen by whoever sets them, such as
// a secret and the domains it may be sent to: HTTP_CONNECTOR_SECRET_<NAME> and
// HTTP_CONNECTOR_SECRET_<NAME>_DOMAINS. Each Member is one variable of every instance, NAME being
// the instance. Read a member with Lookup, not by building its name and reading the environment.
type Family struct {
	Prefix  string
	Members []Member
	// Help explains the family, on the setup page and in the docs.
	Help string
	// AddButton labels the setup page's button that adds an instance, such as "Add secret and
	// allowed domains"; AddHelp explains, in its dialog, what to name it; Example is a name, such
	// as STRIPE.
	AddButton string
	AddHelp   string
	Example   string
	Docs      string // as for Variable
	// check checks every instance in env together and returns them parsed.
	check func(env Env) (any, error)
}

// Member is one variable of each of a family's instances: the prefix, the instance's name, then
// Suffix.
type Member struct {
	Suffix      string // "" for the variable named by the prefix and the instance alone
	Label       string // a short name for the setup page, such as "Secret" or "Allowed domains"
	Help        string
	Placeholder string // an example value for the setup page's empty field
	Required    string // as for Variable
	Secret      bool
}

// InstanceNamePattern is what the setup page accepts as a new instance's name: upper-case letters,
// digits and underscores, starting with a letter. It mustn't end in a member's suffix either. The
// program accepts any name that is set; this keeps new ones conventional and unambiguous.
const InstanceNamePattern = "^[A-Z][A-Z0-9_]*$"

// TypedFamily is a Family whose instances parse to T, such as a map from each to its settings.
type TypedFamily[T any] struct{ *Family }

// DefineFamily declares f, whose instances parse checks together and parses. Its errors should
// name variables and never quote a secret's value.
func DefineFamily[T any](f Family, parse func(env Env) (T, error)) TypedFamily[T] {
	f.check = func(env Env) (any, error) { return parse(env) }
	return TypedFamily[T]{&f}
}

// Get reads and parses t's instances from env alone, without the rest of its program.
func (t TypedFamily[T]) Get(env Env) (T, error) {
	return typed[T](t.check(env))
}

// From is t's value in vals, which Load made. It is T's zero value if t isn't in the program.
func (t TypedFamily[T]) From(vals *Values) T {
	v, _ := typed[T](vals.parsed[t.Family], nil)
	return v
}

// Name is the variable of member m for instance.
func (f *Family) Name(instance string, m Member) string {
	return f.Prefix + instance + m.Suffix
}

// Split finds the instance and member a variable name belongs to. A member with a suffix wins over
// one without: with members "" and "_DOMAINS", X_DOMAINS is the second member of X.
func (f *Family) Split(name string) (instance string, m Member, ok bool) {
	rest, ok := strings.CutPrefix(name, f.Prefix)
	if !ok {
		return "", Member{}, false
	}
	var bare *Member
	for i, m := range f.Members {
		if m.Suffix == "" {
			bare = &f.Members[i]
			continue
		}
		if inst, ok := strings.CutSuffix(rest, m.Suffix); ok {
			return inst, m, inst != "" // no instance: the prefix and a suffix alone
		}
	}
	if bare == nil || rest == "" {
		return "", Member{}, false
	}
	return rest, *bare, true
}

// Instances are the names of f's instances with any member set in env, sorted.
func (f *Family) Instances(env Env) []string {
	set := map[string]bool{}
	for _, kv := range env.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if inst, _, ok := f.Split(name); ok {
			set[inst] = true
		}
	}
	return slices.Sorted(maps.Keys(set))
}

// Lookup reads the variable name if it is one of f's members, and reports whether it is set. A
// name outside the family is never read: ok is false.
func (f *Family) Lookup(env Env, name string) (value string, ok bool) {
	if _, _, member := f.Split(name); !member {
		return "", false
	}
	return env.LookupEnv(name)
}

// Load checks every variable, family and rule of p in env and returns their parsed values. It
// reports every problem at once, joined, one per line, and runs the rules only if every variable
// and family is valid.
func (p *Program) Load(env Env) (*Values, error) {
	vals := &Values{parsed: map[any]any{}}
	var errs []error
	for _, s := range p.Sections {
		for _, v := range s.Variables {
			if v.check == nil {
				continue
			}
			parsed, err := v.check(Get(env, v.Name))
			vals.parsed[v] = parsed
			errs = append(errs, err)
		}
		for _, f := range s.Families {
			if f.check == nil {
				continue
			}
			parsed, err := f.check(env)
			vals.parsed[f] = parsed
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	for _, rule := range p.Rules {
		errs = append(errs, rule(env))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return vals, nil
}

// Values are a program's parsed settings, from Load. Read one with its Typed or TypedFamily.
type Values struct {
	parsed map[any]any // *Variable or *Family → its parsed value
}

// Variables are every variable p lists, in order.
func (p *Program) Variables() []*Variable {
	var out []*Variable
	for _, s := range p.Sections {
		out = append(out, s.Variables...)
	}
	return out
}

// Families are every family p lists, in order.
func (p *Program) Families() []*Family {
	var out []*Family
	for _, s := range p.Sections {
		out = append(out, s.Families...)
	}
	return out
}

// Owns reports whether name is one of p's variables or a member of one of its families.
func (p *Program) Owns(name string) bool {
	_, ok := p.secret(name)
	return ok
}

// External is p's External variable that name is, or is an alias of, or nil.
func (p *Program) External(name string) *Variable {
	for _, v := range p.Variables() {
		if v.External != "" && (v.Name == name || slices.Contains(v.Aliases, name)) {
			return v
		}
	}
	return nil
}

// ExternalValue is v's value in env as External reads it: v's own if it is set and not empty,
// else its first alias's that is, and the name it was read under. name is "" if none is set.
func ExternalValue(env Env, v *Variable) (value, name string) {
	for _, n := range append([]string{v.Name}, v.Aliases...) {
		if val := Get(env, n); val != "" {
			return val, n
		}
	}
	return "", ""
}

// IsSecret reports whether name is a secret variable of p, or a secret member of one of its
// families.
func (p *Program) IsSecret(name string) bool {
	secret, _ := p.secret(name)
	return secret
}

func (p *Program) secret(name string) (secret, owned bool) {
	for _, v := range p.Variables() {
		if v.Name == name {
			return v.Secret, true
		}
	}
	for _, f := range p.Families() {
		if _, m, ok := f.Split(name); ok {
			return m.Secret, true
		}
	}
	return false, false
}

// typed converts a parsed value back to T, passing err through. v is always a T or nil, since
// Define and DefineFamily store parse's result; nil gives T's zero value.
func typed[T any](v any, err error) (T, error) {
	t, ok := v.(T)
	if !ok {
		var zero T
		return zero, err
	}
	return t, err
}
