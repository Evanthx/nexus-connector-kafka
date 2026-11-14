package settings

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// testProgram has a typed variable, a display-only one, a family, and a rule.
func testProgram() (*Program, Typed[int], TypedFamily[[]string]) {
	count := Define(Variable{Name: "COUNT", Default: "1"}, func(v string) (int, error) {
		if v == "" {
			return 1, nil
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("COUNT must be a number; got %q", v)
		}
		return n, nil
	})
	keys := DefineFamily(Family{
		Prefix:  "KEY_",
		Members: []Member{{Secret: true}, {Suffix: "_HOSTS"}},
	}, func(env Env) ([]string, error) {
		var errs []error
		for _, kv := range env.Environ() {
			name, _, _ := strings.Cut(kv, "=")
			if strings.HasPrefix(name, "KEY_") && !strings.HasSuffix(name, "_HOSTS") && Get(env, name+"_HOSTS") == "" {
				errs = append(errs, fmt.Errorf("%s needs %s_HOSTS", name, name))
			}
		}
		return []string{"parsed"}, errors.Join(errs...)
	})
	p := &Program{
		ID: "p",
		Sections: []*Section{
			{Title: "A", Variables: []*Variable{count.Variable, {Name: "SHOWN_ONLY", Secret: true}}},
			{Title: "B", Families: []*Family{keys.Family}},
		},
		Rules: []func(Env) error{func(env Env) error {
			if Get(env, "COUNT") == "7" {
				return errors.New("COUNT can't be 7 with the others")
			}
			return nil
		}},
	}
	return p, count, keys
}

func TestLoad_ParsesEveryValue(t *testing.T) {
	p, count, keys := testProgram()
	vals, err := p.Load(Map{"COUNT": "3", "KEY_A": "s", "KEY_A_HOSTS": "x"})
	require.NoError(t, err)
	require.Equal(t, 3, count.From(vals))
	require.Equal(t, []string{"parsed"}, keys.From(vals))

	vals, err = p.Load(Map{})
	require.NoError(t, err)
	require.Equal(t, 1, count.From(vals), "the default")
}

// Every variable and family is checked before Load fails, so one error lists every problem.
func TestLoad_ReportsEveryProblem(t *testing.T) {
	p, _, _ := testProgram()
	_, err := p.Load(Map{"COUNT": "x", "KEY_A": "s"})
	require.EqualError(t, err, "COUNT must be a number; got \"x\"\nKEY_A needs KEY_A_HOSTS")
}

// Rules run once every variable is valid on its own.
func TestLoad_Rules(t *testing.T) {
	p, _, _ := testProgram()
	_, err := p.Load(Map{"COUNT": "7"})
	require.EqualError(t, err, "COUNT can't be 7 with the others")

	_, err = p.Load(Map{"COUNT": "7", "KEY_A": "s"})
	require.EqualError(t, err, "KEY_A needs KEY_A_HOSTS", "a rule doesn't run on invalid values")
}

func TestTyped_Get(t *testing.T) {
	_, count, _ := testProgram()
	n, err := count.Get(Map{"COUNT": "5", "OTHER": "x"})
	require.NoError(t, err)
	require.Equal(t, 5, n)
	_, err = count.Get(Map{"COUNT": "five"})
	require.Error(t, err)
}

func TestFamily_Split(t *testing.T) {
	f := &Family{Prefix: "KEY_", Members: []Member{{Label: "secret"}, {Suffix: "_HOSTS", Label: "hosts"}}}
	for name, want := range map[string][2]string{
		"KEY_A":             {"A", "secret"},
		"KEY_A_HOSTS":       {"A", "hosts"},
		"KEY_A_HOSTS_HOSTS": {"A_HOSTS", "hosts"},
		"KEY_LONG_NAME":     {"LONG_NAME", "secret"},
	} {
		inst, m, ok := f.Split(name)
		require.True(t, ok, name)
		require.Equal(t, want, [2]string{inst, m.Label}, name)
	}
	for _, name := range []string{"KEY_", "KEY__HOSTS", "OTHER_A", "KEYA"} {
		inst, _, ok := f.Split(name)
		require.False(t, ok, "%s gave %q", name, inst)
	}
}

func TestFamily_InstancesAndLookup(t *testing.T) {
	f := &Family{Prefix: "KEY_", Members: []Member{{}, {Suffix: "_HOSTS"}}}
	env := Map{"KEY_B": "1", "KEY_A_HOSTS": "h", "KEY_C": "", "OTHER": "o"}
	require.Equal(t, []string{"A", "B", "C"}, f.Instances(env), "any member set, even empty")

	v, ok := f.Lookup(env, "KEY_B")
	require.True(t, ok)
	require.Equal(t, "1", v)
	_, ok = f.Lookup(env, "OTHER")
	require.False(t, ok, "a name outside the family is never read")
}

func TestProgram_OwnsAndIsSecret(t *testing.T) {
	p, _, _ := testProgram()
	require.True(t, p.Owns("COUNT"))
	require.False(t, p.IsSecret("COUNT"))
	require.True(t, p.IsSecret("SHOWN_ONLY"))
	require.True(t, p.IsSecret("KEY_X"))
	require.True(t, p.Owns("KEY_X_HOSTS"))
	require.False(t, p.IsSecret("KEY_X_HOSTS"))
	require.False(t, p.Owns("HOME"))
}

func TestExternal(t *testing.T) {
	proxy := &Variable{Name: "HTTPS_PROXY", External: "Go", Aliases: []string{"https_proxy"}}
	p := &Program{Sections: []*Section{{Variables: []*Variable{{Name: "COUNT"}, proxy}}}}
	require.Same(t, proxy, p.External("HTTPS_PROXY"))
	require.Same(t, proxy, p.External("https_proxy"))
	require.Nil(t, p.External("COUNT"), "not External")
	require.Nil(t, p.External("HOME"))

	for env, want := range map[string][2]string{
		"HTTPS_PROXY=a https_proxy=b": {"a", "HTTPS_PROXY"},
		"HTTPS_PROXY= https_proxy=b":  {"b", "https_proxy"}, // empty counts as unset, as in Go's httpproxy
		"https_proxy=b":               {"b", "https_proxy"},
		"":                            {"", ""},
	} {
		m := Map{}
		for _, kv := range strings.Fields(env) {
			k, v, _ := strings.Cut(kv, "=")
			m[k] = v
		}
		value, name := ExternalValue(m, proxy)
		require.Equal(t, want, [2]string{value, name}, env)
	}
}

func TestMaskURLPassword(t *testing.T) {
	for value, want := range map[string]struct {
		masked  string
		secrets []string
	}{
		"http://user:pw@proxy.example:8080":       {"http://user:****@proxy.example:8080", []string{"pw"}},
		"http://user:p%40ss@proxy:8080/path?q=@x": {"http://user:****@proxy:8080/path?q=@x", []string{"p%40ss", "p@ss"}},
		"http://user:p@ss@proxy:8080":             {"http://user:****@proxy:8080", []string{"p@ss"}},
		"user:pw@proxy:8080":                      {"user:****@proxy:8080", []string{"pw"}},
		"http://user@proxy:8080":                  {"http://user@proxy:8080", nil},
		"http://user:@proxy:8080":                 {"http://user:@proxy:8080", nil},
		"http://proxy.example:8080":               {"http://proxy.example:8080", nil},
		"proxy.example:8080":                      {"proxy.example:8080", nil},
	} {
		masked, secrets := MaskURLPassword(value)
		require.Equal(t, want.masked, masked, value)
		require.Equal(t, want.secrets, secrets, value)
	}
}

func TestMap(t *testing.T) {
	env := Map{"B": "2", "A": "1"}
	require.Equal(t, []string{"A=1", "B=2"}, env.Environ())
	_, ok := env.LookupEnv("C")
	require.False(t, ok)
}

func TestMarkdown(t *testing.T) {
	p, _, _ := testProgram()
	p.Sections[0].Variables[0].Help = "How many | pipes"
	p.Sections[0].Variables[0].Docs = "[More](x.md)."
	p.Sections[1].Families[0].Help = "Keys and their hosts."
	p.Sections[1].Families[0].Members[1].Required = "for each key"
	p.Sections[0].Variables[1] = &Variable{Name: "PROXY", Help: "A proxy.", External: "Go's HTTP client", Aliases: []string{"proxy"}}
	require.Equal(t, "### A\n\n"+
		"| Variable | Required | Default | Meaning |\n|---|---|---|---|\n"+
		"| `COUNT` |  | 1 | How many \\| pipes [More](x.md). |\n"+
		"| `PROXY` |  |  | *Read by Go's HTTP client from the process's environment; also as `proxy` when unset or empty.* A proxy. |\n\n"+
		"### B\n\nKeys and their hosts.\n\n"+
		"| Variable | Required | Default | Meaning |\n|---|---|---|---|\n"+
		"| `KEY_<NAME>` (secret) |  |  |  |\n"+
		"| `KEY_<NAME>_HOSTS` | for each key |  |  |\n\n",
		Markdown(p))
}

func TestReplaceGenerated(t *testing.T) {
	doc := "intro\n" + StartMarker + "\nold\n" + EndMarker + "\nrest\n"
	got, err := ReplaceGenerated(doc, "new\n")
	require.NoError(t, err)
	require.Equal(t, "intro\n"+StartMarker+"\nnew\n"+EndMarker+"\nrest\n", got)

	_, err = ReplaceGenerated("no markers", "x")
	require.Error(t, err)
}
