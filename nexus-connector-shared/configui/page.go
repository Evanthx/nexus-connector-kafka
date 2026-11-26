package configui

import (
	"maps"
	"net/http"
	"slices"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

// This file is GET /api/settings: what the page shows, built from the programs' settings lists
// and the environment, never with a secret's value.

// Page is what the page renders from: the programs, their sections, variables, families and
// test fields, and the values set now. A secret's value is never sent, only whether it is set.
type Page struct {
	Title      string            `json:"title"`
	Programs   []programJSON     `json:"programs"`
	Values     map[string]string `json:"values"`     // non-secret variables set now, but not External ones
	SecretsSet map[string]bool   `json:"secretsSet"` // every secret shown, but not External ones: whether it is set now
	// External holds every External variable shown, as it is set where configui runs. The page
	// shows them read-only and never sends them.
	External map[string]PageExternal `json:"external"`
	// TemporalProfile is the Temporal profile file and profile the environment configui runs in
	// selects, and what they set. nil if no program connects to Temporal.
	TemporalProfile *PageProfile `json:"temporalProfile"`
}

// PageExternal is an External variable as its library reads it where configui runs.
type PageExternal struct {
	Set bool `json:"set"`
	// From is the name it was read under: the variable's own, or an alias, such as https_proxy.
	// "" if it isn't set.
	From string `json:"from"`
	// Value is what may be shown of it: its value, with the secret parts masked if the variable
	// has a Mask. "" for a Secret variable or one that isn't set.
	Value string `json:"value"`
}

type programJSON struct {
	ID         string        `json:"id"`
	Name       string        `json:"name"`
	Command    string        `json:"command"`
	Sections   []sectionJSON `json:"sections"`
	TestFields []TestField   `json:"testFields"`
}

type sectionJSON struct {
	Title     string         `json:"title"`
	Variables []variableJSON `json:"variables"`
	Families  []PageFamily   `json:"families"`
}

type variableJSON struct {
	Name     string   `json:"name"`
	Help     string   `json:"help"`
	Default  string   `json:"default"`
	Required string   `json:"required"`
	Secret   bool     `json:"secret"`
	External string   `json:"external"` // what reads it from the process's environment; "" if the program does
	Aliases  []string `json:"aliases"`  // other names External reads it under
	// Multiline, Example and HelpURL are the variable's own: a text area, an "Insert example"
	// button, and a link to more help.
	Multiline bool   `json:"multiline"`
	Example   string `json:"example"`
	HelpURL   string `json:"helpURL"`
}

type PageFamily struct {
	Prefix      string       `json:"prefix"`
	Help        string       `json:"help"`
	AddButton   string       `json:"addButton"`
	AddHelp     string       `json:"addHelp"`
	Example     string       `json:"example"`
	NamePattern string       `json:"namePattern"` // a new instance's name; it mustn't end in a member's suffix either
	Members     []memberJSON `json:"members"`
	Instances   []string     `json:"instances"` // the instances set now
}

type memberJSON struct {
	Suffix      string `json:"suffix"`
	Label       string `json:"label"`
	Help        string `json:"help"`
	Placeholder string `json:"placeholder"`
	Required    string `json:"required"`
	Secret      bool   `json:"secret"`
}

func (s *server) settings(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.describe())
}

// describe is the page's JSON for s.env.
func (s *server) describe() Page {
	out := Page{
		Title: s.title, Values: map[string]string{}, SecretsSet: map[string]bool{}, External: map[string]PageExternal{},
		TemporalProfile: describeProfile(s.env, s.programs),
	}
	for _, p := range s.programs {
		pj := programJSON{ID: p.Settings.ID, Name: p.Settings.Name, Command: p.Settings.Command, TestFields: p.TestFields}
		if pj.TestFields == nil {
			pj.TestFields = []TestField{} // the page iterates it: [], not null
		}
		for _, sec := range p.Settings.Sections {
			sj := sectionJSON{Title: sec.Title, Variables: []variableJSON{}, Families: []PageFamily{}}
			for _, v := range sec.Variables {
				sj.Variables = append(sj.Variables, variableJSON{
					Name: v.Name, Help: v.Help, Default: v.Default, Required: v.Required, Secret: v.Secret,
					External: v.External, Aliases: nonNil(v.Aliases),
					Multiline: v.Multiline, Example: v.Example, HelpURL: v.HelpURL,
				})
				if v.External != "" {
					out.External[v.Name] = describeExternal(s.env, v)
				} else {
					out.show(s.env, v.Name, v.Secret)
				}
			}
			for _, f := range sec.Families {
				sj.Families = append(sj.Families, out.describeFamily(s.env, f))
			}
			pj.Sections = append(pj.Sections, sj)
		}
		out.Programs = append(out.Programs, pj)
	}
	return out
}

// show records name's value from env on the page: whether it is set for a secret, else its value
// if set.
func (out *Page) show(env settings.Env, name string, secret bool) {
	v := settings.Get(env, name)
	switch {
	case secret:
		out.SecretsSet[name] = v != ""
	case v != "":
		out.Values[name] = v
	default: // a non-secret that isn't set: the page shows its default
	}
}

// describeFamily is f's JSON, recording each of its instances' values on the page.
func (out *Page) describeFamily(env settings.Env, f *settings.Family) PageFamily {
	fj := PageFamily{
		Prefix: f.Prefix, Help: f.Help, AddButton: f.AddButton, AddHelp: f.AddHelp, Example: f.Example,
		NamePattern: settings.InstanceNamePattern, Instances: nonNil(instances(f, env, out.TemporalProfile)),
	}
	for _, m := range f.Members {
		fj.Members = append(fj.Members, memberJSON{
			Suffix: m.Suffix, Label: m.Label, Help: m.Help, Placeholder: m.Placeholder, Required: m.Required, Secret: m.Secret,
		})
		for _, inst := range fj.Instances {
			out.show(env, f.Name(inst, m), m.Secret)
		}
	}
	return fj
}

// describeExternal is v as its library reads it from env, masked.
func describeExternal(env settings.Env, v *settings.Variable) PageExternal {
	value, from := settings.ExternalValue(env, v)
	e := PageExternal{Set: from != "", From: from}
	switch {
	case v.Secret || from == "":
	case v.Mask != nil:
		e.Value, _ = v.Mask(value)
	default:
		e.Value = value
	}
	return e
}

// instances are f's instances set in env, and those the Temporal profile sets, sorted.
func instances(f *settings.Family, env settings.Env, profile *PageProfile) []string {
	insts := f.Instances(env)
	if profile != nil {
		for _, name := range slices.Concat(slices.Collect(maps.Keys(profile.Values)), slices.Collect(maps.Keys(profile.SecretsSet))) {
			if inst, _, ok := f.Split(name); ok && !slices.Contains(insts, inst) {
				insts = append(insts, inst)
			}
		}
	}
	slices.Sort(insts)
	return insts
}
