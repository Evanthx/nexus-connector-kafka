// Command configui serves a page for working out the connector's environment variables. It shows
// every variable the connector reads, from the program's settings list, with its help, default
// and the value set where configui was started (secrets only as set or not set), lets you change
// and add them, and tests them: every check the connector makes at startup, the connection to
// Temporal, and the connector's own checks. It lists the `export` lines to set. It never changes
// its own environment and never stores what you type.
//
//	go run ./configui                          # then open http://localhost:8090
//	go run ./configui --addr localhost:9000
//
// It listens on localhost only: it handles credentials and connects wherever it is told to.
//
// This file, temporal.go and index.html are the same in every Go connector repo; connector.go says
// what is particular to this one.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/temporalio/nexus-connector-kafka/settings"
	"go.temporal.io/sdk/client"
)

//go:embed index.html
var page []byte

func main() {
	addr := flag.String("addr", "localhost:8090", "address to listen on; keep it on localhost")
	flag.Parse()
	host, _, err := net.SplitHostPort(*addr)
	if err != nil {
		log.Fatalf("-addr: %v", err)
	}
	if !loopback(host) {
		log.Fatalf("-addr must be on localhost, not %q: the page handles credentials", host)
	}
	log.Printf("Open http://%s", *addr)
	srv := &http.Server{Addr: *addr, Handler: newServer(settings.OS(), programs), ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

// program is one of the repo's programs, as the page shows and tests it. connector.go lists them.
type program struct {
	Settings *settings.Program
	// ClientOptions loads the Temporal client options from env as the program does. nil if the
	// program doesn't connect to Temporal; the Test then doesn't either.
	ClientOptions func(env settings.Env) (client.Options, error)
	// TaskQueue is the task queue the program's Nexus endpoint must target, given env. nil if no
	// endpoint targets the program.
	TaskQueue func(env settings.Env) string
	// EndpointName and EndpointDescriptionFile go in the command the Test suggests for creating
	// a missing endpoint.
	EndpointName            string
	EndpointDescriptionFile string
	// CallsEndpoint is the name of the Nexus endpoint the program calls, given env, such as a
	// relay's; the Temporal check confirms it exists. nil, or "" from it, if the program calls
	// none.
	CallsEndpoint func(env settings.Env) string
	// TestFields are extra inputs for the Test, such as a URL to try. Check gets their values.
	TestFields []testField
	// Check is the program's own check after the settings and Temporal: whatever the program can
	// try without running, such as a request through its real code. nil if it has none.
	Check func(ctx context.Context, env settings.Env, fields map[string]string) check
}

// testField is an extra input for the Test. Kind is "text", or "instance": a choice of the
// family's instances on the page (Family is its prefix), or none.
type testField struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Help        string `json:"help"`
	Kind        string `json:"kind"`
	Placeholder string `json:"placeholder,omitempty"`
	Family      string `json:"family,omitempty"`
}

// server serves the page and its API. env is the environment it shows and tests against, with the
// page's values in place of the program's: the process's, except in tests.
type server struct {
	env      settings.Env
	programs []program
}

func newServer(env settings.Env, programs []program) http.Handler {
	s := &server{env: env, programs: programs}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(page)
	})
	mux.HandleFunc("GET /api/settings", s.settings)
	mux.HandleFunc("POST /api/test", s.test)
	return localOnly(mux)
}

// localOnly refuses requests whose Host isn't localhost, so a web page on another site can't reach
// the API through a DNS name that resolves to this machine, and POSTs that aren't JSON, which a
// page on another site can't send here without the browser asking first.
func localOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.Host)
		if err != nil {
			host = r.Host
		}
		if !loopback(host) {
			http.Error(w, "this page only answers on localhost", http.StatusForbidden)
			return
		}
		if r.Method == http.MethodPost && r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "send JSON", http.StatusUnsupportedMediaType)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func loopback(host string) bool {
	host = strings.Trim(host, "[]")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// pageJSON is what the page renders from: the programs, their sections, variables, families and
// test fields, and the values set now. A secret's value is never sent, only whether it is set.
type pageJSON struct {
	Title      string            `json:"title"`
	Programs   []programJSON     `json:"programs"`
	Values     map[string]string `json:"values"`     // non-secret variables set now, but not External ones
	SecretsSet map[string]bool   `json:"secretsSet"` // every secret shown, but not External ones: whether it is set now
	// External holds every External variable shown, as it is set where configui runs. The page
	// shows them read-only and never sends them.
	External map[string]externalJSON `json:"external"`
	// TemporalProfile is the Temporal profile file and profile the environment configui runs in
	// selects, and what they set. nil if no program connects to Temporal.
	TemporalProfile *profileJSON `json:"temporalProfile"`
}

// externalJSON is an External variable as its library reads it where configui runs.
type externalJSON struct {
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
	TestFields []testField   `json:"testFields"`
}

type sectionJSON struct {
	Title     string         `json:"title"`
	Variables []variableJSON `json:"variables"`
	Families  []familyJSON   `json:"families"`
}

type variableJSON struct {
	Name     string   `json:"name"`
	Help     string   `json:"help"`
	Default  string   `json:"default"`
	Required string   `json:"required"`
	Secret   bool     `json:"secret"`
	External string   `json:"external"` // what reads it from the process's environment; "" if the program does
	Aliases  []string `json:"aliases"`  // other names External reads it under
}

type familyJSON struct {
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
func (s *server) describe() pageJSON {
	out := pageJSON{
		Title: pageTitle, Values: map[string]string{}, SecretsSet: map[string]bool{}, External: map[string]externalJSON{},
		TemporalProfile: describeProfile(s.env, s.programs),
	}
	show := func(name string, secret bool) {
		v := settings.Get(s.env, name)
		switch {
		case secret:
			out.SecretsSet[name] = v != ""
		case v != "":
			out.Values[name] = v
		default: // a non-secret that isn't set: the page shows its default
		}
	}
	for _, p := range s.programs {
		pj := programJSON{ID: p.Settings.ID, Name: p.Settings.Name, Command: p.Settings.Command, TestFields: p.TestFields}
		if pj.TestFields == nil {
			pj.TestFields = []testField{} // the page iterates it: [], not null
		}
		for _, sec := range p.Settings.Sections {
			sj := sectionJSON{Title: sec.Title, Variables: []variableJSON{}, Families: []familyJSON{}}
			for _, v := range sec.Variables {
				sj.Variables = append(sj.Variables, variableJSON{
					Name: v.Name, Help: v.Help, Default: v.Default, Required: v.Required, Secret: v.Secret,
					External: v.External, Aliases: nonNil(v.Aliases),
				})
				if v.External != "" {
					out.External[v.Name] = describeExternal(s.env, v)
				} else {
					show(v.Name, v.Secret)
				}
			}
			for _, f := range sec.Families {
				fj := familyJSON{
					Prefix: f.Prefix, Help: f.Help, AddButton: f.AddButton, AddHelp: f.AddHelp, Example: f.Example,
					NamePattern: settings.InstanceNamePattern, Instances: nonNil(instances(f, s.env, out.TemporalProfile)),
				}
				for _, m := range f.Members {
					fj.Members = append(fj.Members, memberJSON{
						Suffix: m.Suffix, Label: m.Label, Help: m.Help, Placeholder: m.Placeholder, Required: m.Required, Secret: m.Secret,
					})
					for _, inst := range fj.Instances {
						show(f.Name(inst, m), m.Secret)
					}
				}
				sj.Families = append(sj.Families, fj)
			}
			pj.Sections = append(pj.Sections, sj)
		}
		out.Programs = append(out.Programs, pj)
	}
	return out
}

// describeExternal is v as its library reads it from env, masked.
func describeExternal(env settings.Env, v *settings.Variable) externalJSON {
	value, from := settings.ExternalValue(env, v)
	e := externalJSON{Set: from != "", From: from}
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
func instances(f *settings.Family, env settings.Env, profile *profileJSON) []string {
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

// testRequest is the page's values for one program. Values holds every variable the page shows or
// added, a blank value meaning unset, and a secret only if one was typed. KeepSecrets names the
// secrets to use as they are set where configui runs, which the page doesn't know. Fields are the
// program's test fields.
type testRequest struct {
	Program     string            `json:"program"`
	Values      map[string]string `json:"values"`
	KeepSecrets map[string]bool   `json:"keepSecrets"`
	Fields      map[string]string `json:"fields"`
}

// testResponse is a test's outcome: OK if no check found a problem.
type testResponse struct {
	OK     bool    `json:"ok"`
	Checks []check `json:"checks"`
}

// check is one part of a test. Problems would stop the program or its callers; Warnings are worth
// knowing but may be fine; Passed says what was checked and found right. Skipped, if set, says why
// the check didn't run.
type check struct {
	Title    string   `json:"title"`
	Problems []string `json:"problems,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
	Passed   []string `json:"passed,omitempty"`
	Skipped  string   `json:"skipped,omitempty"`
}

func (s *server) test(w http.ResponseWriter, r *http.Request) {
	var req testRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	i := slices.IndexFunc(s.programs, func(p program) bool { return p.Settings.ID == req.Program })
	if i < 0 {
		http.Error(w, fmt.Sprintf("unknown program %q", req.Program), http.StatusBadRequest)
		return
	}
	p := s.programs[i]
	for name := range req.Values {
		if !validName(name) {
			http.Error(w, fmt.Sprintf("%q isn't a variable name: use letters, digits and underscores, not starting with a digit", name), http.StatusBadRequest)
			return
		}
		if v := p.Settings.External(name); v != nil {
			http.Error(w, fmt.Sprintf("%s is read by %s from the environment this page was started in, so the page can't change it: "+
				"set it in your shell or service definition and start the page again", name, v.External), http.StatusBadRequest)
			return
		}
	}
	env := pageEnv(s.env, p.Settings, req)
	resp := run(r.Context(), p, env, req.Fields)
	redactAll(&resp, redactions(p.Settings, env))
	writeJSON(w, resp)
}

// pageEnv is the environment a test runs with: base, with the page's values in place of every
// variable the program owns. An owned variable the page doesn't send is unset, unless it is a
// secret the page keeps. Variables the program doesn't own, such as HOME, stay as they are unless
// the page sends them. External variables stay as they are: their library reads them from the
// process's environment, which is base, whatever the test's env says.
func pageEnv(base settings.Env, p *settings.Program, req testRequest) settings.Map {
	env := settings.Map{}
	for _, kv := range base.Environ() {
		name, v, _ := strings.Cut(kv, "=")
		if !p.Owns(name) || p.External(name) != nil || (req.KeepSecrets[name] && p.IsSecret(name)) {
			env[name] = v
		}
	}
	for name, v := range req.Values {
		if req.KeepSecrets[name] && p.IsSecret(name) {
			continue
		}
		if strings.TrimSpace(v) == "" {
			delete(env, name)
		} else {
			env[name] = v
		}
	}
	return env
}

// validName reports whether name can be an environment variable's name in a shell.
func validName(name string) bool {
	if name == "" || (name[0] >= '0' && name[0] <= '9') {
		return false
	}
	return strings.Trim(name, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_") == ""
}

// run tests env for p: the settings, exactly as the program checks them at startup, then Temporal,
// then the program's own check.
func run(ctx context.Context, p program, env settings.Env, fields map[string]string) testResponse {
	settingsCheck := check{Title: "Settings"}
	if _, err := p.Settings.Load(env); err != nil {
		settingsCheck.Problems = strings.Split(err.Error(), "\n")
	} else {
		settingsCheck.Passed = []string{"Every variable is valid, as " + p.Settings.Command + " checks them at startup."}
	}
	resp := testResponse{Checks: []check{settingsCheck}}
	if p.ClientOptions != nil {
		resp.Checks = append(resp.Checks, checkTemporal(ctx, p, env))
	}
	if p.Check != nil {
		resp.Checks = append(resp.Checks, p.Check(ctx, env, fields))
	}
	resp.OK = !slices.ContainsFunc(resp.Checks, func(c check) bool { return len(c.Problems) > 0 })
	return resp
}

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
func redactAll(resp *testResponse, replacements []string) {
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

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write response: %v", err)
	}
}

// nonNil is s, or an empty list if s is nil, so the page gets [] rather than null.
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
