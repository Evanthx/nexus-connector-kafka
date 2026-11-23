package configui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

// This file is POST /api/test: the Test, run for one program with the page's values.

// TestRequest is the page's values for one program. Values holds every variable the page shows or
// added, a blank value meaning unset, and a secret only if one was typed. KeepSecrets names the
// secrets to use as they are set where configui runs, which the page doesn't know. Fields are the
// program's test fields.
type TestRequest struct {
	Program     string            `json:"program"`
	Values      map[string]string `json:"values"`
	KeepSecrets map[string]bool   `json:"keepSecrets"`
	Fields      map[string]string `json:"fields"`
}

// TestResponse is a test's outcome: OK if no check found a problem.
type TestResponse struct {
	OK     bool    `json:"ok"`
	Checks []Check `json:"checks"`
}

// Check is one part of a test. Problems would stop the program or its callers; Warnings are worth
// knowing but may be fine; Passed says what was checked and found right. Skipped, if set, says why
// the Check didn't run.
type Check struct {
	Title    string   `json:"title"`
	Problems []string `json:"problems,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
	Passed   []string `json:"passed,omitempty"`
	Skipped  string   `json:"skipped,omitempty"`
}

func (s *server) test(w http.ResponseWriter, r *http.Request) {
	var req TestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	i := slices.IndexFunc(s.programs, func(p Program) bool { return p.Settings.ID == req.Program })
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
func pageEnv(base settings.Env, p *settings.Program, req TestRequest) settings.Map {
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
func run(ctx context.Context, p Program, env settings.Env, fields map[string]string) TestResponse {
	settingsCheck := Check{Title: "Settings"}
	if _, err := p.Settings.Load(env); err != nil {
		settingsCheck.Problems = strings.Split(err.Error(), "\n")
	} else {
		settingsCheck.Passed = []string{"Every variable is valid, as " + p.Settings.Command + " checks them at startup."}
	}
	resp := TestResponse{Checks: []Check{settingsCheck}}
	if p.ClientOptions != nil {
		resp.Checks = append(resp.Checks, checkTemporal(ctx, p, env))
	}
	if p.Check != nil {
		resp.Checks = append(resp.Checks, p.Check(ctx, env, fields))
	}
	resp.OK = !slices.ContainsFunc(resp.Checks, func(c Check) bool { return len(c.Problems) > 0 })
	return resp
}
