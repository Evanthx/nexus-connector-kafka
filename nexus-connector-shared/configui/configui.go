// Package configui serves the setup page, a page for working out a program's environment variables. It shows
// every variable the program reads, from its settings list, with its help, default and the
// value set where configui was started (secrets only as set or not set), lets you change and
// add them, and tests them: every check the program makes at startup, the connection to
// Temporal, and the program's own checks. It lists the `export` lines to set. It never changes
// its own environment and never stores what you type.
//
// Each repo runs it from its own configui command, whose connector.go lists the repo's programs and
// their own checks:
//
//	go run ./configui                          # then open http://localhost:8090
//	go run ./configui --addr localhost:9000
//
// It listens on localhost only: it handles credentials and connects wherever it is told to.
package configui

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"go.temporal.io/sdk/client"
)

//go:embed index.html
var page []byte

// Run serves the page titled title for programs, on the address in args' -addr flag, until it
// fails. args are the command's arguments, without its name.
func Run(title string, args []string, programs []Program) error {
	flags := flag.NewFlagSet("configui", flag.ContinueOnError)
	addr := flags.String("addr", "localhost:8090", "address to listen on; keep it on localhost")
	if err := flags.Parse(args); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(*addr)
	if err != nil {
		return fmt.Errorf("-addr: %w", err)
	}
	if !loopback(host) {
		return fmt.Errorf("-addr must be on localhost, not %q: the page handles credentials", host)
	}
	log.Printf("Open http://%s", *addr)
	srv := &http.Server{Addr: *addr, Handler: NewServer(title, settings.OS(), programs), ReadHeaderTimeout: 10 * time.Second}
	return srv.ListenAndServe()
}

// Program is one of the repo's programs, as the page shows and tests it. Each repo's
// configui/connector.go lists them.
type Program struct {
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
	// CallsEndpoint is the name of the Nexus endpoint the program calls, given env, such as the
	// Inbound Connector's; the Temporal check confirms it exists. nil, or "" from it, if the
	// program calls none.
	CallsEndpoint func(env settings.Env) string
	// TestFields are extra inputs for the Test, such as a URL to try. Check gets their values.
	TestFields []TestField
	// Check is the program's own check after the settings and Temporal: whatever the program can
	// try without running, such as a request through its real code. nil if it has none.
	Check func(ctx context.Context, env settings.Env, fields map[string]string) Check
	// TemporalTimeout bounds the Temporal check: connecting, finding the namespace and the
	// endpoints. 0 for 10 seconds.
	TemporalTimeout time.Duration
}

// TestField is an extra input for the Test. Kind is "text", or "instance": a choice of the
// family's instances on the page (Family is its prefix), or none.
type TestField struct {
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
	title    string
	env      settings.Env
	programs []Program
}

// NewServer is the page and its API for programs, titled title, showing and testing env with the
// page's values in place of the program's.
func NewServer(title string, env settings.Env, programs []Program) http.Handler {
	s := &server{title: title, env: env, programs: programs}
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
