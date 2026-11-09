// Command configui serves a page for working out the connector's environment variables. It shows
// the variables set where it was started (passwords as asterisks), what each does and its default,
// and lets you change them and test them by reading the topic's last message. It sets the
// variables in its own process only; the page lists what to set for the streamer or Nexus worker.
//
//	go run ./configui              # then open http://localhost:8090
//	go run ./configui -addr localhost:9000
//
// It listens on localhost only: it handles credentials and connects wherever it is told to.
package main

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/temporalio/nexus-connector-kafka/kafka"
)

//go:embed index.html
var page []byte

// Time limits for a test: finding the topic, then reading its last message.
var (
	identifyTimeout = 10 * time.Second
	readTimeout     = 15 * time.Second
)

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
	log.Fatal(http.ListenAndServe(*addr, newServer()))
}

// server serves the page and its API. mu makes each test set the environment and use it alone.
type server struct {
	mu sync.Mutex
}

func newServer() http.Handler {
	s := &server{}
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

// settingsResponse is what the page loads: the programs and their variables, and the values set
// now. A secret's value is never sent, only whether it is set.
type settingsResponse struct {
	Programs   []program         `json:"programs"`
	Values     map[string]string `json:"values"`
	SecretsSet map[string]bool   `json:"secretsSet"`
}

func (s *server) settings(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, s.current())
}

func (s *server) current() settingsResponse {
	resp := settingsResponse{Programs: programs, Values: map[string]string{}, SecretsSet: map[string]bool{}}
	for name, v := range allVariables() {
		value := os.Getenv(name)
		switch {
		case v.Secret:
			resp.SecretsSet[name] = value != ""
		case value != "":
			resp.Values[name] = value
		}
	}
	return resp
}

// testRequest is the page's values for one program's variables. A blank value unsets the
// variable. KeepSecrets names the secrets to leave as they are, which the page doesn't know.
type testRequest struct {
	Program     string            `json:"program"`
	Values      map[string]string `json:"values"`
	KeepSecrets map[string]bool   `json:"keepSecrets"`
}

// testResponse is a test's outcome. Error is the whole error, for the page to show as it is.
type testResponse struct {
	OK       bool             `json:"ok"`
	Step     string           `json:"step,omitempty"` // which step failed
	Error    string           `json:"error,omitempty"`
	Cluster  string           `json:"cluster,omitempty"`
	Empty    bool             `json:"empty,omitempty"` // the topic has no messages
	Message  *message         `json:"message,omitempty"`
	Settings settingsResponse `json:"settings"` // the values now set, for the page to show
}

// message is a Kafka message for the page. Key and value are text, or base64 if they aren't UTF-8.
type message struct {
	Partition   int64             `json:"partition"`
	Offset      int64             `json:"offset"`
	Timestamp   string            `json:"timestamp"`
	Key         string            `json:"key"`
	KeyBase64   bool              `json:"keyBase64"`
	Value       string            `json:"value"`
	ValueBase64 bool              `json:"valueBase64"`
	Tombstone   bool              `json:"tombstone"`
	Headers     map[string]string `json:"headers,omitempty"`
}

func (s *server) test(w http.ResponseWriter, r *http.Request) {
	var req testRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	resp, err := s.apply(req)
	if err == nil {
		s.check(r.Context(), &resp)
	} else {
		resp = testResponse{Step: "Set the variables", Error: err.Error()}
	}
	resp.Settings = s.current()
	writeJSON(w, resp)
}

// apply sets the program's variables in this process's environment.
func (s *server) apply(req testRequest) (testResponse, error) {
	var prog *program
	for i := range programs {
		if programs[i].ID == req.Program {
			prog = &programs[i]
		}
	}
	if prog == nil {
		return testResponse{}, fmt.Errorf("unknown program %q", req.Program)
	}
	for _, v := range prog.Variables {
		if v.Secret && req.KeepSecrets[v.Name] {
			continue
		}
		value := strings.TrimSpace(req.Values[v.Name])
		var err error
		if value == "" {
			err = os.Unsetenv(v.Name)
		} else {
			err = os.Setenv(v.Name, value)
		}
		if err != nil {
			return testResponse{}, fmt.Errorf("set %s: %w", v.Name, err)
		}
	}
	return testResponse{}, nil
}

// check reads the Kafka settings the way both programs do, finds the topic, and reads its last
// message, stopping at the first step that fails.
func (s *server) check(ctx context.Context, resp *testResponse) {
	cfg, err := kafka.ConfigFromEnv()
	if err != nil {
		resp.Step, resp.Error = "Read the Kafka settings", err.Error()
		return
	}
	idCtx, cancel := context.WithTimeout(ctx, identifyTimeout)
	id, err := kafka.Identify(idCtx, cfg)
	cancel()
	if err != nil {
		resp.Step, resp.Error = "Connect to Kafka and find the topic", withTimeout(err, identifyTimeout)
		return
	}
	resp.Cluster = id.Cluster
	readCtx, cancel := context.WithTimeout(ctx, readTimeout)
	rec, ok, err := kafka.ReadLast(readCtx, cfg)
	cancel()
	if err != nil {
		resp.Step, resp.Error = "Read the topic's last message", withTimeout(err, readTimeout)
		return
	}
	resp.OK, resp.Empty = true, !ok
	if ok {
		m := &message{Partition: rec.Partition, Offset: rec.Offset, Timestamp: rec.Timestamp, Tombstone: rec.Tombstone}
		m.Key, m.KeyBase64 = text(rec.Key)
		m.Value, m.ValueBase64 = text(rec.Value)
		for _, h := range rec.Headers {
			if m.Headers == nil {
				m.Headers = map[string]string{}
			}
			m.Headers[h.Key], _ = text(h.Value)
		}
		resp.Message = m
	}
}

// withTimeout is err's text, saying so when the time limit ran out, which Kafka's client reports
// only as a deadline.
func withTimeout(err error, limit time.Duration) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Sprintf("%v (no answer within %s: check that the brokers are right and reachable)", err, limit)
	}
	return err.Error()
}

func text(b []byte) (string, bool) {
	if utf8.Valid(b) {
		return string(b), false
	}
	return base64.StdEncoding.EncodeToString(b), true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write response: %v", err)
	}
}
