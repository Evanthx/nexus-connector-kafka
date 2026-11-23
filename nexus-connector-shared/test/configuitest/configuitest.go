// Package configuitest is for tests only: helpers for testing a repo's setup page (the configui
// package) through its API.
package configuitest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/configui"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
)

// BaseEnv is the environment configui was started in, for a test: vars, nothing of the developer's
// own, and no Temporal profile file.
func BaseEnv(t *testing.T, vars settings.Map) settings.Map {
	t.Helper()
	env := settings.Map{"TEMPORAL_CONFIG_FILE": filepath.Join(t.TempDir(), "none.toml")}
	for k, v := range vars {
		env[k] = v
	}
	return env
}

// WithTemporalTimeout is a copy of programs whose Temporal checks give up after d, for a test that
// connects to an address nothing answers on.
func WithTemporalTimeout(programs []configui.Program, d time.Duration) []configui.Program {
	out := make([]configui.Program, len(programs))
	for i, p := range programs {
		p.TemporalTimeout = d
		out[i] = p
	}
	return out
}

// Get sends a GET for path to h, as from localhost.
func Get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://localhost:8090"+path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// Post sends body to h's /api/test as JSON, as from localhost.
func Post(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "http://localhost:8090/api/test", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// PostTest posts req and returns the response, raw and decoded.
func PostTest(t *testing.T, h http.Handler, req configui.TestRequest) (string, configui.TestResponse) {
	t.Helper()
	b, err := json.Marshal(req)
	require.NoError(t, err)
	rec := Post(t, h, string(b))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp configui.TestResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	return rec.Body.String(), resp
}

// CheckTitled is resp's check titled title, failing the test if there is none.
func CheckTitled(t *testing.T, resp configui.TestResponse, title string) configui.Check {
	t.Helper()
	for _, c := range resp.Checks {
		if c.Title == title {
			return c
		}
	}
	require.Failf(t, "no check", "%q in %+v", title, resp.Checks)
	return configui.Check{}
}
