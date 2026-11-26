package configui_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/codec"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/configui"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/temporalclient"
	ct "github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/test/configuitest"
	"go.temporal.io/sdk/contrib/envconfig"
)

// The tests here are for what every repo's setup page shares. They use two made-up programs; each
// repo's configui tests its own programs and checks.

var (
	token = settings.Define(settings.Variable{Name: "TOKEN", Help: "The token.", Required: "the token", Secret: true},
		func(v string) (string, error) {
			if v == "" {
				return "", errors.New("set TOKEN: the token")
			}
			return v, nil
		})
	size = settings.Define(settings.Variable{Name: "SIZE", Help: "The size.", Default: "1"},
		func(v string) (string, error) {
			if v != "" && strings.Trim(v, "0123456789") != "" {
				return "", errors.New("SIZE must be a number, not " + v)
			}
			return v, nil
		})
	config = settings.Define(settings.Variable{
		Name: "CONFIG", Help: "The config, as JSON.", Multiline: true, Example: "{\n  \"a\": 1\n}", HelpURL: "https://example.com/config",
	}, settings.Text)
	network = &settings.Section{Title: "Network", Variables: []*settings.Variable{
		{Name: "HTTPS_PROXY", Help: "The proxy.", Default: "none", External: "gRPC", Aliases: []string{"https_proxy"}, Mask: settings.MaskURLPassword},
		{Name: "NO_PROXY", Help: "No proxy for these.", Default: "none", External: "gRPC", Aliases: []string{"no_proxy"}},
	}}
	temporal = settings.Temporal{Namespace: temporalclient.DefaultNamespace, NamespaceHelp: "The namespace."}.Section()

	outbound = &settings.Program{ID: "outbound", Name: "Outbound Connector", Command: "go run ./outbound", Sections: []*settings.Section{
		{Title: "Outbound Connector", Variables: []*settings.Variable{token.Variable, size.Variable}}, temporal, network,
	}}
	inbound = &settings.Program{ID: "inbound", Name: "Inbound Connector", Command: "go run ./inbound", Sections: []*settings.Section{
		{Title: "Inbound Connector", Variables: []*settings.Variable{size.Variable, config.Variable}}, temporal, network,
	}}

	programs = []configui.Program{
		// Nothing answers at the addresses the tests use: give up quickly.
		{Settings: inbound, ClientOptions: temporalclient.Options, TemporalTimeout: time.Second},
		{Settings: outbound, ClientOptions: temporalclient.Options, TaskQueue: func(settings.Env) string { return "q" }, TemporalTimeout: time.Second},
	}
)

func newServer(t *testing.T, vars settings.Map) http.Handler {
	t.Helper()
	return configui.NewServer("Test setup", ct.BaseEnv(t, vars), programs)
}

// The page's checks go through the programs' own clients: configui makes none of its own.
func TestNoOwnClient(t *testing.T) {
	ct.RequireNoOwnClient(t, ".")
}

func TestLocalOnly(t *testing.T) {
	h := newServer(t, nil)

	req := httptest.NewRequest(http.MethodGet, "http://evil.example:8090/api/settings", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code, "a Host other than localhost is refused")

	req = httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8090/api/test", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "text/plain")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnsupportedMediaType, rec.Code, "a POST that isn't JSON is refused")

	require.Equal(t, http.StatusOK, ct.Get(t, h, "/").Code)
	require.Equal(t, http.StatusOK, ct.Get(t, h, "/api/settings").Code)
}

func TestTest_RefusesABadRequest(t *testing.T) {
	h := newServer(t, nil)
	for body, want := range map[string]string{
		`{"program":`:       "bad request",
		`{"program":"web"}`: `unknown program "web"`,
		`{"program":"inbound","values":{"A=B":"x"}}`:   `"A=B" isn't a variable name`,
		`{"program":"outbound","values":{"1ABC":"x"}}`: `"1ABC" isn't a variable name`,
	} {
		rec := ct.Post(t, h, body)
		require.Equal(t, http.StatusBadRequest, rec.Code, body)
		require.Contains(t, rec.Body.String(), want, body)
	}
}

func TestValidName(t *testing.T) {
	for _, ok := range []string{"A", "_A", "a1_B"} {
		require.True(t, configui.ValidName(ok), ok)
	}
	for _, bad := range []string{"", "1A", "A-B", "A=B", "A B", "Ä"} {
		require.False(t, configui.ValidName(bad), bad)
	}
}

// The page's values replace every variable the program owns; others pass through.
func TestPageEnv(t *testing.T) {
	base := settings.Map{
		"HOME":                          "/home/x",
		"HTTPS_PROXY":                   "http://proxy.example:8080", // external: the page can't change it
		"TEMPORAL_TLS_CLIENT_CERT_PATH": "/cert",                     // owned, not sent: unset
		"SIZE":                          "9",
		"TOKEN":                         "kept",
	}
	env := configui.PageEnv(base, outbound, configui.TestRequest{
		Values: map[string]string{
			"SIZE":  "  ",
			"TOKEN": "ignored: kept",
			"EXTRA": "added",
		},
		KeepSecrets: map[string]bool{"TOKEN": true, "HOME": true},
	})
	require.Equal(t, settings.Map{
		"HOME":        "/home/x",
		"HTTPS_PROXY": "http://proxy.example:8080",
		"TOKEN":       "kept",
		"EXTRA":       "added",
	}, env)
}

// The page gets every program and variable, never a secret's value, under the JSON field names
// index.html reads.
func TestSettings_JSON(t *testing.T) {
	h := newServer(t, settings.Map{
		"SIZE":             "3",
		"TOKEN":            "token_never_sent",
		"TEMPORAL_API_KEY": "temporal_key_never_sent",
		"https_proxy":      "http://user:proxy_pw_never_sent@proxy.example:8080",
	})
	rec := ct.Get(t, h, "/api/settings")
	require.Equal(t, http.StatusOK, rec.Code)
	require.NotContains(t, rec.Body.String(), "never_sent")

	var page configui.Page
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
	require.Equal(t, "Test setup", page.Title)
	require.Len(t, page.Programs, 2)
	require.Equal(t, "3", page.Values["SIZE"])
	require.True(t, page.SecretsSet["TOKEN"])
	require.True(t, page.SecretsSet["TEMPORAL_API_KEY"])
	require.Equal(t, map[string]configui.PageExternal{
		"HTTPS_PROXY": {Set: true, From: "https_proxy", Value: "http://user:****@proxy.example:8080"},
		"NO_PROXY":    {},
	}, page.External)
	require.Equal(t, &configui.PageProfile{
		File: page.TemporalProfile.File, Profile: "default", Variable: "TEMPORAL_CONFIG_FILE",
		Values: map[string]string{}, SecretsSet: map[string]bool{},
	}, page.TemporalProfile, "no file")

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	for _, key := range []string{"title", "programs", "values", "secretsSet", "external", "temporalProfile"} {
		require.Contains(t, raw, key)
	}
	variable := raw["programs"].([]any)[0].(map[string]any)["sections"].([]any)[0].(map[string]any)["variables"].([]any)[0].(map[string]any)
	for _, key := range []string{"name", "help", "default", "required", "secret", "external", "aliases", "multiline", "example", "helpURL"} {
		require.Contains(t, variable, key)
	}
	// A variable's own text area, example and help link, for a value such as JSON.
	cfg := page.Programs[0].Sections[0].Variables[1]
	require.Equal(t, "CONFIG", cfg.Name)
	require.True(t, cfg.Multiline)
	require.Equal(t, "{\n  \"a\": 1\n}", cfg.Example)
	require.Equal(t, "https://example.com/config", cfg.HelpURL)
	for _, key := range []string{"file", "profile", "variable", "exists", "found", "error", "values", "secretsSet"} {
		require.Contains(t, raw["temporalProfile"], key)
	}
	for _, key := range []string{"set", "from", "value"} {
		require.Contains(t, raw["external"].(map[string]any)["HTTPS_PROXY"], key)
	}
}

// Every variable envconfig reads is in the Temporal section, and every setting of a profile maps
// to one of them.
func TestTemporalSection_ListsWhatEnvconfigReads(t *testing.T) {
	vars := configui.ProfileVariables(&envconfig.ClientConfigProfile{
		Address: "a", Namespace: "n", APIKey: "k", Authority: "au", GRPCMeta: map[string]string{"x-tenant": "t"},
		TLS: &envconfig.ClientConfigTLS{
			ClientCertPath: "c", ClientCertData: []byte("c"), ClientKeyPath: "k", ClientKeyData: []byte("k"),
			ServerCACertPath: "ca", ServerCACertData: []byte("ca"), ServerName: "s", DisableHostVerification: true,
		},
	})
	for name, value := range vars {
		require.NotEmpty(t, value, name)
		require.True(t, outbound.Owns(name), "%s isn't in the Temporal section", name)
	}
	for _, name := range []string{
		"TEMPORAL_ADDRESS", "TEMPORAL_NAMESPACE", "TEMPORAL_API_KEY", "TEMPORAL_CLIENT_AUTHORITY", "TEMPORAL_TLS",
		"TEMPORAL_TLS_CLIENT_CERT_PATH", "TEMPORAL_TLS_CLIENT_CERT_DATA", "TEMPORAL_TLS_CLIENT_KEY_PATH",
		"TEMPORAL_TLS_CLIENT_KEY_DATA", "TEMPORAL_TLS_SERVER_CA_CERT_PATH", "TEMPORAL_TLS_SERVER_CA_CERT_DATA",
		"TEMPORAL_TLS_SERVER_NAME", "TEMPORAL_TLS_DISABLE_HOST_VERIFICATION", "TEMPORAL_GRPC_META_X_TENANT",
	} {
		require.Contains(t, vars, name)
		require.True(t, outbound.Owns(name), name)
	}
	for _, name := range []string{"TEMPORAL_PROFILE", "TEMPORAL_CONFIG_FILE", codec.EndpointEnvVar, codec.AuthEnvVar} {
		require.True(t, outbound.Owns(name), name)
	}
	for _, secret := range []string{"TEMPORAL_API_KEY", "TEMPORAL_TLS_CLIENT_KEY_DATA", "TEMPORAL_GRPC_META_X_TENANT", codec.AuthEnvVar} {
		require.True(t, outbound.IsSecret(secret), secret)
	}
}

// A value from the Temporal profile file is shown with the file and profile, a secret only as set;
// a gRPC header it sets is an instance; and the Test says which profile it read.
func TestSettings_TemporalProfile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "temporal.toml")
	require.NoError(t, os.WriteFile(file, []byte(`
[profile.default]
address = "wrong:7233"

[profile.staging]
address = "staging.example:7233"
namespace = "from-file"
api_key = "file_key_never_sent"
grpc_meta = { x-tenant = "acme" }

[profile.staging.tls]
server_name = "staging.example"
`), 0o600))
	base := settings.Map{"TEMPORAL_CONFIG_FILE": file, "TEMPORAL_PROFILE": "staging", "TEMPORAL_NAMESPACE": "from-env"}
	h := configui.NewServer("Test setup", base, programs)
	rec := ct.Get(t, h, "/api/settings")
	require.NotContains(t, rec.Body.String(), "file_key_never_sent")
	require.NotContains(t, rec.Body.String(), "acme")
	var page configui.Page
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
	require.Equal(t, &configui.PageProfile{
		File: file, Profile: "staging", Variable: "TEMPORAL_CONFIG_FILE", Exists: true, Found: true,
		Values: map[string]string{
			"TEMPORAL_ADDRESS": "staging.example:7233", "TEMPORAL_NAMESPACE": "from-file",
			"TEMPORAL_TLS": "true", "TEMPORAL_TLS_SERVER_NAME": "staging.example",
		},
		SecretsSet: map[string]bool{"TEMPORAL_API_KEY": true, "TEMPORAL_GRPC_META_X_TENANT": true},
	}, page.TemporalProfile)
	require.Equal(t, "from-env", page.Values["TEMPORAL_NAMESPACE"], "the environment's value is shown as the value; the file's as a note")
	var meta configui.PageFamily
	for _, s := range page.Programs[1].Sections {
		for _, f := range s.Families {
			if f.Prefix == "TEMPORAL_GRPC_META_" {
				meta = f
			}
		}
	}
	require.Equal(t, []string{"X_TENANT"}, meta.Instances, "a header the file sets is shown, so a value can override it")

	raw, resp := ct.PostTest(t, h, configui.TestRequest{Program: "outbound", Values: map[string]string{
		"TEMPORAL_CONFIG_FILE": file, "TEMPORAL_PROFILE": "staging", "TEMPORAL_ADDRESS": "127.0.0.1:1", "TOKEN": "page-token",
	}})
	require.NotContains(t, raw, "file_key_never_sent")
	temporal := ct.CheckTitled(t, resp, "Temporal")
	require.Equal(t, []string{`Read profile "staging" from ` + file + `; it sets TEMPORAL_ADDRESS, TEMPORAL_API_KEY, ` +
		`TEMPORAL_GRPC_META_X_TENANT, TEMPORAL_NAMESPACE, TEMPORAL_TLS, TEMPORAL_TLS_SERVER_NAME, each overridden by its variable if set.`},
		temporal.Passed)
	require.Contains(t, temporal.Problems[0], "Failed to connect to Temporal at 127.0.0.1:1", "the page's address, not the file's")

	_, resp = ct.PostTest(t, h, configui.TestRequest{Program: "outbound", Values: map[string]string{
		"TEMPORAL_CONFIG_FILE": file, "TEMPORAL_PROFILE": "missing", "TOKEN": "page-token",
	}})
	require.Contains(t, ct.CheckTitled(t, resp, "Temporal").Problems[0], "unable to find profile missing")
}

// The Settings check reports exactly what the program's Load does at startup, every problem at once.
func TestTest_SettingsAsStartupDoes(t *testing.T) {
	values := map[string]string{"SIZE": "many", "TEMPORAL_ADDRESS": "127.0.0.1:1"}
	_, resp := ct.PostTest(t, newServer(t, nil), configui.TestRequest{Program: "outbound", Values: values})
	require.False(t, resp.OK)
	_, startupErr := outbound.Load(settings.Map(values))
	require.Error(t, startupErr)
	require.Equal(t, strings.Split(startupErr.Error(), "\n"), ct.CheckTitled(t, resp, "Settings").Problems)
	require.Len(t, ct.CheckTitled(t, resp, "Settings").Problems, 2)
}

// External variables come from where configui runs: the page can't send them, and a proxy's
// password never appears in the Test's output, though a library's error quotes the URL.
func TestTest_ExternalVariables(t *testing.T) {
	h := newServer(t, nil)
	for _, name := range []string{"HTTPS_PROXY", "https_proxy", "NO_PROXY"} {
		rec := ct.Post(t, h, `{"program":"outbound","values":{"`+name+`":"http://x"}}`)
		require.Equal(t, http.StatusBadRequest, rec.Code, name)
		require.Contains(t, rec.Body.String(), name+" is read by gRPC", name)
	}

	resp := configui.TestResponse{Checks: []configui.Check{{
		Title:    "Outbound Connector",
		Problems: []string{`proxyconnect tcp: invalid proxy address "http://u:pw%2Fsecret@proxy:bad": parse error`, "pw/secret alone"},
	}}}
	env := settings.Map{"HTTPS_PROXY": "http://u:pw%2Fsecret@proxy:bad"}
	configui.RedactAll(&resp, configui.Redactions(outbound, env))
	require.Equal(t, []string{`proxyconnect tcp: invalid proxy address "http://u:****@proxy:bad": parse error`, "[REDACTED] alone"},
		resp.Checks[0].Problems)
}
