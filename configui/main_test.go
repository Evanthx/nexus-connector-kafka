package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// clearEnv unsets every variable the page knows, for the test, so the developer's own don't leak in.
func clearEnv(t *testing.T) {
	for name := range allVariables() {
		t.Setenv(name, "") // restores the old value after the test
	}
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://localhost:8090"+path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func postTest(t *testing.T, h http.Handler, body testRequest) testResponse {
	t.Helper()
	b, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "http://localhost:8090/api/test", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp testResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	return resp
}

func TestSettings_ShowsWhatIsSetButNeverASecret(t *testing.T) {
	clearEnv(t)
	t.Setenv("KAFKA_TOPIC", "orders")
	t.Setenv("KAFKA_PASSWORD", "hunter2-secret")

	rec := get(t, newServer(), "/api/settings")
	require.Equal(t, http.StatusOK, rec.Code)
	require.NotContains(t, rec.Body.String(), "hunter2-secret")

	var resp settingsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, map[string]string{"KAFKA_TOPIC": "orders"}, resp.Values)
	require.True(t, resp.SecretsSet["KAFKA_PASSWORD"])
	require.False(t, resp.SecretsSet["TEMPORAL_API_KEY"])
}

func TestTest_ReadsTheLastMessage(t *testing.T) {
	clearEnv(t)
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "orders"))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	brokers := strings.Join(cluster.ListenAddrs(), ",")
	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	require.NoError(t, err)
	require.NoError(t, cl.ProduceSync(context.Background(),
		&kgo.Record{Topic: "orders", Value: []byte("first")},
		&kgo.Record{Topic: "orders", Key: []byte("k"), Value: []byte("last order")}).FirstErr())
	cl.Close()

	h := newServer()
	req := testRequest{Program: "connector", Values: map[string]string{"KAFKA_BROKERS": brokers, "KAFKA_TOPIC": "orders"}}
	for range 2 { // the same message each time
		resp := postTest(t, h, req)
		require.True(t, resp.OK, resp.Error)
		require.NotNil(t, resp.Message)
		require.Equal(t, "last order", resp.Message.Value)
		require.Equal(t, "k", resp.Message.Key)
		require.EqualValues(t, 1, resp.Message.Offset)
	}
	require.Equal(t, "orders", resp(t, h).Values["KAFKA_TOPIC"], "the variables stay set in the server")
}

func resp(t *testing.T, h http.Handler) settingsResponse {
	t.Helper()
	var s settingsResponse
	require.NoError(t, json.Unmarshal(get(t, h, "/api/settings").Body.Bytes(), &s))
	return s
}

func TestTest_ShowsTheWholeError(t *testing.T) {
	clearEnv(t)
	resp := postTest(t, newServer(), testRequest{Program: "relay", Values: map[string]string{
		"KAFKA_BROKERS": "localhost:1", "KAFKA_TOPIC": "orders", "KAFKA_START_FROM": "yesterday",
	}})
	require.False(t, resp.OK)
	require.Equal(t, "Read the Kafka settings", resp.Step)
	require.Equal(t, `KAFKA_START_FROM must be oldest or newest, not "yesterday"`, resp.Error)
}

func TestTest_KeepsAnUnchangedSecretAndUnsetsBlanks(t *testing.T) {
	clearEnv(t)
	t.Setenv("KAFKA_PASSWORD", "kept-secret")
	t.Setenv("TEMPORAL_API_KEY", "old-key")
	t.Setenv("KAFKA_GROUP", "old-group")

	postTest(t, newServer(), testRequest{
		Program:     "relay",
		Values:      map[string]string{"KAFKA_TOPIC": "orders", "TEMPORAL_API_KEY": ""},
		KeepSecrets: map[string]bool{"KAFKA_PASSWORD": true},
	})
	require.Equal(t, "kept-secret", os.Getenv("KAFKA_PASSWORD"))
	require.Empty(t, os.Getenv("TEMPORAL_API_KEY"), "a secret not kept is replaced by its value, here blank")
	require.Empty(t, os.Getenv("KAFKA_GROUP"), "a blank value unsets the variable")
	require.Equal(t, "orders", os.Getenv("KAFKA_TOPIC"))
}

func TestLocalOnly(t *testing.T) {
	h := newServer()

	req := httptest.NewRequest(http.MethodGet, "http://evil.example:8090/api/settings", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code, "a Host other than localhost is refused")

	req = httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8090/api/test", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "text/plain")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnsupportedMediaType, rec.Code, "a POST that isn't JSON is refused")

	require.Equal(t, http.StatusOK, get(t, h, "/").Code)
}
