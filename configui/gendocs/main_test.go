package main

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-kafka/settings"
)

// docs/setup.md's tables of variables must match the settings lists: run `make docs`.
func TestDocsUpToDate(t *testing.T) {
	doc, err := os.ReadFile("../../docs/setup.md")
	require.NoError(t, err)
	_, rest, ok := strings.Cut(string(doc), settings.StartMarker+"\n")
	require.True(t, ok, "no %s line", settings.StartMarker)
	current, _, ok := strings.Cut(rest, settings.EndMarker)
	require.True(t, ok, "no %s line", settings.EndMarker)
	require.Equal(t, settings.Markdown(programs...), current, "docs/setup.md is out of date: run make docs")
}
