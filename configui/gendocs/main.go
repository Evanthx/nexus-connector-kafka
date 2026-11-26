// Command gendocs writes the tables of environment variables in docs/setup.md, between the
// settings.StartMarker and settings.EndMarker lines, from the programs' settings lists, and the
// table of the Inbound Connector's route fields, between RoutesStart and RoutesEnd, from
// setup.RouteFields. Run it
// from the repository root, through `make docs`, after changing a variable; a test fails while
// docs/setup.md is out of date.
package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"strings"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-kafka/setup"
)

// The route fields' table goes between these lines.
const (
	RoutesStart = "<!-- routes:start -->"
	RoutesEnd   = "<!-- routes:end -->"
)

// programs are the programs whose variables the docs list, in order.
var programs = []*settings.Program{setup.Inbound, setup.Outbound}

func main() {
	path := flag.String("file", "docs/setup.md", "the Markdown file to update")
	flag.Parse()
	doc, err := os.ReadFile(*path)
	if err != nil {
		log.Fatal(err)
	}
	updated, err := settings.ReplaceGenerated(string(doc), settings.Markdown(programs...))
	if err != nil {
		log.Fatalf("%s: %v", *path, err)
	}
	updated, err = replaceRoutes(updated)
	if err != nil {
		log.Fatalf("%s: %v", *path, err)
	}
	if err := os.WriteFile(*path, []byte(updated), 0o644); err != nil {
		log.Fatal(err)
	}
}

// replaceRoutes returns doc with the route fields' table, between RoutesStart and RoutesEnd,
// regenerated.
func replaceRoutes(doc string) (string, error) {
	before, rest, ok := strings.Cut(doc, RoutesStart+"\n")
	if !ok {
		return "", errors.New("no " + RoutesStart + " line")
	}
	_, after, ok := strings.Cut(rest, RoutesEnd)
	if !ok {
		return "", errors.New("no " + RoutesEnd + " after " + RoutesStart)
	}
	return before + RoutesStart + "\n" + setup.RoutesMarkdown() + RoutesEnd + after, nil
}
