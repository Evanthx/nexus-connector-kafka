// Command gendocs writes the tables of environment variables in docs/setup.md, between the
// settings.StartMarker and settings.EndMarker lines, from the programs' settings lists. Run it
// from the repository root, through `make docs`, after changing a variable; a test fails while
// docs/setup.md is out of date.
package main

import (
	"flag"
	"log"
	"os"

	"github.com/temporalio/nexus-connector-kafka/settings"
	"github.com/temporalio/nexus-connector-kafka/setup"
)

// programs are the programs whose variables the docs list, in order.
var programs = []*settings.Program{setup.Relay, setup.Connector}

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
	if err := os.WriteFile(*path, []byte(updated), 0o644); err != nil {
		log.Fatal(err)
	}
}
