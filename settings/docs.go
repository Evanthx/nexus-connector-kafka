package settings

import (
	"errors"
	"fmt"
	"strings"
)

// The generated tables go between these lines of a Markdown file; ReplaceGenerated replaces what
// is between them and keeps the rest.
const (
	StartMarker = "<!-- settings:start -->"
	EndMarker   = "<!-- settings:end -->"
)

// Markdown documents programs' variables: for each section, a heading and a table of its
// variables and families' members, with each family's help above the table. With more than one
// program, each program's sections follow a line naming it.
func Markdown(programs ...*Program) string {
	var b strings.Builder
	for _, p := range programs {
		if len(programs) > 1 {
			fmt.Fprintf(&b, "**%s** (`%s`):\n\n", p.Name, p.Command)
		}
		for _, s := range p.Sections {
			fmt.Fprintf(&b, "### %s\n\n", s.Title)
			for _, f := range s.Families {
				fmt.Fprintf(&b, "%s\n\n", withDocs(f.Help, f.Docs))
			}
			b.WriteString("| Variable | Required | Default | Meaning |\n|---|---|---|---|\n")
			for _, v := range s.Variables {
				row(&b, v.Name, v.Secret, v.Required, v.Default, withDocs(external(v)+v.Help, v.Docs))
			}
			for _, f := range s.Families {
				for _, m := range f.Members {
					row(&b, f.Name("<NAME>", m), m.Secret, m.Required, "", m.Help)
				}
			}
			b.WriteString("\n")
		}
	}
	return b.String()
}

func row(b *strings.Builder, name string, secret bool, required, def, help string) {
	if secret {
		name = "`" + name + "` (secret)"
	} else {
		name = "`" + name + "`"
	}
	fmt.Fprintf(b, "| %s | %s | %s | %s |\n", name, cell(required), cell(def), cell(help))
}

// cell makes s fit in one table cell.
func cell(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "|", `\|`)
}

// external says, for an External variable, what reads it and under which other names.
func external(v *Variable) string {
	if v.External == "" {
		return ""
	}
	s := "*Read by " + v.External + " from the process's environment"
	if len(v.Aliases) > 0 {
		s += "; also as `" + strings.Join(v.Aliases, "`, `") + "` when unset or empty"
	}
	return s + ".* "
}

func withDocs(help, docs string) string {
	if docs == "" {
		return help
	}
	return help + " " + docs
}

// ReplaceGenerated returns doc with what is between StartMarker and EndMarker replaced by
// generated.
func ReplaceGenerated(doc, generated string) (string, error) {
	before, rest, ok := strings.Cut(doc, StartMarker+"\n")
	if !ok {
		return "", errors.New("no " + StartMarker + " line")
	}
	_, after, ok := strings.Cut(rest, EndMarker)
	if !ok {
		return "", errors.New("no " + EndMarker + " after " + StartMarker)
	}
	return before + StartMarker + "\n" + generated + EndMarker + after, nil
}
