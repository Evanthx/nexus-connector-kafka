package settings

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// envReaders are the functions that read the process's environment, by import path.
var envReaders = map[string][]string{
	"os":      {"Getenv", "LookupEnv", "Environ", "ExpandEnv"},
	"syscall": {"Getenv", "Environ"},
}

// mayReadEnv are the files, from the repository root, allowed to call an envReader: the one that
// implements OS.
var mayReadEnv = map[string]bool{"settings/env.go": true}

// TestNoBypass fails if any Go file outside tests, generated code and mayReadEnv reads the
// environment directly. Every variable must go through a Program, so the setup page, its Test and
// the docs know about it. The Temporal SDK's own reads (envconfig) are fine: they aren't in this
// repository.
func TestNoBypass(t *testing.T) {
	root := ".."
	var bypasses []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			name := d.Name()
			if rel != "." && (strings.HasPrefix(name, ".") || name == "generated" || name == "testdata" || name == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") || mayReadEnv[rel] {
			return nil
		}
		bypasses = append(bypasses, envReads(t, path, rel)...)
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, bypasses, "read the environment through the settings package (a Program, Typed, Family or settings.Get), not directly")
}

// envReads are the calls to an envReader in the file at path, as "rel:line: pkg.Func".
func envReads(t *testing.T, path, rel string) []string {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	require.NoError(t, err, path)
	if ast.IsGenerated(f) {
		return nil
	}
	local := map[string]string{} // name the file imports a package as → its path
	for _, imp := range f.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		if _, ok := envReaders[p]; !ok {
			continue
		}
		name := filepath.Base(p)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		local[name] = p
	}
	var found []string
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || local[pkg.Name] == "" {
			return true
		}
		for _, fn := range envReaders[local[pkg.Name]] {
			if sel.Sel.Name == fn {
				found = append(found, rel+":"+strconv.Itoa(fset.Position(sel.Pos()).Line)+": "+pkg.Name+"."+fn)
			}
		}
		return true
	})
	return found
}
