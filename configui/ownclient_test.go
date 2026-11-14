package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// clientMakers are what makes or is a client for the outside world in net/http, as "import
// path.Name". The page's checks must go through the connector's own construction (NewWithEnv and
// the like), so that the Test sends what the connector would, with its settings, timeouts and
// redirect and secret handling, rather than through a client of the page's own.
var clientMakers = []string{
	"net/http.Client", "net/http.Transport", "net/http.DefaultClient", "net/http.DefaultTransport",
	"net/http.Get", "net/http.Head", "net/http.Post", "net/http.PostForm",
}

// TestNoOwnClient fails if a non-test Go file of configui names one of clientMakers, or one of
// connectorClientMakers, which each repo's main_test.go lists for the client library its connector
// uses (empty for one that uses net/http alone).
func TestNoOwnClient(t *testing.T) {
	forbidden := map[string]bool{}
	for _, m := range append(clientMakers, connectorClientMakers...) {
		forbidden[m] = true
	}
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var found []string
	for _, path := range files {
		if !strings.HasSuffix(path, "_test.go") {
			found = append(found, namedIn(t, path, forbidden)...)
		}
	}
	require.Empty(t, found, "configui must check through the connector's own client construction, not make its own client")
}

// namedIn are the uses of a forbidden "import path.Name" in the Go file at path, as
// "path:line: name".
func namedIn(t *testing.T, path string, forbidden map[string]bool) []string {
	src, err := os.ReadFile(path)
	require.NoError(t, err)
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.SkipObjectResolution)
	require.NoError(t, err, path)
	imports := map[string]string{} // name the file imports a package as → its path
	for _, imp := range f.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		name := packageName(p)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		imports[name] = p
	}
	var found []string
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || imports[pkg.Name] == "" {
			return true
		}
		if name := imports[pkg.Name] + "." + sel.Sel.Name; forbidden[name] {
			found = append(found, path+":"+strconv.Itoa(fset.Position(sel.Pos()).Line)+": "+name)
		}
		return true
	})
	return found
}

// packageName is the name a package at path is used under when imported without one, by Go's
// convention: the last element, skipping a major version such as v82, without a go- prefix or
// -go suffix. github.com/stripe/stripe-go/v82 is stripe.
func packageName(path string) string {
	elems := strings.Split(path, "/")
	name := elems[len(elems)-1]
	if len(elems) > 1 && len(name) > 1 && name[0] == 'v' && strings.Trim(name[1:], "0123456789") == "" {
		name = elems[len(elems)-2]
	}
	return strings.TrimSuffix(strings.TrimPrefix(name, "go-"), "-go")
}
