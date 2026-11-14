package settings

import (
	"maps"
	"os"
	"slices"
)

// Env is a set of environment variables to read settings from: the process's (OS) or any other,
// such as the values on the setup page (Map). Its methods match the Temporal SDK's
// envconfig.EnvLookup, so an Env can also be handed to envconfig to load the Temporal client
// options from the same variables.
//
// This file is the only one outside tests and generated code that reads the process's
// environment; nobypass_test.go fails if another one does. Read a variable through a Program, a
// Typed variable, a Family, or Get.
type Env interface {
	// LookupEnv returns the variable's value and whether it is set, like os.LookupEnv.
	LookupEnv(name string) (string, bool)
	// Environ returns every variable as "name=value", like os.Environ.
	Environ() []string
}

// OS is the process's environment, read afresh on every call.
func OS() Env { return osEnv{} }

type osEnv struct{}

func (osEnv) LookupEnv(name string) (string, bool) { return os.LookupEnv(name) }
func (osEnv) Environ() []string                    { return os.Environ() }

// Map is an Env holding exactly these variables.
type Map map[string]string

// LookupEnv returns the variable's value and whether it is in m.
func (m Map) LookupEnv(name string) (string, bool) {
	v, ok := m[name]
	return v, ok
}

// Environ returns m's variables as "name=value", sorted by name.
func (m Map) Environ() []string {
	out := make([]string, 0, len(m))
	for _, name := range slices.Sorted(maps.Keys(m)) {
		out = append(out, name+"="+m[name])
	}
	return out
}

// Get is the variable's value in env, or "" if it is unset.
func Get(env Env, name string) string {
	v, _ := env.LookupEnv(name)
	return v
}
