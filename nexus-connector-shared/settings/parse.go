package settings

import (
	"fmt"
	"strconv"
	"time"
)

// The parse functions most variables need, for Define. Each names its variable and the bad value
// in its error, and returns its default for an unset or empty variable.

// Text is the parse function of a variable that takes any value; unset is "".
func Text(v string) (string, error) { return v, nil }

// TextOr is the parse function of a variable that takes any value, def when unset.
func TextOr(def string) func(string) (string, error) {
	return func(v string) (string, error) {
		if v == "" {
			return def, nil
		}
		return v, nil
	}
}

// RequiredText is the parse function of a required variable that takes any value. what says what
// to set it to, for the error when it is unset: "set NAME: what".
func RequiredText(name, what string) func(string) (string, error) {
	return func(v string) (string, error) {
		if v == "" {
			return "", Required(name, what)
		}
		return v, nil
	}
}

// Required is the error for the required variable name when it is unset; what says what to set
// it to.
func Required(name, what string) error {
	return fmt.Errorf("set %s: %s", name, what)
}

// PositiveInt reads name as a whole number, at least 1, def when unset.
func PositiveInt(name string, def int) func(string) (int, error) {
	return func(v string) (int, error) {
		if v == "" {
			return def, nil
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return 0, fmt.Errorf("%s must be a whole number, at least 1, not %q", name, v)
		}
		return n, nil
	}
}

// Bool reads name as true or false (anything strconv.ParseBool takes); unset is false.
func Bool(name string) func(string) (bool, error) {
	return func(v string) (bool, error) {
		if v == "" {
			return false, nil
		}
		b, err := strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("%s must be true or false, not %q", name, v)
		}
		return b, nil
	}
}

// Duration reads name as a duration of at least 0, such as 500ms, def when unset.
func Duration(name string, def time.Duration) func(string) (time.Duration, error) {
	return func(v string) (time.Duration, error) {
		if v == "" {
			return def, nil
		}
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return 0, fmt.Errorf("%s must be a duration such as 500ms or 2s, not %q", name, v)
		}
		return d, nil
	}
}

// Minutes reads name as a whole number of minutes, at least 1, def when unset.
func Minutes(name string, def time.Duration) func(string) (time.Duration, error) {
	return func(v string) (time.Duration, error) {
		if v == "" {
			return def, nil
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return 0, fmt.Errorf("%s must be a whole number of minutes, at least 1, not %q", name, v)
		}
		return time.Duration(n) * time.Minute, nil
	}
}
