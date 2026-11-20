package settings

import (
	"fmt"
	"log/slog"
)

// LogLevelHelp is the end of the help text for a client library's log level variable, after what
// the library logs at warn.
const LogLevelHelp = "`error`, `warn`, `info` or `debug`; `info` and `debug` log much more, for chasing a problem."

// LogLevel parses a log level variable named name: error, warn, info or debug, or empty for warn.
// Use it for the level of a client library's logger, such as `<SYSTEM>_CLIENT_LOG_LEVEL`.
func LogLevel(name string) func(value string) (slog.Level, error) {
	return func(value string) (slog.Level, error) {
		switch value {
		case "", "warn":
			return slog.LevelWarn, nil
		case "error":
			return slog.LevelError, nil
		case "info":
			return slog.LevelInfo, nil
		case "debug":
			return slog.LevelDebug, nil
		default:
			return 0, fmt.Errorf("%s must be error, warn, info or debug, not %q", name, value)
		}
	}
}
