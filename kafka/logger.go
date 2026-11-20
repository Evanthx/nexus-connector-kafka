package kafka

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// clientLogger sends franz-go's own log lines to the program's log, tagged component=kafka-client.
// franz-go logs nothing without one, so without it a broker that went away, a failed login, or a
// consumer group that can't rejoin after startup would leave no trace in the log.
type clientLogger struct {
	level slog.Level
}

// Level is the most detailed level franz-go should log at.
func (l clientLogger) Level() kgo.LogLevel {
	switch {
	case l.level <= slog.LevelDebug:
		return kgo.LogLevelDebug
	case l.level <= slog.LevelInfo:
		return kgo.LogLevelInfo
	case l.level <= slog.LevelWarn:
		return kgo.LogLevelWarn
	default:
		return kgo.LogLevelError
	}
}

// Log writes one franz-go log line. slog.Default is read on each call, so it follows the
// program's logger.
func (l clientLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	var lvl slog.Level
	switch level {
	case kgo.LogLevelError:
		lvl = slog.LevelError
	case kgo.LogLevelWarn:
		lvl = slog.LevelWarn
	case kgo.LogLevelInfo:
		lvl = slog.LevelInfo
	case kgo.LogLevelDebug:
		lvl = slog.LevelDebug
	default: // kgo.LogLevelNone, or a level franz-go adds later
		return
	}
	if lvl < l.level {
		return
	}
	slog.Default().Log(context.Background(), lvl, "Kafka client: "+msg, append([]any{"component", "kafka-client"}, keyvals...)...)
}
