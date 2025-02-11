package logging

import (
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog"
)

// Level represents logging levels
type Level string

const (
	// Debug level for detailed information
	Debug Level = "debug"
	// Info level for general information
	Info Level = "info"
	// Warn level for warning messages
	Warn Level = "warn"
	// Error level for error messages
	Error Level = "error"
)

// Logger wraps zerolog.Logger with additional functionality
type Logger struct {
	*zerolog.Logger
}

// Config holds logger configuration
type Config struct {
	Level  Level  `json:"level"`
	Format string `json:"format"`
}

// New creates a new configured logger
func New(config Config) (*Logger, error) {
	// Set logging level
	level, err := parseLevel(config.Level)
	if err != nil {
		return nil, err
	}
	zerolog.SetGlobalLevel(level)

	// Configure output format
	var output zerolog.ConsoleWriter
	if strings.ToLower(config.Format) == "console" {
		output = zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02 15:04:05",
		}
	} else {
		// Default to JSON format
		output = zerolog.ConsoleWriter{
			Out:             os.Stdout,
			TimeFormat:      "2006-01-02 15:04:05",
			FormatLevel:     formatLevel,
			FormatMessage:   formatMessage,
			FormatTimestamp: formatTimestamp,
		}
	}

	logger := zerolog.New(output).With().Timestamp().Logger()
	return &Logger{&logger}, nil
}

// ParseLevel converts string to Level with validation
func ParseLevel(level string) Level {
	switch strings.ToLower(level) {
	case "debug":
		return Debug
	case "info":
		return Info
	case "warn":
		return Warn
	case "error":
		return Error
	default:
		return Info
	}
}

// parseLevel converts Level to zerolog.Level
func parseLevel(level Level) (zerolog.Level, error) {
	switch strings.ToLower(string(level)) {
	case "debug":
		return zerolog.DebugLevel, nil
	case "info":
		return zerolog.InfoLevel, nil
	case "warn":
		return zerolog.WarnLevel, nil
	case "error":
		return zerolog.ErrorLevel, nil
	default:
		return zerolog.InfoLevel, fmt.Errorf("invalid log level: %s", level)
	}
}

// Custom formatters for console output
func formatLevel(i interface{}) string {
	return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
}

func formatMessage(i interface{}) string {
	return fmt.Sprintf("  %s", i)
}

func formatTimestamp(i interface{}) string {
	return fmt.Sprintf("%s |", i)
}

// WithComponent adds component field to logger
func (l *Logger) WithComponent(component string) *Logger {
	newLogger := l.With().Str("component", component).Logger()
	return &Logger{&newLogger}
}

// WithFields adds multiple fields to logger
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	ctx := l.With()
	for k, v := range fields {
		ctx = ctx.Interface(k, v)
	}
	newLogger := ctx.Logger()
	return &Logger{&newLogger}
}
