package log

import (
	"errors"
	"fmt"
	"strings"
)

const (
	// DefaultScopeName defines the name of the default scope.
	DefaultScopeName          = "default"
	defaultOutputLevel        = InfoLevel
	defaultStackTraceLevel    = NoneLevel
	defaultOutputPath         = "stdout"
	defaultErrorOutputPath    = "stderr"
	defaultRotationMaxAge     = 30
	defaultRotationMaxSize    = 100
	defaultRotationMaxBackups = 1000
)

// Level is an enumeration of all supported log levels.
type Level int

const (
	// NoneLevel disables logging
	NoneLevel Level = iota
	// ErrorLevel enables error level logging
	ErrorLevel
	// WarnLevel enables warn level logging
	WarnLevel
	// InfoLevel enables info level logging
	InfoLevel
	// DebugLevel enables debug level logging
	DebugLevel
)

var levelToString = map[Level]string{
	DebugLevel: "debug",
	InfoLevel:  "info",
	WarnLevel:  "warn",
	ErrorLevel: "error",
	NoneLevel:  "none",
}

var stringToLevel = map[string]Level{
	"debug": DebugLevel,
	"info":  InfoLevel,
	"warn":  WarnLevel,
	"error": ErrorLevel,
	"none":  NoneLevel,
}

// Options defines the set of options supported by Istio's component logging package.
type Options struct {
	// OutputPaths is a list of file system paths to write the log data to.
	// The special values stdout and stderr can be used to output to the
	// standard I/O streams. This defaults to stdout.
	OutputPaths []string

	// ErrorOutputPaths is a list of file system paths to write logger errors to.
	// The special values stdout and stderr can be used to output to the
	// standard I/O streams. This defaults to stderr.
	ErrorOutputPaths []string

	// RotateOutputPath is the path to a rotating log file. This file should
	// be automatically rotated over time, based on the rotation parameters such
	// as RotationMaxSize and RotationMaxAge. The default is to not rotate.
	//
	// This path is used as a foundational path. This is where log output is normally
	// saved. When a rotation needs to take place because the file got too big or too
	// old, then the file is renamed by appending a timestamp to the name. Such renamed
	// files are called backups. Once a backup has been created,
	// output resumes to this path.
	RotateOutputPath string

	// RotationMaxSize is the maximum size in megabytes of a log file before it gets
	// rotated. It defaults to 100 megabytes.
	RotationMaxSize int

	// RotationMaxAge is the maximum number of days to retain old log files based on the
	// timestamp encoded in their filename. Note that a day is defined as 24
	// hours and may not exactly correspond to calendar days due to daylight
	// savings, leap seconds, etc. The default is to remove log files
	// older than 30 days.
	RotationMaxAge int

	// RotationMaxBackups is the maximum number of old log files to retain.  The default
	// is to retain at most 1000 logs.
	RotationMaxBackups int

	// JSONEncoding controls whether the log is formatted as JSON.
	JSONEncoding bool

	// LogGrpc indicates that Grpc logs should be captured. The default is true.
	// This is not exposed through the command-line flags, as this flag is mainly useful for testing: Grpc
	// stack will hold on to the logger even though it gets closed. This causes data races.
	LogGrpc bool

	outputLevels     string
	logCallers       string
	stackTraceLevels string
}

// DefaultOptions returns a new set of options, initialized to the defaults
func DefaultOptions() *Options {
	return &Options{
		OutputPaths:        []string{},
		ErrorOutputPaths:   []string{},
		RotationMaxSize:    defaultRotationMaxSize,
		RotationMaxAge:     defaultRotationMaxAge,
		RotationMaxBackups: defaultRotationMaxBackups,
		outputLevels:       DefaultScopeName + ":" + levelToString[defaultOutputLevel],
		stackTraceLevels:   DefaultScopeName + ":" + levelToString[defaultStackTraceLevel],
		LogGrpc:            false,
	}
}

// ConvertLevel from string to log.Level
func (o *Options) ConvertLevel(level string) (Level, error) {
	if v, ok := stringToLevel[level]; ok {
		return v, nil
	}
	return NoneLevel, errors.New("invalid level")
}

// SetOutputLevel sets the minimum log output level for a given scope.
func (o *Options) SetOutputLevel(scope string, level Level) {
	sl := scope + ":" + levelToString[level]
	levels := strings.Split(o.outputLevels, ",")

	if scope == DefaultScopeName {
		// see if we have an entry without a scope prefix (which represents the default scope)
		for i, ol := range levels {
			if !strings.Contains(ol, ":") {
				levels[i] = sl
				o.outputLevels = strings.Join(levels, ",")
				return
			}
		}
	}

	prefix := scope + ":"
	for i, ol := range levels {
		if strings.HasPrefix(ol, prefix) {
			levels[i] = sl
			o.outputLevels = strings.Join(levels, ",")
			return
		}
	}

	levels = append(levels, sl)
	o.outputLevels = strings.Join(levels, ",")
}

// GetOutputLevel returns the minimum log output level for a given scope.
func (o *Options) GetOutputLevel(scope string) (Level, error) {
	levels := strings.Split(o.outputLevels, ",")

	if scope == DefaultScopeName {
		// see if we have an entry without a scope prefix (which represents the default scope)
		for _, ol := range levels {
			if !strings.Contains(ol, ":") {
				_, l, err := convertScopedLevel(ol)
				return l, err
			}
		}
	}

	prefix := scope + ":"
	for _, ol := range levels {
		if strings.HasPrefix(ol, prefix) {
			_, l, err := convertScopedLevel(ol)
			return l, err
		}
	}

	return NoneLevel, fmt.Errorf("no level defined for scope '%s'", scope)
}

// SetStackTraceLevel sets the minimum stack tracing level for a given scope.
func (o *Options) SetStackTraceLevel(scope string, level Level) {
	sl := scope + ":" + levelToString[level]
	levels := strings.Split(o.stackTraceLevels, ",")

	if scope == DefaultScopeName {
		// see if we have an entry without a scope prefix (which represents the default scope)
		for i, ol := range levels {
			if !strings.Contains(ol, ":") {
				levels[i] = sl
				o.stackTraceLevels = strings.Join(levels, ",")
				return
			}
		}
	}

	prefix := scope + ":"
	for i, ol := range levels {
		if strings.HasPrefix(ol, prefix) {
			levels[i] = sl
			o.stackTraceLevels = strings.Join(levels, ",")
			return
		}
	}

	levels = append(levels, sl)
	o.stackTraceLevels = strings.Join(levels, ",")
}

// GetStackTraceLevel returns the minimum stack tracing level for a given scope.
func (o *Options) GetStackTraceLevel(scope string) (Level, error) {
	levels := strings.Split(o.stackTraceLevels, ",")

	if scope == DefaultScopeName {
		// see if we have an entry without a scope prefix (which represents the default scope)
		for _, ol := range levels {
			if !strings.Contains(ol, ":") {
				_, l, err := convertScopedLevel(ol)
				return l, err
			}
		}
	}

	prefix := scope + ":"
	for _, ol := range levels {
		if strings.HasPrefix(ol, prefix) {
			_, l, err := convertScopedLevel(ol)
			return l, err
		}
	}

	return NoneLevel, fmt.Errorf("no level defined for scope '%s'", scope)
}

// SetLogCallers sets whether to output the caller's source code location for a given scope.
func (o *Options) SetLogCallers(scope string, include bool) {
	scopes := strings.Split(o.logCallers, ",")

	// remove any occurrence of the scope
	for i, s := range scopes {
		if s == scope {
			scopes[i] = ""
		}
	}

	if include {
		// find a free slot if there is one
		for i, s := range scopes {
			if s == "" {
				scopes[i] = scope
				o.logCallers = strings.Join(scopes, ",")
				return
			}
		}

		scopes = append(scopes, scope)
	}

	o.logCallers = strings.Join(scopes, ",")
}

// GetLogCallers returns whether the caller's source code location is output for a given scope.
func (o *Options) GetLogCallers(scope string) bool {
	scopes := strings.Split(o.logCallers, ",")

	for _, s := range scopes {
		if s == scope {
			return true
		}
	}

	return false
}

func convertScopedLevel(sl string) (string, Level, error) {
	var s string
	var l string

	pieces := strings.Split(sl, ":")
	if len(pieces) == 1 {
		s = DefaultScopeName
		l = pieces[0]
	} else if len(pieces) == 2 {
		s = pieces[0]
		l = pieces[1]
	} else {
		return "", NoneLevel, fmt.Errorf("invalid output level format '%s'", sl)
	}

	level, ok := stringToLevel[l]
	if !ok {
		return "", NoneLevel, fmt.Errorf("invalid output level '%s'", sl)
	}

	return s, level, nil
}
