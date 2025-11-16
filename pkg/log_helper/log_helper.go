package log_helper

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/buger/jsonparser"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

// stackTracer is an interface for errors that can provide stack traces
type stackTracer interface {
	StackTrace() errors.StackTrace
}

// CustomStackMarshaler formats stack traces similar to panic output
func CustomStackMarshaler(err error) interface{} {
	// Check if error implements StackTrace
	tracer, ok := err.(stackTracer)
	if !ok {
		return nil
	}

	st := tracer.StackTrace()
	if len(st) == 0 {
		return nil
	}

	var lines []string

	for _, frame := range st {
		// Format: package.function() on one line, file.go:line on next with tab
		// Example:
		// runtime.goexit()
		//         runtime/asm_amd64.s:1693

		// Format the full frame with %+v which gives: function\n\tfile:line
		frameStr := fmt.Sprintf("%+v", frame)

		// Split by newline and tab to extract function and file:line
		parts := strings.Split(frameStr, "\n\t")
		if len(parts) == 2 {
			funcName := parts[0]
			fileLine := parts[1]

			lines = append(lines, funcName+"()")
			lines = append(lines, "\t"+fileLine)
		}
	}

	return "\n" + strings.Join(lines, "\n") + "\n\n"
}

// CustomWriter is a custom writer for zerolog that formats output
type CustomWriter struct {
	out io.Writer
	buf bytes.Buffer // Reusable buffer to reduce allocations
}

// NewCustomWriter creates a new custom writer
func NewCustomWriter(out io.Writer) *CustomWriter {
	return &CustomWriter{
		out: out,
	}
}

func (w *CustomWriter) Write(p []byte) (n int, err error) {
	w.buf.Reset()

	// Timestamp - zero allocations with jsonparser
	if ts, err := jsonparser.GetString(p, "time"); err == nil {
		w.buf.WriteString(ts)
		w.buf.WriteString(" ")
	}

	// Level - format as 3-letter abbreviation
	if level, err := jsonparser.GetString(p, "level"); err == nil {
		switch level {
		case "trace":
			w.buf.WriteString("TRC")
		case "debug":
			w.buf.WriteString("DBG")
		case "info":
			w.buf.WriteString("INF")
		case "warn":
			w.buf.WriteString("WRN")
		case "error":
			w.buf.WriteString("ERR")
		case "fatal":
			w.buf.WriteString("FTL")
		case "panic":
			w.buf.WriteString("PNC")
		default:
			w.buf.WriteString(strings.ToUpper(level))
		}
		w.buf.WriteString(" ")
	}

	// Caller
	if caller, err := jsonparser.GetString(p, "caller"); err == nil {
		w.buf.WriteString(caller)
		w.buf.WriteString(" > ")
	}

	// Message
	if msg, err := jsonparser.GetString(p, "message"); err == nil {
		w.buf.WriteString(msg)
	}

	// System fields to skip when iterating
	systemFields := map[string]bool{
		"time": true, "level": true, "caller": true, "message": true,
		"error": true, "stack": true,
	}

	// Iterate over all fields to find custom fields - zero allocations
	hasCustomFields := false
	jsonparser.ObjectEach(p, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		keyStr := string(key)
		if !systemFields[keyStr] {
			if !hasCustomFields {
				w.buf.WriteString(", ")
				hasCustomFields = true
			} else {
				w.buf.WriteString(", ")
			}

			w.buf.WriteString(keyStr)
			w.buf.WriteString("=")

			// Format value based on type
			switch dataType {
			case jsonparser.String:
				// Remove quotes from string value
				if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
					w.buf.Write(value[1 : len(value)-1])
				} else {
					w.buf.Write(value)
				}
			default:
				// Numbers, booleans, etc - write as-is
				w.buf.Write(value)
			}
		}
		return nil
	})

	// Error field
	if errVal, err := jsonparser.GetString(p, "error"); err == nil {
		w.buf.WriteString(", error=")
		w.buf.WriteString(errVal)
	}

	// Stack trace
	if stack, err := jsonparser.GetString(p, "stack"); err == nil {
		w.buf.WriteString("\nstack:")
		w.buf.WriteString(stack)
	}

	w.buf.WriteString("\n")
	return w.out.Write(w.buf.Bytes())
}

// SetupLogger configures a zerolog logger with custom formatting
func SetupLogger(out io.Writer) zerolog.Logger {
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"
	// Configure zerolog to use our custom stack trace formatter
	zerolog.ErrorStackMarshaler = CustomStackMarshaler

	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return strings.TrimPrefix(file, "github.com/Altinity/clickhouse-backup/v2/") + ":" + strconv.Itoa(line)
	}
	// Use custom writer
	writer := NewCustomWriter(out)
	return zerolog.New(zerolog.SyncWriter(writer)).With().Timestamp().Caller().Logger()
}
