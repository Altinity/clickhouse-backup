// Package logcli implements a colored text handler suitable for command-line interfaces.
package logcli

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/apex/log"
)

// Default handler outputting to stderr.
var Default = New(os.Stderr)

// start time.
var start = time.Now()

// Strings mapping.
var Strings = [...]string{
	log.DebugLevel: "debug",
	log.InfoLevel:  " info",
	log.WarnLevel:  " warn",
	log.ErrorLevel: "error",
	log.FatalLevel: "error",
}

// Handler implementation.
type Handler struct {
	mu      sync.Mutex
	Writer  io.Writer
	Padding int
}

// New handler.
func New(w io.Writer) *Handler {
	if f, ok := w.(*os.File); ok {
		return &Handler{
			Writer:  f,
			Padding: 3,
		}
	}

	return &Handler{
		Writer:  w,
		Padding: 3,
	}
}

// HandleLog implements log.Handler.
func (h *Handler) HandleLog(e *log.Entry) error {
	level := Strings[e.Level]
	names := e.Fields.Names()

	h.mu.Lock()
	defer h.mu.Unlock()

	fmt.Fprintf(h.Writer, "%s %-5s %-25s", e.Timestamp.Format("2006/01/02 15:04:05.000000"), level, e.Message)

	for _, name := range names {
		if name == "source" {
			continue
		}
		fmt.Fprintf(h.Writer, " %s=%v", name, e.Fields.Get(name))
	}

	fmt.Fprintln(h.Writer)

	return nil
}
