// Package logfmt implements a "logfmt" format handler.
package logfmt

import (
	"io"
	"sync"

	"github.com/apex/log"
	"github.com/go-logfmt/logfmt"
)

// Handler implementation.
type Handler struct {
	mu  sync.Mutex
	enc *logfmt.Encoder
}

// New handler.
func New(w io.Writer) *Handler {
	return &Handler{
		enc: logfmt.NewEncoder(w),
	}
}

// HandleLog implements log.Handler.
func (h *Handler) HandleLog(e *log.Entry) error {
	names := e.Fields.Names()

	h.mu.Lock()
	defer h.mu.Unlock()

	_ = h.enc.EncodeKeyval("ts", e.Timestamp)
	_ = h.enc.EncodeKeyval("lvl", e.Level.String())
	_ = h.enc.EncodeKeyval("msg", e.Message)

	for _, name := range names {
		_ = h.enc.EncodeKeyval(name, e.Fields.Get(name))
	}

	_ = h.enc.EndRecord()

	return nil
}
