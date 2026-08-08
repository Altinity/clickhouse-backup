package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

	"github.com/rs/zerolog/log"
)

func (api *APIServer) flushOutput(w http.ResponseWriter, out string) {
	if _, err := fmt.Fprintln(w, out); err != nil {
		log.Warn().Msgf("can't write to http.ResponseWriter: %v", err)
	}
}

func (api *APIServer) writeError(w http.ResponseWriter, statusCode int, operation string, err error) {
	log.Error().Msgf("api.writeError status=%d operation=%s err=%v", statusCode, operation, err)
	w.WriteHeader(statusCode)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	out, _ := json.Marshal(struct {
		Status    string `json:"status"`
		Operation string `json:"operation,omitempty"`
		Error     string `json:"error"`
	}{
		Status:    "error",
		Operation: operation,
		Error:     err.Error(),
	})
	api.flushOutput(w, string(out))
}

func (api *APIServer) sendJSONEachRow(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.WriteHeader(statusCode)
	switch reflect.TypeOf(v).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(v)
		for i := 0; i < s.Len(); i++ {
			if out, err := json.Marshal(s.Index(i).Interface()); err == nil {
				api.flushOutput(w, string(out))
			} else {
				api.flushOutput(w, err.Error())
				log.Warn().Msgf("sendJSONEachRow json.Marshal error: %v", err)
			}
		}
	default:
		if out, err := json.Marshal(v); err == nil {
			api.flushOutput(w, string(out))
		} else {
			api.flushOutput(w, err.Error())
			log.Warn().Msgf("sendJSONEachRow json.Marshal error: %v", err)
		}
	}
	// https://github.com/Altinity/clickhouse-backup/issues/1089
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	} else {
		log.Warn().Msgf("%#v doesn't support Flusher interface", w)
	}
}
