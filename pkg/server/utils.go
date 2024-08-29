package server

import (
	"context"
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
}

// CallbackResponse is the response that is returned to callers
type CallbackResponse struct {
	Status      string `json:"status"`
	Error       string `json:"error"`
	OperationId string `json:"operation_id"`
}

// errorCallback executes callbacks with a payload notifying callers that the operation has failed
func (api *APIServer) errorCallback(ctx context.Context, err error, operationId string, callback callbackFn) {
	payload := &CallbackResponse{
		Status:      "error",
		Error:       err.Error(),
		OperationId: operationId,
	}
	for _, e := range callback(ctx, payload) {
		log.Error().Err(e).Send()
	}
}

// successCallback executes callbacks with a payload notifying callers that the operation succeeded
func (api *APIServer) successCallback(ctx context.Context, operationId string, callback callbackFn) {
	payload := &CallbackResponse{
		Status:      "success",
		Error:       "",
		OperationId: operationId,
	}
	for _, e := range callback(ctx, payload) {
		log.Error().Err(e).Send()
	}
}
