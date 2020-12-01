package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
)

func writeError(w http.ResponseWriter, statusCode int, operation string, err error) {
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
	fmt.Fprintln(w, string(out))
}

func sendJSONEachRow(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	switch reflect.TypeOf(v).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(v)
		for i := 0; i < s.Len(); i++ {
			out, _ := json.Marshal(s.Index(i).Interface())
			fmt.Fprintln(w, string(out))
		}
	default:
		out, _ := json.Marshal(v)
		fmt.Fprintln(w, string(out))
	}
}
