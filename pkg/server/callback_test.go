package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/gorilla/mux"
)

func TestParseCallback(t *testing.T) {
	ctx := context.Background()

	// Test server setup
	type payload struct {
		Key string `json:"key"`
		Val string `json:"val"`
	}

	goodEndpoint1 := "/good1"
	goodEndpoint2 := "/good2"
	badEndpoint := "/bad"
	goodChan1 := make(chan *payload, 5)
	goodChan2 := make(chan *payload, 5)

	passToChanHandler := func(ch chan *payload) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := r.Body.Close(); err != nil {
					t.Fatalf("can't close r.Body: %v", err)
				}
			}()

			var data payload
			if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			ch <- &data
			if _, err := w.Write(nil); err != nil {
				t.Fatalf("unexpected error while writing response from test server: %v", err)
			}
		}
	}
	returnErrHandler := http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "bad endpoint error", http.StatusInternalServerError)
		})

	router := mux.NewRouter()
	router.Handle(goodEndpoint1, passToChanHandler(goodChan1))
	router.Handle(goodEndpoint2, passToChanHandler(goodChan2))
	router.Handle(badEndpoint, returnErrHandler)

	srv := httptest.NewServer(router)
	defer srv.Close()

	t.Run("Test empty callback",
		func(t *testing.T) {
			values := url.Values{}
			cb, err := parseCallback(values)
			if err != nil {
				t.Fatalf("unexpected error when getting callback for empty values: %v", err)
			}
			if err := cb(ctx, nil); err != nil {
				t.Fatalf("unexpected error when calling callback for empty values: %v", err)
			}
		},
	)

	t.Run("Test invalid callback URL",
		func(t *testing.T) {
			values := url.Values{
				"callback": []string{
					"valid",
					"invalid%",
				},
			}
			_, err := parseCallback(values)
			if err == nil {
				t.Fatalf("expected error when passing invalid callback URL")
			}
		},
	)

	t.Run("Test normal callbacks",
		func(t *testing.T) {
			values := url.Values{
				"callback": []string{
					url.QueryEscape(srv.URL + goodEndpoint1),
					url.QueryEscape(srv.URL + goodEndpoint2),
				},
			}
			cb, err := parseCallback(values)
			if err != nil {
				t.Fatalf("unexpected error when getting callback for good endpoints: %v", err)
			}
			pl := payload{Key: "a", Val: "b"}
			if err := cb(ctx, pl); err != nil {
				t.Fatalf("unexpected error when calling callbacks for good endpoints: %v", err)
			}

			if val1 := <-goodChan1; !reflect.DeepEqual(val1, &pl) {
				t.Fatalf("expected %v, got %v", pl, val1)
			}
			if val2 := <-goodChan2; !reflect.DeepEqual(val2, &pl) {
				t.Fatalf("expected %v, got %v", pl, val2)
			}
		},
	)

	t.Run("Test bad callback - unresponsive host",
		func(t *testing.T) {
			values := url.Values{
				"callback": []string{
					url.QueryEscape(srv.URL + goodEndpoint1),
					url.QueryEscape("invalid.url.local"),
				},
			}
			cb, err := parseCallback(values)
			if err != nil {
				t.Fatalf("unexpected error when getting callback for bad host: %v", err)
			}
			pl := payload{Key: "c", Val: "d"}
			if err := cb(ctx, pl); err == nil {
				t.Fatalf("expected error when calling bad host callback")
			}

			if val1 := <-goodChan1; !reflect.DeepEqual(val1, &pl) {
				t.Fatalf("expected %v, got %v", pl, val1)
			}
		},
	)

	t.Run("Test bad callback - invalid endpoint",
		func(t *testing.T) {
			values := url.Values{
				"callback": []string{
					url.QueryEscape(srv.URL + goodEndpoint1),
					url.QueryEscape(srv.URL + badEndpoint),
				},
			}
			cb, err := parseCallback(values)
			if err != nil {
				t.Fatalf("unexpected error when getting callback for bad endpoint: %v", err)
			}
			pl := payload{Key: "e", Val: "f"}
			if err := cb(ctx, pl); err == nil {
				t.Fatalf("expected error when calling bad endpoint callback")
			}

			if val1 := <-goodChan1; !reflect.DeepEqual(val1, &pl) {
				t.Fatalf("expected %v, got %v", pl, val1)
			}
		},
	)

	t.Run("Test nil context error",
		func(t *testing.T) {
			values := url.Values{
				"callback": []string{
					url.QueryEscape(srv.URL + goodEndpoint1),
				},
			}
			cb, err := parseCallback(values)
			if err != nil {
				t.Fatalf("unexpected error when getting callback for good endpoint: %v", err)
			}
			pl := payload{}
			if err := cb(nil, pl); err == nil {
				t.Fatalf("expected error when passing nil context to callback function")
			}
		},
	)

	t.Run("Test bad payload",
		func(t *testing.T) {
			values := url.Values{
				"callback": []string{
					url.QueryEscape(srv.URL + goodEndpoint1),
				},
			}
			cb, err := parseCallback(values)
			if err != nil {
				t.Fatalf("unexpected error when getting callback for good endpoint: %v", err)
			}
			type recursive struct {
				Ref *recursive `json:"Ref"`
			}
			badPl := recursive{}
			badPl.Ref = &badPl
			if err := cb(ctx, badPl); err == nil {
				t.Fatalf("expected error when passing unmarshalable payload")
			}
		},
	)
}
