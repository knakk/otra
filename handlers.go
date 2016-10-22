package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/knakk/otra/db"
)

func recordHandler(otraDB *db.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths := strings.Split(r.URL.Path, "/")
		if len(paths) != 3 || paths[2] == "" {
			http.Error(w, "usage: /record/:id", http.StatusBadRequest)
			return
		}
		n, err := strconv.Atoi(paths[2])
		if err != nil {
			http.Error(w, "usage: /record/:id", http.StatusBadRequest)
			return
		}
		rec, err := otraDB.Get(uint32(n))
		if err == db.ErrNotFound {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/xml")
		if err := xml.NewEncoder(w).Encode(&rec); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func queryHandler(otraDB *db.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths := strings.Split(r.URL.Path, "/")
		if len(paths) != 4 || paths[3] == "" {
			http.Error(w, "usage: /query/index/query", http.StatusBadRequest)
			return
		}

		ids, err := otraDB.Query(paths[2], paths[3], 10)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var hits []Result
		for _, id := range ids {
			p, err := otraDB.Get(id)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			hits = append(hits, extractRes(p))
		}

		if err := json.NewEncoder(w).Encode(&hits); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func scanHandler(otraDB *db.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths := strings.Split(r.URL.Path, "/")
		if len(paths) != 4 || paths[3] == "" {
			http.Error(w, "usage: /autocomplete/index/query", http.StatusBadRequest)
			return
		}

		hits, err := otraDB.Scan(paths[2], paths[3], 10)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		for _, hit := range hits {
			fmt.Fprintln(w, hit)
		}
	})
}
