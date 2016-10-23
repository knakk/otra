package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/knakk/kbp/onix"
	"github.com/knakk/kbp/onix/codes"
	"github.com/knakk/kbp/onix/codes/list15"
	"github.com/knakk/kbp/onix/codes/list150"
	"github.com/knakk/kbp/onix/codes/list163"
	"github.com/knakk/kbp/onix/codes/list17"
	"github.com/knakk/kbp/onix/codes/list22"
	"github.com/knakk/kbp/onix/codes/list5"
	"github.com/knakk/kbp/onix/codes/list74"
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

func indexHandler(otraDB *db.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		indexes := otraDB.Indexes()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(&indexes); err != nil {
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
			hits = append(hits, extractRes(p, id))
		}

		w.Header().Set("Content-Type", "text/html")
		if err := hitsTmpl.Execute(w, hits); err != nil {
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

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(&hits); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

type Result struct {
	ID               string
	Contributors     map[string][]string
	Collection       []string
	Subjects         []string
	Title            string
	Subtitle         string
	OriginalTitle    string
	Language         string
	OriginalLanguage string
	ISBN             string
	Format           string
	Publisher        string
	PublishedYear    string
}

func extractRes(p *onix.Product, id uint32) (res Result) {
	res.ID = strconv.Itoa(int(id))
	for _, id := range p.ProductIdentifier {
		if id.ProductIDType.Value == list5.ISBN13 {
			res.ISBN = id.IDValue.Value
		}
	}
	res.Format = list150.MustItem(p.DescriptiveDetail.ProductForm.Value, codes.Norwegian).Label
	for _, c := range p.DescriptiveDetail.Collection {
		for _, t := range c.TitleDetail {
			for _, tt := range t.TitleElement {
				if tt.TitleText != nil {
					res.Collection = append(res.Collection, tt.TitleText.Value)
				}
			}
		}
	}

	for _, t := range p.DescriptiveDetail.TitleDetail {
		if t.TitleType.Value == list15.DistinctiveTitleBookCoverTitleSerialTitleOnItemSerialContentItemOrReviewedResource {
			res.Title = t.TitleElement[0].TitleText.Value
			if t.TitleElement[0].Subtitle != nil {
				res.Subtitle = t.TitleElement[0].Subtitle.Value
			}
		}
		if t.TitleType.Value == list15.TitleInOriginalLanguage {
			res.OriginalTitle = t.TitleElement[0].TitleText.Value
			if t.TitleElement[0].Subtitle != nil {
				res.OriginalTitle = fmt.Sprintf("%s : %s", res.OriginalTitle, t.TitleElement[0].Subtitle.Value)
			}
		}
	}

	for _, l := range p.DescriptiveDetail.Language {
		if l.LanguageRole.Value == list22.LanguageOfText {
			res.Language = list74.MustItem(l.LanguageCode.Value, codes.Norwegian).Label
		} else if l.LanguageRole.Value == list22.OriginalLanguageOfATranslatedText {
			res.OriginalLanguage = list74.MustItem(l.LanguageCode.Value, codes.Norwegian).Label
		}
	}

	for _, p := range p.PublishingDetail.Publisher {
		res.Publisher = p.PublisherName.Value
		break
	}
	for _, d := range p.PublishingDetail.PublishingDate {
		if d.PublishingDateRole.Value == list163.PublicationDate {
			res.PublishedYear = d.Date.Value
			break
		}
		// TODO list163.DateOfFirstPublication ?
	}

	for _, s := range p.DescriptiveDetail.Subject {
		for _, st := range s.SubjectHeadingText {
			res.Subjects = append(res.Subjects, st.Value)
		}
	}

	res.Contributors = make(map[string][]string)
	for _, c := range p.DescriptiveDetail.Contributor {
		for _, role := range c.ContributorRole {
			roleLabel := list17.MustItem(role.Value, codes.Norwegian).Label
			agentName := ""
			if c.PersonNameInverted != nil {
				agentName = c.PersonNameInverted.Value
			} else if c.PersonName != nil {
				agentName = c.PersonName.Value
			}
			res.Contributors[roleLabel] = append(res.Contributors[roleLabel], agentName)
		}

	}
	return res
}
