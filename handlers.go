package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/knakk/kbp/onix"
	"github.com/knakk/kbp/onix/codes"
	"github.com/knakk/kbp/onix/codes/list15"
	"github.com/knakk/kbp/onix/codes/list150"
	"github.com/knakk/kbp/onix/codes/list163"
	"github.com/knakk/kbp/onix/codes/list17"
	"github.com/knakk/kbp/onix/codes/list22"
	"github.com/knakk/kbp/onix/codes/list5"
	"github.com/knakk/kbp/onix/codes/list74"
	"github.com/knakk/otra/storage"
)

func recordHandler(db *storage.DB) http.Handler {
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
		rec, err := db.Get(uint32(n))
		if err == storage.ErrNotFound {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/xml")
		if err := xml.NewEncoder(w).Encode(&rec); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func indexHandler(db *storage.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		indexes := db.Indexes()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(&indexes); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func queryHandler(db *storage.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths := strings.Split(r.URL.Path, "/")
		if len(paths) != 4 || paths[3] == "" {
			http.Error(w, "usage: /query/:index/:query", http.StatusBadRequest)
			return
		}

		start := time.Now()
		total, ids, err := db.Query(paths[2], paths[3], 10)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		results := searchResults{Total: total}
		for _, id := range ids {
			p, err := db.Get(id)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			results.Hits = append(results.Hits, extractRes(p, id))
		}
		results.Took = strconv.FormatFloat(time.Since(start).Seconds()*1000, 'f', 1, 64)

		w.Header().Set("Content-Type", "text/html")
		if err := hitsTmpl.Execute(w, results); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func scanHandler(db *storage.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths := strings.Split(r.URL.Path, "/")
		if len(paths) != 4 || paths[3] == "" {
			http.Error(w, "usage: /autocomplete/:index/:query", http.StatusBadRequest)
			return
		}

		hits, err := db.Scan(paths[2], paths[3], 10)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(&hits); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

type searchResults struct {
	Hits  []Hit
	Total int
	Took  string
}

type Hit struct {
	ID               string
	Contributors     map[string][]string
	Collection       []string
	Subjects         []string
	Title            string
	Subtitles        []string
	OriginalTitle    string
	Language         string
	OriginalLanguage string
	ISBN             string
	Format           string
	Publisher        string
	PublishedYear    string
}

func extractRes(p *onix.Product, id uint32) (hit Hit) {
	hit.ID = strconv.Itoa(int(id))
	for _, id := range p.ProductIdentifier {
		if id.ProductIDType.Value == list5.ISBN13 {
			hit.ISBN = id.IDValue.Value
		}
	}
	hit.Format = list150.MustItem(p.DescriptiveDetail.ProductForm.Value, codes.Norwegian).Label
	for _, c := range p.DescriptiveDetail.Collection {
		for _, t := range c.TitleDetail {
			for _, tt := range t.TitleElement {
				if tt.TitleText != nil {
					hit.Collection = append(hit.Collection, tt.TitleText.Value)
				}
			}
		}
	}

	for _, t := range p.DescriptiveDetail.TitleDetail {
		if t.TitleType.Value == list15.DistinctiveTitleBookCoverTitleSerialTitleOnItemSerialContentItemOrReviewedResource {
			hit.Title = t.TitleElement[0].TitleText.Value
			if t.TitleElement[0].Subtitle != nil {
				hit.Subtitles = append(hit.Subtitles, t.TitleElement[0].Subtitle.Value)
			}
		}
		if t.TitleType.Value == list15.TitleInOriginalLanguage {
			hit.OriginalTitle = t.TitleElement[0].TitleText.Value
			if t.TitleElement[0].Subtitle != nil {
				hit.OriginalTitle = fmt.Sprintf("%s : %s", hit.OriginalTitle, t.TitleElement[0].Subtitle.Value)
			}
		}
	}

	for _, l := range p.DescriptiveDetail.Language {
		if l.LanguageRole.Value == list22.LanguageOfText {
			hit.Language = list74.MustItem(l.LanguageCode.Value, codes.Norwegian).Label
		} else if l.LanguageRole.Value == list22.OriginalLanguageOfATranslatedText {
			hit.OriginalLanguage = list74.MustItem(l.LanguageCode.Value, codes.Norwegian).Label
		}
	}

	for _, p := range p.PublishingDetail.Publisher {
		hit.Publisher = p.PublisherName.Value
		break
	}
	for _, d := range p.PublishingDetail.PublishingDate {
		if d.PublishingDateRole.Value == list163.PublicationDate {
			hit.PublishedYear = d.Date.Value
			break
		}
		// TODO list163.DateOfFirstPublication ?
	}

	for _, s := range p.DescriptiveDetail.Subject {
		for _, st := range s.SubjectHeadingText {
			hit.Subjects = append(hit.Subjects, st.Value)
		}
	}

	hit.Contributors = make(map[string][]string)
	for _, c := range p.DescriptiveDetail.Contributor {
		for _, role := range c.ContributorRole {
			roleLabel := list17.MustItem(role.Value, codes.Norwegian).Label
			agentName := ""
			if c.PersonNameInverted != nil {
				agentName = c.PersonNameInverted.Value
			} else if c.PersonName != nil {
				agentName = c.PersonName.Value
			} else if c.CorporateName != nil {
				agentName = c.CorporateName.Value
			} else if c.CorporateNameInverted != nil {
				agentName = c.CorporateNameInverted.Value
			}
			hit.Contributors[roleLabel] = append(hit.Contributors[roleLabel], agentName)
		}

	}
	return hit
}
