package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring"
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

var (
	xmlHeader = []byte(`<?xml version="1.0" encoding="utf-8"?><ONIXMessage release="3.0"><Header><Sender><SenderName>Otra</SenderName></Sender></Header>`)
	xmlFooter = []byte(`</ONIXMessage>`)
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
		hasImages := roaring.New()
		b, err := db.MetaGet([]byte("hasImage"))
		if err != nil {
			log.Printf("failed to load image set: %v", err)
		} else if _, err := hasImages.ReadFrom(bytes.NewReader(b)); err != nil {
			log.Printf("failed to load image set %v", err)
		}
		var results searchResults
		if q := r.URL.Query().Get("q"); q != "" {
			paths := strings.Split(q, "/")
			if len(paths) != 2 || paths[1] == "" {
				http.Error(w, "usage: q=index/query", http.StatusBadRequest)
				return
			}

			offset := 0
			pageNum := 1
			if pageP := r.URL.Query().Get("page"); pageP != "" {
				n, err := strconv.Atoi(pageP)
				if err != nil || n < 1 {
					http.Error(w, "page must be an integer >= 1", http.StatusBadRequest)
					return
				}
				pageNum = n
				offset = (pageNum - 1) * 10
			}

			start := time.Now()
			total, ids, err := db.Query(paths[0], paths[1], offset, 10)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			results = searchResults{Total: total, Query: fmt.Sprintf("%s/%s", paths[0], paths[1])}
			for _, id := range ids {
				p, err := db.Get(id)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				hit := extractRes(p, id)
				hit.HasImage = hasImages.Contains(id)
				results.Hits = append(results.Hits, hit)
			}
			results.Took = strconv.FormatFloat(time.Since(start).Seconds()*1000, 'f', 1, 64)
			for i := 0; total > 10 && float64(i) < math.Ceil(float64(total)/10); i++ {
				if len(results.Pages) == 10 {
					break
				}
				results.Pages = append(results.Pages, page{Page: strconv.Itoa(i + 1), Active: i+1 == pageNum})
				if i == 0 && pageNum >= 10 {
					i += (pageNum - 9)
				}
			}
			if pageNum >= 10 {
				results.Pages = append(results.Pages[:1], append([]page{page{Page: "...", Active: true}}, results.Pages[1:]...)...)
			}
		}

		w.Header().Set("Content-Type", "text/html")
		if err := indexTmpl.Execute(w, results); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func xmlQueryHandler(db *storage.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		element := r.PostFormValue("element")
		query := r.PostFormValue("query")
		if element == "" || query == "" {
			http.Error(w, "required parameters: element, entry", http.StatusBadRequest)
			return
		}

		_, ids, err := db.Query(element, query, 0, 10)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/xml")
		if _, err := w.Write(xmlHeader); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		enc := xml.NewEncoder(w)
		for _, id := range ids {
			rec, err := db.Get(id)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if err := enc.Encode(&rec); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		if _, err := w.Write(xmlFooter); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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

func statsHandler(db *storage.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		if err := statsTmpl.Execute(w, db.Stats()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

type page struct {
	Active bool
	Page   string
}

type searchResults struct {
	Hits  []Hit
	Total int
	Query string
	Took  string
	Pages []page
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
	Desc             []string
	HasImage         bool
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
		if d.PublishingDateRole.Value == list163.LastReprintDate {
			// TODO research other date roles
			hit.PublishedYear = d.Date.Value
			break
		}
		// TODO list163.DateOfFirstPublication ?
	}

	subjects := make(map[string]bool)
	for _, s := range p.DescriptiveDetail.Subject {
		for _, st := range s.SubjectHeadingText {
			if subjects[st.Value] {
				// We don't want to display duplicate subjects
				continue
			}
			hit.Subjects = append(hit.Subjects, st.Value)
			subjects[st.Value] = true
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

	if p.CollateralDetail != nil {
		for _, tc := range p.CollateralDetail.TextContent {
			for _, t := range tc.Text {
				hit.Desc = append(hit.Desc, html.UnescapeString(t.Value))
			}
		}
	}

	// Unescape escaped xml characters in selected fields:
	hit.Title = html.UnescapeString(hit.Title)
	hit.Publisher = html.UnescapeString(hit.Publisher) // aschehoug &amp; co

	return hit
}
