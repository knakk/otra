package main

import (
	"encoding/xml"
	"flag"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/knakk/kbp/onix"
	"github.com/knakk/kbp/onix/codes/list1"
	"github.com/knakk/kbp/onix/codes/list15"
	"github.com/knakk/kbp/onix/codes/list163"
	"github.com/knakk/kbp/onix/codes/list5"
	"github.com/knakk/otra/storage"
)

type ONIXMessage struct {
	Product []*onix.Product
}

func main() {
	dbFile := flag.String("db", "otra.db", "database file")
	loadFile := flag.String("load", "", "load onix xml file into db")
	listenAdr := flag.String("l", ":8765", "listening address")
	flag.Parse()

	db, err := storage.Open(*dbFile, indexFn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if *loadFile != "" {
		log.Printf("Attempting to parse onix product file")
		b, err := ioutil.ReadFile("onix.xml")
		if err != nil {
			log.Fatal(err)
		}

		var products ONIXMessage
		if err := xml.Unmarshal(b, &products); err != nil {
			log.Fatal(err)
		}

		log.Printf("Loading %d onix products into db", len(products.Product))
		for _, p := range products.Product {
			switch p.NotificationType.Value {
			case list1.AdvanceNotificationConfirmed, list1.NotificationConfirmedOnPublication:
				// OK store and index
			case list1.Delete:
				log.Println("TODO handle delete notifications")
				continue
			default:
				log.Printf("TODO handle notification: %v", p.NotificationType.Value)
				continue
			}
			if _, err := db.Store(p); err != nil {
				log.Fatal(err)
			}
		}
	}

	http.Handle("/autocomplete/", scanHandler(db))
	http.Handle("/query/", queryHandler(db))
	http.Handle("/record/", recordHandler(db))
	http.Handle("/indexes", indexHandler(db))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write(page); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	log.Printf("Starting otra server. Listening at %s", *listenAdr)
	log.Fatal(http.ListenAndServe(*listenAdr, nil))
}

func indexFn(p *onix.Product) (res []storage.IndexEntry) {
	for _, id := range p.ProductIdentifier {
		if id.ProductIDType.Value == list5.ISBN13 {
			res = append(res, storage.IndexEntry{
				Index: "isbn",
				Term:  id.IDValue.Value,
			})
		}
	}
	for _, c := range p.DescriptiveDetail.Collection {
		for _, t := range c.TitleDetail {
			for _, tt := range t.TitleElement {
				if tt.TitleText != nil {
					res = append(res, storage.IndexEntry{
						Index: "series",
						Term:  tt.TitleText.Value,
					})
				}
			}
		}
	}

	for _, t := range p.DescriptiveDetail.TitleDetail {
		if t.TitleType.Value == list15.DistinctiveTitleBookCoverTitleSerialTitleOnItemSerialContentItemOrReviewedResource {
			res = append(res, storage.IndexEntry{
				Index: "title",
				Term:  t.TitleElement[0].TitleText.Value,
			})
			if t.TitleElement[0].Subtitle != nil {
				res = append(res, storage.IndexEntry{
					Index: "title",
					Term:  t.TitleElement[0].Subtitle.Value,
				})
			}
		}
		if t.TitleType.Value == list15.TitleInOriginalLanguage {
			res = append(res, storage.IndexEntry{
				Index: "title",
				Term:  t.TitleElement[0].TitleText.Value,
			})
			if t.TitleElement[0].Subtitle != nil {
				res = append(res, storage.IndexEntry{
					Index: "title",
					Term:  t.TitleElement[0].Subtitle.Value,
				})
			}
		}
	}

	/*
		for _, l := range p.DescriptiveDetail.Language {
			if l.LanguageRole.Value == list22.LanguageOfText {
				b.Language = list74.MustItem(l.LanguageCode.Value, codes.Norwegian).Label
			} else if l.LanguageRole.Value == list22.OriginalLanguageOfATranslatedText {
				b.OriginalLanguage = list74.MustItem(l.LanguageCode.Value, codes.Norwegian).Label
			}
		}*/

	for _, p := range p.PublishingDetail.Publisher {
		res = append(res, storage.IndexEntry{
			Index: "publisher",
			Term:  p.PublisherName.Value,
		})
		break
	}
	for _, d := range p.PublishingDetail.PublishingDate {
		if d.PublishingDateRole.Value == list163.PublicationDate {
			res = append(res, storage.IndexEntry{
				Index: "year",
				Term:  d.Date.Value,
			})
			break
		}
	}

	for _, s := range p.DescriptiveDetail.Subject {
		for _, st := range s.SubjectHeadingText {
			res = append(res, storage.IndexEntry{
				Index: "subject",
				Term:  st.Value,
			})
		}
	}

	for _, c := range p.DescriptiveDetail.Contributor {
		for _, role := range c.ContributorRole {
			var agent string
			if c.PersonNameInverted != nil {
				agent = c.PersonNameInverted.Value
			} else if c.PersonName != nil {
				agent = c.PersonName.Value
			} else if c.CorporateName != nil {
				agent = c.CorporateName.Value
			} else if c.CorporateNameInverted != nil {
				agent = c.CorporateNameInverted.Value
			}
			res = append(res, storage.IndexEntry{
				Index: "agent",
				Term:  agent,
			})
			var roleIndex string
			switch role.Value {
			case "A01":
				roleIndex = "author"
			case "A06":
				roleIndex = "composer"
			case "A09":
				roleIndex = "creator"
			case "A12":
				roleIndex = "illustrator"
			case "A32":
				roleIndex = "contributor"
			case "A38":
				roleIndex = "originalauthor"
			case "B01":
				roleIndex = "editor"
			case "B06":
				roleIndex = "translator"
			case "E07":
				roleIndex = "reader"
			default:
				roleIndex = "role" + role.Value
			}
			res = append(res, storage.IndexEntry{
				Index: roleIndex,
				Term:  agent,
			})
		}
	}

	return res
}
