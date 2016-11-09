package main

import (
	"flag"
	"html"
	"log"
	"net/http"
	"time"

	"github.com/knakk/kbp/onix"
	"github.com/knakk/kbp/onix/codes/list15"
	"github.com/knakk/kbp/onix/codes/list159"
	"github.com/knakk/kbp/onix/codes/list162"
	"github.com/knakk/kbp/onix/codes/list163"
	"github.com/knakk/kbp/onix/codes/list5"
	"github.com/knakk/otra/storage"
)

type ONIXMessage struct {
	Product []*onix.Product
}

func main() {
	var (
		dbFile         = flag.String("db", "otra.db", "database file")
		listenAdr      = flag.String("l", ":8765", "listening address")
		reindex        = flag.Bool("reindex", false, "reindex all records on startup")
		harvestAdr     = flag.String("harvest-adr", "", "harvesting address")
		harvestAuthAdr = flag.String("harvest-auth", "", "harvesting auth address")
		harvestUser    = flag.String("harvest-user", "", "harvesting auth user")
		harvestPass    = flag.String("harvest-pass", "", "harvesting auth password")
		harvestImgDir  = flag.String("harvest-img", "img", "harvesting images to this directory")
		harvestSize    = flag.Int("harvest-size", 100, "haresting batch size")
		harvestStart   = flag.Duration("harvest-before", time.Hour*24*29*6, "harvesting start duration before current time")
		harvestPoll    = flag.Duration("harvest-poll", time.Hour*12, "harvesting polling frquencey")
	)
	flag.Parse()

	db, err := storage.Open(*dbFile, indexFn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if *reindex {
		go func() {
			log.Println("reindexing all records...")
			start := time.Now()
			if err := db.ReindexAll(); err != nil {
				log.Println("reindexing failed: %v", err)
			}
			log.Printf("done reindexing %d records in %v", db.Stats().Records, time.Since(start))
		}()
	}

	http.Handle("/autocomplete/", scanHandler(db))
	http.Handle("/record/", recordHandler(db))
	http.Handle("/indexes", indexHandler(db))
	http.Handle("/stats", statsHandler(db))
	http.Handle("/img/", http.StripPrefix("/img/", http.FileServer(http.Dir("img"))))
	http.Handle("/favicon.ico", http.NotFoundHandler())
	http.Handle("/", queryHandler(db))

	h := &harvester{
		db:           db,
		endpoint:     *harvestAdr,
		authEndpoint: *harvestAuthAdr,
		username:     *harvestUser,
		password:     *harvestPass,
		imageDir:     *harvestImgDir,
		start:        time.Now().Add(-*harvestStart),
		pollInterval: *harvestPoll,
		batchSize:    *harvestSize,
	}
	go h.Run()

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
	if p.DescriptiveDetail == nil {
		return res
	}
	for _, c := range p.DescriptiveDetail.Collection {
		for _, t := range c.TitleDetail {
			for _, tt := range t.TitleElement {
				if tt.TitleText != nil {
					res = append(res, storage.IndexEntry{
						Index: "series",
						Term:  html.UnescapeString(tt.TitleText.Value),
					})
				}
			}
		}
	}

	for _, t := range p.DescriptiveDetail.TitleDetail {
		if t.TitleType.Value == list15.DistinctiveTitleBookCoverTitleSerialTitleOnItemSerialContentItemOrReviewedResource {
			res = append(res, storage.IndexEntry{
				Index: "title",
				Term:  html.UnescapeString(t.TitleElement[0].TitleText.Value),
			})
			if t.TitleElement[0].Subtitle != nil {
				res = append(res, storage.IndexEntry{
					Index: "title",
					Term:  html.UnescapeString(t.TitleElement[0].Subtitle.Value),
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
			Term:  html.UnescapeString(p.PublisherName.Value),
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
			case "A03":
				roleIndex = "scriptwriter"
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
			case "A99":
				roleIndex = "othercreator"
			case "B01":
				roleIndex = "editor"
			case "B06":
				roleIndex = "translator"
			case "D01":
				roleIndex = "producer"
			case "D02":
				roleIndex = "director"
			case "E01":
				roleIndex = "actor"
			case "E06":
				roleIndex = "solist"
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

func extractLinks(p *onix.Product) (res [][2]string) {
	if p.CollateralDetail == nil {
		return res
	}
	res = make([][2]string, 0)
	for _, r := range p.CollateralDetail.SupportingResource {
		if r.ResourceMode.Value == list159.Image {
			for _, v := range r.ResourceVersion {
				for _, f := range v.ResourceVersionFeature {
					if f.ResourceVersionFeatureType.Value == list162.Filename {
						res = append(res, [2]string{f.FeatureNote[0].Value, v.ResourceLink[0].Value})
					}
				}
			}
		}
	}
	return res
}
