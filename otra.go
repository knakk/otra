package main

import (
	"encoding/xml"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

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
	var (
		dbFile         = flag.String("db", "otra.db", "database file")
		loadFile       = flag.String("load", "", "load onix xml file into db")
		listenAdr      = flag.String("l", ":8765", "listening address")
		reindex        = flag.Bool("reindex", false, "reindex all records on startup")
		harvestAdr     = flag.String("harvest-adr", "", "harvesting address")
		harvestAuthAdr = flag.String("harvest-auth", "", "harvesting auth address")
		harvestUser    = flag.String("harvest-user", "", "harvesting auth user")
		harvestPass    = flag.String("harvest-pass", "", "harvesting auth password")
		harvestImgDir  = flag.String("harvest-img", "img", "harvesting images to this directory")
		harvestStart   = flag.Duration("harvest-before", time.Hour*24*29*6, "harvesting start duration before current time")
		harvestPoll    = flag.Duration("harvest-poll", time.Hour*12, "harvesting polling frquencey")
	)
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
		handleBatch(db, products.Product)
	}

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
	http.Handle("/query/", queryHandler(db))
	http.Handle("/record/", recordHandler(db))
	http.Handle("/indexes", indexHandler(db))
	http.Handle("/stats", statsHandler(db))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		if _, err := w.Write(page); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	go startHarvester(
		db,
		*harvestAdr,
		*harvestAuthAdr,
		*harvestUser,
		*harvestPass,
		*harvestImgDir,
		*harvestStart,
		*harvestPoll,
	)

	log.Printf("Starting otra server. Listening at %s", *listenAdr)
	log.Fatal(http.ListenAndServe(*listenAdr, nil))
}

func startHarvester(db *storage.DB, adr, auth, user, pass, imgDir string, start, poll time.Duration) {
	if adr == "" || auth == "" || user == "" || pass == "" {
		log.Printf("harvester: missing parameters, will not start")
		return
	}

	token, err := getToken(auth, user, pass)
	if err != nil {
		log.Printf("harvester: failed to get authorization token: %v", err)
		return
	}

	timeCursor := time.Now().Add(-start)
	/*b, err := db.MetaGet([]byte("lastharvest"))
	if err != nil && err != storage.ErrNotFound {
		log.Printf("harvester: failed to read last harvest metadata: %v", err)
	}
	lastHarvest, err := time.Parse(b, )
	*/

	nextCursor := ""

	for {
		res, err := getRecords(adr, token, nextCursor, timeCursor)
		if err != nil {
			log.Printf("harvester: failed to get records: %v", err)
		}

		if res.StatusCode != http.StatusOK {
			log.Printf("harvester: request failed: %v", res.Status)
			b, _ := ioutil.ReadAll(res.Body)
			log.Println(string(b))
			continue
		}

		nextCursor = res.Header.Get("Next")
		if res.Header.Get("Link") == "" {
			// (No more records. Next unchanged. Link not present in Response)
			nextCursor = ""
		}

		dec := xml.NewDecoder(res.Body)
		n := 0
		for {
			t, _ := dec.Token()
			if t == nil {
				break
			}
			switch se := t.(type) {
			case xml.StartElement:
				if se.Name.Local == "Product" {
					var p *onix.Product
					if err := dec.DecodeElement(&p, &se); err != nil {
						log.Printf("harvester: xml parsing error: %v", err)
						continue
					}

					if err := handleProduct(db, p); err != nil {
						log.Printf("harvester: error storing product: %v", err)
					} else {
						n++
					}

				}
			}
		}

		res.Body.Close()
		log.Printf("harvester: done processing %d records", n)

		if nextCursor != "" {
			continue
		}

		log.Printf("harvester: sleeping %v before attempting to harvest again", poll)
		time.Sleep(poll)
	}
}

func getRecords(adr, token, next string, start time.Time) (*http.Response, error) {
	req, err := http.NewRequest("GET", adr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Date", time.Now().UTC().Format(time.RFC1123))
	req.Header.Set("Authorization", "Boknett "+token)

	q := req.URL.Query()
	if next != "" {
		q.Add("next", next)
	} else {
		q.Add("after", start.Format("20060102150405")) // yyyyMMddHHmmss
	}
	q.Add("subscription", "extended")
	q.Add("pagesize", "1000")
	req.URL.RawQuery = q.Encode()

	return http.DefaultClient.Do(req)
}

func getToken(auth, user, pass string) (string, error) {
	res, err := http.PostForm(auth,
		url.Values{"username": {user}, "password": {pass}})
	if err != nil {
		return "", err
	}
	res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		return "", errors.New(res.Status)
	}
	return res.Header.Get("Boknett-TGT"), nil
}

func handleBatch(db *storage.DB, records []*onix.Product) {
	for _, p := range records {
		if err := handleProduct(db, p); err != nil {
			log.Println(err)
		}
	}
}

func handleProduct(db *storage.DB, p *onix.Product) error {
	switch p.NotificationType.Value {
	case list1.AdvanceNotificationConfirmed, list1.NotificationConfirmedOnPublication:
		// OK store and index
	case list1.Delete:
		if err := db.DeleteByRef(p.RecordReference.Value); err != nil {
			log.Printf("delete record with ref %q failed: %v", p.RecordReference.Value, err)
		}
		return nil
	default:
		log.Printf("TODO handle notification: %v", p.NotificationType.Value)
		return nil
	}
	_, err := db.Store(p)
	return err
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
