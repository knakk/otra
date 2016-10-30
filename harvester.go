package main

import (
	"encoding/xml"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/knakk/kbp/onix"
	"github.com/knakk/kbp/onix/codes/list1"
	"github.com/knakk/otra/storage"
)

type harvester struct {
	db           *storage.DB
	endpoint     string
	authEndpoint string
	username     string
	password     string
	imageDir     string
	next         string
	token        string
	start        time.Time
	pollInterval time.Duration
}

func (h *harvester) Run() {
	if h.endpoint == "" || h.authEndpoint == "" || h.username == "" || h.password == "" {
		log.Printf("harvester: missing parameters, will not start")
		return
	}

	// Create image directory if it doesn't allready exist
	if _, err := os.Stat(h.imageDir); os.IsNotExist(err) {
		os.Mkdir(h.imageDir, 0666)
	}

	for {
		res, err := h.getRecords()
		if err != nil {
			log.Printf("harvester: failed to get records: %v", err)
		}

		if res.StatusCode != http.StatusOK {
			log.Printf("harvester: request failed: %v", res.Status)
			b, _ := ioutil.ReadAll(res.Body)
			log.Println(string(b))
			h.token = "" // to obtain a new token in next loop iteration
			continue
		}

		h.next = res.Header.Get("Next")
		if res.Header.Get("Link") == "" {
			// No more records
			h.next = ""
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

					if err := handleProduct(h.db, p); err != nil {
						log.Printf("harvester: error storing product: %v", err)
					} else {
						n++
					}
				}
			}
		}

		res.Body.Close()
		log.Printf("harvester: done processing %d records", n)

		if h.next != "" {
			continue
		}

		log.Printf("harvester: sleeping %v before attempting to harvest again", h.pollInterval)
		time.Sleep(h.pollInterval)
	}

}

func (h *harvester) getRecords() (*http.Response, error) {
	if h.token == "" {
		if err := h.getToken(); err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequest("GET", h.endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Date", time.Now().UTC().Format(time.RFC1123))
	req.Header.Set("Authorization", "Boknett "+h.token)

	q := req.URL.Query()
	if h.next != "" {
		q.Add("next", h.next)
	} else {
		q.Add("after", h.start.Format("20060102150405")) // yyyyMMddHHmmss
	}
	q.Add("subscription", "extended")
	q.Add("pagesize", "1000")
	req.URL.RawQuery = q.Encode()

	return http.DefaultClient.Do(req)
}

func (h *harvester) getToken() error {
	res, err := http.PostForm(h.authEndpoint,
		url.Values{"username": {h.username}, "password": {h.password}})
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		errors.New(res.Status)
	}
	h.token = res.Header.Get("Boknett-TGT")
	return nil
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
