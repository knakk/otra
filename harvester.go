package main

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/RoaringBitmap/roaring"
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
	ignoreCursor bool
	batchSize    int
	start        time.Time
	pollInterval time.Duration
	hasImage     *roaring.Bitmap
}

func (h *harvester) Run() {
	if h.endpoint == "" || h.authEndpoint == "" || h.username == "" || h.password == "" {
		log.Printf("harvester: missing parameters, will not start")
		return
	}

	// Create image directory if it doesn't already exist
	if _, err := os.Stat(h.imageDir); os.IsNotExist(err) {
		os.Mkdir(h.imageDir, 0777)
	}

	if h.ignoreCursor {
		log.Println("harvester: ignoring stored cursor")
		h.db.MetaSet([]byte("next"), []byte(""))
	}
	// Load stored next cursor
	b, err := h.db.MetaGet([]byte("next"))
	if err != nil {
		log.Printf("harvester: failed to read stored next cursor: %v", err)
	} else {
		if next := string(b); next != "" {
			log.Printf("harvester: continuing using next cursor: %q", next)
			h.next = string(b)
		}
	}

	// Reset image map
	h.hasImage = roaring.New()

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

		// Prosessing the response can take quite some time, so we are copying the
		// whole response as not to risk the connection beeing reset by peer.
		var body bytes.Buffer
		io.Copy(&body, res.Body)
		res.Body.Close()

		dec := xml.NewDecoder(&body)
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

					if err := h.handleProduct(p); err != nil {
						log.Printf("harvester: error storing product: %v", err)
					} else {
						n++
					}
				}
			}
		}

		if res.Header.Get("Link") == "" {
			// No more records
			h.next = ""
		}
		// Store which products have images
		hasImages := roaring.New()
		b, err := h.db.MetaGet([]byte("hasImage"))
		if err != nil && err != storage.ErrNotFound {
			log.Printf("harvester: failed to read image set: %v\nharvester: stopping", err)
			return
		}
		if b != nil {
			if _, err := hasImages.ReadFrom(bytes.NewReader(b)); err != nil {
				log.Printf("harvester: failed to read image set: %v\nharvester: stopping", err)
				return
			}
		}
		hasImages = roaring.Or(hasImages, h.hasImage)
		ib, err := hasImages.MarshalBinary()
		if err != nil {
			log.Printf("harvester: failed to save image set: %v\nharvester: stopping", err)
			return
		}

		if err := h.db.MetaSet([]byte("hasImage"), ib); err != nil {
			log.Printf("harvester: failed to save image set: %v\nharvester: stopping", err)
			return
		}

		// Store next cursor
		if err := h.db.MetaSet([]byte("next"), []byte(h.next)); err != nil {
			log.Printf("harvester: failed to save next cursor: %v\nharvester: stopping", err)
			return
		}

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
	q.Add("pagesize", strconv.Itoa(h.batchSize))
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
		return errors.New(res.Status)
	}
	h.token = res.Header.Get("Boknett-TGT")
	return nil
}

func (h *harvester) handleProduct(p *onix.Product) error {
	switch p.NotificationType.Value {
	case list1.AdvanceNotificationConfirmed, list1.NotificationConfirmedOnPublication:
		// OK store and index
	case list1.Delete:
		if err := h.db.DeleteByRef(p.RecordReference.Value); err != nil && err != storage.ErrNotFound {
			log.Printf("delete record with ref %q failed: %v", p.RecordReference.Value, err)
		}
		return nil
	default:
		log.Printf("TODO handle notification: %v", p.NotificationType.Value)
		return nil
	}
	id, err := h.db.Store(p)
	if err != nil {
		return err
	}

	imgDir := filepath.Join(h.imageDir, strconv.Itoa(int(id)))
	if _, err := os.Stat(imgDir); os.IsNotExist(err) {
		if err2 := os.Mkdir(imgDir, 0777); err2 != nil {
			return err2
		}
	}

	for _, link := range extractLinks(p) {
		if err := h.download(filepath.Join(imgDir, link[0]), link[1]); err != nil {
			log.Printf("err downloading file %q: %v", link[1], err)
			continue
		}
		h.hasImage.Add(id)
	}
	return nil
}

func (h *harvester) download(path, url string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Date", time.Now().UTC().Format(time.RFC1123))
	req.Header.Set("Authorization", "Boknett "+h.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}
	_, err = io.Copy(f, resp.Body)
	return err
}
