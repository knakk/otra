package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/boltdb/bolt"
	"github.com/knakk/kbp/onix"
)

// Exported errors
var (
	ErrNotFound = errors.New("not found")
	ErrDBFull   = errors.New("database full: id limit reached")
)

// MaxProducts represents the maxiumum number of products the database can store.
const MaxProducts = 4294967295

// DB represents a database which can store, index, query and retrieve
// onix.Product records.
type DB struct {
	kv      *bolt.DB
	encPool sync.Pool
	decPool sync.Pool
	indexFn IndexFn
}

// Open opens a database at the given path, using the given indexing function.
// If the database does not exist, a new will be created.
func Open(path string, fn IndexFn) (*DB, error) {
	kv, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return nil, err
	}
	codec, err := newPrimedCodec(&onix.Product{})
	if err != nil {
		return nil, err
	}
	db := &DB{
		kv:      kv,
		encPool: sync.Pool{New: func() interface{} { return codec.NewMarshaler() }},
		decPool: sync.Pool{New: func() interface{} { return codec.NewUnmarshaler() }},
		indexFn: fn,
	}
	return db.setup()
}

// Close closes the database, releasing the lock on the file.
func (db *DB) Close() error {
	return db.kv.Close()
}

func (db *DB) setup() (*DB, error) {
	// set up required buckets
	err := db.kv.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{[]byte("products"), []byte("indexes")} {
			_, err := tx.CreateBucketIfNotExists(b)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return db, err
}

// Get will retrieve the Product with the give ID, if it exists.
func (db *DB) Get(id uint32) (*onix.Product, error) {
	var b []byte
	if err := db.kv.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("products"))
		b = bkt.Get(u32tob(id))
		if b == nil {
			return ErrNotFound
		}
		return nil
	}); err != nil {
		return nil, err
	}
	dec := db.decPool.Get().(*primedDecoder)
	defer db.decPool.Put(dec)
	return dec.Unmarshal(b)
}

// Store will persist an onix.Product in the database, returning the ID it
// was assigned.
func (db *DB) Store(p *onix.Product) (id uint32, err error) {
	err = db.kv.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("products"))
		n, _ := bkt.NextSequence()
		if n > MaxProducts {
			return ErrDBFull
		}

		id = uint32(n)
		idb := u32tob(uint32(n))
		enc := db.encPool.Get().(*primedEncoder)
		defer db.encPool.Put(enc)
		b, err := enc.Marshal(p)
		if err != nil {
			return err
		}
		if err := bkt.Put(idb, b); err != nil {
			return err
		}

		return db.index(tx, p, id)
	})
	return id, err
}

func (db *DB) index(tx *bolt.Tx, p *onix.Product, id uint32) error {
	entries := db.indexFn(p)
	for _, e := range entries {
		bkt, err := tx.Bucket([]byte("indexes")).CreateBucketIfNotExists([]byte(e.Index))
		if err != nil {
			return err
		}

		term := []byte(strings.ToLower(e.Term))
		hits := roaring.New()

		bo := bkt.Get(term)
		if bo != nil {
			if _, err := hits.ReadFrom(bytes.NewReader(bo)); err != nil {
				return err
			}
		}

		hits.Add(id)

		hitsb, err := hits.MarshalBinary()
		if err != nil {
			return err
		}

		if err := bkt.Put(term, hitsb); err != nil {
			return err
		}
	}
	return nil
}

// IndexEntry represent an term to be indexed.
type IndexEntry struct {
	Index string
	Term  string
}

// IndexFn is a function which returns the terms to be indexed for a given onix.Product.
type IndexFn func(*onix.Product) []IndexEntry

// Indexes returns the list of indicies in use.
func (db *DB) Indexes() (res []string) {
	db.kv.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("indexes"))
		c := bkt.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v == nil {
				res = append(res, string(k))
			}
		}
		return nil
	})
	return res
}

// Scan performs a prefix scan of the given index, starting at the given query, and returns
// up to limit terms which matches.
func (db *DB) Scan(index, start string, limit int) (res []string, err error) {
	err = db.kv.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("indexes")).Bucket([]byte(index))
		if bkt == nil {
			return fmt.Errorf("index not found: %s", index)
		}
		cur := bkt.Cursor()
		n := 0
		term := []byte(strings.ToLower(start))
		for k, _ := cur.Seek(term); k != nil; k, _ = cur.Next() {
			if n > limit {
				break
			}
			if !bytes.HasPrefix(k, term) {
				break
			}
			res = append(res, string(k))
			n++
		}
		return nil
	})
	return res, err
}

// Query performs a query against the given index, returning up to limit matching
// record IDs, as well as a count of total hits..
func (db *DB) Query(index, query string, limit int) (total int, res []uint32, err error) {
	err = db.kv.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("indexes")).Bucket([]byte(index))
		if bkt == nil {
			return fmt.Errorf("index not found: %s", index)
		}
		bo := bkt.Get([]byte(strings.ToLower(query)))

		if bo == nil {
			return nil
		}

		hits := roaring.New()
		if _, err := hits.ReadFrom(bytes.NewReader(bo)); err != nil {
			return err
		}
		res = hits.ToArray()
		total = len(res)
		if len(res) > limit {
			res = res[:limit]
		}

		return nil
	})
	return total, res, err
}

// func (db *DB) DeleteIndex(index string) error

// u32tob converts a uint32 into a 4-byte slice.
func u32tob(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// btou32 converts a 4-byte slice into an uint32.
func btou32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}
