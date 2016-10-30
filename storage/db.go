package storage

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
		for _, b := range [][]byte{[]byte("meta"), []byte("products"), []byte("indexes"), []byte("ref")} {
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
func (db *DB) Get(id uint32) (p *onix.Product, err error) {
	err = db.kv.View(func(tx *bolt.Tx) error {
		var err2 error
		p, err2 = db.get(tx, id)
		if err2 != nil {
			return err2
		}
		return nil
	})
	return p, err
}

func (db *DB) get(tx *bolt.Tx, id uint32) (p *onix.Product, err error) {
	bkt := tx.Bucket([]byte("products"))
	b := bkt.Get(u32tob(id))
	if b == nil {
		err = ErrNotFound
		return p, err
	}
	dec := db.decPool.Get().(*primedDecoder)
	defer db.decPool.Put(dec)
	return dec.Unmarshal(b)
}

// Store will persist an onix.Product in the database, returning the ID it
// was assigned. If there allready exist a prouduct with the same RecordReference,
// it will be overwritten.
func (db *DB) Store(p *onix.Product) (id uint32, err error) {
	err = db.kv.Update(func(tx *bolt.Tx) error {
		var idb []byte
		bkt := tx.Bucket([]byte("products"))

		ref := tx.Bucket([]byte("ref")).Get([]byte(p.RecordReference.Value))
		if ref != nil {
			// There is allready a store product with the same RecordReference
			idb = ref
			id = btou32(idb)

			// We need update the indexes, removing the entires on the existing record,
			// before storing an inserting the (potentially changed) record again.
			if err := db.deIndex(tx, idb); err != nil {
				return err
			}
		} else {
			// Assign a new ID
			n, _ := bkt.NextSequence()
			if n > MaxProducts {
				return ErrDBFull
			}

			id = uint32(n)
			idb = u32tob(uint32(n))
		}

		enc := db.encPool.Get().(*primedEncoder)
		defer db.encPool.Put(enc)
		b, err := enc.Marshal(p)
		if err != nil {
			return err
		}
		if err := bkt.Put(idb, b); err != nil {
			return err
		}

		if ref == nil {
			// Store the record reference
			if err := tx.Bucket([]byte("ref")).Put([]byte(p.RecordReference.Value), idb); err != nil {
				return err
			}
		}

		return db.index(tx, p, id)
	})
	return id, err
}

// Ref returns the product ID for the given product reference. If not found,
// it returns 0
func (db *DB) Ref(ref string) (u uint32) {
	db.kv.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("ref")).Get([]byte(ref))
		if b != nil {
			u = btou32(b)
		}
		return nil
	})
	return u
}

func (db *DB) Delete(id uint32) (err error) {
	err = db.kv.Update(func(tx *bolt.Tx) error {
		p, err2 := db.get(tx, id)
		if err2 != nil {
			return err2
		}

		idb := u32tob(id)
		if err := db.deIndex(tx, idb); err != nil {
			return err
		}

		if err := tx.Bucket([]byte("products")).Delete(idb); err != nil {
			return err
		}

		if err := tx.Bucket([]byte("ref")).Delete([]byte(p.RecordReference.Value)); err != nil {
			return err
		}

		return nil
	})
	return err
}

func (db *DB) DeleteByRef(ref string) (err error) {
	err = db.kv.Update(func(tx *bolt.Tx) error {
		ref := tx.Bucket([]byte("ref")).Get([]byte(ref))
		if ref == nil {
			return ErrNotFound
		}

		idb := ref

		if err := db.deIndex(tx, idb); err != nil {
			return err
		}

		if err := tx.Bucket([]byte("products")).Delete(idb); err != nil {
			return err
		}

		return nil
	})
	return err
}

func (db *DB) index(tx *bolt.Tx, p *onix.Product, id uint32) error {
	entries := db.indexFn(p)
	for _, e := range entries {
		if e.Index == "" || e.Term == "" {
			return fmt.Errorf("both index and term must be non-empty: Index:%q, Term:%q", e.Index, e.Term)
		}
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

func (db *DB) deIndex(tx *bolt.Tx, idb []byte) error {
	// TODO use db.get(tx, id)
	bkt := tx.Bucket([]byte("products"))
	b := bkt.Get(idb)
	if b == nil {
		return errors.New("bug: reference index entry points to non-existing product")
	}
	dec := db.decPool.Get().(*primedDecoder)
	defer db.decPool.Put(dec)
	p, err := dec.Unmarshal(b)
	if err != nil {
		return err
	}
	entries := db.indexFn(p)
	for _, e := range entries {
		idxBkt := tx.Bucket([]byte("indexes")).Bucket([]byte(e.Index))
		if idxBkt == nil {
			// TODO or err?
			continue
		}

		term := []byte(strings.ToLower(e.Term))
		hits := roaring.New()

		bo := idxBkt.Get(term)
		if bo != nil {
			if _, err := hits.ReadFrom(bytes.NewReader(bo)); err != nil {
				return err
			}
		}

		hits.Remove(btou32(idb))

		hitsb, err := hits.MarshalBinary()
		if err != nil {
			return err
		}

		if err := idxBkt.Put(term, hitsb); err != nil {
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

// MetaSet stores a key/value pair in the meta bucket.
func (db *DB) MetaSet(key, val []byte) error {
	return db.kv.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("meta")).Put(key, val)
	})
}

// MetaGet retrieves the value of given keey in the meta bucket.
func (db *DB) MetaGet(key []byte) (val []byte, err error) {
	err = db.kv.View(func(tx *bolt.Tx) error {
		val = tx.Bucket([]byte("meta")).Get(key)
		if val == nil {
			return ErrNotFound
		}
		return nil
	})
	return val, err
}

type idxStat struct {
	Name  string
	Count int
}

type Stats struct {
	Path    string
	Size    int64
	Records int
	Indexes []idxStat
}

func (db *DB) Stats() Stats {
	stats := Stats{
		Path: db.kv.Path(),
	}
	db.kv.View(func(tx *bolt.Tx) error {
		stats.Size = tx.Size()
		stats.Records = tx.Bucket([]byte("products")).Stats().KeyN

		return tx.Bucket([]byte("indexes")).ForEach(func(k, v []byte) error {
			if v == nil {
				stats.Indexes = append(stats.Indexes,
					idxStat{
						Name:  string(k),
						Count: tx.Bucket([]byte("indexes")).Bucket(k).Stats().KeyN,
					})
			}
			return nil
		})
	})
	return stats

}

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
