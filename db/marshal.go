package db

import (
	"bytes"
	"encoding/gob"

	"github.com/knakk/kbp/onix"
)

type gobCodec struct {
	sample *onix.Product
	data   []byte
}

func newPrimedCodec(p *onix.Product) (*gobCodec, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	if err := enc.Encode(p); err != nil {
		return nil, err
	}
	if err := gob.NewDecoder(bytes.NewReader(b.Bytes())).Decode(p); err != nil {
		return nil, err
	}
	return &gobCodec{
		sample: p,
		data:   b.Bytes(),
	}, nil
}

type primedEncoder struct {
	enc *gob.Encoder
	b   *bytes.Buffer
}

type primedDecoder struct {
	dec *gob.Decoder
	b   *bytes.Buffer
}

func (e *primedEncoder) Marshal(p *onix.Product) ([]byte, error) {
	e.b.Reset()
	if err := e.enc.Encode(p); err != nil {
		return nil, err
	}
	return e.b.Bytes(), nil
}

func (d *primedDecoder) Unmarshal(b []byte) (*onix.Product, error) {
	d.b.Reset()
	d.b.Write(b)
	var p onix.Product
	if err := d.dec.Decode(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (c *gobCodec) NewMarshaler() *primedEncoder {
	e := primedEncoder{b: new(bytes.Buffer)}
	e.enc = gob.NewEncoder(e.b)
	e.enc.Encode(c.sample)
	e.b.Reset()
	return &e
}

func (c *gobCodec) NewUnmarshaler() *primedDecoder {
	d := primedDecoder{b: new(bytes.Buffer)}
	d.b.Write(c.data)
	d.dec = gob.NewDecoder(d.b)
	var p onix.Product
	d.dec.Decode(&p)
	d.b.Reset()
	return &d
}
