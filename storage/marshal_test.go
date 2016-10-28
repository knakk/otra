package storage

import (
	"encoding/xml"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/knakk/kbp/onix"
)

func TestPrimedCodec(t *testing.T) {
	xmlbytes, err := ioutil.ReadFile(filepath.Join("testdata", "sample.xml"))
	if err != nil {
		t.Fatal(err)
	}
	var p onix.Product
	if err := xml.Unmarshal(xmlbytes, &p); err != nil {
		t.Fatal(err)
	}

	codec, err := newPrimedCodec(&onix.Product{})
	if err != nil {
		t.Fatal(err)
	}
	dec := codec.NewMarshaler()
	out, err := dec.Marshal(&p)
	if err != nil {
		t.Fatal(err)
	}

	enc := codec.NewUnmarshaler()
	got, err := enc.Unmarshal(out)
	if err != nil {
		t.Fatal(err)
	}

	marshalled, err := xml.MarshalIndent(got, "  ", "  ")
	if err != nil {
		t.Fatal(err)
	}

	var p2 onix.Product
	if err := xml.Unmarshal(marshalled, &p2); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(p, p2) {
		t.Error("encoding/decoding roundtrip failed")
	}
}
