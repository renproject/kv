package codec

import (
	"bytes"
	"encoding/gob"
	"encoding/json"

	"github.com/renproject/kv/db"
)

// JsonCodec is a json implementation of the `db.Codec`. It encodes and decodes
// data using the json standard.
type JsonCodec struct {}

// NewJSON returns a `db.Codec` using JSON.
func NewJSON() db.Codec{
	return JsonCodec{}
}

// Encode implements the `db.Codec`
func (JsonCodec) Encode(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}

// Decode implements the `db.Codec`
func (JsonCodec) Decode(data []byte, value interface{}) error {
	return json.Unmarshal(data, &value)
}

func (JsonCodec) String()string {
	return "json"
}

// GobCodec is a gob implementation of the `db.Codec`. It encodes and decodes
// data using the golang `encoding/gob` package.
type GobCodec struct {

}

// NewGOB returns a `db.Codec` using gob.
func NewGOB() db.Codec{
	return GobCodec{}
}

// Encode implements the `db.Codec`
func (GobCodec) Encode(obj interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(obj); err != nil {
		return nil ,err
	}

	return buf.Bytes(), nil
}

// Decode implements the `db.Codec`
func (GobCodec) Decode(data []byte, value interface{}) error {
	buf := bytes.NewBuffer(data)
	return gob.NewDecoder(buf).Decode(value)
}

func (GobCodec) String()string {
	return "gob"
}
