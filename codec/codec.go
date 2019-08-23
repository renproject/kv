package codec

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

// JSONCodec is a JSON implementation of the `db.Codec`.
var JSONCodec jsonCodec

// jsonCodec is a JSON implementation of the `db.Codec`. It encodes and decodes
// data using the JSON standard.
type jsonCodec struct{}

// Encode implements the `db.Codec`
func (jsonCodec) Encode(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}

// Decode implements the `db.Codec`
func (jsonCodec) Decode(data []byte, value interface{}) error {
	return json.Unmarshal(data, &value)
}

func (jsonCodec) String() string {
	return "json"
}

// GobCodec is a gob implementation of the `db.Codec`.
var GobCodec gobCodec

// gobCodec is a gob implementation of the `db.Codec`. It encodes and decodes
// data using the golang `encoding/gob` package.
type gobCodec struct{}

// Encode implements the `db.Codec`
func (gobCodec) Encode(obj interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(obj); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decode implements the `db.Codec`
func (gobCodec) Decode(data []byte, value interface{}) error {
	buf := bytes.NewBuffer(data)
	return gob.NewDecoder(buf).Decode(value)
}

func (gobCodec) String() string {
	return "gob"
}
