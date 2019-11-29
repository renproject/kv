package codec

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
)

// BinaryCodec is a Binary implementation of the `db.Codec`.
var BinaryCodec binaryCodec

// binaryCodec is a Binary implementation of the `db.Codec`. It encodes and
// decodes data using the Binary standard.
type binaryCodec struct{}

// Encode implements the `db.Codec`
func (binaryCodec) Encode(obj interface{}) ([]byte, error) {
	switch obj.(type) {
	case encoding.BinaryMarshaler:
		return obj.(encoding.BinaryMarshaler).MarshalBinary()
	default:
		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.LittleEndian, obj); err != nil {
			return buf.Bytes(), err
		}
		return buf.Bytes(), nil
	}
}

// Decode implements the `db.Codec`
func (binaryCodec) Decode(data []byte, value interface{}) error {
	switch v := value.(type) {
	case encoding.BinaryUnmarshaler:
		return value.(encoding.BinaryUnmarshaler).UnmarshalBinary(data)
	case *[]byte:
		*v = make([]byte, len(data))
		copy(*v, data)
		return nil
	default:
		buf := bytes.NewBuffer(data)
		return binary.Read(buf, binary.LittleEndian, value)
	}
}

func (binaryCodec) String() string {
	return "binary"
}

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
