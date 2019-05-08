package store

import (
	"fmt"
)

var ErrKeyNotFound = fmt.Errorf("key not found")

type Store interface {
	Read(key string, value interface{}) error
	Write(key string, value interface{}) error
	ReadData(key string) ([]byte, error)
	WriteData(key string, data []byte) error
	Delete(key string) error
}
