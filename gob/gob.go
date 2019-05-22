package gob

import (
	"bytes"
	"encoding/gob"

	"github.com/renproject/kv/db"
	"github.com/renproject/kv/store"
)

type iterable struct {
	db db.Iterable
}

func New(db db.Iterable) store.Iterable {
	return &iterable{
		db: db,
	}
}

func (store *iterable) Insert(key string, value interface{}) error {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(value); err != nil {
		return err
	}
	return store.db.Insert(key, buf.Bytes())
}

func (store *iterable) Get(key string, value interface{}) error {
	data, err := store.db.Get(key)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(data)
	return gob.NewDecoder(buf).Decode(value)
}

func (store *iterable) Delete(key string) error {
	return store.db.Delete(key)
}

func (store *iterable) Size() (int, error) {
	return store.db.Size()
}

func (store *iterable) Iterator() (store.Iterator, error) {
	iter := store.db.Iterator()
	return NewIterator(iter), nil
}

type iterator struct {
	iter db.Iterator
}

func NewIterator(iter db.Iterator) store.Iterator {
	return &iterator{
		iter: iter,
	}
}

func (iter *iterator) Next() bool {
	return iter.iter.Next()
}

func (iter *iterator) Key() (string, error) {
	return iter.iter.Key()
}

func (iter *iterator) Value(value interface{}) error {
	data, err := iter.iter.Value()
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(data)
	return gob.NewDecoder(buf).Decode(value)
}
