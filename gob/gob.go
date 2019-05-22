package gob

import (
	"bytes"
	"encoding/gob"

	"github.com/renproject/kv/db"
)

type Store struct {
	db db.DB
}

func NewStore(db db.DB) *Store {
	return &Store{
		db: db,
	}
}

func (store *Store) Insert(key string, value interface{}) error {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(value); err != nil {
		return err
	}
	return store.db.Insert(key, buf.Bytes())
}

func (store *Store) Get(key string, value interface{}) error {
	data, err := store.db.Get(key)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(data)
	return gob.NewDecoder(buf).Decode(value)
}

func (store *Store) Delete(key string) error {
	return store.db.Delete(key)
}

type IterableStore struct {
	db db.IterableDB
}

func NewIterableStore(db db.IterableDB) *IterableStore {
	return &IterableStore{
		db: db,
	}
}

func (store *IterableStore) Insert(key string, value interface{}) error {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(value); err != nil {
		return err
	}
	return store.db.Insert(key, buf.Bytes())
}

func (store *IterableStore) Get(key string, value interface{}) error {
	data, err := store.db.Get(key)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(data)
	return gob.NewDecoder(buf).Decode(value)
}

func (store *IterableStore) Delete(key string) error {
	return store.db.Delete(key)
}

func (store *IterableStore) Size() (int, error) {
	return store.db.Size()
}

func (store *IterableStore) Iterator() *Iterator {
	iter := store.db.Iterator()
	return NewIterator(iter)
}

type Iterator struct {
	iter db.Iterator
}

func NewIterator(iter db.Iterator) *Iterator {
	return &Iterator{
		iter: iter,
	}
}

func (iter *Iterator) Next() bool {
	return iter.iter.Next()
}

func (iter *Iterator) Key() (string, error) {
	return iter.iter.Key()
}

func (iter *Iterator) Value(value interface{}) error {
	data, err := iter.iter.Value()
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(data)
	return gob.NewDecoder(buf).Decode(value)
}
