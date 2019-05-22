package json

import (
	"encoding/json"

	"github.com/renproject/kv/store"
)

type Store struct {
	store store.Store
}

func NewStore(store store.Store) *Store {
	return &Store{
		store: store,
	}
}

func (kv *Store) Insert(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return kv.store.Insert(key, data)
}

func (kv *Store) Get(key string, value interface{}) error {
	data, err := kv.store.Get(key)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func (kv *Store) Delete(key string) error {
	return kv.store.Delete(key)
}

type IterableStore struct {
	store store.IterableStore
}

func NewIterableStore(store store.IterableStore) *IterableStore {
	return &IterableStore{
		store: store,
	}
}

func (store *IterableStore) Insert(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return store.store.Insert(key, data)
}

func (store *IterableStore) Get(key string, value interface{}) error {
	data, err := store.store.Get(key)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func (store *IterableStore) Delete(key string) error {
	return store.store.Delete(key)
}

func (store *IterableStore) Size() (int, error) {
	return store.store.Size()
}

func (store *IterableStore) Iterator() *Iterator {
	iter := store.store.Iterator()
	return NewIterator(iter)
}

type Iterator struct {
	iter store.Iterator
}

func NewIterator(iter store.Iterator) *Iterator {
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
	return json.Unmarshal(data, value)
}
