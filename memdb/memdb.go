package memdb

import (
	"errors"
	"sync"
	"time"

	"github.com/renproject/kv/db"
)

var (
	// ErrExpired is returned when the key-value tuple has expired.
	ErrExpired = errors.New("expired")

	// ErrEmptyIterator is returned when no more items left in the iterator.
	ErrEmptyIterator = errors.New("empty iterator")
)

type memdb struct {
	mu         *sync.RWMutex
	data       map[string][]byte
	lastSeen   map[string]int64
	timeToLive int64
}

func New(timeToLive int64) db.Iterable {
	return &memdb{
		mu:         new(sync.RWMutex),
		data:       map[string][]byte{},
		lastSeen:   map[string]int64{},
		timeToLive: timeToLive,
	}
}

func (memdb memdb) Insert(key string, value []byte) error {
	memdb.mu.Lock()
	defer memdb.mu.Unlock()

	memdb.data[key] = value
	if memdb.timeToLive > 0 {
		memdb.lastSeen[key] = time.Now().Unix()
	}

	return nil
}

func (memdb memdb) Get(key string) ([]byte, error) {
	memdb.mu.RLock()
	defer memdb.mu.RUnlock()

	// Check if the value is expired.
	if memdb.timeToLive > 0 {
		lastSeen, ok := memdb.lastSeen[key]
		if !ok {
			return nil, db.ErrNotFound
		}
		if (time.Now().Unix() - lastSeen) > memdb.timeToLive {
			return nil, ErrExpired
		}
	}

	val, ok := memdb.data[key]
	if !ok {
		return nil, db.ErrNotFound
	}

	return val, nil
}

// Delete implements the `Store` interface.
func (memdb memdb) Delete(key string) error {
	memdb.mu.Lock()
	defer memdb.mu.Unlock()

	delete(memdb.data, key)
	delete(memdb.lastSeen, key)
	return nil
}

// Size implements the `Store` interface.
func (memdb memdb) Size() (int, error) {
	memdb.mu.RLock()
	defer memdb.mu.RUnlock()

	return len(memdb.data), nil
}

// Iterator implements the `Store` interface.
func (memdb memdb) Iterator() db.Iterator {
	memdb.mu.RLock()
	defer memdb.mu.RUnlock()

	return newCacheIterator(memdb.data)
}

func newCacheIterator(data map[string][]byte) db.Iterator {
	iter := &iterator{
		index:  -1,
		keys:   make([]string, len(data)),
		values: make([][]byte, len(data)),
	}
	index := 0
	for key, value := range data {
		iter.keys[index] = key
		iter.values[index] = value
		index++
	}

	return iter
}

type iterator struct {
	index  int
	keys   []string
	values [][]byte
}

// Next implements the `Iterator` interface.
func (iter *iterator) Next() bool {
	iter.index++
	return iter.index < len(iter.keys)
}

// Key implements the `Iterator` interface.
func (iter *iterator) Key() (string, error) {
	if iter.index >= len(iter.keys) {
		return "", ErrEmptyIterator
	}
	return iter.keys[iter.index], nil
}

// Value implements the `Iterator` interface.
func (iter *iterator) Value() ([]byte, error) {
	if iter.index >= len(iter.keys) {
		return nil, ErrEmptyIterator
	}

	return iter.values[iter.index], nil
}
