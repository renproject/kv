package memdb

import (
	"errors"
	"sync"
	"time"

	"github.com/renproject/kv/db"
)

var (
	// ErrDataExpired is returned when the data is expired.
	ErrDataExpired = errors.New("data expired")

	// ErrNoMoreItems is returned when no more items left in the iterator.
	ErrNoMoreItems = errors.New("no more items in iterator")
)

type memdb struct {
	mu *sync.RWMutex
	db map[string][]byte
}

// NewDB returns a key-value database that is implemented in-memory. This
// implementation is fast, but does not store data on-disk and does not support
// iteration. It is safe for concurrent use.
func NewDB(cap int) db.DB {
	return &memdb{
		mu: new(sync.RWMutex),
		db: make(map[string][]byte, cap),
	}
}

func (memdb *memdb) Insert(key string, value []byte) error {
	memdb.mu.Lock()
	defer memdb.mu.Unlock()

	memdb.db[key] = value
	return nil
}

func (memdb *memdb) Get(key string) ([]byte, error) {
	memdb.mu.RLock()
	defer memdb.mu.RUnlock()

	val, ok := memdb.db[key]
	if !ok {
		return nil, db.ErrNotFound
	}
	return val, nil
}

func (memdb *memdb) Delete(key string) error {
	memdb.mu.Lock()
	defer memdb.mu.Unlock()

	delete(memdb.db, key)
	return nil
}

// NewDB returns a key-value database that is implemented in-memory. This
// implementation is fast, but should not be used for persistent data storage,
// and does not support iteration. An in-memory database will drop key-value
// tuples non-deterministically and should only be used as a cache or temporary
// storage.
type iterableMemDB struct {
	mu         *sync.RWMutex
	data       map[string][]byte
	lastSeen   map[string]int64
	timeToLive int64
}

// NewIterable returns a key-value database that is implemented in-memory and
// supports iteration. This implementation is fast, but does not store data
// on-disk. An iteraable in-memory database will drop key-value tuples after a
// specific duration and should only be used as a cache or temporary storage. It
// is safe for concurrent use.
func NewIterable(timeToLive int64) db.Iterable {
	return &iterableMemDB{
		mu:         new(sync.RWMutex),
		data:       map[string][]byte{},
		lastSeen:   map[string]int64{},
		timeToLive: timeToLive,
	}
}

func (cache iterableMemDB) Insert(key string, value []byte) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	cache.data[key] = value
	if cache.timeToLive > 0 {
		cache.lastSeen[key] = time.Now().Unix()
	}

	return nil
}

func (cache iterableMemDB) Get(key string) ([]byte, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	// Check if the value is expired.
	if cache.timeToLive > 0 {
		lastSeen, ok := cache.lastSeen[key]
		if !ok {
			return nil, db.ErrNotFound
		}
		if (time.Now().Unix() - lastSeen) > cache.timeToLive {
			return nil, ErrDataExpired
		}
	}

	val, ok := cache.data[key]
	if !ok {
		return nil, db.ErrNotFound
	}

	return val, nil
}

// Delete implements the `Store` interface.
func (cache iterableMemDB) Delete(key string) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	delete(cache.data, key)
	delete(cache.lastSeen, key)
	return nil
}

// Size implements the `Store` interface.
func (cache iterableMemDB) Size() (int, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	return len(cache.data), nil
}

// Iterator implements the `Store` interface.
func (cache iterableMemDB) Iterator() db.Iterator {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	return newCacheIterator(cache.data)
}

func newCacheIterator(data map[string][]byte) db.Iterator {
	iter := &cacheIterator{
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

// cacheIterator is a cache implementation of the `Iterator`.
type cacheIterator struct {
	index  int
	keys   []string
	values [][]byte
}

// Next implements the `Iterator` interface.
func (iter *cacheIterator) Next() bool {
	iter.index++
	return iter.index < len(iter.keys)
}

// Key implements the `Iterator` interface.
func (iter *cacheIterator) Key() (string, error) {
	if iter.index >= len(iter.keys) {
		return "", ErrNoMoreItems
	}
	return iter.keys[iter.index], nil
}

// Value implements the `Iterator` interface.
func (iter *cacheIterator) Value() ([]byte, error) {
	if iter.index >= len(iter.keys) {
		return nil, ErrNoMoreItems
	}

	return iter.values[iter.index], nil
}
