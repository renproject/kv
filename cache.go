package store

import (
	"encoding/json"
	"errors"
	"sync"
	"time"
)

var (
	// ErrDataExpired is returned when the data is expired.
	ErrDataExpired = errors.New("data expired")

	// ErrNoMoreItems is returned when no more items left in the iterator.
	ErrNoMoreItems = errors.New("no more items in iterator")
)

type cache map[string][]byte

// NewCache returns a cache implementation of the Store. The returned Store is not safe for concurrent use.
func NewCache() Store {
	return cache{}
}

// Read implements the `Store` interface.
func (cache cache) Read(key string, value interface{}) error {
	val, ok := cache[key]
	if !ok {
		return ErrKeyNotFound
	}
	return json.Unmarshal(val, value)
}

// ReadData implements the `Store` interface.
func (cache cache) ReadData(key string) ([]byte, error) {
	val, ok := cache[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return val, nil
}

// Write implements the `Store` interface.
func (cache cache) Write(key string, value interface{}) error {
	val, err := json.Marshal(value)
	if err != nil {
		return err
	}
	cache[key] = val
	return nil
}

// WriteData implements the `Store` interface.
func (cache cache) WriteData(key string, data []byte) error {
	cache[key] = data
	return nil
}

// Delete implements the `Store` interface.
func (cache cache) Delete(key string) error {
	delete(cache, key)
	return nil
}

// iterableCache is an in-memory implementation of the Store. After the data expires, it returns ErrDataExpired if the data is
// out of date. This store is safe for concurrent read and write.
type iterableCache struct {
	mu         *sync.RWMutex
	data       map[string][]byte
	lastSeen   map[string]int64
	timeToLive int64
}

// NewIterableCache returns a new cached Store. It is safe for concurrent read and write. The stored value will be live
// with the given living time. If `timeToLive` is less than or equal to zero, the data will have be always live.
func NewIterableCache(timeToLive int64) IterableStore {
	return iterableCache{
		mu:         new(sync.RWMutex),
		data:       map[string][]byte{},
		lastSeen:   map[string]int64{},
		timeToLive: timeToLive,
	}
}

// Read implements the `Store` interface.
func (cache iterableCache) Read(key string, value interface{}) error {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	// Check if the value is expired.
	if cache.timeToLive > 0 {
		lastSeen, ok := cache.lastSeen[key]
		if !ok {
			return ErrKeyNotFound
		}
		if (time.Now().Unix() - lastSeen) > cache.timeToLive {
			return ErrDataExpired
		}
	}

	val, ok := cache.data[key]
	if !ok {
		return ErrKeyNotFound
	}

	return json.Unmarshal(val, value)
}

// ReadData implements the `Store` interface.
func (cache iterableCache) ReadData(key string) ([]byte, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	// Check if the value is expired.
	if cache.timeToLive > 0 {
		lastSeen, ok := cache.lastSeen[key]
		if !ok {
			return nil, ErrKeyNotFound
		}
		if (time.Now().Unix() - lastSeen) > cache.timeToLive {
			return nil, ErrDataExpired
		}
	}

	val, ok := cache.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return val, nil
}

// Write implements the `Store` interface.
func (cache iterableCache) Write(key string, value interface{}) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	val, err := json.Marshal(value)
	if err != nil {
		return err
	}
	cache.data[key] = val
	if cache.timeToLive > 0 {
		cache.lastSeen[key] = time.Now().Unix()
	}

	return nil
}

// WriteData impements the `Store` interface.
func (cache iterableCache) WriteData(key string, value []byte) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	cache.data[key] = value
	if cache.timeToLive > 0 {
		cache.lastSeen[key] = time.Now().Unix()
	}

	return nil
}

// Delete implements the `Store` interface.
func (cache iterableCache) Delete(key string) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	delete(cache.data, key)
	delete(cache.lastSeen, key)
	return nil
}

// Entries implements the `Store` interface.
func (cache iterableCache) Entries() (int, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	return len(cache.data), nil
}

// Iterator implements the `Store` interface.
func (cache iterableCache) Iterator() Iterator {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	return newCacheIterator(cache.data)
}

func newCacheIterator(data map[string][]byte) Iterator {
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
func (iter *cacheIterator) Value(value interface{}) error {
	if iter.index >= len(iter.keys) {
		return ErrNoMoreItems
	}

	return json.Unmarshal(iter.values[iter.index], &value)
}
