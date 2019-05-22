package cache

import (
	"sync"
	"time"

	"github.com/renproject/kv/db"
	"github.com/renproject/kv/store"
)

type ttl struct {
	iterable   store.Iterable
	timeToLive time.Duration

	lastSeenMu *sync.RWMutex
	lastSeen   map[string]time.Time
}

// NewTTL returns a cache that wraps an underlying store. Keys that have no been
// accessed for the specified duration will be automatically deleted from the
// underlying store. It is safe for concurrent use, as long as the underlying
// store is also safe for concurrent use.
func NewTTL(iterable store.Iterable, timeToLive time.Duration) store.Iterable {
	return &ttl{
		iterable:   iterable,
		timeToLive: timeToLive,

		lastSeenMu: new(sync.RWMutex),
		lastSeen:   map[string]time.Time{},
	}
}

// Insert a value into the underlying store. The key will have its access time
// set to the current time.
func (cache *ttl) Insert(key string, value interface{}) error {
	if err := cache.iterable.Insert(key, value); err != nil {
		return err
	}

	cache.lastSeenMu.RLock()
	defer cache.lastSeenMu.RUnlock()
	cache.lastSeen[key] = time.Now()

	return nil
}

// Get a value from the underlying store. The key will have its access time
// updated.
func (cache *ttl) Get(key string, value interface{}) error {
	cache.lastSeenMu.RLock()
	defer cache.lastSeenMu.RUnlock()

	lastSeen, ok := cache.lastSeen[key]
	if !ok {
		return db.ErrNotFound
	}
	if time.Now().After(lastSeen.Add(cache.timeToLive)) {
		if err := cache.deleteWithoutLock(key); err != nil {
			return err
		}
		return ErrExpired
	}
	cache.lastSeen[key] = time.Now()

	return cache.iterable.Get(key, value)
}

// Delete a value from the underlying store.
func (cache *ttl) Delete(key string) error {
	cache.lastSeenMu.Lock()
	defer cache.lastSeenMu.Unlock()

	return cache.deleteWithoutLock(key)
}

// Size returns the size of the underlying store.
func (cache *ttl) Size() (int, error) {
	return cache.iterable.Size()
}

// Iterator returns an iterator that can iterate over all key-value tuples in
// the underlying store. All keys in the cache will have their access times
// updated.
func (cache *ttl) Iterator() (store.Iterator, error) {
	cache.lastSeenMu.Lock()
	defer cache.lastSeenMu.Unlock()

	now := time.Now()
	for key := range cache.lastSeen {
		cache.lastSeen[key] = now
	}

	return cache.iterable.Iterator()
}

// The `deleteWithoutLock` method will delete a key-value tuple without locking
// the `lastSeenMu` mutex. This method must only be called from methods that
// have already acquired a lock on the `lastSeenMu` method.
func (cache *ttl) deleteWithoutLock(key string) error {
	if err := cache.iterable.Delete(key); err != nil {
		return err
	}
	delete(cache.lastSeen, key)
	return nil
}
