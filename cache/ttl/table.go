package ttl

import (
	"sync"
	"time"

	"github.com/renproject/kv/cache"
	"github.com/renproject/kv/db"
)

// table is a in-memory TTL cache implementation of the `db.Table`.
type table struct {
	dbTable    db.Table
	timeToLive time.Duration

	lastSeenMu *sync.Mutex
	lastSeen   map[string]time.Time
}

// NewTable returns a new table that wraps a `db.Table` along with an TTL cache.
func NewTable(dbTable db.Table, timeToLive time.Duration) (db.Table, error) {
	lastSeen := map[string]time.Time{}
	now := time.Now()
	iter, err := dbTable.Iterator()
	if err != nil {
		return nil, err
	}
	for iter.Next() {
		key, err := iter.Key()
		if err != nil {
			return nil, err
		}
		lastSeen[key] = now
	}

	return &table{
		dbTable:    dbTable,
		timeToLive: timeToLive,

		lastSeenMu: new(sync.Mutex),
		lastSeen:   lastSeen,
	}, nil
}

// Insert implements the `db.Table` interface.
func (table *table) Insert(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	if err := table.dbTable.Insert(key, value); err != nil {
		return err
	}

	table.lastSeenMu.Lock()
	defer table.lastSeenMu.Unlock()
	table.lastSeen[key] = time.Now()
	return nil
}

// Get implements the `db.Table` interface.
func (table *table) Get(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	table.lastSeenMu.Lock()
	defer table.lastSeenMu.Unlock()

	lastSeen, ok := table.lastSeen[key]
	if !ok {
		return db.ErrKeyNotFound
	}
	if time.Now().After(lastSeen.Add(table.timeToLive)) {
		if err := table.deleteWithoutLock(key); err != nil {
			return err
		}
		return cache.ErrExpired
	}
	table.lastSeen[key] = time.Now()

	return table.dbTable.Get(key, value)
}

// Delete implements the `db.Table` interface.
func (table *table) Delete(key string) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	table.lastSeenMu.Lock()
	defer table.lastSeenMu.Unlock()

	return table.deleteWithoutLock(key)
}

// Size implements the `db.Table` interface.
func (table *table) Size() (int, error) {
	table.lastSeenMu.Lock()
	defer table.lastSeenMu.Unlock()

	counter := 0
	now := time.Now()
	for key, lastSeen := range table.lastSeen {
		if now.Sub(lastSeen) > table.timeToLive {
			if err := table.deleteWithoutLock(key); err != nil {
				return 0, err
			}
		} else {
			counter++
		}
	}

	return counter, nil
}

// Iterator implements the `db.Table` interface.
func (table *table) Iterator() (db.Iterator, error) {
	table.lastSeenMu.Lock()
	defer table.lastSeenMu.Unlock()

	now := time.Now()
	for key, lastSeen := range table.lastSeen {
		if now.Sub(lastSeen) > table.timeToLive {
			if err := table.deleteWithoutLock(key); err != nil {
				return nil, err
			}
		} else {
			table.lastSeen[key] = now
		}
	}

	return table.dbTable.Iterator()
}

// The `deleteWithoutLock` method will delete a key-value tuple without locking
// the `lastSeenMu` mutex. This method must only be called from methods that
// have already acquired a lock on the `lastSeenMu` method.
func (table *table) deleteWithoutLock(key string) error {
	if err := table.dbTable.Delete(key); err != nil {
		return err
	}
	delete(table.lastSeen, key)
	return nil
}
