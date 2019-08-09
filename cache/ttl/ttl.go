package ttl

import (
	"fmt"
	"sync"
	"time"

	"github.com/renproject/kv/db"
)

type inMemTTL struct {
	timeToLive    time.Duration
	pruneInterval time.Duration
	codec         db.Codec

	dbMu *sync.Mutex
	db   db.DB
}

func New(ldb db.DB, timeToLive time.Duration, pruneInterval time.Duration, codec db.Codec) (db.DB, error) {
	table, err := ldb.NewTable("ttl_0", codec)
	if err != nil {
		if err == db.ErrTableAlreadyExists {
			table, err = ldb.Table("ttl_0")
		}
		if err != nil {
			return nil, err
		}
	}
	var lastSlotDeleted int64
	if err := table.Get("lastSlotDeleted", &lastSlotDeleted); err != nil {
		if err == db.ErrKeyNotFound {
			lastSlotDeleted = (time.Now().UnixNano() - int64(timeToLive.Nanoseconds())) / int64(pruneInterval.Nanoseconds())
			err = table.Insert("lastSlotDeleted", lastSlotDeleted)
		}
		if err != nil {
			return nil, err
		}
	}

	ttlDB := inMemTTL{
		timeToLive:    timeToLive,
		pruneInterval: pruneInterval,
		codec:         codec,

		dbMu: new(sync.Mutex),
		db:   ldb,
	}

	go ttlDB.prune(lastSlotDeleted)

	return &ttlDB, nil
}

func (ttlDB *inMemTTL) NewTable(name string, codec db.Codec) (db.Table, error) {
	ttlDB.dbMu.Lock()
	defer ttlDB.dbMu.Unlock()

	return ttlDB.db.NewTable(name, codec)
}

func (ttlDB *inMemTTL) Table(name string) (db.Table, error) {
	ttlDB.dbMu.Lock()
	defer ttlDB.dbMu.Unlock()

	return ttlDB.db.Table(name)
}

func (ttlDB *inMemTTL) Insert(name string, key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	slotTableName := fmt.Sprintf("ttl_%d", time.Now().UnixNano()/int64(ttlDB.pruneInterval.Nanoseconds()))

	ttlDB.dbMu.Lock()
	defer ttlDB.dbMu.Unlock()

	if err := ttlDB.db.Insert(name, key, value); err != nil {
		return err
	}
	slotTable, err := ttlDB.db.Table(slotTableName)
	if err != nil {
		if err == db.ErrTableNotFound {
			slotTable, err = ttlDB.db.NewTable(slotTableName, ttlDB.codec)
		}
		if err != nil {
			return err
		}
	}

	return slotTable.Insert(key, name)
}

func (ttlDB *inMemTTL) Get(name string, key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	ttlDB.dbMu.Lock()
	defer ttlDB.dbMu.Unlock()

	return ttlDB.db.Get(name, key, value)
}

func (ttlDB *inMemTTL) Delete(name string, key string) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	ttlDB.dbMu.Lock()
	defer ttlDB.dbMu.Unlock()

	return ttlDB.db.Delete(name, key)
}

func (ttlDB *inMemTTL) Size(name string) (int, error) {
	ttlDB.dbMu.Lock()
	defer ttlDB.dbMu.Unlock()

	return ttlDB.db.Size(name)
}

func (ttlDB *inMemTTL) Iterator(name string) (db.Iterator, error) {
	ttlDB.dbMu.Lock()
	defer ttlDB.dbMu.Unlock()

	return ttlDB.db.Iterator(name)
}

func (ttlDB *inMemTTL) prune(lastSlotDeleted int64) {
	ticker := time.NewTicker(ttlDB.pruneInterval)
	for range ticker.C {
		func() {
			newSlotToDelete := (time.Now().UnixNano() - int64(ttlDB.timeToLive.Nanoseconds())) / int64(ttlDB.pruneInterval.Nanoseconds())

			func() {
				ttlDB.dbMu.Lock()
				defer ttlDB.dbMu.Unlock()

				for slot := lastSlotDeleted + 1; slot <= newSlotToDelete; slot++ {
					slotTable := fmt.Sprintf("ttl_%d", slot)
					iter, err := ttlDB.db.Iterator(slotTable)
					if err != nil {
						continue
					}
					for iter.Next() {
						key, err := iter.Key()
						if err != nil {
							continue
						}
						var table string
						if err := iter.Value(&table); err != nil {
							continue
						}
						ttlDB.db.Delete(table, key)
						ttlDB.db.Delete(slotTable, key)
					}
				}
				lastSlotDeleted = newSlotToDelete
				ttlDB.db.Insert("ttl_0", "lastSlotDeleted", newSlotToDelete)
			}()
		}()
	}
}
