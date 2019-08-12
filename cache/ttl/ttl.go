package ttl

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/renproject/kv/db"
)

type inMemTTL struct {
	timeToLive    time.Duration
	pruneInterval time.Duration
	codec         db.Codec
	db            db.DB
}

// New returns a new ttl wrapper over the given database.
// The underlying database cannot have any database has a prefix of `ttl_`.
func New(ctx context.Context, database db.DB, timeToLive time.Duration, pruneInterval time.Duration, codec db.Codec) (db.DB, error) {
	ttlDB := inMemTTL{
		timeToLive:    timeToLive,
		pruneInterval: pruneInterval,
		codec:         codec,
		db:            database,
	}

	ttlDB.prune(ctx)

	return &ttlDB, nil
}

// NewTable implements the `db.Table` interface.
func (ttlDB *inMemTTL) NewTable(name string, codec db.Codec) (db.Table, error) {
	return ttlDB.db.NewTable(name, codec)
}

// Table implements the `db.Table` interface.
func (ttlDB *inMemTTL) Table(name string) (db.Table, error) {
	return ttlDB.db.Table(name)
}

// Insert implements the `db.Table` interface.
func (ttlDB *inMemTTL) Insert(name string, key string, value interface{}) error {

	// Insert the new data entry into the underlying DB.
	if key == "" {
		return db.ErrEmptyKey
	}
	if err := ttlDB.db.Insert(name, key, value); err != nil {
		return err
	}

	// Insert the current timestamp for future pruning.
	slot := ttlDB.slotNo(time.Now())
	slotTableName := fmt.Sprintf("ttl_%d", slot)
	table, err := ttlDB.getTable(slotTableName)
	if err != nil {
		return err
	}
	return table.Insert(key, name)
}

// Get implements the `db.Table` interface.
func (ttlDB *inMemTTL) Get(name string, key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	return ttlDB.db.Get(name, key, value)
}

// Delete implements the `db.Table` interface.
func (ttlDB *inMemTTL) Delete(name string, key string) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	return ttlDB.db.Delete(name, key)
}

// Size implements the `db.Table` interface.
func (ttlDB *inMemTTL) Size(name string) (int, error) {
	return ttlDB.db.Size(name)
}

// Iterator implements the `db.Table` interface.
func (ttlDB *inMemTTL) Iterator(name string) (db.Iterator, error) {
	return ttlDB.db.Iterator(name)
}

// prune will periodically prune the underlying database and stores the prune pointer
// in the db.
func (ttlDB *inMemTTL) prune(ctx context.Context) {
	pointer, err := ttlDB.prunePointer()
	if err != nil {
		log.Fatalf("cannot read prune pointer, err = %v", err)
	}

	ticker := time.NewTicker(ttlDB.pruneInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := func() error {
					newSlotToDelete := ttlDB.slotNo(time.Now().Add(-1 * ttlDB.pruneInterval))
					for slot := pointer + 1; slot <= newSlotToDelete; slot++ {
						slotTable := fmt.Sprintf("ttl_%d", slot)

						iter, err := ttlDB.db.Iterator(slotTable)
						if err == db.ErrTableNotFound {
							continue
						} else if err != nil {
							return err
						}

						for iter.Next() {
							key, err := iter.Key()
							if err != nil {
								return err
							}
							var value string
							if err := iter.Value(&value); err != nil {
								return err
							}
							if err := ttlDB.db.Delete(value, key); err != nil {
								if err != db.ErrTableNotFound {
									return err
								}
							}
							if err := ttlDB.db.Delete(slotTable, key); err != nil {
								if err != db.ErrTableNotFound {
									return err
								}
							}
						}
					}
					pointer = newSlotToDelete
					return ttlDB.db.Insert("ttl_0", "pointer", newSlotToDelete)
				}()

				if err != nil {
					log.Printf("prune failed, err = %v", err)
				}
			}
		}
	}()
}

// slotNo returns the slot number in which the given unix timestamp is belonging to.
func (ttlDB *inMemTTL) slotNo(moment time.Time) int64 {
	return moment.UnixNano() / ttlDB.pruneInterval.Nanoseconds()
}

// prunePointer returns the current prune pointer which all slots before or equals to
// it have been pruned. It will initialize the pointer if the db is new.
func (ttlDB *inMemTTL) prunePointer() (int64, error) {
	table, err := ttlDB.getTable("ttl_0")
	if err != nil {
		return 0, err
	}

	var pointer int64
	err = table.Get("pointer", &pointer)
	if err == db.ErrKeyNotFound {
		slot := ttlDB.slotNo(time.Now())
		return slot - 1, table.Insert("pointer", slot-1)
	}
	return pointer, err
}

// getTable will get the table with given name from the underlying database. If
// no such table exists, it will create a new one and return it.
func (ttlDB *inMemTTL) getTable(name string) (db.Table, error) {
	table, err := ttlDB.db.Table(name)
	if err != nil {
		if err == db.ErrTableNotFound {
			return ttlDB.db.NewTable(name, ttlDB.codec)
		}
	}
	return table, err
}
