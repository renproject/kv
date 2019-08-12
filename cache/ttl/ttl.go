package ttl

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/renproject/kv/db"
)

var (
	// Name of the table where we store the prune pointer.
	PrunePointerTableName = "ttl_0"

	// PrunePointerKey is the key of the key-value pair which we can use to
	// query the current prune pointer.
	PrunePointerKey = "prune_pointer"
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
	return ttlDB.db.Insert(slotTableName, key, name)
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
						if err != nil {
							return err
						}

						for iter.Next() {
							key, err := iter.Key()
							if err != nil {
								return err
							}
							var tableName string
							if err := iter.Value(&tableName); err != nil {
								return err
							}

							if err := ttlDB.db.Delete(tableName, key); err != nil {
								return err
							}
							if err := ttlDB.db.Delete(slotTable, key); err != nil {
								return err
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
	var pointer int64
	err := ttlDB.db.Get(PrunePointerTableName, PrunePointerKey, &pointer)
	if err == db.ErrKeyNotFound {
		slot := ttlDB.slotNo(time.Now())
		return slot - 1, ttlDB.db.Insert(PrunePointerTableName, PrunePointerKey, slot-1)
	}
	return pointer, err
}
