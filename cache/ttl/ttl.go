package ttl

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/renproject/kv/db"
	"golang.org/x/crypto/sha3"
)

var (
	// PrunePointerKey is the key of the key-value pair which we can use to
	// query the current prune pointer. This will always stored
	PrunePointerKey = "prunePointer"
)

type inMemTTL struct {
	nameHash      string
	timeToLive    time.Duration
	pruneInterval time.Duration
	db            db.DB
}

// Insert the key into the table and also record timestamp associated the key
// in a corresponding table in the db.
func (ttlTable *inMemTTL) Insert(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}
	if err := ttlTable.db.Insert(ttlTable.keyWithPrefix(key), value); err != nil {
		return fmt.Errorf("error inserting ttl data: %v", err)
	}

	// Insert the current timestamp for future pruning.
	slot := ttlTable.slotNo(time.Now())
	return ttlTable.db.Insert(ttlTable.keyWithSlotPrefix(key, slot), key)
}

// Get implements the db.Table interface.
func (ttlTable *inMemTTL) Get(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	return ttlTable.db.Get(ttlTable.keyWithPrefix(key), value)
}

// Delete only deletes the data, but not the timestamp which will be handled
// by the prune function.
func (ttlTable *inMemTTL) Delete(key string) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	return ttlTable.db.Delete(ttlTable.keyWithPrefix(key))
}

// Size implements the db.Table interface.
func (ttlTable *inMemTTL) Size() (int, error) {
	return ttlTable.db.Size(ttlTable.keyWithPrefix(""))
}

// Iterator implements the db.Table interface.
func (ttlTable *inMemTTL) Iterator() db.Iterator {
	return ttlTable.db.Iterator(ttlTable.keyWithPrefix(""))
}

// New returns a new ttl wrapper over the given database.
// The underlying database cannot have any database has a prefix of `ttl_`.
func New(ctx context.Context, database db.DB, name string, timeToLive time.Duration, pruneInterval time.Duration) db.Table {
	hash := sha3.Sum256([]byte(name))
	ttlDB := &inMemTTL{
		nameHash:      string(hash[:]),
		timeToLive:    timeToLive,
		pruneInterval: pruneInterval,
		db:            database,
	}

	// Start a background goroutine to prune the db from the prune pointer.
	pointer, err := ttlDB.prunePointer()
	if err != nil {
		panic(fmt.Sprintf("cannot read prune pointer, err = %v", err))
	}

	// NOTE: WE NEED TO TAKE A EXTERNAL CONTEXT TELLING US WHEN TO STOP PRUNING
	// OR WHEN THE DB IS CLOSING. THIS IS BECAUSE WE NEED TO CREATE AN ITERATOR
	// WHEN PRUNING AND IT CAN CAUSE PANIC IF THE UNDERLYING DB IS CLOSED.
	go ttlDB.runPruneOnInterval(ctx, pointer)
	return ttlDB
}

// prune will periodically prune the underlying database and stores the prune pointer
// in the db.
func (ttlTable *inMemTTL) runPruneOnInterval(ctx context.Context, pointer int64) {
	ticker := time.NewTicker(ttlTable.pruneInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// todo : how can we catch if the error is caused by the underlying db been closed.
			if err := ttlTable.prune(pointer); err != nil {
				log.Printf("prune failed, err = %v", err)
				return
			}
		}
	}
}

// prune prune the table
func (ttlTable *inMemTTL) prune(pointer int64) error {
	newSlotToDelete := ttlTable.slotNo(time.Now().Add(-ttlTable.pruneInterval))
	for slot := pointer + 1; slot <= newSlotToDelete; slot++ {
		slotTable := ttlTable.keyWithSlotPrefix("", slot)
		iter := ttlTable.db.Iterator(slotTable)
		for iter.Next() {
			key, err := iter.Key()
			if err != nil {
				return err
			}
			var tableName string
			if err := iter.Value(&tableName); err != nil {
				return err
			}

			if err := ttlTable.db.Delete(ttlTable.keyWithPrefix(key)); err != nil {
				return err
			}
			if err := ttlTable.db.Delete(ttlTable.keyWithSlotPrefix(key, slot)); err != nil {
				return err
			}
		}
	}
	pointer = newSlotToDelete
	return ttlTable.db.Insert(ttlTable.keyWithSlotPrefix(PrunePointerKey, 0), newSlotToDelete)
}

// slotNo returns the slot number in which the given unix timestamp is belonging to.
func (ttlTable *inMemTTL) slotNo(moment time.Time) int64 {
	return moment.UnixNano() / ttlTable.pruneInterval.Nanoseconds()
}

// prunePointer returns the current prune pointer which all slots before or equals to
// it have been pruned. It will initialize the pointer if the db is new.
func (ttlTable *inMemTTL) prunePointer() (int64, error) {
	var pointer int64
	err := ttlTable.db.Get(ttlTable.keyWithSlotPrefix(PrunePointerKey, 0), &pointer)
	if err == db.ErrKeyNotFound {
		slot := ttlTable.slotNo(time.Now())
		return slot - 1, ttlTable.db.Insert(ttlTable.keyWithSlotPrefix(PrunePointerKey, 0), slot-1)
	}
	return pointer, err
}

func (ttlTable *inMemTTL) keyWithSlotPrefix(key string, i int64) string {
	return fmt.Sprintf("%v_slot%d_%v", ttlTable.nameHash, i, key)
}

func (ttlTable *inMemTTL) keyWithPrefix(name string) string {
	return fmt.Sprintf("ttlDataTable_%v", name)
}
