package cache

import (
	"sync"

	"github.com/renproject/kv/db"
)

type MuDB struct {
	mu *sync.Mutex
	db db.DB
}

func NewMuDB(db db.DB) db.DB {
	return &MuDB{
		mu: new(sync.Mutex),
		db: db,
	}
}

func (m *MuDB) NewTable(name string, codec db.Codec) (db.Table, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.db.NewTable(name, codec)
}

func (m *MuDB) Table(name string) (db.Table, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.db.Table(name)
}

func (m *MuDB) Insert(name string, key string, value interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.db.Insert(name, key, value)
}

func (m *MuDB) Get(name string, key string, value interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.db.Get(name, key, value)
}

func (m *MuDB) Delete(name string, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.db.Delete(name, key)
}

func (m *MuDB) Size(name string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.db.Size(name)
}

func (m *MuDB) Iterator(table string) (db.Iterator, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.db.Iterator(table)
}
