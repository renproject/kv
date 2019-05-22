package store

type Store interface {
	Insert(key string, value interface{}) error
	Get(key string, value interface{}) error
	Delete(key string) error
}

type Iterable interface {
	Store

	Size() (int, error)
	Iterator() (Iterator, error)
}

type Iterator interface {
	Next() bool
	Key() (string, error)
	Value(value interface{}) error
}
