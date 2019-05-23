package store

// Store is a generic key-value store. The key must be of type string, though
// there are no restrictions on the type of the value.
type Store interface {

	// Insert writes the key-value into the store.
	Insert(key string, value interface{}) error

	// Get the value associated with the given key. This function returns
	// ErrKeyNotFound if the key cannot be found.
	Get(key string, value interface{}) error

	// Delete the value with the given key from the store. It is safe to use
	// this function to delete a key which is not in the store.
	Delete(key string) error
}

// Iterable is a Store that can iterate over its key-value tuples.
type Iterable interface {
	Store

	// Size returns the number of key-value tuples in the Iterable Store.
	Size() (int, error)

	// Iterator returns an Iterator which can be used to iterate through all
	// key-value tuples in the Iterable Store.
	Iterator() Iterator
}

// Iterator is used to iterate through the data in the store.
type Iterator interface {

	// Next will progress the iterator to the next element. If there are more
	// elements in the iterator, then it will return true, otherwise it will
	// return false.
	Next() bool

	// Key of the current key-value tuple. Calling Key() without calling
	// Next() or no next item in the iter will result in `ErrIndexOutOfRange`
	Key() (string, error)

	// Value will unmarshal the stored value into the given interface if
	// there's any. Calling Value() without calling Next() or no next item
	// in the iter will result in `ErrIndexOutOfRange`
	Value(value interface{}) error
}
