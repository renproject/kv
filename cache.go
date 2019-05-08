package store

import "encoding/json"

type cache map[string][]byte

func NewCache() Store {
	return cache{}
}

func (cache cache) Read(key string, value interface{}) error {
	val, ok := cache[key]
	if !ok {
		return ErrKeyNotFound
	}
	return json.Unmarshal(val, value)
}

func (cache cache) ReadData(key string) ([]byte, error) {
	val, ok := cache[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return val, nil
}

func (cache cache) Write(key string, value interface{}) error {
	val, err := json.Marshal(value)
	if err != nil {
		return err
	}
	cache[key] = val
	return nil
}

func (cache cache) WriteData(key string, data []byte) error {
	cache[key] = data
	return nil
}

func (cache cache) Delete(key string) error {
	delete(cache, key)
	return nil
}
