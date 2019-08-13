package ttl_test

import (
	"context"
	"fmt"
	"reflect"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/cache/ttl"

	"github.com/renproject/kv/codec"
	"github.com/renproject/kv/db"
	"github.com/renproject/kv/memdb"
	"github.com/renproject/kv/testutil"
)

var codecs = []db.Codec{
	codec.JSONCodec,
	codec.GobCodec,
}

var _ = Describe("in-memory LRU cache", func() {
	for i := range codecs {
		codec := codecs[i]

		Context("when creating table", func() {

			It("should be able to read and write values to the db", func() {
				readAndWrite := func(name string, key string, value testutil.TestStruct) bool {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					ttlDB, err := New(ctx, memdb.New(codec), 10*time.Second, 5*time.Second, codec)
					Expect(err).NotTo(HaveOccurred())

					// Make sure the key is not nil
					if key == "" {
						return true
					}
					val := testutil.TestStruct{D: []byte{}}
					err = ttlDB.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					// Should be able to read the value after inserting.
					Expect(ttlDB.Insert(name, key, value)).NotTo(HaveOccurred())
					err = ttlDB.Get(name, key, &val)
					Expect(err).NotTo(HaveOccurred())
					Expect(reflect.DeepEqual(val, value)).Should(BeTrue())

					// Expect no value exists after deleting the value.
					Expect(ttlDB.Delete(name, key)).NotTo(HaveOccurred())
					err = ttlDB.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))
					return true
				}

				Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
			})

			It("should be able to iterate through the db using the iterator", func() {
				iteration := func(name string, values []testutil.TestStruct) bool {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					ttlDB, err := New(ctx, memdb.New(codec), time.Second, 100*time.Millisecond, codec)
					Expect(err).NotTo(HaveOccurred())

					// Insert all values and make a map for validation.
					allValues := map[string]testutil.TestStruct{}
					for i, value := range values {
						key := fmt.Sprintf("%v", i)
						Expect(ttlDB.Insert(name, key, value)).NotTo(HaveOccurred())
						allValues[key] = value
					}

					size, err := ttlDB.Size(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).Should(Equal(len(values)))

					// Expect iterator gives us all the key-value pairs we insert.
					iter, err := ttlDB.Iterator(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(iter).ShouldNot(BeNil())

					for iter.Next() {
						key, err := iter.Key()
						Expect(err).NotTo(HaveOccurred())
						value := testutil.TestStruct{D: []byte{}}
						err = iter.Value(&value)
						Expect(err).NotTo(HaveOccurred())

						stored, ok := allValues[key]
						Expect(ok).Should(BeTrue())
						Expect(reflect.DeepEqual(value, stored)).Should(BeTrue())
						delete(allValues, key)
					}
					return len(allValues) == 0
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when doing operations with empty keys", func() {
			It("should return ErrEmptyKey error", func() {
				test := func(name string, value testutil.TestStruct) bool {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					ttlDB, err := New(ctx, memdb.New(codec), time.Second, 100*time.Millisecond, codec)
					Expect(err).NotTo(HaveOccurred())

					Expect(ttlDB.Insert(name, "", value)).Should(Equal(db.ErrEmptyKey))
					Expect(ttlDB.Get(name, "", value)).Should(Equal(db.ErrEmptyKey))
					Expect(ttlDB.Delete(name, "")).Should(Equal(db.ErrEmptyKey))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when reading and writing with data-expiration", func() {
			It("should be able return error if the data has expired", func() {
				readAndWrite := func(name, key string, value testutil.TestStruct) bool {
					if key == "" {
						return true
					}
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					ttlDB, err := New(ctx, memdb.New(codec), 200*time.Millisecond, 100*time.Millisecond, codec)
					Expect(err).NotTo(HaveOccurred())

					newValue := testutil.TestStruct{D: []byte{}}
					Expect(ttlDB.Get(name, key, &newValue)).Should(Equal(db.ErrKeyNotFound))
					Expect(ttlDB.Insert(name, key, &value)).NotTo(HaveOccurred())
					Expect(ttlDB.Get(name, key, &newValue)).NotTo(HaveOccurred())
					Expect(reflect.DeepEqual(value, newValue)).Should(BeTrue())

					Eventually(func() error {
						return ttlDB.Get(name, key, &newValue)
					}, 2).Should(Equal(db.ErrKeyNotFound))

					return true
				}

				Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
			})

			It("should be able to prune the database automatically", func() {
				readAndWrite := func(name, key string, value testutil.TestStruct) bool {
					if key == "" {
						return true
					}

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					ttlDB, err := New(ctx, memdb.New(codec), 200*time.Millisecond, 100*time.Millisecond, codec)
					Expect(err).NotTo(HaveOccurred())

					newValue := testutil.TestStruct{D: []byte{}}
					Expect(ttlDB.Get(name, key, &newValue)).Should(Equal(db.ErrKeyNotFound))
					Expect(ttlDB.Insert(name, key, &value)).NotTo(HaveOccurred())
					Expect(ttlDB.Get(name, key, &newValue)).NotTo(HaveOccurred())
					Expect(reflect.DeepEqual(value, newValue)).Should(BeTrue())
					size, err := ttlDB.Size(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).To(Equal(1))

					Eventually(func() int {
						size, err = ttlDB.Size(name)
						Expect(err).NotTo(HaveOccurred())
						return size
					}, 2).Should(Equal(0))

					return true
				}

				Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
			})
		})
	}
})
