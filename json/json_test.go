package json_test

import (
	"fmt"
	"math/rand"
	"os/exec"
	"reflect"
	"testing/quick"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/renproject/kv/badgerdb"
	"github.com/renproject/kv/db"
	"github.com/renproject/kv/memdb"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/json"
)

var Ran = rand.New(rand.NewSource(time.Now().Unix()))

type testStruct struct {
	A string
	B int
	C bool
	D []byte
	E map[string]float64
}

func randomTestStruct(ran *rand.Rand) testStruct {
	t := reflect.TypeOf(testStruct{})
	vaule, ok := quick.Value(t, ran)
	Expect(ok).Should(BeTrue())
	return vaule.Interface().(testStruct)
}

var _ = Describe("JSON implementation of Store", func() {

	initBadgerDB := func() *badger.DB {
		Expect(exec.Command("mkdir", "-p", ".badgerdb").Run()).NotTo(HaveOccurred())
		opts := badger.DefaultOptions("./.badgerdb")
		opts.Dir = "./.badgerdb"
		opts.ValueDir = "./.badgerdb"
		db, err := badger.Open(opts)
		Expect(err).NotTo(HaveOccurred())
		return db
	}

	initDBs := func() ([]db.Iterable, func()) {
		dbs := make([]db.Iterable, 2)
		dbs[0] = memdb.New()
		bdb := initBadgerDB()
		dbs[1] = badgerdb.New(bdb)

		return dbs, func() {
			Expect(bdb.Close()).NotTo(HaveOccurred())
			Expect(exec.Command("rm", "-rf", "./.badgerdb").Run()).NotTo(HaveOccurred())
		}
	}

	Context("when reading and writing", func() {
		It("should be able read and write value ", func() {
			dbs, close := initDBs()
			defer close()

			readAndWrite := func(iterable db.Iterable) func(key string, value testStruct) bool {
				return func(key string, value testStruct) bool {
					store := New(iterable)
					if key == "" {
						return true
					}

					var newValue testStruct
					Expect(store.Get(key, &newValue)).Should(Equal(db.ErrNotFound))
					Expect(store.Insert(key, value)).NotTo(HaveOccurred())

					Expect(store.Get(key, &newValue)).NotTo(HaveOccurred())
					Expect(reflect.DeepEqual(value, newValue)).Should(BeTrue())

					Expect(store.Delete(key)).NotTo(HaveOccurred())
					Expect(store.Get(key, &newValue)).Should(Equal(db.ErrNotFound))

					return true
				}
			}

			for _, db := range dbs {
				testFunc := readAndWrite(db)
				Expect(quick.Check(testFunc, nil)).NotTo(HaveOccurred())
			}
		})
	})

	Context("when iterating", func() {
		It("should be able iterate through the store", func() {
			dbs, close := initDBs()
			defer close()

			iterating := func(iterable db.Iterable) func(key string, value testStruct) bool {
				return func(key string, value testStruct) bool {
					store := New(iterable)

					// Expect the initial size to be 0.
					size, err := store.Size()
					Expect(err).NotTo(HaveOccurred())
					Expect(size).Should(Equal(0))

					// Insert random number of values into the store.
					num := rand.Intn(128)
					allData := map[string]testStruct{}
					for i := 0; i < num; i++ {
						value := randomTestStruct(Ran)
						value.A = fmt.Sprintf("%v", i)
						allData[value.A] = value
						Expect(store.Insert(value.A, value)).NotTo(HaveOccurred())
					}

					// Expect the size to be the number of value we inserted.
					size, err = store.Size()
					Expect(err).NotTo(HaveOccurred())
					Expect(size).Should(Equal(num))

					// Expect the iterator to be able to give us all values.
					iter, err := store.Iterator()
					Expect(err).NotTo(HaveOccurred())
					for iter.Next() {
						var wrongType []byte
						err := iter.Value(&wrongType)
						Expect(err).To(HaveOccurred())

						var value testStruct
						key, err := iter.Key()
						Expect(err).NotTo(HaveOccurred())
						err = iter.Value(&value)
						_, ok := allData[key]
						Expect(ok).Should(BeTrue())
						Expect(store.Delete(key)).NotTo(HaveOccurred())
						delete(allData, key)
					}

					// Expect the size to be the number of value we inserted.
					size, err = store.Size()
					Expect(err).NotTo(HaveOccurred())
					Expect(size).Should(Equal(0))
					return len(allData) == 0
				}
			}

			for _, db := range dbs {
				testFunc := iterating(db)
				Expect(quick.Check(testFunc, nil)).NotTo(HaveOccurred())
			}
		})
	})

	Context("negative tests for reading and writing", func() {
		It("should return an error when doing something wrong", func() {
			dbs, close := initDBs()
			defer close()

			negativeTest := func(iterable db.Iterable) func(key string, value testStruct) bool {
				return func(key string, value testStruct) bool {
					store := New(iterable)

					// Expect an error returned when trying store unmarshable data type.
					badKey, badValue := "key", make(chan struct{})
					Expect(store.Insert(badKey, badValue)).To(HaveOccurred())

					// Expect an error returned when trying read the value to a invalid type.
					Expect(store.Insert(fmt.Sprintf("%v", value.B), value)).NotTo(HaveOccurred())
					var wrongType []byte
					Expect(store.Get(value.A, &wrongType)).To(HaveOccurred())

					return true
				}
			}

			for _, db := range dbs {
				testFunc := negativeTest(db)
				Expect(quick.Check(testFunc, nil)).NotTo(HaveOccurred())
			}
		})
	})

	Context("negative tests for iterating", func() {
		It("should return an error when doing something wrong", func() {
			dbs, close := initDBs()
			defer close()

			negativeTest := func(iterable db.Iterable) func(key string, value testStruct) bool {
				return func(key string, value testStruct) bool {
					store := New(iterable)

					if key == "" {
						return true
					}

					// Expect an error returned when trying to get key/value without calling Next()
					iter, err := store.Iterator()
					Expect(err).NotTo(HaveOccurred())
					_, err = iter.Key()
					Expect(err).Should(Equal(db.ErrIndexOutOfRange))
					var val testStruct
					err = iter.Value(&val)
					Expect(err).Should(Equal(db.ErrIndexOutOfRange))
					Expect(iter.Next()).Should(BeFalse())

					// Expect an error returned when trying to get key/value when there's not next value.
					Expect(store.Insert(key, value)).NotTo(HaveOccurred())
					iter, err = store.Iterator()
					Expect(err).NotTo(HaveOccurred())
					for iter.Next() {
					}
					_, err = iter.Key()
					Expect(err).Should(Equal(db.ErrIndexOutOfRange))
					err = iter.Value(&val)
					Expect(err).Should(Equal(db.ErrIndexOutOfRange))
					Expect(iter.Next()).Should(BeFalse())
					Expect(store.Delete(key)).NotTo(HaveOccurred())

					return true
				}
			}

			for _, db := range dbs {
				testFunc := negativeTest(db)
				Expect(quick.Check(testFunc, nil)).NotTo(HaveOccurred())
			}
		})
	})
})
