package store_test

import (
	"math/rand"
	"os/exec"
	"reflect"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/republicprotocol/store"

	"github.com/dgraph-io/badger"
)

var _ = Describe("BadgerDB implementation of key-value Store", func() {

	initDB := func() *badger.DB{
		Expect(exec.Command("mkdir", "-p",".tmp").Run()).NotTo(HaveOccurred())
		opts := badger.DefaultOptions
		opts.Dir = "./.tmp"
		opts.ValueDir = "./.tmp"
		db, err := badger.Open(opts)
		Expect(err).NotTo(HaveOccurred())
		return db
	}

	closeDB := func(db *badger.DB) {
		Expect(db.Close()).NotTo(HaveOccurred())
		Expect(exec.Command("rm", "-rf", "./.tmp").Run()).NotTo(HaveOccurred())
	}

	Context("when reading and writing with data-expiration", func() {
		It("should be able to store a struct with pre-defined value type", func() {
				db := initDB()
				defer closeDB(db)
				badgerDB := NewBadgerDB(db)
				Expect(badgerDB.Entries()).Should(Equal(0))

				value := randomTestStruct(rand.New(rand.NewSource(time.Now().Unix())))
				key := value.A
				var newValue testStruct
				Expect(badgerDB.Read(key, &newValue)).Should(Equal(ErrKeyNotFound))
				Expect(badgerDB.Write(key, value)).NotTo(HaveOccurred())

				Expect(badgerDB.Read(key, &newValue)).NotTo(HaveOccurred())
				Expect(reflect.DeepEqual(value, newValue)).Should(BeTrue())
				Expect(badgerDB.Entries()).Should(Equal(1))

				Expect(badgerDB.Delete(key)).NotTo(HaveOccurred())
				Expect(badgerDB.Read(key, &newValue)).Should(Equal(ErrKeyNotFound))
				Expect(badgerDB.Entries()).Should(Equal(0))

		})

		It("should be able to return an iterator of the db and the number of entries in the store.", func() {
			// Init the badgerDB
			db := initDB()
			defer closeDB(db)
			badgerDB := NewBadgerDB(db)
			ran := rand.New(rand.NewSource(time.Now().Unix()))

			// Write random number of values into the DB
			num := rand.Intn(128)
			all := map[string]testStruct{}
			for i := 0; i < num; i++ {
				value := randomTestStruct(ran)
				value.A = string(i)
				all[value.A] = value
				Expect(badgerDB.Write(value.A, value)).NotTo(HaveOccurred())
			}

			// Expect the DB has the right number of entries.
			Expect(badgerDB.Entries()).Should(Equal(num))

			// Expect the iterator gives us all the values we entered
			iter := badgerDB.Iterator()
			for iter.Next() {
				var value testStruct
				Expect(iter.Value(&value)).NotTo(HaveOccurred())
				_, ok := all[value.A]
				Expect(ok).Should(BeTrue())
				delete(all,value.A)
			}
			Expect(len(all)).Should(BeZero())
		})
	})

	Context("some edge cases", func() {
		It("should return a iter which works looping through a db having zero items", func() {
			// Init the badgerDB
			db := initDB()
			defer closeDB(db)
			badgerDB := NewBadgerDB(db)

			iter := badgerDB.Iterator()
			Expect(iter.Next()).Should(BeFalse())
		})
	})
})