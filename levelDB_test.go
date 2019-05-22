package store_test

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"os/exec"
	"reflect"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv"

	"github.com/syndtr/goleveldb/leveldb"
)

var _ = Describe("levelDB implementation of key-value Store", func() {

	initDB := func() *leveldb.DB {
		db, err := leveldb.OpenFile("./.leveldb", nil)
		Expect(err).NotTo(HaveOccurred())
		return db
	}

	closeDB := func(db *leveldb.DB) {
		Expect(db.Close()).NotTo(HaveOccurred())
		Expect(exec.Command("rm", "-rf", "./.leveldb").Run()).NotTo(HaveOccurred())
	}

	Context("when reading and writing", func() {
		It("should be able to store a struct with pre-defined value type", func() {
			db := initDB()
			defer closeDB(db)
			badgerDB := NewLevelDB(db)

			value := randomTestStruct(rand.New(rand.NewSource(time.Now().Unix())))
			key := value.A
			var newValue testStruct
			Expect(badgerDB.Read(key, &newValue)).Should(Equal(ErrKeyNotFound))
			Expect(badgerDB.Write(key, value)).NotTo(HaveOccurred())

			Expect(badgerDB.Read(key, &newValue)).NotTo(HaveOccurred())
			Expect(reflect.DeepEqual(value, newValue)).Should(BeTrue())

			Expect(badgerDB.Delete(key)).NotTo(HaveOccurred())
			Expect(badgerDB.Read(key, &newValue)).Should(Equal(ErrKeyNotFound))
		})

		It("should be able to read and write data in bytes directly", func() {
			// Init the badgerDB
			db := initDB()
			defer closeDB(db)
			badgerDB := NewLevelDB(db)
			ran := rand.New(rand.NewSource(time.Now().Unix()))

			randomStruct := randomTestStruct(ran)
			key := randomStruct.A
			value, err := json.Marshal(randomStruct)
			Expect(err).NotTo(HaveOccurred())

			_, err = badgerDB.ReadData(key)
			Expect(err).Should(Equal(ErrKeyNotFound))

			Expect(badgerDB.WriteData(key, value)).NotTo(HaveOccurred())
			stored, err := badgerDB.ReadData(key)
			Expect(err).NotTo(HaveOccurred())
			Expect(bytes.Compare(stored, value)).Should(BeZero())
		})
	})
})
