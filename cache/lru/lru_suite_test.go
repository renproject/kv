package lru_test

import (
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/syndtr/goleveldb/leveldb"
)

var ldb *leveldb.DB

func TestLeveldb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lru Suite")
}

// Creating a leveldb instance before running the entire test suite.
var _ = BeforeSuite(func() {
	err := exec.Command("mkdir", "-p", ".leveldb").Run()
	Expect(err).NotTo(HaveOccurred())

	ldb, err = leveldb.OpenFile("./.leveldb", nil)
	Expect(err).NotTo(HaveOccurred())
	time.Sleep(time.Second)
})

// Close and remove all the related files after finishing the test suite.
var _ = AfterSuite(func() {
	Expect(ldb.Close()).NotTo(HaveOccurred())
	Expect(exec.Command("rm", "-rf", "./.leveldb").Run()).NotTo(HaveOccurred())
})

// Clean the levelDB instance after each test
var _ = JustAfterEach(func() {
	iter := ldb.NewIterator(nil, nil)
	for iter.Next() {
		Expect(ldb.Delete(iter.Key(), nil)).Should(Succeed())
	}
	defer iter.Release()
})
