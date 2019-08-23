package badgerdb_test

import (
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestBadgerdb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Badgerdb Suite")
}

// Creating a badgerDB instance before running the entire test suite.
var _ = BeforeSuite(func() {
	err := exec.Command("mkdir", "-p", ".badgerdb").Run()
	Expect(err).NotTo(HaveOccurred())
})

// Clean the badgerDB instance after each test
var _ = JustAfterEach(func() {
	Expect(exec.Command("rm", "-rf", "./.badgerdb").Run()).NotTo(HaveOccurred())
})
