package badgerdb_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestBadgerdb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Badgerdb Suite")
}
