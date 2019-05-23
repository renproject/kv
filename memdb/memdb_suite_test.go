package memdb_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestMemdb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Memdb Suite")
}
