package memdb_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMemdb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Memdb Suite")
}
