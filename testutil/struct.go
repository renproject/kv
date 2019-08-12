package testutil

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing/quick"
	"time"
)

var Ran = rand.New(rand.NewSource(time.Now().Unix()))

type TestStruct struct {
	A string
	B int
	C bool
	D []byte
	E map[string]float64
}

func RandomTestStruct() TestStruct {
	t := reflect.TypeOf(TestStruct{})
	value, ok := quick.Value(t, Ran)
	if !ok {
		panic("cannot create random test struct")
	}
	return value.Interface().(TestStruct)
}

func (testStruct *TestStruct) Equal(other TestStruct) bool {
	if len(testStruct.E) != len(other.E) {
		return false
	}
	for key, val := range testStruct.E {
		otherVal, ok := other.E[key]
		if !ok || otherVal != val {
			return false
		}
	}
	return testStruct.A == other.A && testStruct.B == other.B && testStruct.C == other.C && bytes.Equal(testStruct.D, other.D)
}

func RandomTestStructGroups(group, entriesPerGroup int) [][]TestStruct {
	testEntries := make([][]TestStruct, group)
	for i := range testEntries {
		testEntries[i] = make([]TestStruct, entriesPerGroup)
		for j := range testEntries[i] {
			testEntries[i][j] = RandomTestStruct()
		}
	}

	return testEntries
}

func RandomNonDupStrings(i int) []string {
	cap := rand.Intn(i)
	dup := map[string]struct{}{}
	res := make([]string, 0, cap)

	for len(dup) < cap {
		t := reflect.TypeOf("")
		value, ok := quick.Value(t, Ran)
		if !ok {
			panic("cannot create random test struct")
		}
		v := value.Interface().(string)
		if _, ok := dup[v]; ok {
			continue
		}
		dup[v] = struct{}{}
		res = append(res, v)
	}

	return res
}

// CheckErrors takes a list of errors and check if any of them is not nil.
// It returns the first non-nil error or nil if all of the errors are nil.
func CheckErrors(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
