package testutil

import (
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
