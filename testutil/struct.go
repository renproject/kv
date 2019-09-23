package testutil

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"testing/quick"
	"time"
)

var ran = rand.New(rand.NewSource(time.Now().Unix()))

// TestStruct is a struct which includes some commonly used types.
type TestStruct struct {
	A string
	B int
	C bool
	D []byte
	E map[string]float64
}

func (s TestStruct) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	aBytes := []byte(s.A)
	if err := binary.Write(buf, binary.LittleEndian, uint64(len(aBytes))); err != nil {
		return buf.Bytes(), fmt.Errorf("cannot write s.A len: %v", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, aBytes); err != nil {
		return buf.Bytes(), fmt.Errorf("cannot write s.A data: %v", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint64(s.B)); err != nil {
		return buf.Bytes(), fmt.Errorf("cannot write s.B: %v", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, s.C); err != nil {
		return buf.Bytes(), fmt.Errorf("cannot write s.C: %v", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint64(len(s.D))); err != nil {
		return buf.Bytes(), fmt.Errorf("cannot write s.D len: %v", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, s.D); err != nil {
		return buf.Bytes(), fmt.Errorf("cannot write s.D data: %v", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint64(len(s.E))); err != nil {
		return buf.Bytes(), fmt.Errorf("cannot write s.E len: %v", err)
	}
	for key, val := range s.E {
		keyBytes := []byte(key)
		if err := binary.Write(buf, binary.LittleEndian, uint64(len(keyBytes))); err != nil {
			return buf.Bytes(), fmt.Errorf("cannot write key len: %v", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, keyBytes); err != nil {
			return buf.Bytes(), fmt.Errorf("cannot write key data: %v", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, val); err != nil {
			return buf.Bytes(), fmt.Errorf("cannot write val: %v", err)
		}
	}
	return buf.Bytes(), nil
}

func (s *TestStruct) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	var numBytes uint64
	if err := binary.Read(buf, binary.LittleEndian, &numBytes); err != nil {
		return fmt.Errorf("cannot read s.A len: %v", err)
	}
	aBytes := make([]byte, numBytes)
	if _, err := buf.Read(aBytes); err != nil {
		return fmt.Errorf("cannot read s.A data: %v", err)
	}
	s.A = string(aBytes)
	var b int64
	if err := binary.Read(buf, binary.LittleEndian, &b); err != nil {
		return fmt.Errorf("cannot read s.B: %v", err)
	}
	s.B = int(b)
	if err := binary.Read(buf, binary.LittleEndian, &s.C); err != nil {
		return fmt.Errorf("cannot read s.C: %v", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &numBytes); err != nil {
		return fmt.Errorf("cannot read s.D len: %v", err)
	}
	dBytes := make([]byte, numBytes)
	if _, err := buf.Read(dBytes); err != nil {
		return fmt.Errorf("cannot read s.D data: %v", err)
	}
	s.D = dBytes
	var lenE uint64
	if err := binary.Read(buf, binary.LittleEndian, &lenE); err != nil {
		return fmt.Errorf("cannot read s.E len: %v", err)
	}
	s.E = make(map[string]float64, lenE)
	for i := uint64(0); i < lenE; i++ {
		if err := binary.Read(buf, binary.LittleEndian, &numBytes); err != nil {
			return fmt.Errorf("cannot read key len: %v", err)
		}
		keyBytes := make([]byte, numBytes)
		if _, err := buf.Read(keyBytes); err != nil {
			return fmt.Errorf("cannot read key data: %v", err)
		}
		var val float64
		if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
			return fmt.Errorf("cannot read val: %v", err)
		}
		s.E[string(keyBytes)] = val
	}
	return nil
}

// RandomTestStruct returns a random `TestStruct`
func RandomTestStruct() TestStruct {
	t := reflect.TypeOf(TestStruct{})
	value, ok := quick.Value(t, ran)
	if !ok {
		panic("cannot create random test struct")
	}
	return value.Interface().(TestStruct)
}

// RandomTestStructGroups creates a group of random TestStructs.
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

// RandomNonDupStrings returns a list of non-duplicate strings.
func RandomNonDupStrings(i int) []string {
	cap := rand.Intn(i)
	dup := map[string]struct{}{}
	res := make([]string, 0, cap)

	for len(res) < cap {
		t := reflect.TypeOf("")
		value, ok := quick.Value(t, ran)
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
