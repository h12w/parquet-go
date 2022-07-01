package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/compress/snappy"
)

type (
	// Tuple is a list of values of different types, the number and types of elements are generated dynamically at runtime
	// however, once generated, the number and types are fixed throughout a whole parquet file
	Tuple []interface{}

	tupleField struct {
		parquet.Node
		index int
		name  string
	}
	tupleNode struct {
		gotype reflect.Type
		fields []parquet.Field
		parquet.Group
	}
)

func (n tupleNode) Fields() []parquet.Field                 { return n.fields }
func (f tupleField) Value(base reflect.Value) reflect.Value { return base.Index(f.index) }
func (f tupleField) Name() string                           { return f.name }

func (t Tuple) Node() parquet.Node {
	fields := make([]parquet.Field, len(t))
	for i, value := range t {
		fields[i] = tupleField{
			Node:  parquet.Optional(parquet.NodeOf(value)),
			index: i,
			name:  strconv.Itoa(i),
		}
	}
	return tupleNode{
		gotype: reflect.TypeOf(t),
		fields: fields,
	}
}

func (t Tuple) String() string {
	var b strings.Builder
	b.WriteString("[")
	for i, v := range t {
		if i > 0 {
			b.WriteString(",")
		}
		if v != nil {
			fmt.Fprintf(&b, "%v(%v)", reflect.TypeOf(v), v)
		} else {
			b.WriteString(fmt.Sprint(v))
		}
	}
	b.WriteString("]")
	return b.String()
}

type testTupleStruct struct {
	Keys   []string
	Values Tuple
}

func newCustomTuple() Tuple {
	return Tuple{
		// all primitive values
		bool(false),
		int8(0),
		int16(0),
		int32(0),
		int64(0),
		uint8(0),
		uint16(0),
		uint32(0),
		uint64(0),
		float32(0.0),
		float64(0.0),
		int(0),
		uint(0),
		uintptr(0),
		string(""),
		[]byte(nil),
		[5]byte{},

		// slices
		[]bool(nil),
		[]int8(nil),
		[]int16(nil),
		[]int32(nil),
		[]int64(nil),
		[]uint8(nil),
		[]uint16(nil),
		[]uint32(nil),
		[]uint64(nil),
		[]float32(nil),
		[]float64(nil),
		[]int(nil),
		[]uint(nil),
		[]uintptr(nil),
		[]string(nil),

		// arrays are not supported except byte array
	}
}

func newTestTupleStruct() testTupleStruct {
	return testTupleStruct{
		Values: newCustomTuple(),
	}
}

func ExampleTestTupleStructSchema_String() {
	schema := parquet.NewSchema("TestTupleStruct", parquet.NodeOf(testTupleStruct{
		Values: newCustomTuple(),
	}))
	fmt.Println(strings.ReplaceAll(schema.String(), "\t", "    "))
	// Output:
	// message TestTupleStruct {
	//     repeated binary Keys (STRING);
	//     required group Values {
	//         optional boolean 0;
	//         optional int32 1 (INT(8,true));
	//         optional int32 2 (INT(16,true));
	//         optional int32 3 (INT(32,true));
	//         optional int64 4 (INT(64,true));
	//         optional int32 5 (INT(8,false));
	//         optional int32 6 (INT(16,false));
	//         optional int32 7 (INT(32,false));
	//         optional int64 8 (INT(64,false));
	//         optional float 9;
	//         optional double 10;
	//         optional int64 11 (INT(64,true));
	//         optional int64 12 (INT(64,false));
	//         optional int64 13 (INT(64,false));
	//         optional binary 14 (STRING);
	//         optional binary 15;
	//         optional fixed_len_byte_array(5) 16;
	//         repeated boolean 17;
	//         repeated int32 18 (INT(8,true));
	//         repeated int32 19 (INT(16,true));
	//         repeated int32 20 (INT(32,true));
	//         repeated int64 21 (INT(64,true));
	//         optional binary 22;
	//         repeated int32 23 (INT(16,false));
	//         repeated int32 24 (INT(32,false));
	//         repeated int64 25 (INT(64,false));
	//         repeated float 26;
	//         repeated double 27;
	//         repeated int64 28 (INT(64,true));
	//         repeated int64 29 (INT(64,false));
	//         repeated int64 30 (INT(64,false));
	//         repeated binary 31 (STRING);
	//     }
	// }
}

func TestWriteReadTupleStructs(t *testing.T) {
	// prepare test data
	schemaRowValue := newTestTupleStruct()
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	randStr := func() string {
		s, ok := quick.Value(reflect.TypeOf(string("")), random)
		if !ok {
			t.Fatal("cannot create a random string")
		}
		return s.String()
	}
	randomValue := func(typ reflect.Type) interface{} {
		v, ok := quick.Value(typ, random)
		if !ok {
			t.Fatalf("cannot create a random %v", typ)
		}
		if typ.Kind() == reflect.Slice && v.Len() == 0 {
			return nil
		}
		return v.Interface()
	}
	randTuple := func() Tuple {
		tuple := newCustomTuple()
		for i := range tuple {
			typ := reflect.TypeOf(tuple[i])
			tuple[i] = randomValue(typ)
		}
		return tuple
	}
	testRows := make([]testTupleStruct, len(schemaRowValue.Values))
	for i := range testRows {
		tuple := randTuple()
		tuple[i] = nil
		testRows[i] = testTupleStruct{
			Keys:   []string{randStr()},
			Values: tuple,
		}
	}

	// write
	outputFile := bytes.NewBuffer(nil)
	w := parquet.NewWriter(
		outputFile,
		&parquet.WriterConfig{
			Schema:      parquet.NewSchema("", parquet.NodeOf(schemaRowValue)),
			Compression: &snappy.Codec{},
		},
	)
	for _, row := range testRows {
		if err := w.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// read
	r := bytes.NewReader(outputFile.Bytes())
	rd := parquet.NewReader(r)
	i := 0
	for {
		row := newTestTupleStruct()
		if err := rd.Read(&row); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		expected := testRows[i]
		if !reflect.DeepEqual(row, expected) {
			if !reflect.DeepEqual(row.Keys, expected.Keys) {
				t.Fatalf("expect \n%v\ngot\n%v", expected.Keys, row.Keys)
			}
			if !reflect.DeepEqual(row.Values, expected.Values) {
				t.Fatalf("expect \n%v\ngot\n%v", expected.Values, row.Values)
			}
		}
		i++
	}
	if i != len(testRows) {
		t.Fatalf("not enough rows are read, expect %d, got %d", len(testRows), i)
	}
	if err := rd.Close(); err != nil {
		t.Fatal(err)
	}
}
