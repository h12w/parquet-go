package parquet_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/segmentio/parquet-go"
)

var dictionaryTypes = [...]parquet.Type{
	parquet.BooleanType,
	parquet.Int32Type,
	parquet.Int64Type,
	parquet.Int96Type,
	parquet.FloatType,
	parquet.DoubleType,
	parquet.ByteArrayType,
	parquet.FixedLenByteArrayType(10),
	parquet.FixedLenByteArrayType(16),
	parquet.Uint(32).Type(),
	parquet.Uint(64).Type(),
}

func TestDictionary(t *testing.T) {
	for _, typ := range dictionaryTypes {
		t.Run(typ.String(), func(t *testing.T) {
			testDictionary(t, typ)
		})
	}
}

func testDictionary(t *testing.T, typ parquet.Type) {
	const columnIndex = 1
	const numValues = 500

	dict := typ.NewDictionary(columnIndex, 0, nil)
	values := make([]parquet.Value, numValues)
	indexes := make([]int32, numValues)
	lookups := make([]parquet.Value, numValues)

	f := randValueFuncOf(typ)
	r := rand.New(rand.NewSource(0))

	for i := range values {
		values[i] = f(r)
		values[i] = values[i].Level(0, 0, columnIndex)
	}

	mapping := make(map[int32]parquet.Value, numValues)

	for i := 0; i < numValues; {
		j := i + ((numValues-i)/2 + 1)
		if j > numValues {
			j = numValues
		}

		dict.Insert(indexes[i:j], values[i:j])

		for k, v := range values[i:j] {
			mapping[indexes[i+k]] = v
		}

		for _, index := range indexes[i:j] {
			if index < 0 || index >= int32(dict.Len()) {
				t.Fatalf("index out of bounds: %d", index)
			}
		}

		r.Shuffle(j-i, func(a, b int) {
			indexes[a+i], indexes[b+i] = indexes[b+i], indexes[a+i]
		})

		dict.Lookup(indexes[i:j], lookups[i:j])

		for lookupIndex, valueIndex := range indexes[i:j] {
			want := mapping[valueIndex]
			got := lookups[lookupIndex+i]

			if !parquet.DeepEqual(want, got) {
				t.Fatalf("wrong value looked up at index %d: want=%#v got=%#v", valueIndex, want, got)
			}
		}

		minValue := values[i]
		maxValue := values[i]

		for _, value := range values[i+1 : j] {
			switch {
			case typ.Compare(value, minValue) < 0:
				minValue = value
			case typ.Compare(value, maxValue) > 0:
				maxValue = value
			}
		}

		lowerBound, upperBound := dict.Bounds(indexes[i:j])
		if !parquet.DeepEqual(lowerBound, minValue) {
			t.Errorf("wrong lower bound betwen indexes %d and %d: want=%#v got=%#v", i, j, minValue, lowerBound)
		}
		if !parquet.DeepEqual(upperBound, maxValue) {
			t.Errorf("wrong upper bound between indexes %d and %d: want=%#v got=%#v", i, j, maxValue, upperBound)
		}

		i = j
	}

	for i := range lookups {
		lookups[i] = parquet.Value{}
	}

	dict.Lookup(indexes, lookups)

	for lookupIndex, valueIndex := range indexes {
		want := mapping[valueIndex]
		got := lookups[lookupIndex]

		if !parquet.Equal(want, got) {
			t.Fatalf("wrong value looked up at index %d: want=%+v got=%+v", valueIndex, want, got)
		}
	}
}

func BenchmarkDictionary(b *testing.B) {
	const numValues = 1000

	tests := []struct {
		scenario string
		init     func(parquet.Dictionary, []int32, []parquet.Value)
		test     func(parquet.Dictionary, []int32, []parquet.Value)
	}{
		{
			scenario: "Bounds",
			init:     parquet.Dictionary.Insert,
			test: func(dict parquet.Dictionary, indexes []int32, _ []parquet.Value) {
				dict.Bounds(indexes)
			},
		},

		{
			scenario: "Insert",
			test:     parquet.Dictionary.Insert,
		},

		{
			scenario: "Lookup",
			init:     parquet.Dictionary.Insert,
			test:     parquet.Dictionary.Lookup,
		},
	}

	for _, test := range tests {
		b.Run(test.scenario, func(b *testing.B) {
			for _, typ := range dictionaryTypes {
				dict := typ.NewDictionary(0, 0, make([]byte, 0, 4*numValues))
				values := make([]parquet.Value, numValues)

				f := randValueFuncOf(typ)
				r := rand.New(rand.NewSource(0))

				for i := range values {
					values[i] = f(r)
				}

				indexes := make([]int32, len(values))
				if test.init != nil {
					test.init(dict, indexes, values)
				}

				b.Run(typ.String(), func(b *testing.B) {
					start := time.Now()

					for i := 0; i < b.N; i++ {
						test.test(dict, indexes, values)
					}

					seconds := time.Since(start).Seconds()
					b.ReportMetric(float64(numValues*b.N)/seconds, "value/s")
				})
			}
		})
	}
}
