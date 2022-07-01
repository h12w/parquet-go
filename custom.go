package parquet

import "reflect"

// Noder allows a custom Go type that can return its schema node
type Noder interface {
	Node() Node
}

// NodeOf returns a schema node from a Go value
//
// It panics if it cannot do so
func NodeOf(v interface{}) Node {
	return nodeOfValue(reflect.ValueOf(v))
}
