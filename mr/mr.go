// Package mr contains the core types for the MapReduce implementation.
// Stage 1 (this file): sequential, single-process, no concurrency.
// You'll fill in the actual map/reduce logic and the sequential driver.
package mr

// KeyValue is the type emitted by a Map function and consumed by a Reduce function.
type KeyValue struct {
	Key   string
	Value string
}

// MapFunc takes the name of a file and its contents, and returns a slice
// of key/value pairs. This mirrors the "emit" semantics discussed earlier:
// each call conceptually emits one KeyValue; here, for the sequential
// version, we just collect them all into a slice and return it.
type MapFunc func(filename string, contents string) []KeyValue

// ReduceFunc takes a key and all the values associated with that key
// (already grouped together) and returns the single output value for
// that key. For word count, this would sum the values.
type ReduceFunc func(key string, values []string) string
