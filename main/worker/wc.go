// Word count Map/Reduce functions.
// This is the "app" — the part a MapReduce user would write —
// as opposed to mr/mr.go and main/sequential.go, which are the
// generic framework plumbing.
package main

import (
	"index/mr"
	"strings"
	"unicode"
	"strconv"
	"fmt"
)

// Map should split contents into words and emit (word, "1") for each one.
// Remember from our discussion: map does NOT sum anything here — it just
// emits one KeyValue per occurrence. The summing happens in Reduce, after
// grouping.
func Map(filename string, contents string) []mr.KeyValue {
	// TODO(you): split `contents` into words and build the []mr.KeyValue slice.
	// Hints:
	//   - strings.FieldsFunc or strings.Fields for splitting
	//   - unicode.IsLetter if you want to strip punctuation

	var keyValArr []mr.KeyValue
	wordList := strings.Fields(contents)
	for _, word := range wordList {
		wordKv := mr.KeyValue{cleanupWord(word), "1"}
		keyValArr = append(keyValArr, wordKv)
	}

	return keyValArr
}

// Reduce receives one key (a word) and all its values (a slice of "1"s,
// one per occurrence, already grouped by the driver). It should return
// the total count as a string.
func Reduce(key string, values []string) string {
	// TODO(you): return the count of values as a string (strconv.Itoa)
	total := 0
	for _, value := range values {
		n, err := strconv.Atoi(value)
		if err != nil {
			// handle malformed input — shouldn't happen if Map only emits "1"
			fmt.Println("Map output is nor valid, recheck")
			fmt.Println(value)
		}
		total += n
	}

	return strconv.Itoa(total)
}

func cleanupWord(word string) string {
	word = strings.ToLower(word)

	word = strings.Map(func(r rune) rune {
		if !unicode.IsLetter(r) {
			return -1 // remove punctuation
		}
		return r
	}, word)

	return word
}
