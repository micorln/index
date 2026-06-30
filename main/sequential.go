// Sequential MapReduce driver.
//
// Usage:
//   go run main/sequential.go data/*.txt
//
// This runs map and reduce in a single process, single goroutine —
// no concurrency yet. The goal of this stage is to get the
// map -> group-by-key -> reduce pipeline logically correct before
// adding any goroutines, channels, or RPC.
package main

import (
	"fmt"
	"os"
	"sort"
	"index/mr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s inputfile1 [inputfile2 ...]\n", os.Args[0])
		os.Exit(1)
	}

	filenames := os.Args[1:]

	// Wired up for you: Map and Reduce (from wc.go) are passed in as
	// plain function values — Go functions are first-class values,
	// so MapFunc/ReduceFunc (defined in mr/mr.go) can just be "Map"
	// and "Reduce" directly, no interface or wrapper needed.
	var mapF mr.MapFunc = Map
	var reduceF mr.ReduceFunc = Reduce

	var allKVs []mr.KeyValue

	for _, filename := range filenames {
		// TODO(you): read the file's contents with os.ReadFile,
		// convert the []byte to a string, then call:
		//   kvs := mapF(filename, contents)
		// and append kvs into allKVs.
		databytes, err := os.ReadFile(filename)
		data := string(databytes)
		if err != nil {
    		fmt.Println("Read failed!")
			fmt.Println(err)
			return
		}
		kvFile := mapF(filename, data)
		for _, kv := range kvFile {
			allKVs = append(allKVs, kv)
		}
		_ = filename
	}

	sort.Slice(allKVs, func(i, j int) bool {
		return allKVs[i].Key > allKVs[j].Key
	})

	fmt.Println("Map output : ")
	for key, count := range allKVs {
		fmt.Println(key)
		fmt.Println(count)
		fmt.Println("---")
	}

	// TODO(you): sort allKVs by Key (sort.Slice).

	// TODO(you): walk the sorted allKVs, grouping contiguous entries
	// with the same Key into a []string of values, and call:
	//   output := reduceF(key, values)
	// for each group. Write "key output\n" to an output file
	// (e.g. os.Create("mr-out-0")) as you go.
	prev := ""
	var valForKey []string
	var outputs []mr.KeyValue
	output := ""
	for _, kv := range allKVs {
		k := kv.Key
		v := kv.Value
		if prev != k {
			if prev != "" {
				output = reduceF(prev, valForKey)
				outputs = append(outputs, mr.KeyValue{ prev, output })
			}
			valForKey = valForKey[:0]
		}
		prev = k
		valForKey = append(valForKey, v)
	}
	if len(valForKey) != 0 {
		output = reduceF(prev, valForKey)
		outputs = append(outputs, mr.KeyValue{ prev, output })
	}

	fmt.Println("Reduce output : ")
	for key, count := range outputs {
		fmt.Println(key)
		fmt.Println(count)
		fmt.Println("---")
	}

	_ = mapF
	_ = reduceF
	_ = allKVs

	fmt.Println("processed files:", filenames)
}
