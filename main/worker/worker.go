package main

import (
    "fmt"
    "net/rpc"
	"index/mr"
	"hash/fnv"
	"os"
	"sort"
	"log"
	"strconv"
	"bufio"
	"strings"
)

func ensureDir(path string) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return os.MkdirAll(path, 0755)
	}
	return err // nil if it exists, or another error
}

func processTask(task mr.TaskResponse, workerId int) {
	if task.TaskType == "map" {
		nReduce := task.NumReduce
		// Read the file contents
		databytes, err := os.ReadFile(task.Filename)
		if err != nil {
			fmt.Println("Read failed!")
			fmt.Println(err)
			return
		}
		data := string(databytes)

		// Call the Map function
		var mapF mr.MapFunc = Map
		allKVs := mapF(task.Filename, data)

		// Here you would typically write the output to intermediate files
		fmt.Printf("Map task %d processed %s and produced %d key-value pairs.\n", task.TaskID, task.Filename, len(allKVs))

		sort.Slice(allKVs, func(i, j int) bool {
			return allKVs[i].Key < allKVs[j].Key
		})

		files := make([]*os.File, nReduce)
		err = ensureDir("map-outputs/")
		if err != nil {
			log.Fatal(err)
		}

		err = ensureDir("map-outputs/" + strconv.Itoa(workerId))
		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < nReduce; i++ {
    		filename := fmt.Sprintf("map-outputs/%d/mr-%d", workerId, i)

    		f, err := os.Create(filename)
    		if err != nil {
        		log.Fatalf("cannot create %s: %v", filename, err)
    		}

    		files[i] = f
			defer f.Close()
		}

		for _, kv := range allKVs {
			index := ihash(kv.Key) % nReduce
			fmt.Fprintf(files[index], "%s\t%s\n", kv.Key, kv.Value)
		}

	} else if task.TaskType == "reduce" {
		// For reduce tasks, you would typically read intermediate files and call Reduce
		entries, err := os.ReadDir("./map-outputs")
		if err != nil {
			fmt.Println(err)
			return
		}

		var allKVs []mr.KeyValue

		for _, entry := range entries {
			file, err := os.Open("./" + entry.Name() + strconv.Itoa(task.TaskID))
			if err != nil {
				log.Fatal(err)
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				fields := strings.Fields(scanner.Text())
				if len(fields) != 2 {
					continue // or return an error
				}

				allKVs = append(allKVs, mr.KeyValue{
					Key:   fields[0],
					Value: fields[1],
				})
			}

			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}

		}

		prev := ""
		var valForKey []string
		var outputs []mr.KeyValue
		output := ""
		var reduceF mr.ReduceFunc = Reduce
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
		for ind, kv := range outputs {
			fmt.Printf("%d: %s = %s\n", ind, kv.Key, kv.Value)
		}		
	}
}

func ihash(key string) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32() & 0x7fffffff)
}

func main() {
    // 4. Dial the same socket the coordinator is listening on
    client, err := rpc.Dial("unix", "/tmp/mr.sock")
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // 5. Fill in the args, declare an empty reply, then Call
	var workerIdReply mr.GetWorkerId

	err = client.Call("Coordinator.GetWorkerId", 1, &workerIdReply)
    if err != nil {
        panic(err)
    }
	id := workerIdReply.WorkerId

    args := mr.TaskRequest{WorkerID: id}
    var reply mr.TaskResponse

    err = client.Call("Coordinator.GetTask", &args, &reply)
    if err != nil {
        panic(err)
    }

	fmt.Println("Worker received task:")

	processTask(reply, id)

	doneArgs := mr.ReportDoneArgs{
		WorkerID: id,
		TaskType: reply.TaskType,
		TaskID: reply.TaskID,
	}
	var doneReply mr.ReportDoneReply

	client.Call("Coordinator.ReportDone", &doneArgs, &doneReply)

    fmt.Println("result:", reply.TaskType, reply.TaskID, reply.Filename) 
}