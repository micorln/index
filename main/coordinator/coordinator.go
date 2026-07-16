package main

import (
    "fmt"
    "net"
    "net/rpc"
	"os"
	"index/mr"
	"sync"
)

// 1. Define the struct that will be registered as the RPC server.
//    Any exported method on this struct becomes a callable RPC.
type Coordinator struct{
	mu          sync.Mutex
    files       []string     // input files, one per map task
    mapDone     []bool       // has map task i completed?
	mapInProgress []bool       // is map task i in progress?
    allMapsDone bool         // have all map tasks finished?
    nReduce     int 
	launchedReducers int
	completedReducers int
	workers 	int
}

// 3. The handler method. Signature must be exactly:
//    func (t *T) MethodName(args *ArgType, reply *ReplyType) error
//    net/rpc enforces this shape — anything else won't be registered.
func (c *Coordinator) GetTask(args *mr.TaskRequest, reply *mr.TaskResponse) error {
    // Implementation for getting a task
	fmt.Printf("Worker %d requested a task.\n", args.WorkerID)
	if !c.allMapsDone {
		for i, inProgress := range c.mapInProgress {
			if !inProgress {
				reply.TaskType = "map"
				reply.TaskID = i
				reply.Filename = c.files[i]
				reply.NumReduce = c.nReduce
				c.mapInProgress[i] = true 
				fmt.Println("Map task = %d is assigned to worker = %d", i, args.WorkerID)
				return nil

			}
		}
		fmt.Println("No map tasks available. All map tasks are either in progress or completed.")
	} else {
		if c.launchedReducers == c.nReduce {
			fmt.Println("All reducers already launched!")
			return nil
		}
		reply.TaskType = "reduce"
		reply.TaskID = c.launchedReducers
		c.launchedReducers++
		reply.Filename = "bad design asking reduce to take a filename"
		reply.NumReduce = c.nReduce
		fmt.Println("Map task = %d is assigned to worker = %d", c.launchedReducers - 1, args.WorkerID)
		return nil
	}

	return nil
}

func (c *Coordinator) ReportDone(args *mr.ReportDoneArgs, reply *mr.ReportDoneReply) error {
    // Implementation for reporting a completed task
	if args.TaskType == "map" {
		for i, done := range c.mapDone {
			if !done && args.TaskType == "map" && args.TaskID == i {
				c.mapDone[i] = true
				break
			}
		}
	} else {
		c.completedReducers++
	}
	
	reply.Good = true
    return nil
}

func (c *Coordinator) GetWorkerId(_ int, reply *mr.GetWorkerId) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.WorkerId = c.workers
	c.workers++
	return nil
} 


func main() {
    c := &Coordinator{}
    rpc.Register(c)                          // expose Coordinator's methods
	os.Remove("/tmp/mr.sock") // Ignore error if it doesn't exist

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s inputfile1 [inputfile2 ...]\n", os.Args[0])
		os.Exit(1)
	}

	filenames := os.Args[1:]
	mapInProgress := make([]bool, len(filenames))

	for _, filename := range filenames {
		c.files = append(c.files, filename)
		c.mapDone = append(c.mapDone, false)
	}
	c.mapInProgress = mapInProgress
	c.nReduce = 1
	c.allMapsDone = false
	c.workers = 0


    l, _ := net.Listen("unix", "/tmp/mr.sock") // Unix socket — simplest option
    fmt.Println("coordinator listening...")
    for {
        conn, _ := l.Accept()
        go rpc.ServeConn(conn)               // each connection gets its own goroutine
    }
}