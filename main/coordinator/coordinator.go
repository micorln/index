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
	completedMaps int
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
				fmt.Printf("Map task = %d is assigned to worker = %d\n", i, args.WorkerID)
				return nil
			}
		}
		fmt.Printf("No map tasks available. All map tasks have been scheduled!\n")
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
		fmt.Printf("Reduce task = %d is assigned to worker = %d\n", c.launchedReducers - 1, args.WorkerID)
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
				fmt.Printf("Map task = %d is reported done by worker = %d!\n", args.TaskID, args.WorkerID)
				break
			}
		}
		c.completedMaps++
		if c.completedMaps == len(c.mapInProgress) {
			c.allMapsDone = true
		}
	} else {
		c.completedReducers++
		fmt.Printf("Reduce task = %d is reported done by worker = %d!\n", args.TaskID, args.WorkerID)
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
	c.nReduce = 2
	c.allMapsDone = false
	c.workers = 0
	c.completedMaps = 0


    l, _ := net.Listen("unix", "/tmp/mr.sock") // Unix socket — simplest option
    fmt.Println("coordinator listening...")
    for {
        conn, _ := l.Accept()
		
        go rpc.ServeConn(conn)               // each connection gets its own goroutine
		if c.completedReducers == c.nReduce {
			break
		}
		
    }
	fmt.Println("Reducers completed, thanks for playing!")
}