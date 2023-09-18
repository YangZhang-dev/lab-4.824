package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// Map task's map
type MapTasks struct {
	// TODO chose a better key
	MapTask map[string]status
	*sync.RWMutex
}

// Reduce task's map
type ReduceTasks struct {
	buketNum   int
	ReduceTask map[string]status
	*sync.Mutex
}

// Mark task's status
type status int8

const (
	UN_ALLOCATION = 0
	ALLOCATION    = 1
	COMPLETE      = 2
	TIMEOUT       = 3
)

// be used on taskMap key

type Coordinator struct {
	ReduceTasks
	MapTasks
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) PullTask(args *PullTaskReq, reply *PullTaskRsp) error {
	has, path := c.MapTasks.getUnAllocateTask()
	if has {
		reply.T = MapTask
	} else {
		reply.T = NoTask
	}
	reply.FilePath = path
	fmt.Println("now have %v tasks", c.MapTasks.getCanAllocateTaskNumber())

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	r := ReduceTasks{
		buketNum:   nReduce,
		Mutex:      &sync.Mutex{},
		ReduceTask: nil,
	}

	m := MapTasks{
		// TODO choose a better hash function and update init cap
		MapTask: make(map[string]status, len(files)),
		RWMutex: &sync.RWMutex{},
	}
	m.init(files)
	c := Coordinator{
		ReduceTasks: r,
		MapTasks:    m,
	}
	c.server()
	return &c
}

func (m *MapTasks) init(files []string) {
	for _, file := range files {
		m.MapTask[file] = UN_ALLOCATION
	}
	fmt.Printf("now have %v tasks", len(files))
}
func (m *MapTasks) getCanAllocateTaskNumber() int {
	m.RLock()
	defer m.RUnlock()
	count := 0
	for _, status := range m.MapTask {
		if status == UN_ALLOCATION || status == TIMEOUT {
			count++
		}
	}
	return count
}
func (m *MapTasks) getUnAllocateTask() (bool, string) {
	m.Lock()
	defer m.Unlock()
	for fileName, status := range m.MapTask {
		if status == UN_ALLOCATION || status == TIMEOUT {
			m.MapTask[fileName] = ALLOCATION
			return true, fileName
		}
	}

	return false, ""
}
