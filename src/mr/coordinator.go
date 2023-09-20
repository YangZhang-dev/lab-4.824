package mr

import (
	"encoding/gob"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MapTasks struct {
	// TODO chose a better key
	MapTaskList           []MapTask
	CanAllocateTaskNumber int
	CompleteTaskNumber    int
	AllTaskNumber         int
	*sync.RWMutex
}

type ReduceTasks struct {
	BuketNumber           int
	ReduceTaskList        []ReduceTask
	CompleteTaskNumber    int
	CanAllocateTaskNumber int
	*sync.RWMutex
}

type Coordinator struct {
	ReduceTasks
	MapTasks
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	r := ReduceTasks{
		BuketNumber:           nReduce,
		RWMutex:               &sync.RWMutex{},
		CanAllocateTaskNumber: 0,
		ReduceTaskList:        []ReduceTask{},
	}

	m := MapTasks{
		// TODO choose a better hash function and update init cap
		MapTaskList:           []MapTask{},
		CanAllocateTaskNumber: 0,
		AllTaskNumber:         0,
		RWMutex:               &sync.RWMutex{},
	}
	m.init(files)
	c := Coordinator{
		ReduceTasks: r,
		MapTasks:    m,
	}
	c.server()
	go c.tailCheck()
	return &c
}
func (c *Coordinator) tailCheck() {
	for {
		time.Sleep(10)
		c.MapTasks.Lock()
		for i, task := range c.MapTasks.MapTaskList {
			if task.Status == ALLOCATION {
				t := time.Since(time.Unix(task.startTime, 0))
				if t > time.Second*10 {
					c.MapTaskList[i].Status = TIMEOUT
					c.MapTasks.CanAllocateTaskNumber++
					log.Printf("a map task timeout,t:%v\n", t)
				}
			}
		}
		c.MapTasks.Unlock()
		c.ReduceTasks.Lock()
		for i, task := range c.ReduceTasks.ReduceTaskList {
			if task.Status == ALLOCATION {
				t := time.Since(time.Unix(task.startTime, 0))
				if t > time.Second*10 {
					c.ReduceTaskList[i].Status = TIMEOUT
					c.ReduceTasks.CanAllocateTaskNumber++
					log.Println("a reduce task timeout")
				}
			}
		}
		c.ReduceTasks.Unlock()
	}
}
func (m *MapTasks) init(files []string) {
	m.Lock()
	defer m.Unlock()
	for _, file := range files {
		m.MapTaskList = append(m.MapTaskList, MapTask{
			Task: Task{
				T:              TMapTask,
				TargetFilePath: "",
				startTime:      0,
				Status:         UN_ALLOCATION,
				ID:             len(m.MapTaskList),
			},
			SourceFilePath: file,
		})
	}
	m.CanAllocateTaskNumber = len(files)
	m.AllTaskNumber = len(files)
	log.Printf("now have %v maptask\n", len(files))
}
func (r *ReduceTasks) init(files []string) {
	r.Lock()
	defer r.Unlock()
	for i := 0; i < r.BuketNumber; i++ {
		r.ReduceTaskList = append(r.ReduceTaskList, ReduceTask{
			Task: Task{
				T:              TReduceTask,
				TargetFilePath: "",
				Status:         UN_ALLOCATION,
				ID:             len(r.ReduceTaskList),
				startTime:      0,
			},
			BuketNumber:  r.BuketNumber,
			BuketKey:     i,
			FilePathList: files,
		})
	}
}
func (c *Coordinator) PullTask(args *PullTaskReq, reply *PullTaskRsp) error {
	mt := c.getMapTask()
	reply.Task = mt
	reply.T = mt.T
	if mt.T != TNoTask {
		log.Printf("Allocate a MapTask , id is %v,type is %v, path is: %v\n",
			mt.ID, mt.T, mt.SourceFilePath)
		return nil
	}
	rt := c.getReduceTask()
	reply.Task = rt
	reply.T = rt.T
	if rt.T != TNoTask {
		log.Printf("Allocate a ReduceTaskList , id is %v,type is %v\n",
			rt.ID, rt.T)
		return nil
	}
	log.Printf("have not a task to allocate\n")
	return nil
}
func (c *Coordinator) CallbackFinishMapTask(args *CallbackFinishTaskReq, reply *CallbackFinishTaskRsp) error {
	taskId := args.TaskId
	filePath := args.FilePath
	f := false
	c.MapTasks.Lock()
	c.MapTaskList[taskId].Status = COMPLETE
	log.Println("a map task finish")
	c.MapTasks.CompleteTaskNumber++
	// all Map task finish
	if c.MapTasks.CompleteTaskNumber == c.MapTasks.AllTaskNumber {
		log.Println("all map task finish")
		f = true
	}
	c.MapTaskList[taskId].TargetFilePath = filePath
	c.MapTasks.Unlock()

	if f {
		c.MapTasks.RLock()
		var fileList []string
		for _, mapTask := range c.MapTaskList {
			fileList = append(fileList, mapTask.TargetFilePath)
		}
		c.MapTasks.RUnlock()
		c.ReduceTasks.init(fileList)
		log.Println("start reduce tasks")
	}
	return nil
}
func (c *Coordinator) CallbackFinishReduceTask(args *CallbackFinishTaskReq, reply *CallbackFinishTaskRsp) error {
	taskId := args.TaskId
	filePath := args.FilePath
	c.ReduceTasks.Lock()
	defer c.ReduceTasks.Unlock()
	c.ReduceTaskList[taskId].Status = COMPLETE
	c.ReduceTasks.CompleteTaskNumber++
	log.Println("a reduce task finish")
	c.ReduceTaskList[taskId].TargetFilePath = filePath
	return nil
}
func (c *Coordinator) getCanAllocateTaskNumber() (int, int) {
	c.MapTasks.RLock()
	mts := c.MapTasks.CanAllocateTaskNumber
	c.MapTasks.RUnlock()
	c.ReduceTasks.RLock()
	rts := c.ReduceTasks.CanAllocateTaskNumber
	c.ReduceTasks.RUnlock()
	return mts, rts
}
func (c *Coordinator) getReduceTask() ReduceTask {
	c.ReduceTasks.Lock()
	defer c.ReduceTasks.Unlock()
	for i, task := range c.ReduceTaskList {
		if task.Status == UN_ALLOCATION || task.Status == TIMEOUT {
			c.ReduceTaskList[i].Status = ALLOCATION
			c.ReduceTasks.CanAllocateTaskNumber--
			c.ReduceTaskList[i].startTime = time.Now().Unix()
			return task
		}
	}
	return ReduceTask{Task: Task{T: TNoTask}}
}

func (c *Coordinator) getMapTask() MapTask {
	c.MapTasks.Lock()
	defer c.MapTasks.Unlock()
	for i, task := range c.MapTasks.MapTaskList {
		if task.Status == UN_ALLOCATION || task.Status == TIMEOUT {
			c.MapTasks.CanAllocateTaskNumber--
			c.MapTaskList[i].Status = ALLOCATION
			c.MapTaskList[i].startTime = time.Now().Unix()
			return task
		}
	}
	return MapTask{Task: Task{T: TNoTask}}
}

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

func (c *Coordinator) Done() bool {
	ret := false
	c.ReduceTasks.RLock()
	defer c.ReduceTasks.RUnlock()
	if c.ReduceTasks.CompleteTaskNumber == c.ReduceTasks.BuketNumber {
		ret = true
	}
	return ret
}
