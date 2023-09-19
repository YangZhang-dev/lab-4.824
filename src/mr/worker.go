package mr

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type MapWorker struct {
	MapTask MapTask
	MapFunc func(string, string) []KeyValue
}
type ReduceWorker struct {
	ReduceTask ReduceTask
	ReduceFunc func(string, []string) string
}
type TWorker int8

const (
	NoWorker      = -1
	TMapWorker    = 1
	TReduceWorker = 2
)

type Worker struct {
	T TWorker
	MapWorker
	ReduceWorker
}

func WorkerInit(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	worker := &Worker{
		T: NoWorker,
		MapWorker: MapWorker{
			MapFunc: mapf,
		},
		ReduceWorker: ReduceWorker{
			ReduceFunc: reducef,
		},
	}
	count := 0
	for {
		t, task := PullTask()
		log.Printf("get a new task,info:%+v\n", task)
		if t == TMapTask {
			log.Println("will start mapTask")
			worker.T = TMapWorker
			worker.MapWorker.MapTask = task.(MapTask)
			worker.MapWorker.invoke()
		} else if t == TReduceTask {
			worker.T = TReduceWorker
			worker.ReduceWorker.ReduceTask = task.(ReduceTask)
			log.Println("will start reduceTask")
			worker.ReduceWorker.invoke()
		} else {
			count++
			if count > 10 {
				break
			}
			time.Sleep(1 * time.Second)
		}
		log.Println("will request a new task")
	}

}
func (m *MapWorker) invoke() {
	filename := m.MapTask.SourceFilePath
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := m.MapFunc(filename, string(content))
	sort.Sort(ByKey(kva))
	intermediate := "mr-" + strconv.Itoa(m.MapTask.ID)
	ofile, _ := os.Create(intermediate)
	m.MapTask.TargetFilePath = intermediate
	// TODO think if two worker rename on one time,how to deal it
	enc := json.NewEncoder(ofile)
	for _, kv := range kva {
		enc.Encode(&kv)
	}
	//for _, kv := range kva {
	//	fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
	//}
	log.Printf("success create file in %v\n", ofile.Name())
	m.CallbackFinishMapTask()
}
func (m *MapWorker) CallbackFinishMapTask() {
	args := CallbackFinishTaskReq{}
	args.TaskId = m.MapTask.ID
	args.FilePath = m.MapTask.TargetFilePath
	rsp := CallbackFinishTaskRsp{}
	f := call("Coordinator.CallbackFinishMapTask", &args, &rsp)
	if f {
		log.Println("commit a mapTask")
	} else {
		log.Fatalf("commit a mapTask fail")
	}
}
func (r *ReduceWorker) invoke() {
	bucketKey := r.ReduceTask.BuketKey
	buketNumber := r.ReduceTask.BuketNumber
	var kva []KeyValue
	for _, intermediate := range r.ReduceTask.FilePathList {
		file, err := os.Open(intermediate)
		if err != nil {
			log.Fatalf("cannot open %v", intermediate)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if ihash(kv.Key)%buketNumber == bucketKey {
				kva = append(kva, kv)
			}
		}
		file.Close()
	}
	outPutFileName := "mr-out-" + strconv.Itoa(r.ReduceTask.ID)
	f, _ := os.Create(outPutFileName)
	i := 0
	sort.Sort(ByKey(kva))
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := r.ReduceFunc(kva[i].Key, values)
		fmt.Fprintf(f, "%v %v\n", kva[i].Key, output)
		i = j
	}
	r.CallbackFinishReduceTask()
}
func (r *ReduceWorker) CallbackFinishReduceTask() {
	args := CallbackFinishTaskReq{}
	rsp := CallbackFinishTaskRsp{}
	args.FilePath = r.ReduceTask.TargetFilePath
	args.TaskId = r.ReduceTask.ID

	f := call("Coordinator.CallbackFinishReduceTask", &args, &rsp)
	if f {
		log.Println("commit a reduceTask")
	} else {
		log.Fatalf("commit a reduceTask fail")
	}

}
func PullTask() (TaskType, interface{}) {
	args := PullTaskReq{}
	rsp := PullTaskRsp{}
	for {
		// TODO 循环停止
		call("Coordinator.PullTask", &args, &rsp)
		if rsp.T == TNoTask {
			time.Sleep(1 * time.Second)
		} else {
			return rsp.T, rsp.Task
		}
	}
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// bucketNumber 2
/**
-- 1
a 1
a 1
b 1
*/
/**
-- 2
a 1
b 1
c 1
*/

// bucketKey 1
