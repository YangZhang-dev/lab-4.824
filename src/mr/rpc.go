package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType int8

const (
	NoTask     = 0
	MapTask    = 1
	ReduceTask = 2
)

type PullTaskReq struct {
}

type PullTaskRsp struct {
	T        TaskType
	FilePath string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can'T use the current directory since
// Athena AFS doesn'T support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
