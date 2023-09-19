package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type PullTaskReq struct {
}

type PullTaskRsp struct {
	T    TaskType
	Task interface{}
}

type CallbackFinishTaskReq struct {
	FilePath string
	TaskId   int
}
type CallbackFinishTaskRsp struct {
}

func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
