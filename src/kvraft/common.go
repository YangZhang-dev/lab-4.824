package kvraft

import "time"

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrNoKey       = "ErrNoKey"
)
const (
	AGREE_TIMEOUT = 100 * time.Millisecond
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	RequestId int
	ClientId  int
}

type PutAppendReply struct {
	Err   Err
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
