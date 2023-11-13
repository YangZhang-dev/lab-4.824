package kvraft

import (
	"6.824/labrpc"
	"log"
	"os"
)
import "crypto/rand"
import "math/big"

var id = 1

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	serverId  int
	requestId int
	me        int
}

func (ck *Clerk) nextServer() {
	if ck.serverId >= len(ck.servers)-1 {
		ck.serverId = 0
		return
	}
	ck.serverId++
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.serverId = 0
	// You'll have to add code here.
	prefix := "log/kv"
	logFile, err := os.OpenFile(prefix+".log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Lmicroseconds)
	ck.requestId = 1
	ck.me = id
	id++
	return ck
}

// fetch the current Value for a Key.
// returns "" if the Key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	for {
		args := GetArgs{
			Key: key,
		}
		ck.clog("[Get] send to server %d,args:%+v", ck.serverId, args)
		reply := GetReply{}
		ok := ck.servers[ck.serverId].Call("KVServer.Get", &args, &reply)
		ck.clog("reply is %+v,ok %+v", reply, ok)

		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.nextServer()
			continue
		}
		if reply.Err == OK {
			ck.clog("success get a [%+v]/[%+v]", args.Key, reply.Value)
			return reply.Value
		}
		if reply.Err != "" {
			ck.clog("get a error: %s", reply.Err)
			break
		}
	}
	// You will have to modify this function.
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestId := ck.requestId
	for {
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			RequestId: requestId,
			ClientId:  ck.me,
		}
		ck.clog("[%s] send to server %d,args:%+v", args.Op, ck.serverId, args)
		reply := PutAppendReply{}
		ok := ck.servers[ck.serverId].Call("KVServer.PutAppend", &args, &reply)
		ck.clog("reply is %+v,ok %+v", reply, ok)

		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.nextServer()
			continue
		}
		if reply.Err == OK {
			ck.requestId++
			ck.clog("success %s a [%+v]/[%+v]", args.Op, args.Key, reply.Value)
			ck.clog("requestId is %+v", ck.requestId)
			break
		}
		if reply.Err != "" {
			ck.clog("get a Error %s", reply.Err)
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
