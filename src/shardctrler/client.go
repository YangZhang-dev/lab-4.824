package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"encoding/gob"
	"log"
	"os"
)
import "time"
import "crypto/rand"
import "math/big"

var id = 1

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	serverId  int
	requestId int
	me        int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
func (ck *Clerk) nextServer() {
	if ck.serverId >= len(ck.servers)-1 {
		ck.serverId = 0
		return
	}
	ck.serverId++
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	gob.Register(map[int][]string{})
	ck.servers = servers
	prefix := "log/sc"
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

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	requestId := ck.requestId
	args.ClientID = ck.me
	args.Num = num
	for {
		args.RequestID = requestId
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ck.clog("[Query] send to server %d,args:%+v", ck.serverId, args)
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			ck.clog("reply is %+v,ok %+v", reply, ok)
			if !ok || reply.WrongLeader || reply.Err == ErrTimeout {
				ck.nextServer()
				continue
			}
			if reply.Err == OK {
				ck.requestId++
				ck.clog("success")
				return reply.Config
			}
			if reply.Err != "" {
				ck.clog("get a Error %s", reply.Err)
				return Config{}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	requestId := ck.requestId
	args := &JoinArgs{}
	args.ClientID = ck.me
	args.Servers = servers
	for {
		args.RequestID = requestId
		for _, srv := range ck.servers {
			var reply JoinReply
			ck.clog("[Join] send to server %d,args:%+v", ck.serverId, args)
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			ck.clog("reply is %+v,ok %+v", reply, ok)
			if !ok || reply.WrongLeader || reply.Err == ErrTimeout {
				ck.nextServer()
				continue
			}
			if reply.Err == OK {
				ck.requestId++
				ck.clog("success")
				return
			}
			if reply.Err != "" {
				ck.clog("get a Error %s", reply.Err)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	requestId := ck.requestId
	args := &LeaveArgs{}
	args.ClientID = ck.me
	args.GIDs = gids

	for {
		args.RequestID = requestId
		for _, srv := range ck.servers {
			var reply LeaveReply
			ck.clog("[Leave] send to server %d,args:%+v", ck.serverId, args)
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			ck.clog("reply is %+v,ok %+v", reply, ok)
			if !ok || reply.WrongLeader || reply.Err == ErrTimeout {
				ck.nextServer()
				continue
			}
			if reply.Err == OK {
				ck.requestId++
				ck.clog("success")
				return
			}
			if reply.Err != "" {
				ck.clog("get a Error %s", reply.Err)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	requestId := ck.requestId
	args := &MoveArgs{}
	args.Shard = shard
	args.GID = gid
	args.ClientID = ck.me

	for {
		args.RequestID = requestId
		for _, srv := range ck.servers {
			var reply MoveReply
			ck.clog("[Join] send to server %d,args:%+v", ck.serverId, args)
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			ck.clog("reply is %+v,ok %+v", reply, ok)
			if !ok || reply.WrongLeader || reply.Err == ErrTimeout {
				ck.nextServer()
				continue
			}
			if reply.Err == OK {
				ck.requestId++
				ck.clog("success")
				return
			}
			if reply.Err != "" {
				ck.clog("get a Error %s", reply.Err)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
