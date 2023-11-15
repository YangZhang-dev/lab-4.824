package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

const (
	GET    = "GET"
	PUT    = "Put"
	APPEND = "Append"
)

type P struct {
	Table        map[string]string
	RequestTable map[int]int
}
type Op struct {
	Operation string
	Key       string
	Value     string
	ClientId  int
	RequestId int
}

type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	sync.Mutex
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	table        map[string]string
	chTable      map[int]chan string
	requestTable map[int]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.klog("[Get] request,args is %+v", args)
	k := args.Key
	op := Op{
		Operation: GET,
		Key:       k,
		Value:     "",
	}
	logIndex, _, ok := kv.rf.Start(op)
	if !ok {
		kv.klog("wrong leader return")
		reply.Err = ErrWrongLeader
		return
	}
	kv.klog("finish Start,op:%+v", op)
	kv.klog("wait logIndex %d", logIndex)
	ch := kv.getChByLogIndex(logIndex)
	defer kv.delChByLogIndex(logIndex)
	select {
	case result := <-ch:
		reply.Value, reply.Err = result, OK
	case <-time.After(AGREE_TIMEOUT):
		reply.Value, reply.Err = "", ErrTimeout
		kv.klog("timeout return")
	}
	kv.klog("return from get request,%+v", reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.klog("[%s] request,args is %+v", args.Op, args)
	operation := args.Op
	k := args.Key
	v := args.Value
	clientId := args.ClientId
	requestId := args.RequestId

	op := Op{
		Operation: operation,
		Key:       k,
		Value:     v,
		ClientId:  clientId,
		RequestId: requestId,
	}
	logIndex, _, ok := kv.rf.Start(op)

	if !ok {
		reply.Err = ErrWrongLeader
		kv.klog("wrong leader return")
		return
	}

	kv.klog("finish Start,op:%+v", op)
	ch := kv.getChByLogIndex(logIndex)

	kv.klog("wait logIndex %d", logIndex)
	select {
	case result := <-ch:
		reply.Value, reply.Err = result, OK
	case <-time.After(AGREE_TIMEOUT):
		reply.Value, reply.Err = "", ErrTimeout
		kv.klog("timeout return")
	}
	kv.delChByLogIndex(logIndex)
	kv.klog("return from putAppend request,reply is %+v", reply)
}
func (kv *KVServer) delChByLogIndex(logIndex int) {
	kv.Lock()
	defer kv.Unlock()
	delete(kv.chTable, logIndex)
}

func (kv *KVServer) getChByLogIndex(logIndex int) chan string {
	kv.Lock()
	defer kv.Unlock()
	if _, ok := kv.chTable[logIndex]; !ok {
		kv.chTable[logIndex] = make(chan string)
	}
	return kv.chTable[logIndex]
}
func (kv *KVServer) sendByLogIndex(logIndex int, v string) bool {
	kv.Lock()
	defer kv.Unlock()
	ch, ok := kv.chTable[logIndex]
	if ok {
		select {
		case ch <- v:
		default:
			return false
		}
		return true
	}
	return false
}
func (kv *KVServer) applier() {

	for !kv.killed() {
		msg := <-kv.applyCh
		if kv.killed() {
			return
		}
		if msg.SnapshotValid {
			kv.klog("get a msg,snapshotIndex %+v, snapshotTerm %+v", msg.SnapshotIndex, msg.SnapshotTerm)
		} else {
			kv.klog("get a msg,%+v", msg)
		}
		switch {
		case msg.CommandValid:
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}
			kv.klog("maxreq %d,req %d", kv.requestTable[op.ClientId], op.RequestId)
			if kv.requestTable[op.ClientId] < op.RequestId {
				if op.Operation == PUT {
					kv.klog("PUT operation")
					kv.table[op.Key] = op.Value
					kv.requestTable[op.ClientId] = op.RequestId
				} else if op.Operation == APPEND {
					kv.klog("APPEND operation")
					kv.table[op.Key] = kv.table[op.Key] + op.Value
					kv.requestTable[op.ClientId] = op.RequestId
				}
			}
			kv.klog("ready get raft state")
			term, isLeader := kv.rf.GetState()
			kv.klog("success get raft state %+v,%+v", term, isLeader)
			if !isLeader {
				kv.klog("not leader return")
				continue
			}
			if term != msg.CommandTerm {
				kv.klog("msg not current term return")
				continue
			}
			kv.klog("ready get ch")
			ok = kv.sendByLogIndex(msg.CommandIndex, kv.table[op.Key])
			if ok {
				kv.klog("send ok")
			} else {
				kv.klog("server already return")
			}
			// 不能单纯的判断大小
			if float32(kv.rf.GetRaftSize()/kv.maxraftstate) > 0.9 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				p := P{
					Table:        kv.table,
					RequestTable: kv.requestTable,
				}
				e.Encode(p)
				size := kv.rf.GetRaftSize()
				kv.rf.Snapshot(msg.CommandIndex, w.Bytes())
				kv.klog("send a snapshot request,old size %d,new size %d", size, kv.rf.GetRaftSize())
			}
		case msg.SnapshotValid:
			kv.klog("get a snapshot request")
			ok := kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
			if ok {
				r := bytes.NewBuffer(msg.Snapshot)
				d := labgob.NewDecoder(r)
				var p P
				if d.Decode(&p) == nil {
					if p.RequestTable != nil {
						kv.klog("replace requestTable ")
						kv.requestTable = p.RequestTable
					}
					if p.Table != nil {
						kv.klog("replace table")
						kv.table = p.Table
					}
				}
			}
		}

	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.klog("server %d will stop", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {

	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.table = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.chTable = make(map[int]chan string)
	kv.requestTable = make(map[int]int)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	r := bytes.NewBuffer(persister.ReadSnapshot())
	d := labgob.NewDecoder(r)
	var p P
	if d.Decode(&p) == nil {
		if p.RequestTable != nil {
			kv.requestTable = p.RequestTable
		}
		if p.Table != nil {
			kv.table = p.Table
		}
	}
	go kv.applier()
	return kv
}
