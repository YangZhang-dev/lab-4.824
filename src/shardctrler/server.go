package shardctrler

import (
	"6.824/raft"
	"sort"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32
	// Your data here.
	chTable      map[int]chan any
	requestTable map[int]int
	configs      []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Operation int
	V         any
	ClientId  int
	RequestId int
}
type MoveReq struct {
	GID     int
	ShardId int
}
type GroupInfo map[int][]string
type Group2Shards map[int][]int

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	var groupInfo GroupInfo
	groupInfo = args.Servers
	op := Op{
		Operation: JOIN,
		V:         groupInfo,
		ClientId:  args.ClientID,
		RequestId: args.RequestID,
	}
	reply.WrongLeader, reply.Err = sc.execRaft(op)
	sc.slog("return from get request,%+v", reply)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Operation: LEAVE,
		V:         args.GIDs,
		ClientId:  args.ClientID,
		RequestId: args.RequestID,
	}
	reply.WrongLeader, reply.Err = sc.execRaft(op)
	sc.slog("return from get request,%+v", reply)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		Operation: MOVE,
		V: MoveReq{
			GID:     args.GID,
			ShardId: args.Shard,
		},
		ClientId:  args.ClientID,
		RequestId: args.RequestID,
	}
	reply.WrongLeader, reply.Err = sc.execRaft(op)
	sc.slog("return from get request,%+v", reply)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// 0 1 2 3

	op := Op{
		Operation: QUERY,
		V:         args.Num,
		ClientId:  args.ClientID,
		RequestId: args.RequestID,
	}
	logIndex, _, ok := sc.rf.Start(op)
	if !ok {
		sc.slog("wrong leader return")
		reply.WrongLeader = true
		return
	}
	sc.slog("finish Start,op:%+v", op)
	sc.slog("wait logIndex %d", logIndex)
	ch := sc.getChByLogIndex(logIndex)
	defer sc.delChByLogIndex(logIndex)
	select {
	case result := <-ch:
		c, ok := result.(Config)
		if !ok {
			reply.Config, reply.Err = Config{}, ErrUnknown
		} else {
			reply.Config, reply.Err = c, OK
		}
	case <-time.After(AGREE_TIMEOUT):
		reply.Config, reply.Err = Config{}, ErrTimeout
		sc.slog("timeout return")
	}
	sc.slog("return from get request,%+v", reply)
}
func (sc *ShardCtrler) execRaft(op Op) (bool, Err) {
	logIndex, _, ok := sc.rf.Start(op)
	if !ok {
		sc.slog("wrong leader return")
		return true, ""
	}
	sc.slog("finish Start,op:%+v", op)
	sc.slog("wait logIndex %d", logIndex)
	ch := sc.getChByLogIndex(logIndex)
	defer sc.delChByLogIndex(logIndex)
	select {
	case <-ch:
		return false, OK
	case <-time.After(AGREE_TIMEOUT):
		sc.slog("timeout return")
		return false, ErrTimeout
	}
}
func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if sc.killed() {
			return
		}
		sc.slog("get a msg,%+v", msg)
		switch {
		case msg.CommandValid:
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}
			var result any
			sc.slog("maxreq %d,req %d", sc.requestTable[op.ClientId], op.RequestId)
			if sc.requestTable[op.ClientId] < op.RequestId {
				if op.Operation == QUERY {
					req, ok := (op.V).(int)
					if ok {
						result = sc.QueryHandler(req)
					}
					sc.slog("QUERY operation")
				} else if op.Operation == MOVE {
					req, ok := (op.V).(MoveReq)
					if ok {
						sc.MoveHandler(req)
					}
					sc.slog("MOVE operation")
				} else if op.Operation == LEAVE {
					req, ok := (op.V).([]int)
					if ok {
						sc.slog("req:%+v", req)
						sc.LeaveHandler(req)
					}
					sc.slog("LEAVE operation")
				} else if op.Operation == JOIN {
					req, ok := (op.V).(GroupInfo)
					if ok {
						sc.JoinHandler(req)
					}
					sc.slog("JOIN operation")
				}
				sc.requestTable[op.ClientId] = op.RequestId
			}
			term, isLeader := sc.rf.GetState()
			sc.slog("success get raft state %+v,%+v", term, isLeader)
			if !isLeader {
				continue
			}
			if term != msg.CommandTerm {
				continue
			}
			ok = sc.sendByLogIndex(msg.CommandIndex, result)
		}

	}
}
func (sc *ShardCtrler) LeaveHandler(GIDs []int) {
	lastConfig := sc.configs[len(sc.configs)-1]

	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: deepCopySlice(lastConfig.Shards),
		Groups: deepCopyMap(lastConfig.Groups),
	}
	for _, GID := range GIDs {
		delete(newConfig.Groups, GID)
		for i, shard := range newConfig.Shards {
			if shard == GID {
				newConfig.Shards[i] = EMPTY_GID
			}
		}
	}
	group2Shards, unallocated := getGroup2Shards(newConfig)
	sc.slog("group2Shards is %+v, unallocated is %+v", group2Shards, unallocated)
	sc.rebalancedShards(group2Shards, unallocated)
	newConfig.Shards = sc.buildShards(group2Shards)
	sc.configs = append(sc.configs, newConfig)
	sc.slog("after join,new config is %+v", newConfig)
}
func (sc *ShardCtrler) JoinHandler(req GroupInfo) {
	lastConfig := sc.configs[len(sc.configs)-1]

	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: deepCopySlice(lastConfig.Shards),
		Groups: deepCopyMap(lastConfig.Groups),
	}
	for GID, servers := range req {
		newConfig.Groups[GID] = servers
	}

	group2Shards, unallocated := getGroup2Shards(newConfig)
	sc.slog("group2Shards is %+v, unallocated is %+v", group2Shards, unallocated)
	sc.rebalancedShards(group2Shards, unallocated)
	newConfig.Shards = sc.buildShards(group2Shards)
	sc.configs = append(sc.configs, newConfig)
	sc.slog("after join,new config is %+v", newConfig)
}

func (sc *ShardCtrler) buildShards(group2Shards Group2Shards) [NShards]int {
	var newShard [NShards]int
	for GID, shards := range group2Shards {
		for _, shardId := range shards {
			newShard[shardId] = GID
		}
	}
	return newShard
}

func (sc *ShardCtrler) rebalancedShards(group2Shards Group2Shards, unallocated []int) {
	// 4 0 0
	// 3 1 0
	// 2 1 1
	// 1 2 1
	if len(group2Shards) <= 0 {
		return
	}

	for len(unallocated) > 0 {
		minShardGID := getMinShardGroup(group2Shards)
		group2Shards[minShardGID] = append(group2Shards[minShardGID], unallocated[0])
		unallocated = unallocated[1:]
	}
	sc.slog("after allocate 0 group is %+v", group2Shards)
	maxShardGID, minShardGID := getMaxShardGroup(group2Shards), getMinShardGroup(group2Shards)
	for {
		sc.slog("move %d GID to %d GID", maxShardGID, minShardGID)
		maxShardGID, minShardGID = sc.moveShards(maxShardGID, minShardGID, group2Shards)
		sc.slog("current group2Shards is %+v", group2Shards)
		if len(group2Shards[maxShardGID])-len(group2Shards[minShardGID]) <= 1 {
			break
		}
	}
	// 0 2 2

}

func (sc *ShardCtrler) moveShards(srcGID, dstGID int, group2Shards Group2Shards) (int, int) {

	group2Shards[dstGID] = append(group2Shards[dstGID], group2Shards[srcGID][0])
	group2Shards[srcGID] = group2Shards[srcGID][1:]
	return getMaxShardGroup(group2Shards), getMinShardGroup(group2Shards)

}
func getGroup2Shards(c Config) (Group2Shards, []int) {
	result := make(Group2Shards)
	unallocated := make([]int, EMPTY_GID)
	for i := range c.Groups {
		result[i] = make([]int, 0)
	}
	for shardId, shardGID := range c.Shards {
		if shardGID == EMPTY_GID {
			unallocated = append(unallocated, shardId)
		} else {
			result[shardGID] = append(result[shardGID], shardId)
		}
	}
	return result, unallocated
}
func getMinShardGroup(req Group2Shards) int {
	var GIDs []int
	var counter = NShards + 1
	for i, shards := range req {
		if len(shards) < counter {
			counter = len(shards)
			GIDs = []int{i}
		} else if len(shards) == counter {
			GIDs = append(GIDs, i)
		}
	}
	sort.Ints(GIDs)
	return GIDs[len(GIDs)-1]
}
func getMaxShardGroup(req Group2Shards) int {
	var GIDs []int
	var counter int
	for i, shards := range req {
		if len(shards) > counter {
			counter = len(shards)
			GIDs = []int{i}
		} else if len(shards) == counter {
			GIDs = append(GIDs, i)
		}
	}
	sort.Ints(GIDs)
	return GIDs[0]
}

//	2
//
// 0 1 2   l:3
func (sc *ShardCtrler) QueryHandler(idx int) Config {
	l := len(sc.configs)
	if idx == -1 || idx >= l {
		idx = l - 1
	}
	return sc.configs[idx]
}

func (sc *ShardCtrler) MoveHandler(req MoveReq) {
	lastConfig := sc.configs[len(sc.configs)-1]

	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: deepCopySlice(lastConfig.Shards),
		Groups: deepCopyMap(lastConfig.Groups),
	}
	newConfig.Shards[req.ShardId] = req.GID
	sc.configs = append(sc.configs, newConfig)
}
func (sc *ShardCtrler) delChByLogIndex(logIndex int) {
	sc.Lock()
	defer sc.Unlock()
	delete(sc.chTable, logIndex)
}

func (sc *ShardCtrler) getChByLogIndex(logIndex int) chan any {
	sc.Lock()
	defer sc.Unlock()
	if _, ok := sc.chTable[logIndex]; !ok {
		sc.chTable[logIndex] = make(chan any)
	}
	return sc.chTable[logIndex]
}
func (sc *ShardCtrler) sendByLogIndex(logIndex int, v any) bool {
	sc.Lock()
	defer sc.Unlock()
	ch, ok := sc.chTable[logIndex]
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

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.configs[0].Shards = [NShards]int{}
	labgob.Register(Op{})
	labgob.Register(GroupInfo{})
	labgob.Register(MoveReq{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.chTable = make(map[int]chan any)
	sc.requestTable = make(map[int]int)
	go sc.applier()
	return sc
}
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func deepCopyMap(original GroupInfo) GroupInfo {
	copied := make(GroupInfo)

	for key, value := range original {
		// 深拷贝值
		copiedValue := make([]string, len(value))
		copy(copiedValue, value)
		// 将深拷贝的值存入新map中
		copied[key] = copiedValue
	}
	return copied
}
func deepCopySlice(src [NShards]int) [NShards]int {
	var res [NShards]int
	for i, s := range src {
		res[i] = s
	}
	return res
}
