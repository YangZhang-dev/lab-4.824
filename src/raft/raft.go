package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"os"
	//	"bytes"
	"sync"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	LEADER    int32 = 1
	CANDIDATE int32 = 2
	FOLLOWER  int32 = 3
)
const (
	BASE_VOTE_TIMEOUT  = 150
	VOTE_TIMEOUT_RANGE = 150
	HEARTBEAT_DURATION = 100
)
const (
	VOTE_NO = -1
	NO_LOG  = -1
)

type State int8

const (
	UNCOMMITED = 1
	COMMITED   = 2
	APPLIIED   = 3
)

type Log struct {
	Term    int32
	State   State
	Content interface{}
}
type Logs struct {
	LogList     []Log
	CommitIndex int
	mu          sync.RWMutex
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	//VoteMu    sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	commitIndex   int
	niMu          sync.RWMutex
	nextIndex     []int
	matchIndex    []int
	VoteCond      *sync.Cond
	HeartBeatCond *sync.Cond
	memberShip    int32
	logs          Logs
	voteEndTime   int64
	currentTerm   int32
	voteTimeout   int64
	voteFor       int32
	majority      int
	Rand          *rand.Rand
	applyCh       chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.getMembership() != LEADER {
		return -1, -1, false
	}
	rf.xlog("接收到app请求")
	go rf.storeCommand(command)
	return rf.logs.getLastLogIndex() + 1, int(rf.getCurrentTerm()), true
}
func (rf *Raft) storeCommand(command interface{}) {
	// store to local
	rf.logs.storeLog(command, rf.getCurrentTerm())

	//send copy request to all follower
	ch := make(chan EntityReply, len(rf.peers))
	group := sync.WaitGroup{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		group.Add(1)
		go func(i int) {
			defer func() {
				if r := recover(); r != nil {
					rf.xlog("error:%+v", r)
				}
			}()
			logIndex := rf.nextIndex[i]
			rf.xlog("准备发送idx：%v", logIndex)
			l := rf.logs.getLogByIndex(logIndex)
			pre := rf.logs.getLogByIndex(logIndex - 1)
			args := RequestEntityArgs{
				LeaderId:     rf.me,
				Term:         rf.getCurrentTerm(),
				LeaderCommit: rf.commitIndex,
				PrevLogTerm:  pre.Term,
				PrevLogIndex: logIndex - 1,
				Entry:        l,
			}
			reply := RequestEntityReply{}
			ok := rf.sendRequestEntity(i, &args, &reply)
			entityReply := EntityReply{
				RequestEntityReply: reply,
				Ok:                 ok,
			}
			ch <- entityReply
			group.Done()
		}(i)
	}
	group.Wait()
	close(ch)
	rf.xlog("after wait......")
	// wait majority servers response right
	count := 1
	for reply := range ch {

		followerId := reply.RequestEntityReply.FollowerId
		if !reply.Ok {
			rf.xlog("服务器%v无响应", followerId)
			continue
		}
		if reply.RequestEntityReply.Success {
			rf.xlog("服务器%v已经存储成功", reply.RequestEntityReply.FollowerId)
			count++
			rf.nextIndex[followerId]++
			rf.matchIndex[followerId] = rf.nextIndex[followerId] - 1
			continue
		}
		// current term less than follower
		if rf.getCurrentTerm() < reply.RequestEntityReply.Term {
			rf.setCurrentTerm(reply.RequestEntityReply.Term)
			rf.setMembership(FOLLOWER)
		}
		// TODO follower log loss

	}
	if count >= rf.majority {
		rf.xlog("取得大多数服务器的success，对log进行commit")
		index := rf.logs.getLastLogIndex()

		msg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
		}
		rf.commitIndex = index
		// commit option
		rf.applyCh <- msg
		rf.xlog("当前的log：%+v", rf.logs.LogList)
	} else {
		rf.xlog("未取得大多数follower的success，无法进行commit")
	}

}

func (rf *Raft) RequestEntity(args *RequestEntityArgs, reply *RequestEntityReply) {
	// ok,leader is live
	term := args.Term
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	leaderCommit := args.LeaderCommit
	entry := args.Entry
	reply.Success = false
	reply.FollowerId = rf.me
	reply.Term = rf.getCurrentTerm()
	if rf.getCurrentTerm() > term {
		rf.xlog("leader的term小，已经拒绝")
		return
	}
	// if have log entry,append the log
	if entry != (Log{}) {
		rf.xlog("收到来自raft%v的 EV request", args.LeaderId)
		// check log
		lastLog := rf.logs.getLastLog()
		//rf.xlog("last log is %+v", lastLog)
		if lastLog.Term != prevLogTerm || rf.logs.getLastLogIndex() != prevLogIndex {
			rf.xlog("prevLogTerm:%v,term:%v,prevLogIndex:%v,index:%v", prevLogTerm, lastLog.Term, prevLogIndex, rf.logs.getLastLogIndex())
			rf.xlog("log不匹配，已经拒绝")
			return
		}
		rf.logs.storeLog(entry.Content, term)

		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Content,
			CommandIndex: prevLogIndex + 1,
		}
		// commit option
		rf.applyCh <- msg
		// if commit is behind leader,reset commitId
		// TODO data race
		rf.commitIndex = leaderCommit
		rf.xlog("当前的log：%+v", rf.logs.LogList)

	} else {
		rf.xlog("收到来自raft%v的 heartbeat", args.LeaderId)
	}
	rf.setCurrentTerm(term)
	rf.RestartVoteEndTime()
	rf.setMembership(FOLLOWER)
	rf.setVoteFor(VOTE_NO)

	rf.xlog("设置时间戳：%v", rf.getVoteEndTime())
	reply.Success = true
}
func (rf *Raft) sendFollowerLog(serverId int) (bool, RequestEntityReply) {
	logIndex := rf.nextIndex[serverId]
	l := rf.logs.LogList[logIndex]
	pre := rf.logs.LogList[logIndex-1]
	args := RequestEntityArgs{
		LeaderId:     rf.me,
		Term:         rf.getCurrentTerm(),
		LeaderCommit: rf.commitIndex,
		PrevLogTerm:  pre.Term,
		PrevLogIndex: logIndex - 1,
		Entry:        l,
	}
	reply := RequestEntityReply{}
	ok := rf.sendRequestEntity(serverId, &args, &reply)
	if !ok {
		return false, reply
	}
	return true, reply
}
func (rf *Raft) heartBeat() {
	peers := rf.peers
	me := rf.me
	for {
		if rf.getMembership() != LEADER {
			rf.HeartBeatCond.L.Lock()
			for rf.getMembership() != LEADER {
				rf.HeartBeatCond.Wait()
			}
			rf.HeartBeatCond.L.Unlock()
			// when raft's membership become leader,reset match index and next index
			rf.matchIndex = make([]int, len(rf.peers))
			rf.nextIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = rf.logs.getLastLogIndex() + 1
			}
		}
		rf.xlog("发送心跳,时间戳为：%v", time.Now().UnixMilli())
		args := RequestEntityArgs{}
		args.Term = rf.getCurrentTerm()
		args.LeaderId = rf.me
		for i := range peers {
			if i == me {
				continue
			}
			reply := RequestEntityReply{}
			rf.xlog("向%v号服务器发送心跳，时间戳为：%v", i, time.Now().UnixMilli())
			go rf.sendRequestEntity(i, &args, &reply)
		}
		time.Sleep(time.Duration(HEARTBEAT_DURATION) * time.Millisecond)
	}
}

func (rf *Raft) sendRequestEntity(server int, args *RequestEntityArgs, reply *RequestEntityReply) bool {
	ok := rf.peers[server].Call("Raft.RequestEntity", args, reply)
	return ok
}
func (rf *Raft) GetState() (int, bool) {
	return int(rf.getCurrentTerm()), rf.getMembership() == LEADER
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	prefix := "log/raft"
	logFile, err := os.OpenFile(prefix+".log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Lmicroseconds)
	// Your initialization code here (2A, 2B, 2C).
	rf.setMembership(FOLLOWER)
	rf.RestartVoteEndTime()
	t := len(peers) % 2
	rf.majority = t
	if t != 0 {
		rf.majority++
	}
	rf.VoteCond = sync.NewCond(&sync.Mutex{})
	rf.HeartBeatCond = sync.NewCond(&sync.Mutex{})
	rf.setVoteFor(VOTE_NO)
	rf.Rand = rand.New(rand.NewSource(int64(rf.me * rand.Int())))
	rf.setVoteTimeout(int64(rf.Rand.Intn(VOTE_TIMEOUT_RANGE) + BASE_VOTE_TIMEOUT))

	rf.logs = Logs{
		LogList: make([]Log, 1),
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeat()
	go rf.checkVoteTimeout()
	return rf
}
