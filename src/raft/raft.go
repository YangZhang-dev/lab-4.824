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
	LEADER    = 1
	CANDIDATE = 2
	FOLLOWER  = 3
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

type Log struct {
	Term    int
	Index   int
	Content interface{}
}
type Logs struct {
	LogList     []Log
	CommitIndex int
	mu          sync.RWMutex
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	voteFor     int
	memberShip  int
	voteEndTime int64
	currentTerm int
	voteTimeout int64

	logs        Logs
	nextIndex   []int
	matchIndex  []int
	commitIndex int

	majority      int
	Rand          *rand.Rand
	applyCh       chan ApplyMsg
	EVMu          sync.RWMutex
	VoteCond      *sync.Cond
	HeartBeatCond *sync.Cond
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if rf.memberShip != LEADER {
		return -1, -1, false
	}
	rf.xlog("接收到app请求,log:%+v", command)
	//go rf.storeCommand(command)
	return rf.logs.getLastLogIndex() + 1, rf.currentTerm, true
}
func (rf *Raft) storeCommand(command interface{}) {
	// store to local
	rf.logs.storeLog(command, rf.currentTerm)
	rf.EVMu.Lock()
	defer rf.EVMu.Unlock()
	//send copy request to all follower
	ch := make(chan EntityReply, len(rf.peers))
	for i := range rf.peers {

		if i == rf.me {
			continue
		}

		if rf.memberShip != LEADER {
			return
		}
		go func(i int) {
			defer func() {
				if r := recover(); r != nil {
					rf.xlog("error:%+v", r)
				}
			}()
			logIndex := rf.logs.getLastLogIndex()
			rf.xlog("准备向服务器%v发送idx：%v", i, logIndex)
			l := rf.logs.getLastLog()
			pre := rf.logs.getLogByIndex(logIndex - 1)
			args := RequestEntityArgs{
				LeaderId:     rf.me,
				Term:         rf.currentTerm,
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
			rf.xlog("收到服务器%v对于log%v的结果（有可能无响应）", i, logIndex)
			ch <- entityReply
		}(i)
	}
	resCh := make(chan bool)
	go func() {
		count := 1
		defer func() {
			rf.xlog("所有的服务器响应都已经处理完毕")
		}()
		for reply := range ch {
			followerId := reply.RequestEntityReply.FollowerId
			if !reply.Ok {
				rf.xlog("服务器无响应")
				continue
			}
			if reply.RequestEntityReply.Success {
				rf.xlog("服务器%v已经存储成功", followerId)
				count++
				rf.nextIndex[followerId]++
				rf.matchIndex[followerId] = rf.nextIndex[followerId] - 1
				if count >= rf.majority {
					resCh <- true
				}

				continue
			}
			// current term less than follower
			if rf.currentTerm < reply.RequestEntityReply.Term {
				rf.xlog("我的term小，转换为follower")
				rf.currentTerm = reply.RequestEntityReply.Term
				rf.setMembership(FOLLOWER)
				return
			} else {
				rf.xlog("follower log loss")
				// follower log loss
				//go rf.pursueLog(reply.RequestEntityReply.FollowerId)
			}

		}

	}()
	success := <-resCh
	if success {
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
		rf.xlog("当前的nextIndex：%+v", rf.nextIndex)
	} else {
		rf.xlog("未取得大多数follower的success，无法进行commit")
	}

}
func (rf *Raft) pursueLog(serverId int) {

	rf.xlog("在term%v，开启对服务器%v的日志追赶,nextId:%v", rf.currentTerm, serverId, rf.nextIndex[serverId])
	for {
		logIndex := rf.nextIndex[serverId]
		l := rf.logs.getLogByIndex(logIndex)
		pre := rf.logs.getLogByIndex(logIndex - 1)
		rf.xlog("寻找匹配点：本次发送logIndex：%v,preTerm:%v,log:%+v", logIndex, pre.Term, l)
		args := RequestEntityArgs{
			LeaderId:     rf.me,
			Term:         rf.currentTerm,
			LeaderCommit: rf.commitIndex,
			PrevLogTerm:  pre.Term,
			PrevLogIndex: logIndex - 1,
			Entry:        l,
		}
		reply := RequestEntityReply{}
		ok := rf.sendRequestEntity(serverId, &args, &reply)
		if !ok {
			rf.xlog("服务器%v无响应", serverId)
			return
		}
		if reply.Success {
			break
		} else {
			rf.nextIndex[serverId]--
		}
		time.Sleep(time.Duration(5) * time.Millisecond)
	}
	rf.xlog("找到最低同步点,nextId:%v,matchId:%v", rf.nextIndex[serverId], rf.matchIndex[serverId])
	for rf.logs.getLastLogIndex() >= rf.nextIndex[serverId] {
		logIndex := rf.nextIndex[serverId]
		l := rf.logs.getLogByIndex(logIndex)
		pre := rf.logs.getLogByIndex(logIndex - 1)
		rf.xlog("日志追赶：本次发送logIndex：%v,pre:%+v,log:%+v", logIndex, pre, l)
		args := RequestEntityArgs{
			LeaderId:     rf.me,
			Term:         rf.currentTerm,
			LeaderCommit: rf.commitIndex,
			PrevLogTerm:  pre.Term,
			PrevLogIndex: logIndex - 1,
			Entry:        l,
		}
		reply := RequestEntityReply{}
		ok := rf.sendRequestEntity(serverId, &args, &reply)
		if !ok {
			rf.xlog("服务器%v无响应", serverId)
			return
		}
		if reply.Success {
			rf.nextIndex[serverId]++
			rf.xlog("成功追赶日志")
		} else {
			rf.xlog("在追赶日志时发生错误：服务器%v", serverId)
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
	rf.matchIndex[serverId] = rf.nextIndex[serverId] - 1
}

func (rf *Raft) appendEntries() {
	peers := rf.peers
	me := rf.me
	for {
		rf.mu.RLock()
		memberShip := rf.memberShip
		rf.mu.RUnlock()
		if memberShip != LEADER {
			rf.HeartBeatCond.L.Lock()
			for {
				rf.mu.RLock()
				if rf.memberShip == LEADER {
					rf.mu.RUnlock()
					break
				}
				rf.mu.RUnlock()
				rf.HeartBeatCond.Wait()
			}
			rf.HeartBeatCond.L.Unlock()
			rf.mu.Lock()
			rf.matchIndex = make([]int, len(rf.peers))
			rf.nextIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = rf.logs.getLastLogIndex() + 1
			}
			rf.mu.Unlock()
		}

		for i := range peers {
			if i == me {
				continue
			}
			reply := RequestEntityReply{}
			go func(serverId int) {
				rf.mu.RLock()
				logIndex := rf.nextIndex[serverId]
				l := rf.logs.getLogByIndex(logIndex)
				pre := rf.logs.getLogByIndex(logIndex - 1)
				args := RequestEntityArgs{
					LeaderId:     rf.me,
					Term:         rf.currentTerm,
					LeaderCommit: rf.commitIndex,
					PrevLogTerm:  pre.Term,
					PrevLogIndex: logIndex - 1,
					Entry:        l,
				}
				rf.mu.RUnlock()
				ok := rf.sendRequestEntity(serverId, &args, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.startNewTerm(reply.Term)
					return
				}
				if reply.Success {
					// TODO 当前就是直接加一，未来可能需要改变
					rf.nextIndex[serverId]++
					rf.matchIndex[serverId] = rf.nextIndex[serverId] - 1
				} else {
					rf.nextIndex[serverId]--
				}
			}(i)
		}
		time.Sleep(time.Duration(HEARTBEAT_DURATION) * time.Millisecond)
	}
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.memberShip == LEADER
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
	rf.setMembership(FOLLOWER)
	rf.RestartVoteEndTime()
	t := len(peers) % 2
	rf.majority = t
	if t != 0 {
		rf.majority++
	}
	rf.VoteCond = sync.NewCond(&sync.Mutex{})
	rf.HeartBeatCond = sync.NewCond(&sync.Mutex{})
	rf.voteFor = VOTE_NO
	rf.Rand = rand.New(rand.NewSource(int64(rf.me * rand.Int())))
	rf.voteTimeout = int64(rf.Rand.Intn(VOTE_TIMEOUT_RANGE) + BASE_VOTE_TIMEOUT)
	rf.logs = Logs{
		LogList: make([]Log, 1),
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.appendEntries()
	return rf
}
