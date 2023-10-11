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
	COMMIT_DURATION    = 50
)
const (
	VOTE_NO = -1
)

type Log struct {
	Term    int
	Index   int
	Content interface{}
}
type Logs struct {
	LogList []Log
	mu      sync.RWMutex
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
	lastApplied int
	logs        Logs
	nextIndex   []int
	matchIndex  []int
	commitIndex int

	majority      int
	Rand          *rand.Rand
	applyCh       chan ApplyMsg
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
	rf.logs.storeLog(command, rf.currentTerm)
	//go rf.appendEntries(false)
	return rf.logs.getLastLogIndex(), rf.currentTerm, true
}

func (rf *Raft) appendEntries(isHeartBeat bool) {
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
			go rf.applier()
		}
		rf.mu.RLock()
		commitId := rf.commitIndex
		term := rf.currentTerm
		rf.xlog("next indexes is %+v", rf.nextIndex)
		rf.mu.RUnlock()
		for i := range peers {
			if i == me {
				continue
			}
			go rf.leaderSendEntries(i, term, commitId)
		}
		if !isHeartBeat {
			return
		}
		time.Sleep(time.Duration(HEARTBEAT_DURATION) * time.Millisecond)
	}
}
func (rf *Raft) leaderSendEntries(serverId int, term int, commitId int) {
	reply := RequestEntityReply{}
	rf.mu.RLock()
	logIndex := rf.nextIndex[serverId]
	logs := make([]Log, 0)
	for i := logIndex; i <= rf.logs.getLastLogIndex(); i++ {
		logs = append(logs, rf.logs.getLogByIndex(i))
	}
	pre := rf.logs.getLogByIndex(logIndex - 1)
	args := RequestEntityArgs{
		LeaderId:     rf.me,
		Term:         term,
		LeaderCommit: commitId,
		PrevLogTerm:  pre.Term,
		PrevLogIndex: pre.Index,
		Entries:      logs,
	}
	successNextIndex := rf.nextIndex[serverId] + len(logs)
	rf.mu.RUnlock()
	successMatchIndex := args.PrevLogIndex + len(logs)
	rf.xlog("send to server%v, args: %+v", serverId, args)
	ok := rf.sendRequestEntity(serverId, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.xlog("reply from server%v,reply:%+v", serverId, reply)
	if !reply.Success {
		if !reply.Conflict {
			if reply.Term > args.Term {
				rf.startNewTerm(reply.Term)
				return
			}
		} else {
			rf.nextIndex[serverId] = reply.XIndex
		}
	} else {
		if logs != nil || len(logs) != 0 {
			rf.nextIndex[serverId] = successNextIndex
			rf.matchIndex[serverId] = successMatchIndex
			rf.commitHandler(rf.logs.getLastLogIndex(), args.Term)
		}
	}
}
func (rf *Raft) applier() {
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
		}
		rf.mu.Lock()

		for rf.commitIndex-rf.lastApplied > 0 {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs.getLogByIndex(rf.lastApplied).Content,
				CommandIndex: rf.lastApplied,
			}
			rf.xlog("apply a log:%+v", msg)
			rf.applyCh <- msg
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(COMMIT_DURATION) * time.Millisecond)
	}
}
func (rf *Raft) commitHandler(index int, term int) {
	if index <= rf.commitIndex {
		return
	}
	//rf.xlog("args term %v, matchIndex:%+v, check Log index:%v Term:%v", term, rf.matchIndex, index, rf.logs.getLogByIndex(index).Term)
	//rf.xlog("nextIndex is :%+v, logs is %+v", rf.nextIndex, rf.logs.LogList)
	counter := 0
	for serverId := range rf.peers {
		if rf.logs.getLogByIndex(index).Term == term {
			if serverId == rf.me {
				counter++
			} else {
				matchIndex := rf.matchIndex[serverId]
				//rf.xlog("server %v, index is: %v,matchIndex is %v", serverId, rf.logs.getLogByIndex(index).Term, matchIndex)
				if matchIndex >= index {
					counter++
				}
			}
		}

		if counter >= rf.majority {
			rf.xlog("commit a log: %+v,majority is %v", rf.logs.getLogByIndex(index), rf.majority)
			rf.commitIndex = index
			return
		}
	}
	rf.commitHandler(index-1, term)
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
	rf.RestartVoteEndTime()
	t := len(peers) / 2
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
	go rf.appendEntries(true)
	return rf
}
