package raft

import (
	"log"
	"math/rand"
	"os"
	"sync/atomic"

	//	"bytes"
	"sync"
	//	"6.824/labgob"
	"6.824/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
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
	BASE_VOTE_TIMEOUT  = 600
	VOTE_TIMEOUT_RANGE = 200
	HEARTBEAT_DURATION = 60
	APPLIY_DURATION    = 20
)
const (
	VOTE_NO        = -1
	TIMEOUT_OFFSET = 2
	SENDCHAN_CAP   = 100
)

type Log struct {
	Term    int
	Index   int
	Content interface{}
}
type Logs struct {
	LogList            []Log
	lastIncludedTerm   int
	tLastIncludedTerm  int
	lastIncludedIndex  int
	tLastIncludedIndex int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	// --------persistent---------------

	currentTerm int
	voteFor     int
	logs        Logs

	// --------volatile---------------

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	state         int
	voteEndTime   int64
	voteTimeout   int64
	majority      int
	rand          *rand.Rand
	applyCh       chan ApplyMsg
	sendCh        chan ApplyMsg
	voteCond      *sync.Cond
	heartBeatCond *sync.Cond
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	if IsRaft {
		prefix := "log/raft"
		logFile, err := os.OpenFile(prefix+".log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Panic(err)
		}
		log.SetOutput(logFile)
		log.SetFlags(log.Lmicroseconds)
	}
	// Your initialization code here (2A, 2B, 2C).
	rf.setState(FOLLOWER)
	t := len(peers) / 2
	rf.majority = t
	if t != 0 {
		rf.majority++
	}
	rf.voteCond = sync.NewCond(&sync.Mutex{})
	rf.heartBeatCond = sync.NewCond(&sync.Mutex{})
	rf.voteFor = VOTE_NO
	rf.rand = rand.New(rand.NewSource(int64(rf.me * rand.Int())))
	rf.voteTimeout = int64(rf.rand.Intn(VOTE_TIMEOUT_RANGE) + BASE_VOTE_TIMEOUT)
	rf.logs = Logs{
		LogList: make([]Log, 0),
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.logs.getLastLogIndex() + 1
	}
	rf.logs.lastIncludedIndex = 0
	rf.logs.lastIncludedTerm = 0
	rf.sendCh = make(chan ApplyMsg, SENDCHAN_CAP)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	rf.restartVoteEndTime()
	go rf.ticker()
	go rf.appendEntries(true)
	go rf.applier()
	return rf
}
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.xlog("当前日志%+v，commitIndex：%v,snapshotIndex is %d", rf.getLogHeadAndTail(), rf.commitIndex, rf.logs.lastIncludedIndex)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
