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
	"strconv"

	//	"bytes"
	"sync"
	"sync/atomic"
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
type MemberShip int

const (
	LEADER    MemberShip = 1
	CANDIDATE MemberShip = 2
	FOLLOWER  MemberShip = 3
)
const (
	BASE_VOTE_TIMEOUT  = 250
	VOTE_TIMEOUT_RANGE = 200
	HEARTBEAT_TIMEOUT  = 120
)

type Log struct {
}
type Logs struct {
	LogList      []Log
	LastLogIndex int
	LastLogTerm  int
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	memberShip  MemberShip
	logs        Logs
	voteEndTime int64
	currentTerm int
	voteTimeout int64
	voteFor     int
	majority    int
	// Your data here (2A, 2B, 2C).
	// TODO
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.memberShip == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// TODO
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// TODO
	// Your data here (2A).
	VoteGranted bool
	Term        int
}
type RequestEntityArgs struct {
}

type RequestEntityReply struct {
}

func (rf *Raft) RequestEntity(args *RequestEntityArgs, reply *RequestEntityReply) {
	// ok,leader is live
	rf.xlog("收到心跳")
	rf.mu.Lock()
	rf.voteFor = -1
	rf.RestartVoteTime()
	rf.memberShip = FOLLOWER
	//rf.xlog("设置时间戳：%v", rf.voteEndTime)
	rf.mu.Unlock()
}
func (rf *Raft) sendRequestEntity(server int, args *RequestEntityArgs, reply *RequestEntityReply) bool {
	ok := rf.peers[server].Call("Raft.RequestEntity", args, reply)
	return ok
}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO
	// Your code here (2A, 2B).
	term := args.Term
	candidateId := args.CandidateId
	lastLogIndex := args.LastLogIndex
	lastLogTerm := args.LastLogTerm
	rf.xlog("接收到来自%v号服务器的投票请求", candidateId)

	rf.mu.RLock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// first,check candidate's term
	if term < rf.currentTerm {
		rf.xlog("他的term小，已经拒绝")
		rf.mu.RUnlock()
		return
	}
	// check does self vote for other of self
	voteFor := rf.voteFor
	if voteFor != -1 {
		rf.xlog("已经向他人投过票，已经拒绝")
		rf.mu.RUnlock()
		return
	}
	// check log's term and index
	if lastLogTerm < rf.logs.LastLogTerm {
		rf.xlog("他log的term小，已经拒绝")
		rf.mu.RUnlock()
		return
	}
	if lastLogTerm == rf.currentTerm && lastLogIndex < rf.logs.LastLogIndex {
		rf.xlog("log的term相同，但他的log的index小，已经拒绝")
		rf.mu.RUnlock()
		return
	}
	rf.xlog("投他一票")
	reply.VoteGranted = true
	rf.mu.RUnlock()
	rf.mu.Lock()
	rf.voteFor = candidateId
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	rf.memberShip = FOLLOWER
	rf.RestartVoteTime()
	rf.mu.Unlock()
}

// RestartVoteTime 必须在外部lock
func (rf *Raft) RestartVoteTime() {
	rf.voteEndTime = time.Now().UnixMilli()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// TODO
	// lossy network,server fail or network fail all can
	// whatever will return false or true
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// TODO
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.memberShip = FOLLOWER
	rf.RestartVoteTime()
	rf.voteTimeout = int64(rand.Intn(VOTE_TIMEOUT_RANGE) + BASE_VOTE_TIMEOUT)
	t := len(peers) % 2
	rf.majority = t
	if t != 0 {
		rf.majority++
	}
	rf.voteFor = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.CheckVoteTimeout()
	return rf
}
func (rf *Raft) init() {
	prefix := "raft"
	logFile, err := os.OpenFile(prefix+".log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags)
}
func (rf *Raft) CheckVoteTimeout() {
	rf.init()
	rf.xlog("当前Raft信息：%+v", rf)
	rf.xlog("初始化完成，开始投票倒计时")
	for {
		rf.mu.RLock()
		t := time.Since(time.UnixMilli(rf.voteEndTime))
		rf.mu.RUnlock()

		if t.Milliseconds() > rf.voteTimeout {
			rf.mu.RLock()
			rf.xlog("投票超时了，当前设置的超时时间：%vms，当前时间：%vms,我要发起投票了", rf.voteTimeout, t.Milliseconds())
			rf.xlog("发起第%v轮投票", rf.currentTerm)
			rf.mu.RUnlock()
			rf.InitiateVote()
		}
	}
}
func (rf *Raft) InitiateVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// plus 1 to currentTerm
	if rf.memberShip == FOLLOWER {
		rf.currentTerm++
	}
	// timeout,start a new vote
	// update membership to candidate
	rf.memberShip = CANDIDATE
	rf.voteFor = -1

	for rf.memberShip == CANDIDATE {

		// vote for myself
		getVoteNum := 0
		getVoteNum++
		rf.voteFor = rf.me
		// restart time
		rf.RestartVoteTime()
		// send RV RPC to all server

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.logs.LastLogIndex,
			LastLogTerm:  rf.logs.LastLogTerm,
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			reply := RequestVoteReply{}
			rf.xlog("向%v号服务器发出请求", i)
			rf.mu.Unlock()
			ok := rf.sendRequestVote(i, &args, &reply)
			rf.mu.Lock()
			// network error,server error
			if !ok {
				rf.xlog("服务器%v无响应", i)
				continue
			}
			if !reply.VoteGranted {
				rf.xlog("服务器%v没有投给我票，我的term是%v，他的term是%v", i, rf.currentTerm, reply.Term)
				// check term
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.xlog("我比服务器%v的term小，我不再发起选票", i)
					// update self membership to follower
					rf.memberShip = FOLLOWER
					break
				}
				continue
			}
			rf.xlog("获得来自服务器%v的选票", i)
			getVoteNum++
		}
		rf.xlog("获得%v张票", getVoteNum)
		// get majority server vote
		if getVoteNum >= rf.majority {
			rf.xlog("我获得的大多数选票,当选term为%v的leader", rf.currentTerm)
			// update membership to leader
			rf.memberShip = LEADER
			rf.voteFor = -1
			rf.xlog("开启心跳")
			go rf.HeartBeat()
		} else {
			rf.xlog("oh, 我没有获得大多数选票")
			break
		}
	}
	if rf.memberShip == FOLLOWER {
		rf.xlog("结束选票，我现在的身份是FOLLOWER")
	} else if rf.memberShip == LEADER {
		rf.xlog("结束选票，我现在的身份是LEADER")
	} else {
		rf.xlog("结束选票，我现在的身份是CANDIDATE")
	}
}
func (rf *Raft) HeartBeat() {
	rf.mu.RLock()
	ship := rf.memberShip
	peers := rf.peers
	me := rf.me
	rf.mu.RUnlock()
	for ship == LEADER {
		rf.mu.Lock()
		rf.RestartVoteTime()
		rf.mu.Unlock()
		rf.xlog("发送心跳")
		for i := range peers {
			if i == me {
				continue
			}
			args := RequestEntityArgs{}
			reply := RequestEntityReply{}
			//rf.xlog("向%v号服务器发送心跳，时间戳为：%v", i, time.Now().UnixMilli())
			go rf.sendRequestEntity(i, &args, &reply)
		}
		time.Sleep(time.Duration(HEARTBEAT_TIMEOUT) * time.Millisecond)
	}
}

func (rf *Raft) xlog(desc string, v ...interface{}) {
	log.Printf("raft-"+strconv.Itoa(rf.me)+"-"+desc+"\n", v...)
}
