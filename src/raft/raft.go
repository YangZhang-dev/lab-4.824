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

const (
	LEADER    int32 = 1
	CANDIDATE int32 = 2
	FOLLOWER  int32 = 3
)
const (
	BASE_VOTE_TIMEOUT  = 150
	VOTE_TIMEOUT_RANGE = 150
	HEARTBEAT_TIMEOUT  = 100
)
const VOTE_NO = -1

type Log struct {
}
type Logs struct {
	LogList      []Log
	LastLogIndex int
	LastLogTerm  int
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	//VoteMu    sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	cond        *sync.Cond
	memberShip  int32
	logs        Logs
	voteEndTime int64
	currentTerm int32
	voteTimeout int64
	voteFor     int32
	majority    int
	Rand        *rand.Rand
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) RestartVoteEndTime() {
	atomic.StoreInt64(&rf.voteEndTime, time.Now().UnixMilli())
}
func (rf *Raft) getVoteEndTime() int64 {
	return atomic.LoadInt64(&rf.voteEndTime)
}
func (rf *Raft) getVoteTimeout() int64 {
	return atomic.LoadInt64(&rf.voteTimeout)
}
func (rf *Raft) setVoteTimeout(t int64) {
	atomic.StoreInt64(&rf.voteTimeout, t)
}
func (rf *Raft) getMembership() int32 {
	return atomic.LoadInt32(&rf.memberShip)
}
func (rf *Raft) setMembership(m int32) {
	member := atomic.LoadInt32(&rf.memberShip)
	atomic.StoreInt32(&rf.memberShip, m)
	if member == LEADER && m != LEADER {
		rf.cond.Signal()
	}
}
func (rf *Raft) setVoteFor(m int32) {
	atomic.StoreInt32(&rf.voteFor, m)
}
func (rf *Raft) getVoteFor() int32 {
	return atomic.LoadInt32(&rf.voteFor)
}
func (rf *Raft) setCurrentTerm(m int32) {
	atomic.StoreInt32(&rf.currentTerm, m)
}
func (rf *Raft) getCurrentTerm() int32 {
	return atomic.LoadInt32(&rf.currentTerm)
}
func (rf *Raft) GetState() (int, bool) {
	return int(rf.getCurrentTerm()), rf.getMembership() == LEADER
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
	Term         int32
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
	FollowerId  int
	VoteGranted bool
	Term        int32
}
type VoteReply struct {
	RequestVoteReply RequestVoteReply
	Ok               bool
}
type RequestEntityArgs struct {
	LeaderId int
	Term     int32
}

type RequestEntityReply struct {
	FollowerId int
	Term       int32
	Success    bool
}
type EntityReply struct {
	RequestEntityReply RequestEntityReply
	Ok                 bool
}

func (rf *Raft) RequestEntity(args *RequestEntityArgs, reply *RequestEntityReply) {
	// ok,leader is live
	// rf.log("收到来自raft%v的心跳", args.LeaderId)
	term := args.Term
	reply.Success = false
	reply.FollowerId = rf.me
	reply.Term = rf.getCurrentTerm()
	if rf.getCurrentTerm() > term {
		// rf.log("leader的term小，已经拒绝")
		return
	}
	rf.setCurrentTerm(term)
	rf.RestartVoteEndTime()
	rf.setMembership(FOLLOWER)
	rf.setVoteFor(VOTE_NO)
	// rf.log("设置时间戳：%v", rf.getVoteEndTime())
	reply.Success = true
}
func (rf *Raft) sendRequestEntity(server int, args *RequestEntityArgs, reply *RequestEntityReply) bool {
	ok := rf.peers[server].Call("Raft.RequestEntity", args, reply)
	return ok
}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	term := args.Term
	candidateId := args.CandidateId
	lastLogIndex := args.LastLogIndex
	lastLogTerm := args.LastLogTerm
	// rf.log("接收到来自%v号服务器的投票请求", candidateId)

	reply.Term = rf.getCurrentTerm()
	reply.FollowerId = rf.me
	reply.VoteGranted = false
	// check candidate's term
	if term < rf.getCurrentTerm() {
		// rf.log("他的term小，已经拒绝")
		return
	}
	if term == rf.getCurrentTerm() {
		// check does self vote for other of self
		if rf.getVoteFor() != -1 {
			// rf.log("已经向他人投过票，已经拒绝")
			return
		}
		// check log's term and index
		if lastLogTerm < rf.logs.LastLogTerm {
			// rf.log("他log的term小，已经拒绝")
			return
		}
		if lastLogTerm == rf.logs.LastLogTerm && lastLogIndex < rf.logs.LastLogIndex {
			// rf.log("log的term相同，但他的log的index小，已经拒绝")
			return
		}
	}
	// rf.log("投他一票")
	reply.VoteGranted = true
	rf.RestartVoteEndTime()
	rf.setVoteFor(int32(candidateId))
	if term > rf.getCurrentTerm() {
		// rf.log("当前term为：%v,更新term为%v", rf.getCurrentTerm(), term)
		rf.setCurrentTerm(term)
	}
	rf.setMembership(FOLLOWER)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
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

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.setMembership(FOLLOWER)
	rf.RestartVoteEndTime()
	t := len(peers) % 2
	rf.majority = t
	if t != 0 {
		rf.majority++
	}
	rf.cond = sync.NewCond(&sync.Mutex{})
	rf.setVoteFor(VOTE_NO)
	rf.Rand = rand.New(rand.NewSource(int64(rf.me * rand.Int())))
	rf.setVoteTimeout(int64(rf.Rand.Intn(VOTE_TIMEOUT_RANGE) + BASE_VOTE_TIMEOUT))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.HeartBeat()
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
	log.SetFlags(log.Lmicroseconds)
}

// true 超时
func checkTime(lastTime, timeout int64) (bool, time.Duration) {
	currentTime := time.Now()
	voteEndTime := time.UnixMilli(lastTime)
	voteTimeout := time.Duration(timeout) * time.Millisecond

	elapsed := currentTime.Sub(voteEndTime)
	duration := voteTimeout - time.Duration(2)*time.Microsecond - elapsed
	if duration > 0 {
		return false, duration
	}
	return true, duration

}
func (rf *Raft) CheckVoteTimeout() {
	rf.init()
	// rf.log("当前Raft信息：%+v", rf)
	// rf.log("初始化完成，开始投票倒计时")
	ticker := time.NewTicker(time.Duration(rf.getVoteTimeout()) * time.Millisecond)
	for {
		if rf.getMembership() == LEADER {
			rf.cond.L.Lock()
			for rf.getMembership() == LEADER {
				rf.cond.Wait()
			}
			rf.cond.L.Unlock()
		}
		select {
		case <-ticker.C:
			timeout, duration := checkTime(rf.getVoteEndTime(), rf.getVoteTimeout())
			if timeout {
				// rf.log("投票超时,超时时间：%vms,设置的timeout：%vms", duration.Milliseconds(), rf.voteTimeout)
				rf.InitiateVote()
			} else {
				// rf.log("投票未超时，还剩下：%vms", duration.Milliseconds())
				ticker.Reset(duration - time.Duration(5)*time.Microsecond)
			}
		}
	}

	//for {
	//	rf.cond.L.Lock()
	//	for rf.getMembership() == LEADER {
	//		rf.cond.Wait()
	//	}
	//	rf.cond.L.Unlock()
	//	t := time.Since(time.UnixMilli(rf.getVoteEndTime()))
	//	// rf.log("距离上一次计时过去了：%vms,睡眠：%vms", t.Milliseconds(), rf.getVoteTimeout()-t.Milliseconds())
	//	time.Sleep(time.Duration(rf.getVoteTimeout()-t.Milliseconds()) * time.Millisecond)
	//	t = time.Since(time.UnixMilli(rf.getVoteEndTime()))
	//	// rf.log("睡醒了，现在距离上一次计时：%vms", t.Milliseconds(), rf.getVoteTimeout()-t.Milliseconds())
	//	if t.Milliseconds() >= rf.getVoteTimeout() {
	//		// rf.log("设置的超时时间为:%vms,当前时间戳为：%v，上次的时间戳是：%v,当前过了：%vms,我要发起投票了", rf.getVoteTimeout(), time.Now().UnixMilli(), rf.getVoteEndTime(), t.Milliseconds())
	//		rf.InitiateVote()
	//	}
	//}
}
func (rf *Raft) InitiateVote() {
	// rf.log("发起第%v轮投票", rf.getCurrentTerm())
	// if from follower to candidate,plus 1 to currentTerm
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)
	//// rf.log("由follower转化为candidate，term加一变为：%v", rf.getCurrentTerm())
	// if last membership is candidate,update voteTimeout to avoid conflict
	if rf.getMembership() == CANDIDATE {
		i := int64(rf.Rand.Intn(VOTE_TIMEOUT_RANGE) + BASE_VOTE_TIMEOUT)
		rf.setVoteTimeout(i)
	}
	// update membership to candidate
	rf.setMembership(CANDIDATE)
	rf.setVoteFor(VOTE_NO)

	if rf.getMembership() == CANDIDATE {
		getVoteNum := rf.handleVote()
		// rf.log("获得%v张票", getVoteNum)
		// get majority server vote
		if getVoteNum >= rf.majority {
			// rf.log("我获得的大多数选票,当选term为%v的leader", rf.getCurrentTerm())
			// update membership to leader
			rf.setMembership(LEADER)
			rf.setVoteFor(VOTE_NO)
			// rf.log("开启心跳")
		} else {
			rf.RestartVoteEndTime()
			// rf.log("oh, 我没有获得大多数选票")
		}
	}
	if rf.getMembership() == FOLLOWER {
		// rf.log("结束选票，我现在的身份是FOLLOWER")
	} else if rf.getMembership() == LEADER {
		// rf.log("结束选票，我现在的身份是LEADER")
	} else {
		// rf.log("结束选票，我现在的身份是CANDIDATE")
	}
}
func (rf *Raft) handleVote() int {
	defer func() {
		if r := recover(); r != nil {

		}
	}()

	// vote for myself
	getVoteNum := 0
	getVoteNum++
	args := RequestVoteArgs{
		Term:         rf.getCurrentTerm(),
		CandidateId:  rf.me,
		LastLogIndex: rf.logs.LastLogIndex,
		LastLogTerm:  rf.logs.LastLogTerm,
	}
	ch := make(chan VoteReply, len(rf.peers))
	rf.setVoteFor(int32(rf.me))
	// restart time
	rf.RestartVoteEndTime()
	// send RV RPC to all server
	group := sync.WaitGroup{}
	//var j int32 = 1
	group.Add(rf.majority - 1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		//group.Add(1)
		go func(i int) {
			defer func() {
				if r := recover(); r != nil {
				}
			}()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)

			//if atomic.LoadInt32(&j) < int32(rf.majority) {
			voteReply := VoteReply{
				RequestVoteReply: reply,
				Ok:               ok,
			}
			// rf.log("将服务器%v的投票结果放入", reply.FollowerId)
			//atomic.AddInt32(&j, 1)
			ch <- voteReply
			group.Done()
			//}
		}(i)
	}
	// TODO 如果投票结果请求时间大于超时时间，会出现错误
	group.Wait()
	close(ch)
	for reply := range ch {
		if rf.getMembership() != CANDIDATE {
			break
		}
		if !reply.Ok {
			// rf.log("服务器%v无响应", reply.RequestVoteReply.FollowerId)
			continue
		}
		if !reply.RequestVoteReply.VoteGranted {
			// rf.log("服务器%v没有投给我票，我的term是%v，他的term是%v", reply.RequestVoteReply.FollowerId, rf.getCurrentTerm(), reply.RequestVoteReply.Term)
			// check term
			if reply.RequestVoteReply.Term > rf.getCurrentTerm() {
				rf.setCurrentTerm(reply.RequestVoteReply.Term)
				// rf.log("我比服务器%v的term小，我不再发起选票", reply.RequestVoteReply.FollowerId)
				// update self membership to follower
				rf.setMembership(FOLLOWER)
				break
			}
			continue
		}
		// rf.log("获得来自服务器%v的选票", reply.RequestVoteReply.FollowerId)
		getVoteNum++
		if getVoteNum >= rf.majority {
			break
		}
	}
	return getVoteNum
}
func (rf *Raft) HeartBeat() {
	peers := rf.peers
	me := rf.me
	for {
		if rf.getMembership() != LEADER {
			continue
		}
		// rf.log("发送心跳,时间戳为：%v", time.Now().UnixMilli())
		args := RequestEntityArgs{}
		args.Term = rf.getCurrentTerm()
		args.LeaderId = rf.me
		for i := range peers {
			if i == me {
				continue
			}
			reply := RequestEntityReply{}
			// rf.log("向%v号服务器发送心跳，时间戳为：%v", i, time.Now().UnixMilli())
			go rf.sendRequestEntity(i, &args, &reply)
		}
		//// rf.log("发送完一次心跳，时间戳为：%v", time.Now().UnixMilli())
		time.Sleep(time.Duration(HEARTBEAT_TIMEOUT) * time.Millisecond)
		//// rf.log("我睡醒了，当前时间戳是：%v", time.Now().UnixMilli())
	}
}

//func (rf *Raft) HeartBeat() {
//	peers := rf.peers
//	me := rf.me
//	for {
//		if rf.getMembership() != LEADER {
//			break
//		}
//		// rf.log("发送心跳,时间戳为：%v", time.Now().UnixMilli())
//		ch := make(chan EntityReply, len(peers))
//		group := sync.WaitGroup{}
//		for i := range peers {
//			if i == me {
//				continue
//			}
//			group.Add(1)
//			go func(i int) {
//				args := RequestEntityArgs{}
//				args.Term = rf.getCurrentTerm()
//				args.LeaderId = rf.me
//				reply := RequestEntityReply{}
//				// rf.log("向%v号服务器发送心跳，时间戳为：%v", i, time.Now().UnixMilli())
//				ok := rf.sendRequestEntity(i, &args, &reply)
//				entityReply := EntityReply{
//					RequestEntityReply: reply,
//					Ok:                 ok,
//				}
//				ch <- entityReply
//				group.Done()
//			}(i)
//		}
//		group.Wait()
//		close(ch)
//		rf.RestartVoteEndTime()
//		for reply := range ch {
//			if !reply.Ok {
//				// rf.log("服务器%v无响应", reply.RequestEntityReply.FollowerId)
//				continue
//			}
//			if reply.RequestEntityReply.Success {
//				continue
//			}
//			// TODO 目前就是直接更新term
//			rf.setCurrentTerm(reply.RequestEntityReply.Term)
//			rf.setMembership(FOLLOWER)
//			break
//		}
//		// rf.log("发送完一次心跳，时间戳为：%v", time.Now().UnixMilli())
//		time.Sleep(time.Duration(HEARTBEAT_TIMEOUT) * time.Millisecond)
//		//// rf.log("我睡醒了，当前时间戳是：%v", time.Now().UnixMilli())
//	}
//}

func (rf *Raft) xlog(desc string, v ...interface{}) {
	log.Printf("raft-"+strconv.Itoa(rf.me)+"-term-"+strconv.FormatInt(int64(rf.getCurrentTerm()), 10)+"-"+desc+"\n", v...)
}
