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

	pursueLogFlag []bool
	commitIndex   int
	EVMu          sync.RWMutex
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
	rf.xlog("接收到app请求,log:%+v", command)
	go rf.storeCommand(command)
	return rf.logs.getLastLogIndex() + 1, int(rf.getCurrentTerm()), true
}
func (rf *Raft) storeCommand(command interface{}) {
	// store to local
	rf.logs.storeLog(command, rf.getCurrentTerm())
	rf.EVMu.Lock()
	defer rf.EVMu.Unlock()
	//send copy request to all follower
	ch := make(chan EntityReply, len(rf.peers))
	for i := range rf.peers {

		if i == rf.me {
			continue
		}
		if rf.pursueLogFlag[i] {
			rf.xlog("服务器%v正在追赶日志，不去发送请求", i)
			continue
		}
		if rf.getMembership() != LEADER {
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
			if rf.getCurrentTerm() < reply.RequestEntityReply.Term {
				rf.xlog("我的term小，转换为follower")
				rf.setCurrentTerm(reply.RequestEntityReply.Term)
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
	rf.pursueLogFlag[serverId] = true
	defer func() {
		if rf.logs.getLastLogIndex() == rf.nextIndex[serverId] {
			rf.pursueLogFlag[serverId] = false
		}
	}()
	rf.xlog("在term%v，开启对服务器%v的日志追赶,nextId:%v", rf.getCurrentTerm(), serverId, rf.nextIndex[serverId])
	for {
		logIndex := rf.nextIndex[serverId]
		l := rf.logs.getLogByIndex(logIndex)
		pre := rf.logs.getLogByIndex(logIndex - 1)
		rf.xlog("寻找匹配点：本次发送logIndex：%v,preTerm:%v,log:%+v", logIndex, pre.Term, l)
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
			Term:         rf.getCurrentTerm(),
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
func (rf *Raft) RequestEntity(args *RequestEntityArgs, reply *RequestEntityReply) {
	// ok,leader is live
	rf.xlog("收到leader-%v-term%v的request", args.LeaderId, args.Term)
	term := args.Term
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	leaderCommit := args.LeaderCommit
	entry := args.Entry
	reply.Success = false
	reply.FollowerId = rf.me
	rf.EVMu.Lock()
	defer rf.EVMu.Unlock()
	reply.Term = rf.getCurrentTerm()
	if rf.getCurrentTerm() > term && rf.getMembership() != CANDIDATE {
		rf.xlog("leader的term小，已经拒绝")
		return
	}

	rf.setCurrentTerm(term)

	l := rf.logs.getLastLog()
	if rf.logs.getLastLogIndex() != prevLogIndex || l.Term != prevLogTerm {
		rf.xlog("prevLogTerm:%v,term:%v,prevLogIndex:%v,index:%v", prevLogTerm, l.Term, prevLogIndex, rf.logs.getLastLogIndex())
		rf.xlog("log不匹配，已经拒绝")
		return
	}
	// if have log entry,append the log
	if entry != (Log{}) {
		rf.xlog("收到来自raft%v的 EV request", args.LeaderId)
		// check log
		//rf.xlog("last log is %+v", lastLog)
		//l := rf.logs.getLogByIndex(prevLogIndex)
		//if (l == (Log{}) && prevLogIndex != 0) || l.Term != prevLogTerm {
		//	rf.xlog("prevLogTerm:%v,term:%v,prevLogIndex:%v,index:%v", prevLogTerm, l.Term, prevLogIndex, prevLogIndex)
		//	rf.xlog("log不匹配，已经拒绝")
		//	return
		//}
		if prevLogIndex < rf.logs.getLastLogIndex() {
			rf.xlog("当前最低匹配点index:%v,最新logindex:%v", prevLogIndex, rf.logs.getLastLogIndex())
			rf.logs.removeLogs(prevLogIndex)
		}
		rf.xlog("对log%+v进行存储", entry)
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
		//rf.xlog("收到来自raft%v的 heartbeat", args.LeaderId)
	}
	rf.RestartVoteEndTime()
	rf.setMembership(FOLLOWER)
	rf.setVoteFor(VOTE_NO)
	//rf.xlog("设置时间戳：%v", rf.getVoteEndTime())
	reply.Success = true
}

func (rf *Raft) heartBeat() {
	peers := rf.peers
	me := rf.me
	for {
		if rf.getMembership() != LEADER {
			rf.HeartBeatCond.L.Lock()
			for rf.getMembership() != LEADER {
				rf.xlog("无法发送heartbeat")
				rf.HeartBeatCond.Wait()
			}
			rf.xlog("变为leader")
			rf.HeartBeatCond.L.Unlock()
			// when raft's membership become leader,reset match index and next index
			rf.matchIndex = make([]int, len(rf.peers))
			rf.nextIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = rf.logs.getLastLogIndex() + 1
			}
			rf.pursueLogFlag = make([]bool, len(rf.peers))
			for i := range rf.pursueLogFlag {
				rf.pursueLogFlag[i] = false
			}
		}
		//rf.xlog("发送心跳,时间戳为：%v", time.Now().UnixMilli())

		for i := range peers {
			if i == me {
				continue
			}
			reply := RequestEntityReply{}

			//rf.xlog("向%v号服务器发送心跳，时间戳为：%v", i, time.Now().UnixMilli())

			go func(i int) {
				logIndex := rf.nextIndex[i]
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
				rf.sendRequestEntity(i, &args, &reply)
			}(i)
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
