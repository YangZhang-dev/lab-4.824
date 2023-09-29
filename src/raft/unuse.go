package raft

import (
	"log"
	"strconv"
	"sync/atomic"
)

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

//	func (rf *Raft) heartBeat() {
//		peers := rf.peers
//		me := rf.me
//		if rf.getMembership() != LEADER {
//			rf.HeartBeatCond.L.Lock()
//			for rf.getMembership() == LEADER {
//				rf.HeartBeatCond.Wait()
//			}
//			rf.HeartBeatCond.L.Unlock()
//			// when raft's membership become leader,reset match index and next index
//			rf.matchIndex = make([]int, 0)
//			rf.nextIndex = make([]int, rf.logs.getLastLogIndex()+1)
//		}
//		rf.xlog("发送心跳,时间戳为：%v", time.Now().UnixMilli())
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
//				rf.xlog("向%v号服务器发送心跳，时间戳为：%v", i, time.Now().UnixMilli())
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
//		go func() {
//			rf.RestartVoteEndTime()
//			for reply := range ch {
//				if !reply.Ok {
//					rf.xlog("服务器%v无响应", reply.RequestEntityReply.FollowerId)
//					continue
//				}
//				if reply.RequestEntityReply.Success {
//					continue
//				}
//				// TODO 目前就是直接更新term
//				rf.setCurrentTerm(reply.RequestEntityReply.Term)
//				rf.setMembership(FOLLOWER)
//				break
//			}
//		}()
//
//		time.Sleep(time.Duration(HEARTBEAT_DURATION) * time.Millisecond)
//	}
const DEBUG = true

func (rf *Raft) xlog(desc string, v ...interface{}) {
	if DEBUG {
		log.Printf("raft-"+strconv.Itoa(rf.me)+"-term-"+strconv.FormatInt(int64(rf.getCurrentTerm()), 10)+"-"+desc+"\n", v...)
	}
}
