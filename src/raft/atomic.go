package raft

import (
	"sync/atomic"
	"time"
)

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
		rf.VoteCond.Signal()
		return
	}
	if member != LEADER && m == LEADER {
		rf.HeartBeatCond.Signal()
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
func (lf *Logs) getLastLog() Log {
	lf.mu.RLock()
	defer lf.mu.RUnlock()
	return lf.getLogByIndex(lf.getLastLogIndex())
}
func (lf *Logs) getLogByIndex(index int) Log {
	if index < 1 || index > lf.getLastLogIndex() {
		return Log{}
	}
	lf.mu.RLock()
	defer lf.mu.RUnlock()
	return lf.LogList[index]
}
func (lf *Logs) getLastLogIndex() int {
	lf.mu.RLock()
	defer lf.mu.RUnlock()
	return len(lf.LogList) - 1
}

//	func (rf *Raft) setNextLogIndex(serverId, index int) {
//		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&serverId)), unsafe.Pointer(&index))
//	}
//
//	func (rf *Raft) getNextLogIndex(serverId int) int {
//		return *(*int)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&rf.nextIndex[serverId]))))
//	}
func (lf *Logs) storeLog(command interface{}, term int32) {
	lf.mu.Lock()
	defer lf.mu.Unlock()

	lf.LogList = append(lf.LogList, Log{
		Term:    term,
		Content: command,
		State:   UNCOMMITED,
	})

}
func (lf *Logs) removeLogs(startIndex int) {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	lf.LogList = lf.LogList[:startIndex+1]
}
