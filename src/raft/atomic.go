package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) RestartVoteEndTime() {
	atomic.StoreInt64(&rf.voteEndTime, time.Now().UnixMilli())
}

func (rf *Raft) setMembership(m int) {
	member := rf.memberShip
	rf.memberShip = m
	if member == LEADER && m != LEADER {
		rf.VoteCond.Signal()
		return
	}
	if member != LEADER && m == LEADER {
		rf.HeartBeatCond.Signal()
	}
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

func (lf *Logs) storeLog(command interface{}, term int) {
	lf.mu.Lock()
	defer lf.mu.Unlock()

	lf.LogList = append(lf.LogList, Log{
		Term:    term,
		Content: command,
		Index:   len(lf.LogList),
	})
}
func (lf *Logs) removeLogs(startIndex int) {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	lf.LogList = lf.LogList[:startIndex+1]
}
