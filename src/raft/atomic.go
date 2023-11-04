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
	if len(lf.LogList) < 1 {
		// 所有的调用都不会用到content
		return Log{
			Term:    lf.lastIncludedTerm,
			Index:   lf.lastIncludedIndex,
			Content: nil,
		}
	}
	return lf.LogList[len(lf.LogList)-1]
}

func (lf *Logs) getLogByIndex(index int) Log {

	lf.mu.RLock()
	defer lf.mu.RUnlock()
	if index == lf.lastIncludedIndex {
		return Log{
			Term:    lf.lastIncludedTerm,
			Index:   lf.lastIncludedIndex,
			Content: nil,
		}
	}
	if index < lf.lastIncludedIndex || index > lf.getLastLogIndex() {
		return Log{}
	}
	return lf.LogList[index-lf.lastIncludedIndex-1]
}

func (lf *Logs) getLastLogIndex() int {
	lf.mu.RLock()
	lf.mu.RUnlock()
	if len(lf.LogList) < 1 {
		return lf.lastIncludedIndex
	}
	return lf.LogList[len(lf.LogList)-1].Index
}

func (lf *Logs) storeLog(logs ...Log) {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	lf.LogList = append(lf.LogList, logs...)
}

// 保留index
func (lf *Logs) removeTailLogs(index int) {
	if index > lf.getLastLogIndex() {
		return
	}
	lf.mu.Lock()
	defer lf.mu.Unlock()

	if index < lf.lastIncludedIndex {
		lf.LogList = []Log{}
		return
	}
	//if index <= lf.lastIncludedIndex || index > lf.LogList[len(lf.LogList)-1].Index {
	//	return
	//}

	lf.LogList = lf.LogList[:index-lf.lastIncludedIndex]
}

// 保留index
func (lf *Logs) removeHeadLogs(index int) {

	if index > lf.getLastLogIndex() {
		lf.mu.Lock()
		lf.LogList = []Log{}
		lf.mu.Unlock()
		return
	}
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if index < lf.lastIncludedIndex {
		return
	}

	//if index <= lf.lastIncludedIndex || index > lf.LogList[len(lf.LogList)-1].Index {
	//	return
	//}
	lf.LogList = lf.LogList[index-lf.lastIncludedIndex-1:]
}
