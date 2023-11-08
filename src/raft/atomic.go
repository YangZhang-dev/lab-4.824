package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) restartVoteEndTime() {
	atomic.StoreInt64(&rf.voteEndTime, time.Now().UnixMilli())
}

func (rf *Raft) setState(m int) {
	state := rf.state
	rf.state = m
	if state == LEADER && m != LEADER {
		rf.voteCond.Signal()
		return
	}
	if state != LEADER && m == LEADER {
		rf.heartBeatCond.Signal()
	}
}
func (lf *Logs) getLastLog() Log {
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
	if len(lf.LogList) < 1 {
		return lf.lastIncludedIndex
	}
	return lf.LogList[len(lf.LogList)-1].Index
}

func (lf *Logs) storeLog(logs ...Log) {
	lf.LogList = append(lf.LogList, logs...)
}

// 保留index
func (lf *Logs) removeTailLogs(index int) {
	if index > lf.getLastLogIndex() {
		return
	}
	if index < lf.lastIncludedIndex {
		lf.LogList = []Log{}
		return
	}

	lf.LogList = lf.LogList[:index-lf.lastIncludedIndex]
}

// 保留index
func (lf *Logs) removeHeadLogs(index int) {

	if index > lf.getLastLogIndex() {
		lf.LogList = []Log{}
		return
	}
	if index < lf.lastIncludedIndex {
		return
	}

	lf.LogList = lf.LogList[index-lf.lastIncludedIndex-1:]
}
