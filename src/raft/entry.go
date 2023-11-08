package raft

import "time"

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return -1, -1, false
	}
	rf.xlog("接收到app请求,log:%+v", command)
	index := rf.logs.getLastLogIndex() + 1
	currentTerm := rf.currentTerm
	rf.logs.storeLog(Log{
		Term:    currentTerm,
		Index:   index,
		Content: command,
	})
	rf.xlog("current log index is %+v", index)
	rf.persist()
	rf.xlog("store down")
	rf.mu.Unlock()
	rf.appendEntries(false)
	return index, currentTerm, true
}
func (rf *Raft) sendNoOp() {
	if rf.logs.getLastLogIndex() > 1 && rf.logs.getLastLog().Term != rf.currentTerm {
		index := rf.logs.getLastLogIndex() + 1
		rf.xlog("send no-op,log index %+v", index)
		rf.logs.storeLog(Log{
			Term:    rf.currentTerm,
			Index:   index,
			Content: 0,
		})
	}
}
func (rf *Raft) appendEntries(isHeartBeat bool) {
	peers := rf.peers
	me := rf.me
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if isHeartBeat && state != LEADER {
			rf.heartBeatCond.L.Lock()
			for {
				rf.mu.Lock()
				if rf.state == LEADER {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				rf.heartBeatCond.Wait()
			}
			rf.heartBeatCond.L.Unlock()
			rf.mu.Lock()
			rf.matchIndex = make([]int, len(rf.peers))
			rf.nextIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = rf.logs.getLastLogIndex() + 1
			}
			rf.sendNoOp()
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		commitId := rf.commitIndex
		term := rf.currentTerm
		rf.xlog("before send,next indexes is %+v,match indexes is %+v，logList is %+v", rf.nextIndex, rf.matchIndex, rf.logs.LogList)
		rf.mu.Unlock()
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
func (rf *Raft) leaderSendEntries(serverId, term, commitId int) {
	reply := RequestEntityReply{}
	logs := make([]Log, 0)
	rf.mu.Lock()

	lastIncludedIndex := rf.logs.lastIncludedIndex
	nextIndex := rf.nextIndex[serverId]
	if nextIndex <= lastIncludedIndex {
		go rf.snapshotHandler(serverId)
		rf.mu.Unlock()
		return
	}
	for i := nextIndex; i <= rf.logs.getLastLogIndex(); i++ {
		logs = append(logs, rf.logs.getLogByIndex(i))
	}
	pre := rf.logs.getLogByIndex(nextIndex - 1)
	successNextIndex := nextIndex + len(logs)
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	args := RequestEntityArgs{
		LeaderId:     rf.me,
		Term:         term,
		LeaderCommit: commitId,
		PrevLogTerm:  pre.Term,
		PrevLogIndex: pre.Index,
		Entries:      logs,
	}
	successMatchIndex := args.PrevLogIndex + len(logs)
	if len(logs) >= 1 {
		rf.xlog("send to server%v, start log: %+v,last log: %+v", serverId, logs[:1], logs[len(logs)-1:])
	} else {
		rf.xlog("send to server%v,args:%+v", serverId, args)
	}
	ok := rf.sendRequestEntity(serverId, &args, &reply)
	if !ok {
		//rf.xlog("收到server%d 超时日志响应", serverId)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.startNewTerm(reply.Term)
		return
	}
	if args.Term != rf.currentTerm ||
		rf.state != LEADER ||
		nextIndex != rf.nextIndex[serverId] ||
		rf.logs.lastIncludedIndex != lastIncludedIndex {
		return
	}

	rf.xlog("reply from server%v,reply:%+v", serverId, reply)
	if !reply.Success {
		if reply.Conflict {
			if reply.XIndex <= lastIncludedIndex {
				rf.xlog("start snapshot for server %d, index %d", serverId, reply.XIndex)
				go rf.snapshotHandler(serverId)
			} else {
				rf.nextIndex[serverId] = reply.XIndex
			}
		}
	} else {
		rf.nextIndex[serverId] = successNextIndex
		rf.matchIndex[serverId] = successMatchIndex
		rf.commitHandler(rf.logs.getLastLogIndex(), args.Term)
	}
}

func (rf *Raft) commitHandler(index int, term int) {
	//rf.xlog("commit handler")
	if index <= rf.commitIndex || rf.state != LEADER {
		return
	}
	//rf.xlog("args term %v, matchIndex:%+v, check Log index:%v Term:%v", term, rf.matchIndex, index, rf.logs.getLogByIndex(index).Term)
	//rf.xlog("nextIndex is :%+v, logs is %+v", rf.nextIndex, rf.logs.LogList)
	counter := 0
	maxIndex := -1
	for serverId := range rf.peers {
		if rf.logs.getLogByIndex(index).Term == term {
			if serverId == rf.me {
				counter++
			} else {
				matchIndex := rf.matchIndex[serverId]
				if matchIndex < index {
					maxIndex = max(matchIndex, maxIndex)
				}
				//rf.xlog("server %v, index is: %v,matchIndex is %v", serverId, rf.logs.getLogByIndex(index).Term, matchIndex)
				if matchIndex >= index {
					counter++
				}
			}
		}
		if counter >= rf.majority {
			rf.xlog("commit a log: %+v,majority is %v", rf.logs.getLogByIndex(index), rf.majority)
			rf.commitIndex = index
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs.getLogByIndex(rf.lastApplied).Content,
					CommandIndex: rf.lastApplied,
				}
				rf.sendCh <- msg
			}
			rf.lastApplied = rf.commitIndex
			rf.persist()
			break
		}
	}
	//rf.xlog("next indexes is %+v,match indexes is %+v", rf.nextIndex, rf.matchIndex)
	//rf.xlog("for index %d,current counter is %d", index, counter)
	rf.commitHandler(maxIndex, term)
}
func (rf *Raft) applier() {
	for msg := range rf.sendCh {
		if rf.killed() == true {
			return
		}
		rf.applyCh <- msg
		rf.xlog("apply a log:%+v", msg)
	}
}
