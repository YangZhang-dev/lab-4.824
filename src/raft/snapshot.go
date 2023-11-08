package raft

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedTerm != rf.logs.tLastIncludedTerm || lastIncludedIndex != rf.logs.tLastIncludedIndex {
		rf.mu.Unlock()
		rf.xlog("snapshot uninstall oldTerm %d,newTerm %d,oldIndex %d,newIndex %d", lastIncludedTerm, rf.logs.lastIncludedTerm, lastIncludedIndex, rf.logs.lastIncludedIndex)
		return false
	}
	if lastIncludedIndex > rf.commitIndex {
		rf.lastApplied = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
	}
	rf.xlog("remove from index %d", rf.logs.lastIncludedIndex+1)
	rf.logs.removeHeadLogs(rf.logs.tLastIncludedIndex + 1)

	rf.logs.lastIncludedIndex = lastIncludedIndex
	rf.logs.lastIncludedTerm = lastIncludedTerm
	rf.persister.mu.Lock()
	rf.persister.snapshot = snapshot
	rf.persister.mu.Unlock()
	rf.persist()
	rf.xlog("snapshot install,current log is %+v", rf.logs.LogList)
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.xlog("from service get a snapshot request")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs.lastIncludedTerm = rf.logs.getLogByIndex(index).Term
	rf.xlog("log %+v,request index is %d", rf.logs.LogList, index)
	// 0--15
	// 10
	// 10-0=10
	// 10--15
	rf.logs.removeHeadLogs(index + 1)
	rf.logs.lastIncludedIndex = index
	rf.persister.mu.Lock()
	rf.persister.snapshot = snapshot
	rf.persister.mu.Unlock()
	rf.persist()
	//rf.xlog("current log is %+v", rf.logs.LogList)
	//rf.xlog("current first log is %+v,last log is %+v", rf.logs.LogList[:1], rf.logs.LogList[len(rf.logs.LogList)-1])
}
func (rf *Raft) snapshotHandler(serverId int) {
	rf.mu.Lock()
	rf.persister.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.logs.lastIncludedIndex,
		LastIncludedTerm:  rf.logs.lastIncludedTerm,
		Data:              rf.persister.snapshot,
	}
	rf.persister.mu.Unlock()
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	ok := rf.sendRequestSnapshot(serverId, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.startNewTerm(reply.Term)
		return
	}
	if args.Term != rf.currentTerm || rf.state != LEADER {
		return
	}

	rf.xlog("snapshot reply from serverId %d,response is %+v", serverId, reply)
	rf.nextIndex[serverId] = rf.logs.lastIncludedIndex + 1
}
