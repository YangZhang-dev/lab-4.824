package raft

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.xlog("Condsnapshot request, lastApplied %d, oldTerm %d,newTerm %d,oldIndex %d,newIndex %d", rf.lastApplied, lastIncludedTerm, rf.logs.tLastIncludedTerm, lastIncludedIndex, rf.logs.tLastIncludedIndex)
	// TODO LAST CHANGE
	if lastIncludedTerm != rf.logs.tLastIncludedTerm || lastIncludedIndex != rf.logs.tLastIncludedIndex {
		rf.logs.tLastIncludedIndex = rf.logs.lastIncludedIndex
		rf.logs.tLastIncludedTerm = rf.logs.lastIncludedTerm
		rf.xlog("snapshot uninstall, lastApplied %d, oldTerm %d,newTerm %d,oldIndex %d,newIndex %d", rf.lastApplied, lastIncludedTerm, rf.logs.tLastIncludedTerm, lastIncludedIndex, rf.logs.tLastIncludedIndex)
		return false
	}
	if lastIncludedIndex <= rf.lastApplied {
		rf.logs.tLastIncludedIndex = rf.logs.lastIncludedIndex
		rf.logs.tLastIncludedTerm = rf.logs.lastIncludedTerm
		return false
	}
	if lastIncludedIndex > rf.lastApplied {
		rf.lastApplied = lastIncludedIndex
	}
	if lastIncludedIndex > rf.commitIndex {
		rf.commitIndex = lastIncludedIndex
	}
	rf.xlog("remove from index %d", rf.logs.tLastIncludedIndex+1)
	rf.logs.removeHeadLogs(rf.logs.tLastIncludedIndex + 1)

	rf.logs.lastIncludedIndex = lastIncludedIndex
	rf.logs.lastIncludedTerm = lastIncludedTerm
	rf.persister.mu.Lock()
	rf.persister.snapshot = snapshot
	rf.persister.mu.Unlock()
	rf.persist()
	rf.xlog("snapshot install,current log is %+v", rf.getLogHeadAndTail())
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.xlog("from service get a snapshot request")
	rf.logs.lastIncludedTerm = rf.logs.getLogByIndex(index).Term
	rf.xlog("log %+v,request index is %d", rf.getLogHeadAndTail(), index)

	rf.logs.removeHeadLogs(index + 1)
	rf.logs.lastIncludedIndex = index
	rf.persister.mu.Lock()
	rf.persister.snapshot = snapshot
	rf.persister.mu.Unlock()
	rf.persist()
	for serverId := range rf.peers {
		if serverId != rf.me {
			go rf.snapshotHandler(serverId)
		}
	}
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
