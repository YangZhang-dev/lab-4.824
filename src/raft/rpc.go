package raft

type RequestVoteReply struct {
	// Your data here (2A).
	FollowerId  int
	VoteGranted bool
	Term        int
}

type RequestEntityArgs struct {
	LeaderId     int
	Term         int
	LeaderCommit int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []Log
}

type RequestEntityReply struct {
	FollowerId int
	Term       int
	Conflict   bool
	XTerm      int
	XIndex     int
	Success    bool
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) RequestEntity(args *RequestEntityArgs, reply *RequestEntityReply) {
	if len(args.Entries) >= 1 {
		rf.xlog("get EV request,leader %d,first log is %+v, last log is %+v", args.LeaderId, args.Entries[:1], args.Entries[len(args.Entries)-1:])
	} else {
		rf.xlog("get EV request,leader %d, args %+v", args.LeaderId, args)
	}
	//rf.xlog("current log is %+v", rf.logs.LogList)
	term := args.Term
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	leaderCommit := args.LeaderCommit
	entries := args.Entries
	reply.Success = false
	reply.Conflict = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.FollowerId = rf.me
	reply.Term = rf.currentTerm
	if term > rf.currentTerm {
		rf.startNewTerm(term)
	}
	if term < rf.currentTerm {
		return
	}

	rf.RestartVoteEndTime()
	rf.setMembership(FOLLOWER)

	l := rf.logs.getLogByIndex(prevLogIndex)
	if l.Term != prevLogTerm {
		reply.Conflict = true
		xTerm := prevLogTerm
		xIndex := prevLogIndex
		firstTerm := 0

		for i := prevLogIndex; i > 0; i-- {
			if i <= rf.logs.lastIncludedIndex {
				xIndex = i
				rf.xlog("from index %d start snapshot", i)
				break
			}
			log := rf.logs.getLogByIndex(i)
			if (log == Log{}) {
				xIndex = i
				xTerm = log.Term
			} else {
				firstTerm = log.Term
				break
			}
		}
		rf.xlog("get conflict term tail index %d", xIndex)
		for i := xIndex - 1; i > 0; i-- {
			if i <= rf.logs.lastIncludedIndex {
				rf.xlog("from index %d start snapshot", i)
				break
			}
			log := rf.logs.getLogByIndex(i)
			if firstTerm != log.Term {
				rf.xlog("not match ,firstTerm: %d,logTerm %d", firstTerm, log.Term)
				break
			}
			xIndex = i
			xTerm = log.Term
		}
		rf.xlog("get conflict log head index %d", xIndex)
		reply.XTerm = xTerm
		reply.XIndex = xIndex
		rf.xlog("term do not match,return false，xterm:%v,xindex:%v", xTerm, xIndex)
		rf.xlog("my log is %+v", rf.logs.LogList)
		return
	}
	//rf.xlog("last log index is :%v", rf.logs.getLastLog().Index)
	if prevLogIndex != rf.logs.getLastLog().Index {
		rf.xlog("current log is %+v", rf.logs.LogList)
		rf.logs.removeTailLogs(prevLogIndex)
		rf.xlog("remove log is %+v", rf.logs.LogList)
	}
	if entries != nil || len(entries) != 0 {
		rf.logs.storeLog(entries...)
		rf.xlog("store log,current log is %+v", rf.logs.LogList)

	}
	if rf.commitIndex < leaderCommit {
		if leaderCommit > rf.logs.getLastLogIndex() {
			leaderCommit = rf.logs.getLastLogIndex()
		}

		for rf.commitIndex < leaderCommit {
			rf.commitIndex++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs.getLogByIndex(rf.commitIndex).Content,
				CommandIndex: rf.commitIndex,
			}
			rf.sendCh <- msg
			rf.xlog("从leader%v,同步完成,msg:%+v", args.LeaderId, msg)
		}
		rf.commitIndex = leaderCommit
		rf.lastApplied = rf.commitIndex
		rf.xlog("从leader%v,同步完成,log is %+v", args.LeaderId, rf.logs.LogList)
	}
	rf.persist()
	reply.Success = true
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	term := args.Term
	candidateId := args.CandidateId
	lastLogIndex := args.LastLogIndex
	lastLogTerm := args.LastLogTerm

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.FollowerId = rf.me
	reply.VoteGranted = false

	if term < rf.currentTerm {
		return
	}
	if term > rf.currentTerm {
		rf.startNewTerm(term)
	}

	if rf.voteFor != -1 && rf.voteFor != candidateId {
		return
	}
	lastLog := rf.logs.getLastLog()
	if lastLogTerm < lastLog.Term {
		return
	}
	if lastLogTerm == lastLog.Term && lastLogIndex < lastLog.Index {
		return
	}

	reply.VoteGranted = true
	rf.RestartVoteEndTime()
	rf.voteFor = candidateId
	rf.setMembership(FOLLOWER)
	rf.persist()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.xlog("get a snapshot request,last index %d,last term %d", args.LastIncludedIndex, args.LastIncludedTerm)
	data := args.Data
	term := args.Term
	lastIncludedIndex := args.LastIncludedIndex
	lastIncludedTerm := args.LastIncludedTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if term < rf.currentTerm {
		return
	}
	if term > rf.currentTerm {
		rf.startNewTerm(term)
	}
	if lastIncludedIndex <= rf.commitIndex {
		return
	}
	rf.logs.lastIncludedIndex = lastIncludedIndex
	rf.logs.lastIncludedTerm = lastIncludedTerm
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      data,
		SnapshotTerm:  lastIncludedTerm,
		SnapshotIndex: lastIncludedIndex,
	}
	rf.sendCh <- applyMsg
}

func (rf *Raft) sendRequestEntity(server int, args *RequestEntityArgs, reply *RequestEntityReply) bool {
	ok := rf.peers[server].Call("Raft.RequestEntity", args, reply)
	return ok
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendRequestSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
