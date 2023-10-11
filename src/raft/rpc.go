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

func (rf *Raft) RequestEntity(args *RequestEntityArgs, reply *RequestEntityReply) {
	rf.xlog("get EV request,args:%+v", args)
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
	//rf.xlog("for this log: leader idx:%v,term %v. follower idx:%v,term %v", prevLogIndex, prevLogTerm, l.Index, l.Term)
	if l.Term != prevLogTerm {
		reply.Conflict = true
		xTerm := prevLogTerm
		xIndex := prevLogIndex
		for i := prevLogIndex; i > 0; i-- {
			if rf.logs.getLogByIndex(i).Term != prevLogTerm {
				break
			}
			xIndex = i
		}
		reply.XTerm = xTerm
		reply.XIndex = xIndex
		rf.xlog("term do not match,return false，xterm:%v,xindex:%v", xTerm, xIndex)
		return
	}
	rf.xlog("last log index is :%v", rf.logs.getLastLog().Index)
	if prevLogIndex != rf.logs.getLastLog().Index {
		rf.xlog("current log is %+v", rf.logs.LogList)
		rf.logs.removeLogs(prevLogIndex)
		rf.xlog("remove logs,remain log is: %+v", rf.logs.LogList)
	}
	if entries != nil || len(entries) != 0 {
		for _, entry := range entries {
			rf.logs.storeLog(entry.Content, entry.Term)
		}
		rf.xlog("store log,current log is %+v", rf.logs.LogList)
	}
	if rf.commitIndex < leaderCommit {
		if leaderCommit > rf.logs.getLastLogIndex() {
			leaderCommit = rf.logs.getLastLogIndex()
		}
		for rf.commitIndex <= leaderCommit {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs.getLogByIndex(rf.commitIndex).Content,
				CommandIndex: rf.commitIndex,
			}
			rf.applyCh <- msg
			rf.xlog("从leader%v,同步完成,msg:%+v", args.LeaderId, msg)
			rf.commitIndex++
		}
		rf.commitIndex = leaderCommit
	}
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
}
func (rf *Raft) sendRequestEntity(server int, args *RequestEntityArgs, reply *RequestEntityReply) bool {
	ok := rf.peers[server].Call("Raft.RequestEntity", args, reply)
	return ok
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
