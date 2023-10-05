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
	Entry        Log
}

type RequestEntityReply struct {
	FollowerId int
	Term       int
	Success    bool
}
type EntityReply struct {
	RequestEntityReply RequestEntityReply
	Ok                 bool
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (rf *Raft) RequestEntity(args *RequestEntityArgs, reply *RequestEntityReply) {
	term := args.Term
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	leaderCommit := args.LeaderCommit
	entry := args.Entry
	reply.Success = false
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

	l := rf.logs.getLastLog()
	if l.Index != prevLogIndex || l.Term != prevLogTerm {
		return
	}
	if entry != (Log{}) {
		rf.logs.storeLog(entry.Content, term)
		// TODO 更改为goroutine commit
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Content,
			CommandIndex: prevLogIndex + 1,
		}
		rf.applyCh <- msg
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
