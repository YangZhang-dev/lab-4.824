package raft

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// TODO
	// Your data here (2A).
	FollowerId  int
	VoteGranted bool
	Term        int32
}
type VoteReply struct {
	RequestVoteReply RequestVoteReply
	Ok               bool
}
type RequestEntityArgs struct {
	LeaderId     int
	Term         int32
	LeaderCommit int
	PrevLogTerm  int32
	PrevLogIndex int
	Entry        Log
}

type RequestEntityReply struct {
	FollowerId int
	Term       int32
	Success    bool
}
type EntityReply struct {
	RequestEntityReply RequestEntityReply
	Ok                 bool
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// TODO
	// Your data here (2A, 2B).
	Term         int32
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int32
}
