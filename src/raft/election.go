package raft

import (
	"time"
)

// true 超时
func checkTime(lastTime, timeout int64) (bool, time.Duration) {
	currentTime := time.Now()
	voteEndTime := time.UnixMilli(lastTime)
	voteTimeout := time.Duration(timeout) * time.Millisecond

	elapsed := currentTime.Sub(voteEndTime)
	duration := voteTimeout - time.Duration(2)*time.Microsecond - elapsed
	if duration > 0 {
		return false, duration
	}
	return true, duration

}

func (rf *Raft) ticker() {
	ticker := time.NewTicker(time.Duration(rf.voteTimeout) * time.Millisecond)
	for rf.killed() == false {
		rf.mu.RLock()
		memberShip := rf.memberShip
		voteTimeout := rf.voteTimeout
		rf.mu.RUnlock()
		if memberShip == LEADER {
			rf.VoteCond.L.Lock()
			for {
				rf.mu.RLock()
				if rf.memberShip != LEADER {
					rf.mu.RUnlock()
					break
				}
				rf.mu.RUnlock()
				rf.VoteCond.Wait()
			}
			rf.VoteCond.L.Unlock()
			ticker.Reset(time.Duration(voteTimeout) * time.Millisecond)
		}

		select {
		case <-ticker.C:
			rf.mu.RLock()
			if rf.memberShip == LEADER {
				rf.mu.RUnlock()
				break
			}
			timeout, duration := checkTime(rf.voteEndTime, rf.voteTimeout)
			rf.mu.RUnlock()
			if timeout {
				rf.election()
			} else {
				ticker.Reset(duration)
			}
		}
	}

}
func (rf *Raft) election() {
	rf.mu.Lock()
	rf.xlog("start a election")
	rf.currentTerm++
	rf.voteFor = rf.me

	if rf.memberShip == CANDIDATE {
		rf.voteTimeout = int64(rf.Rand.Intn(VOTE_TIMEOUT_RANGE) + BASE_VOTE_TIMEOUT)
	}
	rf.RestartVoteEndTime()
	rf.setMembership(CANDIDATE)

	rf.mu.Unlock()
	go rf.electionHandler()
}
func (rf *Raft) electionHandler() {
	counter := 1
	lastLog := rf.logs.getLastLog()
	rf.mu.RLock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	rf.mu.RUnlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(serverId int, counter *int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(serverId, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.startNewTerm(reply.Term)
				return
			}
			if reply.VoteGranted {
				*counter++
			}
			if *counter >= rf.majority {
				rf.setMembership(LEADER)
			}
		}(i, &counter)
	}
}

// must lock
func (rf *Raft) startNewTerm(term int) {
	rf.currentTerm = term
	rf.setMembership(FOLLOWER)
	rf.voteFor = VOTE_NO
}
