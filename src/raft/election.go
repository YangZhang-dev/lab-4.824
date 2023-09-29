package raft

import (
	"sync"
	"time"
)

// -------------rpc------------
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	term := args.Term
	candidateId := args.CandidateId
	lastLogIndex := args.LastLogIndex
	lastLogTerm := args.LastLogTerm
	rf.xlog("接收到来自%v号服务器的投票请求", candidateId)

	reply.Term = rf.getCurrentTerm()
	reply.FollowerId = rf.me
	reply.VoteGranted = false
	// check candidate's term
	if term < rf.getCurrentTerm() {
		rf.xlog("他的term小，已经拒绝")
		return
	}
	if term == rf.getCurrentTerm() {
		// check does self vote for other of self
		if rf.getVoteFor() != -1 {
			rf.xlog("已经向他人投过票，已经拒绝")
			return
		}
		// check log's term and index
		if lastLogTerm < rf.logs.getLastLog().Term {
			rf.xlog("他log的term小，已经拒绝")
			return
		}
		if lastLogTerm == rf.logs.getLastLog().Term && lastLogIndex < rf.logs.getLastLogIndex() {
			rf.xlog("log的term相同，但他的log的index小，已经拒绝")
			return
		}
	}
	rf.xlog("投他一票")
	reply.VoteGranted = true
	rf.RestartVoteEndTime()
	rf.setVoteFor(int32(candidateId))
	if term > rf.getCurrentTerm() {
		rf.xlog("当前term为：%v,更新term为%v", rf.getCurrentTerm(), term)
		rf.setCurrentTerm(term)
	}
	rf.setMembership(FOLLOWER)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// lossy network,server fail or network fail all can
	// whatever will return false or true
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// ----------------------------
// true 超时
func (rf *Raft) checkVoteTimeout() {
	rf.xlog("当前Raft信息：%+v", rf)
	rf.xlog("初始化完成，开始投票倒计时")
	ticker := time.NewTicker(time.Duration(rf.getVoteTimeout()) * time.Millisecond)
	for {
		if rf.getMembership() == LEADER {
			rf.VoteCond.L.Lock()
			for rf.getMembership() == LEADER {
				rf.VoteCond.Wait()
			}
			rf.VoteCond.L.Unlock()
		}
		select {
		case <-ticker.C:
			timeout, duration := checkTime(rf.getVoteEndTime(), rf.getVoteTimeout())
			if timeout {
				rf.xlog("投票超时,超时时间：%vms,设置的timeout：%vms", duration.Milliseconds(), rf.voteTimeout)
				rf.election()
			} else {
				rf.xlog("投票未超时，还剩下：%vms", duration.Milliseconds())
				ticker.Reset(duration - time.Duration(5)*time.Microsecond)
			}
		}
	}

}
func (rf *Raft) election() {
	rf.xlog("发起第%v轮投票", rf.getCurrentTerm())
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)
	//// rf.xlog("由follower转化为candidate，term加一变为：%v", rf.getCurrentTerm())
	// if last membership is candidate,update voteTimeout to avoid conflict
	if rf.getMembership() == CANDIDATE {
		i := int64(rf.Rand.Intn(VOTE_TIMEOUT_RANGE) + BASE_VOTE_TIMEOUT)
		rf.setVoteTimeout(i)
	}
	// update membership to candidate
	rf.setMembership(CANDIDATE)
	rf.setVoteFor(VOTE_NO)

	if rf.getMembership() == CANDIDATE {
		getVoteNum := rf.handleVote()
		rf.xlog("获得%v张票", getVoteNum)
		// get majority server vote
		if getVoteNum >= rf.majority {
			rf.xlog("我获得的大多数选票,当选term为%v的leader", rf.getCurrentTerm())
			// update membership to leader
			rf.setMembership(LEADER)
			rf.setVoteFor(VOTE_NO)
			rf.xlog("开启心跳")
		} else {
			rf.RestartVoteEndTime()
			rf.xlog("oh, 我没有获得大多数选票")
		}
	}
	if rf.getMembership() == FOLLOWER {
		rf.xlog("结束选票，我现在的身份是FOLLOWER")
	} else if rf.getMembership() == LEADER {
		rf.xlog("结束选票，我现在的身份是LEADER")
	} else {
		rf.xlog("结束选票，我现在的身份是CANDIDATE")
	}
}
func (rf *Raft) handleVote() int {
	defer func() {
		if r := recover(); r != nil {
			rf.xlog("%v", r)
		}
	}()
	// vote for myself
	getVoteNum := 1
	args := RequestVoteArgs{
		Term:         rf.getCurrentTerm(),
		CandidateId:  rf.me,
		LastLogIndex: rf.logs.getLastLogIndex(),
		LastLogTerm:  rf.logs.getLastLog().Term,
	}
	ch := make(chan VoteReply, len(rf.peers))
	rf.setVoteFor(int32(rf.me))
	// restart time
	rf.RestartVoteEndTime()
	// send RV RPC to all server
	group := sync.WaitGroup{}
	group.Add(rf.majority - 1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		//group.Add(1)
		go func(i int) {
			defer func() {
				if r := recover(); r != nil {
				}
			}()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)

			voteReply := VoteReply{
				RequestVoteReply: reply,
				Ok:               ok,
			}
			rf.xlog("将服务器%v的投票结果放入", reply.FollowerId)
			ch <- voteReply
			group.Done()
		}(i)
	}
	// TODO 如果投票结果请求时间大于超时时间，会出现错误
	group.Wait()
	close(ch)
	for reply := range ch {
		if rf.getMembership() != CANDIDATE {
			break
		}
		if !reply.Ok {
			rf.xlog("服务器%v无响应", reply.RequestVoteReply.FollowerId)
			continue
		}
		if !reply.RequestVoteReply.VoteGranted {
			rf.xlog("服务器%v没有投给我票，我的term是%v，他的term是%v", reply.RequestVoteReply.FollowerId, rf.getCurrentTerm(), reply.RequestVoteReply.Term)
			// check term
			if reply.RequestVoteReply.Term > rf.getCurrentTerm() {
				rf.setCurrentTerm(reply.RequestVoteReply.Term)
				rf.xlog("我比服务器%v的term小，我不再发起选票", reply.RequestVoteReply.FollowerId)
				// update self membership to follower
				rf.setMembership(FOLLOWER)
				break
			}
			continue
		}
		rf.xlog("获得来自服务器%v的选票", reply.RequestVoteReply.FollowerId)
		getVoteNum++
		if getVoteNum >= rf.majority {
			break
		}
	}
	return getVoteNum
}
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
