package raft

import (
	"6.824/labgob"
	"bytes"
)

type P struct {
	CurrentTerm       int
	VoteFor           int
	LastIncludedIndex int
	LastIncludedTerm  int
	LogList           []Log
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	p := P{}
	p.CurrentTerm = rf.currentTerm
	p.VoteFor = rf.voteFor

	rf.logs.mu.RLock()
	p.LastIncludedIndex = rf.logs.lastIncludedIndex
	p.LastIncludedTerm = rf.logs.lastIncludedTerm
	p.LogList = rf.logs.LogList
	rf.logs.mu.RUnlock()

	rf.persister.mu.Lock()
	snapshot := rf.persister.snapshot
	rf.persister.mu.Unlock()

	e.Encode(p)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, snapshot)
}
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var p P
	if d.Decode(&p) != nil {
	} else {
		rf.mu.Lock()
		rf.currentTerm = p.CurrentTerm
		rf.voteFor = p.VoteFor
		rf.logs.mu.Lock()
		rf.logs.lastIncludedTerm = p.LastIncludedTerm
		rf.logs.lastIncludedIndex = p.LastIncludedIndex
		rf.logs.LogList = p.LogList
		rf.logs.mu.Unlock()
		rf.commitIndex = p.LastIncludedIndex
		rf.lastApplied = p.LastIncludedIndex
		rf.mu.Unlock()
	}
	rf.xlog("startup,log: %+v", rf.logs.LogList)
}
