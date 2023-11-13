package raft

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
)

const DEBUG = false
const patch = false
const IsRaft = false

func (rf *Raft) xlog(desc string, v ...interface{}) {
	if DEBUG {
		s := "candidate"
		if rf.state == FOLLOWER {
			s = "follower"
		} else if rf.state == LEADER {
			s = "leader"
		}
		if patch {
			fmt.Printf("raft-"+strconv.Itoa(rf.me)+"-go-"+strconv.Itoa(GoID())+"-term-"+strconv.Itoa(rf.currentTerm)+"-ci-"+strconv.Itoa(rf.commitIndex)+"-si-"+strconv.Itoa(rf.logs.lastIncludedIndex)+"-"+s+"| "+desc+"\n", v...)
		} else {
			log.Printf("raft-"+strconv.Itoa(rf.me)+"-go-"+strconv.Itoa(GoID())+"-term-"+strconv.Itoa(rf.currentTerm)+"-ci-"+strconv.Itoa(rf.commitIndex)+"-si-"+strconv.Itoa(rf.logs.lastIncludedIndex)+"-"+s+"| "+desc+"\n", v...)
		}
	}
}
func GoID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// 得到id字符串
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}
