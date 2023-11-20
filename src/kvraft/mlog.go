package kvraft

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
)

const DEBUG = false
const patch = false

func (kv *KVServer) klog(desc string, v ...interface{}) {
	if DEBUG {
		if patch {
			fmt.Printf("server-"+strconv.Itoa(kv.me)+"-go-"+strconv.Itoa(GoID())+"| "+desc+"\n", v...)
		} else {
			log.Printf("server-"+strconv.Itoa(kv.me)+"-go-"+strconv.Itoa(GoID())+"| "+desc+"\n", v...)
		}
	}
}
func (ck *Clerk) clog(desc string, v ...interface{}) {
	if DEBUG {
		if patch {
			fmt.Printf("clerk-"+strconv.Itoa(ck.me)+"-go-"+strconv.Itoa(GoID())+"| "+desc+"\n", v...)
		} else {
			log.Printf("clerk-"+strconv.Itoa(ck.me)+"-go-"+strconv.Itoa(GoID())+"| "+desc+"\n", v...)
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
