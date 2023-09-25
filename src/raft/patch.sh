#!/bin/bash

start() {
    rm -f *.log
    rm runtime
    go test -run TestInitialElection2A > runtime

#    cat raft.log | grep raft-1 > raft-1.log
#    cat raft.log | grep raft-2 > raft-2.log
#    cat raft.log | grep raft-0 > raft-0.log

    l=$(cat runtime | grep FAIL | wc -l)
    return $l
}

start
l=$?
ti=1
while [ $l -le 0 ]
do 
    # shellcheck disable=SC2003
    echo "第 $(expr $ti + 1) 次"
    start
    l=$?
    # shellcheck disable=SC2003
    ti=$(expr $ti + 1)
done
#echo "run $ti times get success"
